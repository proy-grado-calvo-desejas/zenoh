//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
#![cfg(feature = "observer")]

use std::{
    any::Any,
    sync::{Arc, Mutex},
    time::Duration,
};

use zenoh_config::{Config, ZenohId};
use zenoh_core::ztimeout;
use zenoh_keyexpr::keyexpr;

use crate::{
    api::observer::{
        Direction, MessageKind, MessageObserver, MessageObserverFactory, ObservedMessage,
        ObserverHandle, Space, TransportInfo,
    },
    config::WhatAmI,
    init_log_from_env_or, open,
    qos::Priority,
    Session,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const SLEEP: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq)]
struct Event {
    direction: Direction,
    kind: MessageKind,
    space: Space,
    key_expr: Option<String>,
    payload_len: usize,
    priority: Priority,
    /// The remote node of the transport the observer is attached to.
    transport: Option<ZenohId>,
    /// What `key_cache` returned for this key expression, if the message referred to it.
    cached_key: Option<String>,
}

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<Event>>,
    transports: Mutex<Vec<(Option<ZenohId>, Direction)>>,
}

impl Recorder {
    fn events_on(&self, key: &str) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.key_expr.as_deref() == Some(key))
            .cloned()
            .collect()
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

struct RecordingFactory(Arc<Recorder>);

impl MessageObserverFactory for RecordingFactory {
    fn new_transport(
        &self,
        transport: &TransportInfo,
        direction: Direction,
    ) -> Option<Arc<dyn MessageObserver>> {
        self.0
            .transports
            .lock()
            .unwrap()
            .push((transport.zid, direction));
        Some(Arc::new(RecordingObserver {
            recorder: self.0.clone(),
            transport: transport.zid,
        }))
    }
}

struct RecordingObserver {
    recorder: Arc<Recorder>,
    transport: Option<ZenohId>,
}

impl MessageObserver for RecordingObserver {
    fn key_cache(&self, key_expr: &keyexpr) -> Option<Box<dyn Any + Send + Sync>> {
        Some(Box::new(key_expr.to_string()))
    }

    fn observe(&self, msg: &ObservedMessage<'_>, cache: Option<&(dyn Any + Send + Sync)>) {
        self.recorder.events.lock().unwrap().push(Event {
            direction: msg.direction(),
            kind: msg.kind(),
            space: msg.space(),
            key_expr: msg.key_expr().map(|k| k.to_string()),
            payload_len: msg.payload_len(),
            priority: msg.priority(),
            transport: self.transport,
            cached_key: cache.and_then(|c| c.downcast_ref::<String>()).cloned(),
        });
    }
}

fn register(router: &Session, recorder: &Arc<Recorder>) -> ObserverHandle {
    router
        .static_runtime()
        .unwrap()
        .add_message_observer(Arc::new(RecordingFactory(recorder.clone())))
}

fn router_config() -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Router)).unwrap();
    config
        .listen
        .endpoints
        .set(vec!["tcp/127.0.0.1:0".parse().unwrap()])
        .unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
}

fn client_config(endpoint: zenoh_link::EndPoint) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Client)).unwrap();
    config.connect.endpoints.set(vec![endpoint.into()]).unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
}

async fn tcp_endpoint(session: &Session) -> zenoh_link::EndPoint {
    session
        .info()
        .locators()
        .await
        .into_iter()
        .map(|l| l.to_endpoint())
        .find(|ep| ep.to_string().starts_with("tcp/"))
        .expect("router should listen on tcp")
}

/// A router with two clients connected to it.
struct Setup {
    config: Config,
    router: Session,
    client1: Session,
    client2: Session,
}

async fn open_router() -> (Config, Session) {
    init_log_from_env_or("error");
    let mut config = router_config();
    let router = ztimeout!(open(config.clone())).unwrap();
    let endpoint = tcp_endpoint(&router).await;
    config.listen.endpoints.set(vec![endpoint]).unwrap();
    (config, router)
}

async fn connect_clients(config: Config, router: Session) -> Setup {
    let endpoint = tcp_endpoint(&router).await;
    let client1 = ztimeout!(open(client_config(endpoint.clone()))).unwrap();
    let client2 = ztimeout!(open(client_config(endpoint))).unwrap();
    tokio::time::sleep(SLEEP).await;
    Setup {
        config,
        router,
        client1,
        client2,
    }
}

/// Publishes `payload` on `key` from client1 to a subscriber on client2, and checks it arrives
/// unchanged.
async fn put_and_receive(setup: &Setup, key: &'static str, payload: &str) {
    let sub = ztimeout!(setup.client2.declare_subscriber(key)).unwrap();
    let publisher = ztimeout!(setup.client1.declare_publisher(key)).unwrap();
    tokio::time::sleep(SLEEP).await;
    ztimeout!(publisher.put(payload.to_owned())).unwrap();
    let sample = ztimeout!(sub.recv_async()).unwrap();
    assert_eq!(
        sample.payload().try_to_string().unwrap().as_ref(),
        payload,
        "an observer must not change delivery"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn puts(events: &[Event], direction: Direction) -> Vec<&Event> {
    events
        .iter()
        .filter(|e| e.kind == MessageKind::Put && e.direction == direction)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_sees_put_on_ingress_and_egress() {
    let recorder = Arc::new(Recorder::default());
    let (config, router) = open_router().await;
    // Registered before any client connects: new transports must pick it up.
    let _handle = register(&router, &recorder);
    let setup = connect_clients(config, router).await;

    put_and_receive(&setup, "test/observer/put", "hello").await;

    let events = recorder.events_on("test/observer/put");
    let ingress = puts(&events, Direction::Ingress);
    let egress = puts(&events, Direction::Egress);
    assert_eq!(ingress.len(), 1, "{events:#?}");
    assert_eq!(egress.len(), 1, "{events:#?}");

    assert_eq!(ingress[0].transport, Some(setup.client1.zid()));
    assert_eq!(egress[0].transport, Some(setup.client2.zid()));
    for e in [ingress[0], egress[0]] {
        assert_eq!(e.payload_len, "hello".len());
        assert_eq!(e.space, Space::User);
        assert_eq!(e.priority, Priority::Data);
    }

    // Each client transport gets one observer per direction.
    let transports = recorder.transports.lock().unwrap().clone();
    for zid in [setup.client1.zid(), setup.client2.zid()] {
        for direction in [Direction::Ingress, Direction::Egress] {
            assert!(
                transports.contains(&(Some(zid), direction)),
                "{transports:?}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_registered_late_reaches_existing_transports() {
    let recorder = Arc::new(Recorder::default());
    let (config, router) = open_router().await;
    let setup = connect_clients(config, router).await;
    // Registered after both clients are connected.
    let _handle = register(&setup.router, &recorder);

    put_and_receive(&setup, "test/observer/late", "late").await;

    let events = recorder.events_on("test/observer/late");
    assert_eq!(puts(&events, Direction::Ingress).len(), 1, "{events:#?}");
    assert_eq!(puts(&events, Direction::Egress).len(), 1, "{events:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_handle_stops_observation() {
    let recorder = Arc::new(Recorder::default());
    let (config, router) = open_router().await;
    let setup = connect_clients(config, router).await;
    let handle = register(&setup.router, &recorder);

    put_and_receive(&setup, "test/observer/drop", "before").await;
    assert!(!recorder.events_on("test/observer/drop").is_empty());

    drop(handle);
    // Messages already being routed when the handle is dropped (here, the undeclares sent as
    // `put_and_receive` drops its subscriber and publisher) may still reach the observer. Let
    // them drain before checking that nothing new is observed.
    tokio::time::sleep(SLEEP).await;
    recorder.clear();
    put_and_receive(&setup, "test/observer/drop", "after").await;
    assert!(
        recorder.events.lock().unwrap().is_empty(),
        "{:#?}",
        recorder.events.lock().unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_update_keeps_observers() {
    let recorder = Arc::new(Recorder::default());
    let (config, router) = open_router().await;
    let setup = connect_clients(config, router).await;
    let _handle = register(&setup.router, &recorder);

    setup
        .router
        .static_runtime()
        .unwrap()
        .router()
        .tables
        .update_config(&setup.config)
        .unwrap();
    tokio::time::sleep(SLEEP).await;

    put_and_receive(&setup, "test/observer/config", "still here").await;
    let events = recorder.events_on("test/observer/config");
    assert_eq!(puts(&events, Direction::Ingress).len(), 1, "{events:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_cache_is_passed_back_for_declared_key_expressions() {
    let recorder = Arc::new(Recorder::default());
    let (config, router) = open_router().await;
    let _handle = register(&router, &recorder);
    let setup = connect_clients(config, router).await;

    // A declared publisher makes the put refer to a declared resource, which carries the cache.
    put_and_receive(&setup, "test/observer/cache", "cached").await;

    let events = recorder.events_on("test/observer/cache");
    let ingress = puts(&events, Direction::Ingress);
    assert_eq!(ingress.len(), 1, "{events:#?}");
    assert_eq!(
        ingress[0].cached_key.as_deref(),
        Some("test/observer/cache"),
        "{events:#?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_sees_queries_and_replies() {
    let recorder = Arc::new(Recorder::default());
    let (config, router) = open_router().await;
    let _handle = register(&router, &recorder);
    let setup = connect_clients(config, router).await;

    let queryable = ztimeout!(setup.client2.declare_queryable("test/observer/query")).unwrap();
    tokio::time::sleep(SLEEP).await;
    let replies = ztimeout!(setup.client1.get("test/observer/query")).unwrap();
    let query = ztimeout!(queryable.recv_async()).unwrap();
    ztimeout!(query.reply("test/observer/query", "answer")).unwrap();
    drop(query);
    let reply = ztimeout!(replies.recv_async()).unwrap();
    assert_eq!(
        reply
            .result()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap()
            .as_ref(),
        "answer"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let events = recorder.events_on("test/observer/query");
    let has = |kind, direction, transport| {
        events
            .iter()
            .any(|e| e.kind == kind && e.direction == direction && e.transport == Some(transport))
    };
    let (c1, c2) = (setup.client1.zid(), setup.client2.zid());
    assert!(
        has(MessageKind::Query, Direction::Ingress, c1),
        "{events:#?}"
    );
    assert!(
        has(MessageKind::Query, Direction::Egress, c2),
        "{events:#?}"
    );
    assert!(
        has(MessageKind::Reply, Direction::Ingress, c2),
        "{events:#?}"
    );
    assert!(
        has(MessageKind::Reply, Direction::Egress, c1),
        "{events:#?}"
    );
}
