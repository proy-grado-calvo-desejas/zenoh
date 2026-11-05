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

//! Adapts the public message observer API to the internal interceptor framework.

use std::{any::Any, sync::Arc};

use zenoh_keyexpr::keyexpr;
use zenoh_protocol::network::NetworkMessageMut;
use zenoh_transport::{multicast::TransportMulticast, unicast::TransportUnicast};

use super::{
    EgressInterceptor, IngressInterceptor, Interceptor, InterceptorContext,
    InterceptorFactoryTrait, InterceptorTrait,
};
use crate::api::observer::{
    Direction, MessageObserver, MessageObserverFactory, ObservedMessage, TransportInfo,
};

pub(crate) struct ObserverInterceptorFactory {
    factory: Arc<dyn MessageObserverFactory>,
}

impl ObserverInterceptorFactory {
    pub(crate) fn new(factory: Arc<dyn MessageObserverFactory>) -> Self {
        Self { factory }
    }

    fn interceptor(&self, transport: &TransportInfo, direction: Direction) -> Option<Interceptor> {
        let observer = self.factory.new_transport(transport, direction)?;
        Some(Box::new(ObserverInterceptor {
            observer,
            direction,
        }))
    }
}

impl InterceptorFactoryTrait for ObserverInterceptorFactory {
    fn new_transport_unicast(
        &self,
        transport: &TransportUnicast,
    ) -> (Option<IngressInterceptor>, Option<EgressInterceptor>) {
        let info = TransportInfo::unicast(
            transport.get_zid().ok().map(Into::into),
            transport.get_whatami().ok(),
        );
        (
            self.interceptor(&info, Direction::Ingress),
            self.interceptor(&info, Direction::Egress),
        )
    }

    fn new_transport_multicast(
        &self,
        _transport: &TransportMulticast,
    ) -> Option<EgressInterceptor> {
        self.interceptor(&TransportInfo::multicast(), Direction::Egress)
    }

    fn new_peer_multicast(&self, _transport: &TransportMulticast) -> Option<IngressInterceptor> {
        self.interceptor(&TransportInfo::multicast(), Direction::Ingress)
    }
}

struct ObserverInterceptor {
    observer: Arc<dyn MessageObserver>,
    direction: Direction,
}

impl InterceptorTrait for ObserverInterceptor {
    fn compute_keyexpr_cache(&self, key_expr: &keyexpr) -> Option<Box<dyn Any + Send + Sync>> {
        self.observer.key_cache(key_expr)
    }

    fn intercept(&self, msg: &mut NetworkMessageMut, ctx: &mut dyn InterceptorContext) -> bool {
        let msg: &NetworkMessageMut = msg;
        let ctx: &dyn InterceptorContext = ctx;
        let cache = ctx
            .get_cache(msg)
            .map(|cache| &**cache as &(dyn Any + Send + Sync));
        let key_expr = || ctx.full_expr(msg);
        self.observer
            .observe(&ObservedMessage::new(self.direction, msg, &key_expr), cache);
        // Observers are read-only: never drop the message.
        true
    }
}
