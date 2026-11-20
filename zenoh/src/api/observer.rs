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

//! Read-only observation of the messages a node routes.
//!
//! A plugin registers a [`MessageObserverFactory`] on the runtime it receives. The factory is
//! asked for a [`MessageObserver`] for each transport and direction, and that observer is called
//! for every message crossing it. Observers run inside the node's interceptor chains, after the
//! interceptors built from the config (ACL, downsampling, low-pass, QoS overwrite), so they only
//! see messages that passed them. An observer can never change or drop a message.
//!
//! Observers run on the routing hot path: [`MessageObserver::observe`] must be cheap and must
//! not block, allocate on every call, or take contended locks.
//!
//! Only transports to other nodes are observed. Sessions running inside the node itself (e.g.
//! plugins' own sessions) have no interceptor chains, so their messages are only seen on egress,
//! when forwarded to another node.

use std::{
    any::Any,
    fmt,
    sync::{Arc, Weak},
};

use zenoh_config::{WhatAmI, ZenohId};
use zenoh_keyexpr::keyexpr;
use zenoh_protocol::{
    core::Reliability,
    network::{NetworkBodyRef, NetworkMessageExt, NetworkMessageMut},
    zenoh::{PushBody, ResponseBody},
};

use crate::{api::publisher::Priority, net::routing::dispatcher::tables::TablesLock};

/// Whether a message is entering the node from a transport or leaving the node through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Received from the transport, before routing.
    Ingress,
    /// Sent to the transport, after routing. A message routed to N transports is observed N
    /// times on egress.
    Egress,
}

/// The kind of a network message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MessageKind {
    Put,
    Del,
    Query,
    Reply,
    ReplyErr,
    ResponseFinal,
    Declare,
    Interest,
    Oam,
}

/// Whether a message belongs to Zenoh's own admin space (`@/...`) or to user data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Space {
    Admin,
    User,
}

/// The transport an observer is attached to.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TransportInfo {
    /// The remote node, for unicast transports. `None` for multicast.
    pub zid: Option<ZenohId>,
    /// The remote node's mode, for unicast transports. `None` for multicast.
    pub whatami: Option<WhatAmI>,
    pub multicast: bool,
}

impl TransportInfo {
    pub(crate) fn unicast(zid: Option<ZenohId>, whatami: Option<WhatAmI>) -> Self {
        Self {
            zid,
            whatami,
            multicast: false,
        }
    }

    pub(crate) fn multicast() -> Self {
        Self {
            zid: None,
            whatami: None,
            multicast: true,
        }
    }
}

/// A read-only view of a message, valid for the duration of [`MessageObserver::observe`].
///
/// [`ObservedMessage::key_expr`] (and [`ObservedMessage::space`], which uses it) is resolved on
/// access and is the most expensive part: prefer the per-key-expression cache (see
/// [`MessageObserver::key_cache`]) when possible.
pub struct ObservedMessage<'a> {
    direction: Direction,
    kind: MessageKind,
    payload_len: usize,
    priority: Priority,
    reliability: Reliability,
    key_expr: &'a dyn Fn() -> Option<&'a str>,
}

impl<'a> ObservedMessage<'a> {
    pub(crate) fn new(
        direction: Direction,
        msg: &NetworkMessageMut,
        key_expr: &'a dyn Fn() -> Option<&'a str>,
    ) -> Self {
        let kind = match msg.body() {
            NetworkBodyRef::Push(push) => match push.payload {
                PushBody::Put(_) => MessageKind::Put,
                PushBody::Del(_) => MessageKind::Del,
            },
            NetworkBodyRef::Request(_) => MessageKind::Query,
            NetworkBodyRef::Response(response) => match response.payload {
                ResponseBody::Reply(_) => MessageKind::Reply,
                ResponseBody::Err(_) => MessageKind::ReplyErr,
            },
            NetworkBodyRef::ResponseFinal(_) => MessageKind::ResponseFinal,
            NetworkBodyRef::Interest(_) => MessageKind::Interest,
            NetworkBodyRef::Declare(_) => MessageKind::Declare,
            NetworkBodyRef::OAM(_) => MessageKind::Oam,
        };
        Self {
            direction,
            kind,
            payload_len: msg.payload_size().unwrap_or(0),
            priority: Priority::try_from(msg.priority()).unwrap_or_default(),
            reliability: msg.reliability,
            key_expr,
        }
    }

    pub fn direction(&self) -> Direction {
        self.direction
    }

    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    /// The full key expression of the message, if it has one.
    pub fn key_expr(&self) -> Option<&'a keyexpr> {
        keyexpr::new((self.key_expr)()?).ok()
    }

    /// [`Space::Admin`] for key expressions starting with `@`, [`Space::User`] otherwise
    /// (including messages without a key expression).
    pub fn space(&self) -> Space {
        match self.key_expr() {
            Some(key_expr) if key_expr.starts_with('@') => Space::Admin,
            _ => Space::User,
        }
    }

    /// Size of the user payload in bytes (not of the encoded message). 0 for messages without
    /// a payload.
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    pub fn priority(&self) -> Priority {
        self.priority
    }

    pub fn reliability(&self) -> Reliability {
        self.reliability
    }
}

impl fmt::Debug for ObservedMessage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedMessage")
            .field("direction", &self.direction())
            .field("kind", &self.kind())
            .field("key_expr", &self.key_expr())
            .field("payload_len", &self.payload_len())
            .finish_non_exhaustive()
    }
}

/// Creates observers for the node's transports.
pub trait MessageObserverFactory: Send + Sync {
    /// Called once per transport and direction, including for transports that already exist
    /// when the factory is registered. Return `None` to not observe it. Returning the same
    /// observer for several transports is allowed.
    fn new_transport(
        &self,
        transport: &TransportInfo,
        direction: Direction,
    ) -> Option<Arc<dyn MessageObserver>>;
}

/// Observes the messages crossing one transport in one direction.
pub trait MessageObserver: Send + Sync {
    /// Computes state for a key expression, cached on the routing resource and passed back to
    /// [`MessageObserver::observe`] for messages on that key expression. Called when the
    /// resource is set up for the transport, not on every message. Use it to resolve per-key
    /// counters once instead of on every message.
    fn key_cache(&self, _key_expr: &keyexpr) -> Option<Box<dyn Any + Send + Sync>> {
        None
    }

    /// Called for every message. `cache` is what [`MessageObserver::key_cache`] returned for
    /// the message's key expression, when the message refers to a declared resource; otherwise
    /// `None`.
    fn observe(&self, msg: &ObservedMessage<'_>, cache: Option<&(dyn Any + Send + Sync)>);
}

/// Keeps a registered [`MessageObserverFactory`] active. Dropping it unregisters the factory
/// and removes its observers from every transport.
///
/// A plugin must drop its handles before it is unloaded, since the observers' code lives in the
/// plugin's library. Messages already being routed may still reach an observer shortly after
/// the handle is dropped.
#[must_use = "the observer is unregistered when the handle is dropped"]
pub struct ObserverHandle {
    tables: Weak<TablesLock>,
    id: usize,
}

impl ObserverHandle {
    pub(crate) fn new(tables: Weak<TablesLock>, id: usize) -> Self {
        Self { tables, id }
    }
}

impl fmt::Debug for ObserverHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObserverHandle")
            .field("id", &self.id)
            .finish()
    }
}

impl Drop for ObserverHandle {
    fn drop(&mut self) {
        if let Some(tables) = self.tables.upgrade() {
            tables.remove_runtime_interceptor(self.id);
        }
    }
}
