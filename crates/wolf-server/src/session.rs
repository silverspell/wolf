//! Connection sessions and AMQP frame dispatch.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc::UnboundedSender;
use wolf_amqp_codec::Frame;
use wolf_broker::{Broker, ConnectionId as BrokerConnectionId};

use crate::amqp::MethodKey;
use crate::amqp::connection::Connection;
use crate::amqp::error::AmqpError;
use crate::delivery::{DeliveryHub, ServerEvent};

/// Identifier assigned to an accepted connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Creates an identifier from its raw value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Generates the next process-local connection identifier.
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the raw numeric identifier.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A failure produced while dispatching an AMQP method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchError {
    /// No handler is registered for the method.
    UnknownMethod(MethodKey),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMethod(method) => {
                write!(f, "unknown method {}.{}", method.class_id, method.method_id)
            }
        }
    }
}

impl std::error::Error for DispatchError {}

/// State for a single client connection.
#[derive(Debug)]
pub struct Session {
    id: ConnectionId,
    connection: Connection,
}

impl Session {
    /// Creates a session for the given connection identifier and peer.
    pub fn new(id: ConnectionId, peer: SocketAddr, broker: Arc<Broker>) -> Self {
        Self {
            id,
            connection: Connection::with_connection_id(
                peer,
                broker,
                BrokerConnectionId::new(id.get()),
            ),
        }
    }

    /// Creates a session that routes deliveries through a shared hub.
    pub fn with_delivery(
        id: ConnectionId,
        peer: SocketAddr,
        broker: Arc<Broker>,
        hub: Arc<DeliveryHub>,
        outbox: UnboundedSender<ServerEvent>,
    ) -> Self {
        Self {
            id,
            connection: Connection::with_delivery(
                peer,
                broker,
                BrokerConnectionId::new(id.get()),
                hub,
                outbox,
            ),
        }
    }

    /// Returns the identifier of this session's connection.
    pub const fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the server's `connection.start` frame.
    pub fn start_frame(&self) -> Frame {
        self.connection.start_frame()
    }

    /// Dispatches one decoded client frame to the connection state machine.
    pub fn dispatch(&mut self, frame: Frame) -> Result<Vec<Frame>, AmqpError> {
        self.connection.handle_frame(frame)
    }

    /// Returns the negotiated heartbeat interval.
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(u64::from(self.connection.heartbeat()))
    }

    /// Returns whether the connection has closed.
    pub const fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    /// Converts a cross-connection delivery event into frames for this session.
    pub fn delivery_event(&mut self, event: ServerEvent) -> Result<Vec<Frame>, AmqpError> {
        self.connection.delivery_event(event)
    }

    /// Releases consumers and requeues unacknowledged deliveries on exit.
    pub fn shutdown(&mut self) {
        self.connection.cleanup_connection();
    }

    /// Returns the underlying connection state.
    pub const fn connection(&self) -> &Connection {
        &self.connection
    }
}

#[cfg(test)]
mod tests {
    use wolf_amqp_codec::{
        ConnectionMethod, FieldTable, Frame, LongString, Method, ShortString, StartOk, connection,
    };
    use wolf_broker::Broker;

    use super::*;

    fn method_id(frame: &Frame) -> u16 {
        match frame {
            Frame::Method { method, .. } => method.method_id(),
            other => panic!("expected a method frame, got {other:?}"),
        }
    }

    fn broker() -> Arc<Broker> {
        Arc::new(Broker::new())
    }

    #[test]
    fn session_delegates_start_and_start_ok_to_the_connection() {
        let peer = "127.0.0.1:1234".parse().expect("valid address");
        let mut session = Session::new(ConnectionId::new(7), peer, broker());

        assert_eq!(session.id(), ConnectionId::new(7));
        assert_eq!(method_id(&session.start_frame()), connection::START);

        let frames = session
            .dispatch(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::StartOk(StartOk {
                    client_properties: FieldTable::new(),
                    mechanism: ShortString::new("PLAIN"),
                    response: LongString::from(b"\0guest\0guest".to_vec()),
                    locale: ShortString::new("en_US"),
                })),
            })
            .expect("start-ok is accepted");
        assert_eq!(frames.len(), 1);
        assert_eq!(method_id(&frames[0]), connection::TUNE);
        assert!(!session.is_closed());
    }

    #[test]
    fn heartbeats_keep_the_session_open() {
        let peer = "127.0.0.1:1234".parse().expect("valid address");
        let mut session = Session::new(ConnectionId::new(8), peer, broker());

        let frames = session
            .dispatch(Frame::Heartbeat { channel: 0 })
            .expect("heartbeat is accepted");
        assert!(frames.is_empty());
        assert!(!session.is_closed());
    }
}
