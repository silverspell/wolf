//! Connection sessions and AMQP method dispatch.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::amqp::MethodKey;

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
}

impl Session {
    /// Creates a session for the given connection identifier.
    pub fn new(id: ConnectionId) -> Self {
        Self { id }
    }

    /// Returns the identifier of this session's connection.
    pub const fn id(&self) -> ConnectionId {
        self.id
    }

    /// Dispatches an AMQP method to its handler.
    ///
    /// The AMQP codec is still a stub, so every method is currently unknown.
    pub fn dispatch(&mut self, method: MethodKey) -> Result<(), DispatchError> {
        Err(DispatchError::UnknownMethod(method))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_methods_return_an_error_without_panicking() {
        let mut session = Session::new(ConnectionId::new(7));

        let result = session.dispatch(MethodKey::new(60, 40));

        assert_eq!(session.id(), ConnectionId::new(7));
        assert_eq!(
            result,
            Err(DispatchError::UnknownMethod(MethodKey::new(60, 40)))
        );
    }
}
