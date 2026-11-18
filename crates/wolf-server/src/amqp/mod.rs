//! AMQP protocol adaptation shared by connection sessions.

pub mod arguments;
pub mod auth;
pub mod basic_publish;
pub mod channel;
pub mod confirm;
pub mod connection;
pub mod consumer;
pub mod error;
pub mod exchange;
pub mod queue;
pub mod unsupported;

/// Identifier of an AMQP method as a class id and method id pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodKey {
    /// AMQP class identifier.
    pub class_id: u16,
    /// AMQP method identifier within the class.
    pub method_id: u16,
}

impl MethodKey {
    /// Creates a method key from its class and method identifiers.
    pub const fn new(class_id: u16, method_id: u16) -> Self {
        Self {
            class_id,
            method_id,
        }
    }
}
