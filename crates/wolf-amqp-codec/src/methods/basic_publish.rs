//! AMQP Basic class publish and return methods.

use crate::types::ShortString;

/// `basic.publish` method id.
pub const PUBLISH: u16 = 40;
/// `basic.return` method id.
pub const RETURN: u16 = 50;

/// `basic.publish` — publish a message to an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicPublish {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Exchange to publish to.
    pub exchange: ShortString,
    /// Routing key used to select queues.
    pub routing_key: ShortString,
    /// Whether the server must return the message when it cannot be routed.
    pub mandatory: bool,
    /// Deprecated immediate flag.
    pub immediate: bool,
}

/// `basic.return` — server return of an unroutable mandatory message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicReturn {
    /// AMQP reply code describing why the message was returned.
    pub reply_code: u16,
    /// Human-readable reply text.
    pub reply_text: ShortString,
    /// Exchange the message was published to.
    pub exchange: ShortString,
    /// Routing key the message was published with.
    pub routing_key: ShortString,
}
