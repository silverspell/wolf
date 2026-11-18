//! AMQP Basic class acknowledgement methods.

/// `basic.ack` method id.
pub const ACK: u16 = 80;
/// `basic.reject` method id.
pub const REJECT: u16 = 90;
/// `basic.nack` method id.
pub const NACK: u16 = 120;

/// `basic.ack` — acknowledge one or more delivered messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicAck {
    /// Delivery tag to acknowledge.
    pub delivery_tag: u64,
    /// Whether every outstanding delivery up to the tag is acknowledged.
    pub multiple: bool,
}

/// `basic.reject` — reject a single delivered message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicReject {
    /// Delivery tag to reject.
    pub delivery_tag: u64,
    /// Whether the message should be returned to the queue.
    pub requeue: bool,
}

/// `basic.nack` — reject one or more delivered messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicNack {
    /// Delivery tag to reject.
    pub delivery_tag: u64,
    /// Whether every outstanding delivery up to the tag is rejected.
    pub multiple: bool,
    /// Whether rejected messages should be returned to the queue.
    pub requeue: bool,
}
