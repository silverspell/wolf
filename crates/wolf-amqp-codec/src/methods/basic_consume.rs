//! AMQP Basic class consume and get methods.

use crate::types::{FieldTable, ShortString};

/// `basic.qos` method id.
pub const QOS: u16 = 10;
/// `basic.qos-ok` method id.
pub const QOS_OK: u16 = 11;
/// `basic.consume` method id.
pub const CONSUME: u16 = 20;
/// `basic.consume-ok` method id.
pub const CONSUME_OK: u16 = 21;
/// `basic.cancel` method id.
pub const CANCEL: u16 = 30;
/// `basic.cancel-ok` method id.
pub const CANCEL_OK: u16 = 31;
/// `basic.deliver` method id.
pub const DELIVER: u16 = 60;
/// `basic.get` method id.
pub const GET: u16 = 70;
/// `basic.get-ok` method id.
pub const GET_OK: u16 = 71;
/// `basic.get-empty` method id.
pub const GET_EMPTY: u16 = 72;

/// `basic.qos` — request to limit the number of unacknowledged deliveries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicQos {
    /// Maximum prefetch window size in octets, or zero for no limit.
    pub prefetch_size: u32,
    /// Maximum number of unacknowledged messages, or zero for no limit.
    pub prefetch_count: u16,
    /// Whether the limit applies to the whole connection rather than the channel.
    pub global: bool,
}

/// `basic.qos-ok` — acknowledgement of `basic.qos`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicQosOk;

/// `basic.consume` — request to start a consumer on a queue.
#[derive(Debug, Clone, PartialEq)]
pub struct BasicConsume {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name.
    pub queue: ShortString,
    /// Consumer tag, or empty for a server-generated tag.
    pub consumer_tag: ShortString,
    /// Whether the server should not deliver messages published by this client.
    pub no_local: bool,
    /// Whether the server should not expect acknowledgements for deliveries.
    pub no_ack: bool,
    /// Whether this consumer has exclusive access to the queue.
    pub exclusive: bool,
    /// Whether the client expects no `basic.consume-ok` reply.
    pub nowait: bool,
    /// Consumer-specific arguments.
    pub arguments: FieldTable,
}

/// `basic.consume-ok` — acknowledgement of `basic.consume`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicConsumeOk {
    /// Consumer tag assigned by the server.
    pub consumer_tag: ShortString,
}

/// `basic.cancel` — request to cancel a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicCancel {
    /// Consumer tag to cancel.
    pub consumer_tag: ShortString,
    /// Whether the client expects no `basic.cancel-ok` reply.
    pub nowait: bool,
}

/// `basic.cancel-ok` — acknowledgement of `basic.cancel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicCancelOk {
    /// Cancelled consumer tag.
    pub consumer_tag: ShortString,
}

/// `basic.deliver` — server delivery of a message to a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicDeliver {
    /// Consumer tag the message is delivered to.
    pub consumer_tag: ShortString,
    /// Server-assigned delivery tag.
    pub delivery_tag: u64,
    /// Whether the message has been delivered before.
    pub redelivered: bool,
    /// Exchange the message was published to.
    pub exchange: ShortString,
    /// Routing key the message was published with.
    pub routing_key: ShortString,
}

/// `basic.get` — request to fetch a single message from a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicGet {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name.
    pub queue: ShortString,
    /// Whether the server should not expect an acknowledgement for the message.
    pub no_ack: bool,
}

/// `basic.get-ok` — successful `basic.get` returning a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicGetOk {
    /// Server-assigned delivery tag.
    pub delivery_tag: u64,
    /// Whether the message has been delivered before.
    pub redelivered: bool,
    /// Exchange the message was published to.
    pub exchange: ShortString,
    /// Routing key the message was published with.
    pub routing_key: ShortString,
    /// Number of messages remaining in the queue.
    pub message_count: u32,
}

/// `basic.get-empty` — `basic.get` response for an empty queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicGetEmpty {
    /// Deprecated reserved cluster id.
    pub cluster_id: ShortString,
}
