//! Broker-to-session delivery contract.
//!
//! These types are protocol-neutral: sessions translate them to and from the
//! wire protocol they speak. `basic.cancel` is represented only as a
//! [`ConsumerCancel`] with an opaque consumer id and a domain [`CancelReason`].

use crate::message::Message;

/// An opaque consumer identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerId(u64);

impl ConsumerId {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// An opaque channel identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChannelId(u64);

impl ChannelId {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// An opaque delivery tag assigned by the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeliveryTag(u64);

impl DeliveryTag {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// A message delivered to a consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub consumer: ConsumerId,
    pub delivery_tag: DeliveryTag,
    pub message: Message,
    pub redelivered: bool,
}

/// A prefetch/credit grant, scoped either to one consumer or a whole channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreditSignal {
    Consumer { consumer: ConsumerId, credit: u32 },
    Channel { channel: ChannelId, credit: u32 },
}

/// The broker-side receiver for credit signals sent by sessions.
pub type CreditReceiver = tokio::sync::mpsc::Receiver<CreditSignal>;

/// A protocol-neutral consumer cancellation (`basic.cancel` trigger).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerCancel {
    pub consumer: ConsumerId,
    pub reason: CancelReason,
}

/// Why a consumer was cancelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelReason {
    QueueDeleted,
    VhostDeleted,
    BrokerShutdown,
    Other(String),
}

/// The broker-to-session sink for deliveries and cancellations.
pub trait DeliverySink {
    fn deliver(&mut self, delivery: Delivery);
    fn cancel(&mut self, cancel: ConsumerCancel);
}
