//! Broker-to-session delivery contract.
//!
//! These types are protocol-neutral: sessions translate them to and from the
//! wire protocol they speak. `basic.cancel` is represented only as a
//! [`ConsumerCancel`] with an opaque consumer id and a domain [`CancelReason`].

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::message::Message;

/// An opaque consumer identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerId(u64);

impl ConsumerId {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Generates a process-local consumer identifier.
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
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

    /// Generates a process-local channel identifier.
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
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

/// The result of handing a delivery to a session sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverOutcome {
    /// The delivery was accepted by the sink.
    Delivered,
    /// The sink's route is closed and dispatching should stop.
    Closed,
}

/// The broker-to-session sink for deliveries and cancellations.
pub trait DeliverySink {
    /// Hands one delivery to the sink, awaiting outbox capacity if necessary.
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send;

    /// Hands one server-initiated cancellation to the sink.
    fn cancel(&mut self, cancel: ConsumerCancel) -> impl Future<Output = ()> + Send;
}
