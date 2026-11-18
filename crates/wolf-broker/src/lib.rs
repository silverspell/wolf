//! Protocol-independent broker domain: vhosts, exchanges, bindings, routing,
//! queues, streams, TTL and dead-lettering.
//!
//! This crate deliberately knows nothing about AMQP or any other wire
//! protocol. Wire decoding and encoding live in the codec crates, while
//! `wolf-server` maps [`DomainError`] values onto protocol reply codes and
//! adapts the delivery contract to concrete sessions.

pub mod binding;
pub mod broker;
pub mod delivery;
pub mod error;
pub mod exchange;
pub mod message;
pub mod queue;
pub mod router;
pub mod stream;
pub mod vhost;

pub use broker::Broker;
pub use delivery::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerId, CreditReceiver, CreditSignal, Delivery,
    DeliverySink, DeliveryTag,
};
pub use error::{DomainError, ResourceKind};
pub use message::Message;
pub use vhost::{Vhost, VhostId, VhostName};
