//! Protocol-independent broker domain: vhosts, exchanges, bindings, routing,
//! queues, streams, TTL and dead-lettering.
//!
//! This crate deliberately knows nothing about AMQP or any other wire
//! protocol. Wire decoding and encoding live in the codec crates, while
//! `wolf-server` maps [`DomainError`] values onto protocol reply codes and
//! adapts the delivery contract to concrete sessions.

pub mod binding;
pub mod broker;
pub mod consumer;
pub mod delivery;
pub mod error;
pub mod exchange;
pub mod message;
pub mod publish;
pub mod queue;
pub mod router;
pub mod stream;
pub mod topic;
pub mod validation;
pub mod vhost;

pub use binding::{Binding, QueueName};
pub use broker::Broker;
pub use consumer::{ConsumerDispatcher, SettleOutcome};
pub use delivery::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerId, CreditReceiver, CreditSignal, Delivery,
    DeliverySink, DeliveryTag,
};
pub use error::{DomainError, ResourceKind};
pub use exchange::{
    ArgumentValue, Exchange, ExchangeArguments, ExchangeConfig, ExchangeKind, ExchangeName,
    ExchangeRegistry,
};
pub use message::Message;
pub use publish::{
    AckState, GetMessage, GetOutcome, PublishOutcome, PublishRequest, Returned, get_message,
    publish,
};
pub use queue::{
    ConnectionCloseOutcome, ConnectionId, Queue, QueueConfig, QueueDeleteOutcome, QueueRegistry,
};
pub use topic::{RoutingKey, TopicPattern, TopicTrie};
pub use validation::{QueueArguments, UNSUPPORTED_QUEUE_ARGUMENTS, validate_queue_arguments};
pub use vhost::{Vhost, VhostId, VhostName};
