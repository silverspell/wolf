//! Publishing to routed queues and point-to-point `basic.get`.
//!
//! Publishing is split into routing, stale-binding pruning and fan-out. The
//! same [`Message`] body handle is cloned into every destination queue, so the
//! payload allocation is shared rather than copied. Mandatory publishes that
//! reach no live queue produce a [`Returned`] value, which callers map onto
//! `basic.return`; non-mandatory unroutable publishes are silently dropped.
//!
//! This module is protocol-neutral: it reports domain outcomes and never
//! encodes wire methods or allocates consumer delivery tags.

use crate::binding::QueueName;
use crate::delivery::DeliveryTag;
use crate::error::DomainError;
use crate::exchange::ExchangeName;
use crate::message::Message;
use crate::queue::{ConnectionId, QueueRegistry};
use crate::router::Router;
use crate::topic::RoutingKey;

/// A request to publish one message through an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest<'a, P> {
    /// The source exchange; the empty string denotes the default exchange.
    pub exchange: &'a str,
    /// The routing key to route with.
    pub routing_key: &'a str,
    /// Whether an unroutable message must be returned instead of dropped.
    pub mandatory: bool,
    /// The message whose body is shared across all destination queues.
    pub message: Message,
    /// Opaque application properties preserved for `basic.return`.
    pub properties: P,
}

/// An unroutable mandatory message that should be sent back to the publisher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Returned<P> {
    /// The exchange the message was published to.
    pub exchange: ExchangeName,
    /// The routing key the message was published with.
    pub routing_key: RoutingKey,
    /// The returned message, preserving the original body handle.
    pub message: Message,
    /// The application properties supplied with the publish.
    pub properties: P,
}

/// The result of a publish request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome<P> {
    /// The message was written to the given number of live queues.
    Routed { queues: usize },
    /// The message was unroutable and mandatory, so it must be returned.
    Returned(Returned<P>),
    /// The message was unroutable and non-mandatory, so it was discarded.
    Dropped,
}

/// Whether a `basic.get` result must be acknowledged by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckState {
    /// The client must acknowledge the message.
    Required,
    /// The message was fetched with `no_ack` and needs no acknowledgement.
    NotRequired,
}

/// A single message fetched from a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMessage {
    /// The fetched message.
    pub message: Message,
    /// Whether the message had previously been delivered and requeued.
    pub redelivered: bool,
    /// Whether the client must acknowledge the message.
    pub ack_state: AckState,
    /// How many messages remain in the queue after this fetch.
    pub remaining: usize,
    /// Delivery tag assigned when the fetch is tracked for acknowledgement.
    pub delivery_tag: Option<DeliveryTag>,
}

/// The result of a `basic.get` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetOutcome {
    /// The queue was empty.
    Empty,
    /// A message was available.
    Message(GetMessage),
}

/// Routes and publishes a message to every live destination queue.
///
/// Stale bindings to deleted queues are pruned from `destinations` before the
/// fan-out. Every destination is access-checked up front so an exclusive-queue
/// error cannot leave a partial fan-out behind.
pub fn publish<P>(
    router: &Router,
    queues: &mut QueueRegistry,
    destinations: &mut Vec<QueueName>,
    connection: ConnectionId,
    request: PublishRequest<'_, P>,
) -> Result<PublishOutcome<P>, DomainError> {
    destinations.clear();
    router.route_into(request.exchange, request.routing_key, destinations)?;
    destinations.retain(|queue| queues.contains(queue.as_str()));

    if destinations.is_empty() {
        if request.mandatory {
            return Ok(PublishOutcome::Returned(Returned {
                exchange: request.exchange.into(),
                routing_key: request.routing_key.into(),
                message: request.message,
                properties: request.properties,
            }));
        }
        return Ok(PublishOutcome::Dropped);
    }

    for destination in destinations.iter() {
        queues.queue_for(destination.as_str(), connection)?;
    }

    for destination in destinations.iter() {
        queues.publish(destination.as_str(), connection, request.message.clone())?;
    }

    Ok(PublishOutcome::Routed {
        queues: destinations.len(),
    })
}

/// Fetches one message from a queue for `basic.get`.
///
/// The queue must exist and be accessible to `connection`. The returned
/// [`AckState`] is informational only; allocating consumer delivery tags and
/// tracking acknowledgements is the caller's responsibility.
pub fn get_message(
    queues: &mut QueueRegistry,
    connection: ConnectionId,
    queue: impl AsRef<str>,
    no_ack: bool,
) -> Result<GetOutcome, DomainError> {
    let name = queue.as_ref();
    queues.queue_for(name, connection)?;

    let Some((message, redelivered)) = queues.take_message(name) else {
        return Ok(GetOutcome::Empty);
    };

    let remaining = queues.queue(name).map_or(0, |queue| queue.message_count());
    let ack_state = if no_ack {
        AckState::NotRequired
    } else {
        AckState::Required
    };

    Ok(GetOutcome::Message(GetMessage {
        message,
        redelivered,
        ack_state,
        remaining,
        delivery_tag: None,
    }))
}
