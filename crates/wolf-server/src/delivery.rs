//! Cross-connection routing of broker deliveries and cancellations.
//!
//! A [`DeliveryHub`] maps each broker [`ConsumerId`] to the bounded outbox
//! channel of the connection that owns it. The broker dispatcher writes through
//! the hub as a [`DeliverySink`], so a queue consumed on one connection can
//! receive messages published on another without either connection knowing about
//! the other. Routes whose receivers have gone away are dropped.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{self, Receiver, Sender};
use wolf_broker::{ConsumerCancel, ConsumerId, DeliverOutcome, Delivery, DeliverySink};

/// The default capacity of each connection's bounded outbox.
pub const DEFAULT_OUTBOX_CAPACITY: usize = 256;

/// An event sent from the broker dispatch path to a connection's outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerEvent {
    /// A message delivery for a registered consumer.
    Delivery(Delivery),
    /// A server-initiated consumer cancellation.
    Cancel(ConsumerCancel),
}

/// Routes deliveries and cancellations to their owning connection outboxes.
#[derive(Debug, Clone)]
pub struct DeliveryHub {
    routes: Arc<Mutex<BTreeMap<ConsumerId, Sender<ServerEvent>>>>,
    capacity: usize,
}

impl Default for DeliveryHub {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_OUTBOX_CAPACITY)
    }
}

impl DeliveryHub {
    /// Creates an empty routing hub using the default outbox capacity.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty routing hub with an explicit outbox capacity.
    ///
    /// A capacity of zero is clamped to one so every connection can always
    /// accept at least one pending event.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            routes: Arc::new(Mutex::new(BTreeMap::new())),
            capacity: capacity.max(1),
        }
    }

    /// Creates one connection outbox sender/receiver pair.
    pub fn subscribe(&self) -> (Sender<ServerEvent>, Receiver<ServerEvent>) {
        mpsc::channel(self.capacity)
    }

    /// Registers the outbox that owns a consumer.
    pub fn register(&self, consumer: ConsumerId, sender: Sender<ServerEvent>) {
        if let Ok(mut routes) = self.routes.lock() {
            routes.insert(consumer, sender);
        }
    }

    /// Removes a consumer route if it exists.
    pub fn unregister(&self, consumer: ConsumerId) {
        if let Ok(mut routes) = self.routes.lock() {
            routes.remove(&consumer);
        }
    }

    /// Sends a delivery to its registered consumer, dropping stale routes.
    pub async fn deliver(&self, delivery: Delivery) -> DeliverOutcome {
        let consumer = delivery.consumer;
        self.send(consumer, ServerEvent::Delivery(delivery)).await
    }

    /// Sends a cancellation to its registered consumer, dropping stale routes.
    pub async fn cancel(&self, cancel: ConsumerCancel) {
        let consumer = cancel.consumer;
        let _ = self.send(consumer, ServerEvent::Cancel(cancel)).await;
    }

    /// Returns the number of live consumer routes.
    pub fn route_count(&self) -> usize {
        self.routes.lock().map_or(0, |routes| routes.len())
    }

    /// Returns a [`DeliverySink`] adapter over this hub.
    pub fn sink(&self) -> DeliveryHubSink {
        DeliveryHubSink { hub: self.clone() }
    }

    async fn send(&self, consumer: ConsumerId, event: ServerEvent) -> DeliverOutcome {
        let sender = {
            let Ok(routes) = self.routes.lock() else {
                return DeliverOutcome::Closed;
            };
            routes.get(&consumer).cloned()
        };
        let Some(sender) = sender else {
            return DeliverOutcome::Closed;
        };

        if sender.send(event).await.is_ok() {
            return DeliverOutcome::Delivered;
        }

        if let Ok(mut routes) = self.routes.lock() {
            if routes
                .get(&consumer)
                .is_some_and(|current| current.same_channel(&sender))
            {
                routes.remove(&consumer);
            }
        }
        DeliverOutcome::Closed
    }
}

impl DeliverySink for DeliveryHub {
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send {
        DeliveryHub::deliver(self, delivery)
    }

    fn cancel(&mut self, cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        DeliveryHub::cancel(self, cancel)
    }
}

/// A cloneable sink adapter so callers can pass an owned value to the broker.
#[derive(Debug, Clone)]
pub struct DeliveryHubSink {
    hub: DeliveryHub,
}

impl DeliverySink for DeliveryHubSink {
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send {
        self.hub.deliver(delivery)
    }

    fn cancel(&mut self, cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        self.hub.cancel(cancel)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use wolf_broker::{CancelReason, ConsumerCancel, ConsumerId, Delivery, DeliveryTag, Message};

    use super::*;

    fn delivery(consumer: ConsumerId) -> Delivery {
        Delivery {
            consumer,
            delivery_tag: DeliveryTag::new(1),
            message: Message::new(Bytes::from_static(b"payload")),
            redelivered: false,
        }
    }

    #[tokio::test]
    async fn deliveries_route_to_the_owning_connection() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        hub.register(ConsumerId::new(7), sender);

        let outcome = hub.deliver(delivery(ConsumerId::new(7))).await;

        assert_eq!(outcome, DeliverOutcome::Delivered);
        assert_eq!(
            receiver.try_recv().expect("delivery"),
            ServerEvent::Delivery(delivery(ConsumerId::new(7)))
        );
        assert_eq!(hub.route_count(), 1);
    }

    #[tokio::test]
    async fn cancellations_route_to_the_owning_connection() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        hub.register(ConsumerId::new(8), sender);
        let cancel = ConsumerCancel {
            consumer: ConsumerId::new(8),
            reason: CancelReason::QueueDeleted,
        };

        hub.cancel(cancel.clone()).await;

        assert_eq!(
            receiver.try_recv().expect("cancel"),
            ServerEvent::Cancel(cancel)
        );
    }

    #[tokio::test]
    async fn unknown_consumers_are_ignored() {
        let hub = DeliveryHub::new();
        let outcome = hub.deliver(delivery(ConsumerId::new(99))).await;
        assert_eq!(outcome, DeliverOutcome::Closed);
        assert_eq!(hub.route_count(), 0);
    }

    #[tokio::test]
    async fn dropping_the_receiver_removes_the_route() {
        let hub = DeliveryHub::new();
        let (sender, receiver) = hub.subscribe();
        let consumer = ConsumerId::new(9);
        hub.register(consumer, sender);
        drop(receiver);

        let outcome = hub.deliver(delivery(consumer)).await;

        assert_eq!(outcome, DeliverOutcome::Closed);
        assert_eq!(hub.route_count(), 0);
    }

    #[tokio::test]
    async fn unregister_removes_a_live_route() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        let consumer = ConsumerId::new(10);
        hub.register(consumer, sender);
        hub.unregister(consumer);

        let outcome = hub.deliver(delivery(consumer)).await;

        assert_eq!(outcome, DeliverOutcome::Closed);
        assert!(receiver.try_recv().is_err());
        assert_eq!(hub.route_count(), 0);
    }

    #[tokio::test]
    async fn sink_adapter_routes_events() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        let consumer = ConsumerId::new(11);
        hub.register(consumer, sender);
        let mut sink = hub.sink();

        let outcome = sink.deliver(delivery(consumer)).await;

        assert_eq!(outcome, DeliverOutcome::Delivered);
        assert!(matches!(
            receiver.try_recv().expect("delivery"),
            ServerEvent::Delivery(_)
        ));
    }

    #[tokio::test]
    async fn deliveries_fill_the_bounded_outbox() {
        let hub = DeliveryHub::with_capacity(1);
        let (sender, receiver) = hub.subscribe();
        let consumer = ConsumerId::new(12);
        hub.register(consumer, sender);

        assert_eq!(
            hub.deliver(delivery(consumer)).await,
            DeliverOutcome::Delivered
        );
        assert_eq!(receiver.len(), 1);
    }

    #[test]
    fn zero_capacity_is_clamped_to_one() {
        let hub = DeliveryHub::with_capacity(0);
        let (_sender, receiver) = hub.subscribe();
        assert_eq!(receiver.max_capacity(), 1);
    }
}
