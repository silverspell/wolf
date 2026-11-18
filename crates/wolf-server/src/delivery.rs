//! Cross-connection routing of broker deliveries and cancellations.
//!
//! A [`DeliveryHub`] maps each broker [`ConsumerId`] to the outbox channel of
//! the connection that owns it. The broker dispatcher writes through the hub as
//! a [`DeliverySink`], so a queue consumed on one connection can receive
//! messages published on another without either connection knowing about the
//! other. Routes whose receivers have gone away are dropped.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use wolf_broker::{ConsumerCancel, ConsumerId, Delivery, DeliverySink};

/// An event sent from the broker dispatch path to a connection's outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerEvent {
    /// A message delivery for a registered consumer.
    Delivery(Delivery),
    /// A server-initiated consumer cancellation.
    Cancel(ConsumerCancel),
}

/// Routes deliveries and cancellations to their owning connection outboxes.
#[derive(Debug, Clone, Default)]
pub struct DeliveryHub {
    routes: Arc<Mutex<BTreeMap<ConsumerId, UnboundedSender<ServerEvent>>>>,
}

impl DeliveryHub {
    /// Creates an empty routing hub.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates one connection outbox sender/receiver pair.
    pub fn subscribe(&self) -> (UnboundedSender<ServerEvent>, UnboundedReceiver<ServerEvent>) {
        unbounded_channel()
    }

    /// Registers the outbox that owns a consumer.
    pub fn register(&self, consumer: ConsumerId, sender: UnboundedSender<ServerEvent>) {
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
    pub fn deliver(&self, delivery: Delivery) {
        let consumer = delivery.consumer;
        self.send(consumer, ServerEvent::Delivery(delivery));
    }

    /// Sends a cancellation to its registered consumer, dropping stale routes.
    pub fn cancel(&self, cancel: ConsumerCancel) {
        let consumer = cancel.consumer;
        self.send(consumer, ServerEvent::Cancel(cancel));
    }

    /// Returns the number of live consumer routes.
    pub fn route_count(&self) -> usize {
        self.routes.lock().map_or(0, |routes| routes.len())
    }

    /// Returns a [`DeliverySink`] adapter over this hub.
    pub fn sink(&self) -> DeliveryHubSink {
        DeliveryHubSink { hub: self.clone() }
    }

    fn send(&self, consumer: ConsumerId, event: ServerEvent) {
        let Ok(mut routes) = self.routes.lock() else {
            return;
        };
        let Some(sender) = routes.get(&consumer) else {
            return;
        };
        if sender.send(event).is_err() {
            routes.remove(&consumer);
        }
    }
}

impl DeliverySink for DeliveryHub {
    fn deliver(&mut self, delivery: Delivery) {
        DeliveryHub::deliver(self, delivery);
    }

    fn cancel(&mut self, cancel: ConsumerCancel) {
        DeliveryHub::cancel(self, cancel);
    }
}

/// A cloneable sink adapter so callers can pass an owned value to the broker.
#[derive(Debug, Clone)]
pub struct DeliveryHubSink {
    hub: DeliveryHub,
}

impl DeliverySink for DeliveryHubSink {
    fn deliver(&mut self, delivery: Delivery) {
        self.hub.deliver(delivery);
    }

    fn cancel(&mut self, cancel: ConsumerCancel) {
        self.hub.cancel(cancel);
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

    #[test]
    fn deliveries_route_to_the_owning_connection() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        hub.register(ConsumerId::new(7), sender);

        hub.deliver(delivery(ConsumerId::new(7)));

        assert_eq!(
            receiver.try_recv().expect("delivery"),
            ServerEvent::Delivery(delivery(ConsumerId::new(7)))
        );
        assert_eq!(hub.route_count(), 1);
    }

    #[test]
    fn cancellations_route_to_the_owning_connection() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        hub.register(ConsumerId::new(8), sender);
        let cancel = ConsumerCancel {
            consumer: ConsumerId::new(8),
            reason: CancelReason::QueueDeleted,
        };

        hub.cancel(cancel.clone());

        assert_eq!(
            receiver.try_recv().expect("cancel"),
            ServerEvent::Cancel(cancel)
        );
    }

    #[test]
    fn unknown_consumers_are_ignored() {
        let hub = DeliveryHub::new();
        hub.deliver(delivery(ConsumerId::new(99)));
        assert_eq!(hub.route_count(), 0);
    }

    #[test]
    fn dropping_the_receiver_removes_the_route() {
        let hub = DeliveryHub::new();
        let (sender, receiver) = hub.subscribe();
        let consumer = ConsumerId::new(9);
        hub.register(consumer, sender);
        drop(receiver);

        hub.deliver(delivery(consumer));

        assert_eq!(hub.route_count(), 0);
    }

    #[test]
    fn unregister_removes_a_live_route() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        let consumer = ConsumerId::new(10);
        hub.register(consumer, sender);
        hub.unregister(consumer);

        hub.deliver(delivery(consumer));

        assert!(receiver.try_recv().is_err());
        assert_eq!(hub.route_count(), 0);
    }

    #[test]
    fn sink_adapter_routes_events() {
        let hub = DeliveryHub::new();
        let (sender, mut receiver) = hub.subscribe();
        let consumer = ConsumerId::new(11);
        hub.register(consumer, sender);
        let mut sink = hub.sink();

        sink.deliver(delivery(consumer));

        assert!(matches!(
            receiver.try_recv().expect("delivery"),
            ServerEvent::Delivery(_)
        ));
    }
}
