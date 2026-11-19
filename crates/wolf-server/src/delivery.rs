//! Cross-connection routing of broker deliveries and cancellations.
//!
//! A [`DeliveryHub`] maps each broker [`ConsumerId`] to the bounded outbox
//! channel of the connection that owns it. Routes are used for consume and
//! cancel bookkeeping; deliveries captured by a queue task bypass this table and
//! write directly to the consumer's outbox. The hub also remains a
//! [`DeliverySink`] fallback for consumers registered without a captured
//! outbox. Routes whose receivers have gone away are dropped.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{self, Receiver, Sender};
use wolf_broker::{ConsumerCancel, ConsumerId, DeliverOutcome, Delivery, DeliverySink};

/// The default capacity of each connection's bounded outbox.
pub const DEFAULT_OUTBOX_CAPACITY: usize = 256;

/// An event sent from the broker dispatch path to a connection's outbox.
pub use wolf_broker::DeliveryEvent as ServerEvent;

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
    use std::time::Duration;

    use bytes::Bytes;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use wolf_broker::{
        CancelReason, ChannelId, ConnectionId, ConsumerCancel, ConsumerId, ConsumerOptions,
        Delivery, DeliveryTag, Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
    };

    use super::*;

    const SAFETY: Duration = Duration::from_secs(10);

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_to_captured_outbox_does_not_lock_the_route_table() {
        let vhost = Vhost::new(VhostId::new(1), VhostName::new("/").expect("valid vhost"));
        let connection = ConnectionId::new(1);
        let channel = ChannelId::new(1);
        let consumer = ConsumerId::new(1);

        vhost
            .declare_queue("jobs", QueueConfig::default(), connection)
            .await
            .expect("declare queue");
        let (outbox, mut outbox_rx) = mpsc::channel(4);
        vhost
            .consume_with_outbox(
                connection,
                channel,
                consumer,
                "jobs",
                ConsumerOptions::default(),
                outbox,
            )
            .await
            .expect("consume queue");
        vhost
            .publish(
                connection,
                PublishRequest {
                    exchange: "",
                    routing_key: "jobs",
                    mandatory: false,
                    message: Message::new(Bytes::from_static(b"payload")),
                    properties: (),
                },
            )
            .await
            .expect("publish succeeds");

        let hub = DeliveryHub::new();
        let routes = Arc::clone(&hub.routes);
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let guard = routes.lock().expect("route table lock is not poisoned");
            locked_tx.send(()).expect("signal the held route lock");
            release_rx.recv().expect("wait for the release signal");
            drop(guard);
        });
        locked_rx.recv().expect("the route table is held");

        let dispatched = timeout(SAFETY, vhost.dispatch("jobs", hub.sink()))
            .await
            .expect("dispatch must not wait for the route table lock")
            .expect("dispatch succeeds");
        assert_eq!(dispatched, 1);

        let event = timeout(SAFETY, outbox_rx.recv())
            .await
            .expect("the delivery event arrives within the safety timeout")
            .expect("the outbox stays open");
        let ServerEvent::Delivery(delivery) = event else {
            panic!("expected a delivery event, got {event:?}");
        };
        assert_eq!(delivery.consumer, consumer);
        assert_eq!(delivery.message.body().as_ref(), b"payload");

        release_tx.send(()).expect("release the route table");
        holder.join().expect("the route lock holder finishes");
    }
}
