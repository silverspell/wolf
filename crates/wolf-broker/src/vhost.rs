//! Virtual hosts: queue actors and their identifiers.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::binding::QueueName;
use crate::consumer::{ConsumerDispatcher, ConsumerOptions, SettleOutcome};
use crate::delivery::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerId, CreditSignal, DeliverOutcome,
    DeliverySink, DeliveryTag,
};
use crate::error::DomainError;
use crate::exchange::ExchangeConfig;
use crate::message::Message;
use crate::publish::{GetOutcome, PublishOutcome, PublishRequest, Returned};
use crate::queue::{
    ConnectionCloseOutcome, ConnectionId, Queue, QueueConfig, QueueDeleteOutcome, QueueRegistry,
};
use crate::router::Router;
#[cfg(feature = "test-hooks")]
use crate::test_hooks::AckPauseHook;
use crate::validation::validate_queue_arguments;

/// A validated virtual host name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VhostName(Arc<str>);

impl VhostName {
    pub fn new(name: impl Into<Arc<str>>) -> Result<Self, DomainError> {
        let name = name.into();
        if name.is_empty() {
            return Err(DomainError::InvalidArgument {
                message: "vhost name must not be empty".to_string(),
            });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VhostName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A stable, opaque virtual host identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VhostId(u64);

impl VhostId {
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

type Dispatcher = Arc<Mutex<ConsumerDispatcher>>;

/// A dispatch future owned by a queue task.
type DispatchFuture = Pin<Box<dyn Future<Output = Result<usize, DomainError>> + Send + 'static>>;

/// The default capacity of the bounded channel feeding each queue task.
pub const DEFAULT_QUEUE_CHANNEL_CAPACITY: usize = 64;

/// Configuration for a virtual host's per-queue task channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VhostConfig {
    queue_channel_capacity: usize,
}

impl VhostConfig {
    /// Creates a configuration with the given queue channel capacity.
    ///
    /// A capacity of zero is clamped to one so every queue task has room for
    /// at least one pending command.
    pub fn new(queue_channel_capacity: usize) -> Self {
        Self {
            queue_channel_capacity: queue_channel_capacity.max(1),
        }
    }

    /// Returns the capacity of each queue task's bounded channel.
    pub fn queue_channel_capacity(&self) -> usize {
        self.queue_channel_capacity
    }
}

impl Default for VhostConfig {
    fn default() -> Self {
        Self::new(DEFAULT_QUEUE_CHANNEL_CAPACITY)
    }
}

enum QueueCommand {
    Enqueue {
        connection: ConnectionId,
        message: Message,
        reply: oneshot::Sender<Result<(), DomainError>>,
    },
    Deliver {
        future: DispatchFuture,
        reply: oneshot::Sender<Result<usize, DomainError>>,
    },
    DeliverDetached {
        future: DispatchFuture,
    },
}

struct QueueSlot {
    queue: Arc<Mutex<Queue>>,
    sender: mpsc::Sender<QueueCommand>,
    receiver: Mutex<Option<mpsc::Receiver<QueueCommand>>>,
    dispatcher: Dispatcher,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl QueueSlot {
    fn new(queue: Queue, dispatcher: Dispatcher, capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            queue: Arc::new(Mutex::new(queue)),
            sender,
            receiver: Mutex::new(Some(receiver)),
            dispatcher,
            task: Mutex::new(None),
        }
    }

    fn ensure_task(&self) {
        let mut task = self.task.lock().expect("queue task lock is not poisoned");
        if task.is_some() {
            return;
        }
        let Some(receiver) = self
            .receiver
            .lock()
            .expect("queue receiver lock is not poisoned")
            .take()
        else {
            return;
        };
        *task = Some(spawn_queue_task(Arc::clone(&self.queue), receiver));
    }
}

fn spawn_queue_task(
    queue: Arc<Mutex<Queue>>,
    mut receiver: mpsc::Receiver<QueueCommand>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(command) = receiver.recv().await {
            match command {
                QueueCommand::Enqueue {
                    connection,
                    message,
                    reply,
                } => {
                    let result = enqueue_message(&queue, connection, message);
                    let _ = reply.send(result);
                }
                QueueCommand::Deliver { future, reply } => {
                    let _ = reply.send(future.await);
                }
                QueueCommand::DeliverDetached { future } => {
                    let _ = future.await;
                }
            }
        }
    })
}

fn enqueue_message(
    queue: &Mutex<Queue>,
    connection: ConnectionId,
    message: Message,
) -> Result<(), DomainError> {
    let mut queue = queue.lock().map_err(|_| DomainError::BrokerClosed)?;
    queue.check_access(connection)?;
    queue.push_message(message);
    Ok(())
}

fn dispatch_future<S: DeliverySink + Send + 'static>(
    queue: Arc<Mutex<Queue>>,
    dispatcher: Dispatcher,
    sink: S,
) -> DispatchFuture {
    Box::pin(async move { deliver_ready(&queue, &dispatcher, sink).await })
}

async fn deliver_ready<S: DeliverySink>(
    queue: &Mutex<Queue>,
    dispatcher: &Mutex<ConsumerDispatcher>,
    mut sink: S,
) -> Result<usize, DomainError> {
    let mut delivered = 0;
    loop {
        let next = {
            let mut dispatcher = dispatcher.lock().map_err(|_| DomainError::BrokerClosed)?;
            let mut queue = queue.lock().map_err(|_| DomainError::BrokerClosed)?;
            dispatcher.take_next_delivery(&mut queue)
        };
        let Some(delivery) = next else {
            break;
        };
        if sink.deliver(delivery).await == DeliverOutcome::Closed {
            break;
        }
        delivered += 1;
    }
    Ok(delivered)
}

/// A virtual host: identity, name and its per-queue actors.
pub struct Vhost {
    id: VhostId,
    name: VhostName,
    router: RwLock<Router>,
    queues: Mutex<BTreeMap<QueueName, Arc<QueueSlot>>>,
    dispatcher: Dispatcher,
    next_server_name: AtomicU64,
    queue_channel_capacity: usize,
    #[cfg(feature = "test-hooks")]
    ack_pause: Option<Arc<AckPauseHook>>,
}

impl fmt::Debug for Vhost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Vhost")
            .field("id", &self.id)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Vhost {
    pub fn new(id: VhostId, name: VhostName) -> Self {
        Self::with_config(id, name, VhostConfig::default())
    }

    /// Creates a virtual host with an explicit queue channel configuration.
    pub fn with_config(id: VhostId, name: VhostName, config: VhostConfig) -> Self {
        Self::with_routing_and_config(id, name, Router::default(), QueueRegistry::new(), config)
    }

    /// Creates a virtual host with caller-supplied routing state.
    pub fn with_routing(
        id: VhostId,
        name: VhostName,
        router: Router,
        queues: QueueRegistry,
    ) -> Self {
        Self::with_routing_and_config(id, name, router, queues, VhostConfig::default())
    }

    /// Creates a virtual host with caller-supplied routing state and configuration.
    pub fn with_routing_and_config(
        id: VhostId,
        name: VhostName,
        router: Router,
        queues: QueueRegistry,
        config: VhostConfig,
    ) -> Self {
        let queue_channel_capacity = config.queue_channel_capacity();
        let dispatcher = Arc::new(Mutex::new(ConsumerDispatcher::new()));
        let slots: BTreeMap<QueueName, Arc<QueueSlot>> = queues
            .iter()
            .map(|(name, queue)| {
                (
                    name.clone(),
                    Arc::new(QueueSlot::new(
                        queue.clone(),
                        Arc::clone(&dispatcher),
                        queue_channel_capacity,
                    )),
                )
            })
            .collect();

        if tokio::runtime::Handle::try_current().is_ok() {
            for slot in slots.values() {
                slot.ensure_task();
            }
        }

        Self {
            id,
            name,
            router: RwLock::new(router),
            queues: Mutex::new(slots),
            dispatcher,
            next_server_name: AtomicU64::new(0),
            queue_channel_capacity,
            #[cfg(feature = "test-hooks")]
            ack_pause: None,
        }
    }

    #[cfg(feature = "test-hooks")]
    #[must_use]
    pub fn with_ack_pause_hook(mut self, hook: Arc<AckPauseHook>) -> Self {
        self.ack_pause = Some(hook);
        self
    }

    pub fn id(&self) -> VhostId {
        self.id
    }

    pub fn name(&self) -> &VhostName {
        &self.name
    }

    fn slot(&self, name: &str) -> Result<Arc<QueueSlot>, DomainError> {
        self.queues
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?
            .get(name)
            .cloned()
            .ok_or_else(|| DomainError::QueueNotFound {
                vhost: self.name.as_str().to_string(),
                queue: name.to_string(),
            })
    }

    fn maybe_slot(&self, name: &str) -> Option<Arc<QueueSlot>> {
        self.queues.lock().ok()?.get(name).cloned()
    }

    fn lock_queue<'a>(
        &self,
        slot: &'a QueueSlot,
    ) -> Result<std::sync::MutexGuard<'a, Queue>, DomainError> {
        slot.queue.lock().map_err(|_| DomainError::BrokerClosed)
    }

    /// Publishes a message using this virtual host's routing state.
    pub async fn publish<P>(
        &self,
        connection: ConnectionId,
        request: PublishRequest<'_, P>,
    ) -> Result<PublishOutcome<P>, DomainError> {
        let mut destinations = Vec::new();
        {
            let router = self.router.read().map_err(|_| DomainError::BrokerClosed)?;
            router.route_into(request.exchange, request.routing_key, &mut destinations)?;
        }

        let slots: Vec<Arc<QueueSlot>> = {
            let queues = self.queues.lock().map_err(|_| DomainError::BrokerClosed)?;
            destinations
                .iter()
                .filter_map(|name| queues.get(name.as_str()).cloned())
                .collect()
        };

        if slots.is_empty() {
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

        for slot in &slots {
            slot.ensure_task();
            let (reply, response) = oneshot::channel();
            slot.sender
                .send(QueueCommand::Enqueue {
                    connection,
                    message: request.message.clone(),
                    reply,
                })
                .await
                .map_err(|_| DomainError::BrokerClosed)?;
            response.await.map_err(|_| DomainError::BrokerClosed)??;
        }

        Ok(PublishOutcome::Routed {
            queues: slots.len(),
        })
    }

    /// Registers a consumer on a queue.
    pub async fn consume(
        &self,
        connection: ConnectionId,
        channel: ChannelId,
        consumer: ConsumerId,
        queue: impl AsRef<str>,
        options: ConsumerOptions,
    ) -> Result<(), DomainError> {
        let slot = self.slot(queue.as_ref())?;
        let mut dispatcher = self
            .dispatcher
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        if dispatcher.contains_consumer(consumer) {
            return Err(DomainError::InvalidArgument {
                message: format!("consumer {} is already registered", consumer.get()),
            });
        }

        let queue_name = {
            let mut queue = self.lock_queue(&slot)?;
            queue.check_access(connection)?;
            queue.add_consumer(connection, consumer);
            queue.name().clone()
        };

        dispatcher.register_detached(connection, channel, consumer, queue_name, options)
    }

    /// Cancels a consumer, returning whether it was registered.
    pub async fn cancel(&self, consumer: ConsumerId) -> Result<bool, DomainError> {
        let queue_name = {
            let mut dispatcher = self
                .dispatcher
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            dispatcher.detach_consumer(consumer)
        };
        let Some(queue_name) = queue_name else {
            return Ok(false);
        };
        if let Some(slot) = self.maybe_slot(queue_name.as_str()) {
            let mut queue = self.lock_queue(&slot)?;
            queue.remove_consumer(consumer);
        }
        Ok(true)
    }

    /// Applies a prefetch limit to a channel and returns credit signals.
    pub async fn set_prefetch(
        &self,
        channel: ChannelId,
        prefetch_count: u32,
        global: bool,
    ) -> Result<Vec<CreditSignal>, DomainError> {
        let mut dispatcher = self
            .dispatcher
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        Ok(dispatcher.set_prefetch(channel, prefetch_count, global))
    }

    /// Fetches one message for `basic.get`.
    pub async fn get(
        &self,
        connection: ConnectionId,
        channel: ChannelId,
        queue: impl AsRef<str>,
        no_ack: bool,
    ) -> Result<GetOutcome, DomainError> {
        let slot = self.slot(queue.as_ref())?;
        let mut dispatcher = self
            .dispatcher
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        let mut queue = self.lock_queue(&slot)?;
        dispatcher.get_detached(connection, channel, &mut queue, no_ack)
    }

    /// Acknowledges one delivery or every delivery up to the tag.
    pub async fn ack(
        &self,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
    ) -> Result<SettleOutcome, DomainError> {
        #[cfg(feature = "test-hooks")]
        if let Some(hook) = &self.ack_pause {
            hook.pause().await;
        }

        let mut dispatcher = self
            .dispatcher
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        dispatcher.ack(channel, delivery_tag, multiple)
    }

    /// Rejects one delivery or every delivery up to the tag.
    pub async fn nack(
        &self,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
        requeue: bool,
    ) -> Result<SettleOutcome, DomainError> {
        let (mut outcome, entries) = self.settle(channel, delivery_tag, multiple, requeue)?;
        outcome.requeued = self.requeue_entries(entries);
        Ok(outcome)
    }

    /// Rejects a single delivery.
    pub async fn reject(
        &self,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        requeue: bool,
    ) -> Result<SettleOutcome, DomainError> {
        let (mut outcome, entries) = self.settle(channel, delivery_tag, false, requeue)?;
        outcome.requeued = self.requeue_entries(entries);
        Ok(outcome)
    }

    fn settle(
        &self,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
        requeue: bool,
    ) -> Result<(SettleOutcome, Vec<(QueueName, Message)>), DomainError> {
        let mut dispatcher = self
            .dispatcher
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        dispatcher.settle_detached(channel, delivery_tag, multiple, requeue)
    }

    /// Requeues every unacknowledged delivery on a channel.
    pub async fn recover(&self, channel: ChannelId) -> Result<SettleOutcome, DomainError> {
        let (mut outcome, entries) = {
            let mut dispatcher = self
                .dispatcher
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            dispatcher.recover_detached(channel)
        };
        outcome.requeued = self.requeue_entries(entries);
        Ok(outcome)
    }

    /// Dispatches ready messages for one queue to the given sink.
    pub async fn dispatch<S>(&self, queue: impl AsRef<str>, sink: S) -> Result<usize, DomainError>
    where
        S: DeliverySink + Send + 'static,
    {
        let slot = self.slot(queue.as_ref())?;
        slot.ensure_task();
        let future = dispatch_future(Arc::clone(&slot.queue), Arc::clone(&slot.dispatcher), sink);
        let (reply, response) = oneshot::channel();
        slot.sender
            .send(QueueCommand::Deliver { future, reply })
            .await
            .map_err(|_| DomainError::BrokerClosed)?;
        response.await.map_err(|_| DomainError::BrokerClosed)?
    }

    /// Dispatches ready messages for every queue in this virtual host.
    pub async fn dispatch_all<S>(&self, sink: S) -> Result<usize, DomainError>
    where
        S: DeliverySink + Send + Clone + 'static,
    {
        let names: Vec<QueueName> = {
            let queues = self.queues.lock().map_err(|_| DomainError::BrokerClosed)?;
            queues.keys().cloned().collect()
        };

        let mut pending = Vec::with_capacity(names.len());
        for name in names {
            let slot = self.slot(name.as_str())?;
            slot.ensure_task();
            let future = dispatch_future(
                Arc::clone(&slot.queue),
                Arc::clone(&slot.dispatcher),
                sink.clone(),
            );
            let (reply, response) = oneshot::channel();
            slot.sender
                .send(QueueCommand::Deliver { future, reply })
                .await
                .map_err(|_| DomainError::BrokerClosed)?;
            pending.push(response);
        }

        let mut delivered = 0;
        for response in pending {
            delivered += response.await.map_err(|_| DomainError::BrokerClosed)??;
        }
        Ok(delivered)
    }

    /// Queues a dispatch for one queue without waiting for its completion.
    ///
    /// When the queue command channel is full, a task is spawned to wait for
    /// capacity so the caller's reader loop never blocks on its own outbox.
    pub fn dispatch_detached<S>(&self, queue: impl AsRef<str>, sink: S) -> Result<(), DomainError>
    where
        S: DeliverySink + Send + 'static,
    {
        let slot = self.slot(queue.as_ref())?;
        slot.ensure_task();
        let future = dispatch_future(Arc::clone(&slot.queue), Arc::clone(&slot.dispatcher), sink);
        let command = QueueCommand::DeliverDetached { future };
        match slot.sender.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let sender = slot.sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send(command).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
        Ok(())
    }

    /// Queues a dispatch for every queue without waiting for completion.
    pub fn dispatch_all_detached<S>(&self, sink: S) -> Result<(), DomainError>
    where
        S: DeliverySink + Send + Clone + 'static,
    {
        let names: Vec<QueueName> = {
            let queues = self.queues.lock().map_err(|_| DomainError::BrokerClosed)?;
            queues.keys().cloned().collect()
        };

        for name in names {
            let slot = self.slot(name.as_str())?;
            slot.ensure_task();
            let future = dispatch_future(
                Arc::clone(&slot.queue),
                Arc::clone(&slot.dispatcher),
                sink.clone(),
            );
            let command = QueueCommand::DeliverDetached { future };
            match slot.sender.try_send(command) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(command)) => {
                    let sender = slot.sender.clone();
                    tokio::spawn(async move {
                        let _ = sender.send(command).await;
                    });
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        Ok(())
    }

    /// Requeues unacknowledged deliveries on a closing channel and removes its
    /// consumers.
    pub async fn close_channel(&self, channel: ChannelId) -> Result<SettleOutcome, DomainError> {
        let (mut outcome, entries, consumers) = {
            let mut dispatcher = self
                .dispatcher
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            dispatcher.close_channel_detached(channel)
        };

        for (consumer, queue) in consumers {
            self.remove_consumer_from_queue(queue.as_str(), consumer)?;
        }
        outcome.requeued = self.requeue_entries(entries);
        Ok(outcome)
    }

    /// Requeues unacknowledged deliveries for a closed connection and removes
    /// its consumers.
    pub async fn close_connection(&self, connection: ConnectionId) -> ConnectionCloseOutcome {
        let (entries, _consumers) = {
            let Ok(mut dispatcher) = self.dispatcher.lock() else {
                return ConnectionCloseOutcome::default();
            };
            dispatcher.close_connection_detached(connection)
        };

        self.requeue_entries(entries);

        let names: Vec<QueueName> = self
            .queues
            .lock()
            .map(|queues| queues.keys().cloned().collect())
            .unwrap_or_default();
        let mut outcome = ConnectionCloseOutcome::default();

        for name in names {
            let Some(slot) = self.maybe_slot(name.as_str()) else {
                continue;
            };
            let mut remove = false;
            let mut cancels = Vec::new();
            {
                let Ok(mut queue) = self.lock_queue(&slot) else {
                    continue;
                };
                if queue.is_exclusive() && queue.owner() == connection {
                    remove = true;
                    cancels.extend(queue.consumers().map(|consumer| ConsumerCancel {
                        consumer,
                        reason: CancelReason::QueueDeleted,
                    }));
                } else {
                    let removed = queue.remove_consumers_for_connection(connection);
                    if queue.is_auto_delete() && !removed.is_empty() && queue.consumer_count() == 0
                    {
                        remove = true;
                    }
                }
            }

            if remove {
                if let Ok(mut queues) = self.queues.lock() {
                    queues.remove(name.as_str());
                }
                outcome.deleted_queues.push(name);
                outcome.cancels.extend(cancels);
            }
        }

        outcome
    }

    /// Returns the number of messages currently stored in `queue`.
    pub fn queue_message_count(&self, queue: impl AsRef<str>) -> usize {
        let Some(slot) = self.maybe_slot(queue.as_ref()) else {
            return 0;
        };
        self.lock_queue(&slot)
            .map(|queue| queue.message_count())
            .unwrap_or(0)
    }

    /// Declares an exchange, enforcing reserved names and redeclaration rules.
    pub async fn declare_exchange(
        &self,
        name: impl AsRef<str>,
        config: ExchangeConfig,
    ) -> Result<(), DomainError> {
        let mut router = self.router.write().map_err(|_| DomainError::BrokerClosed)?;
        router.registry_mut().declare(name.as_ref(), config)
    }

    /// Looks up an exchange for a passive declaration.
    pub async fn declare_exchange_passive(&self, name: impl AsRef<str>) -> Result<(), DomainError> {
        let router = self.router.read().map_err(|_| DomainError::BrokerClosed)?;
        router.registry().exchange(name.as_ref()).map(|_| ())
    }

    /// Deletes an exchange, returning whether it existed.
    ///
    /// The default and `amq.*` exchanges are reserved and cannot be deleted.
    /// With `if_unused`, an exchange that still has bindings is rejected.
    pub async fn delete_exchange(
        &self,
        name: impl AsRef<str>,
        if_unused: bool,
    ) -> Result<bool, DomainError> {
        let name = name.as_ref();
        let mut router = self.router.write().map_err(|_| DomainError::BrokerClosed)?;

        if name.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "the default exchange cannot be deleted".to_string(),
            });
        }
        if name.starts_with("amq.") {
            return Err(DomainError::AccessRefused {
                message: format!("exchange name '{name}' is reserved"),
            });
        }

        let registry = router.registry_mut();
        if !registry.contains(name) {
            return Ok(false);
        }
        if if_unused {
            let exchange = registry.exchange(name)?;
            if !exchange.bindings().is_empty() {
                return Err(DomainError::PreconditionFailed {
                    message: format!("exchange '{name}' still has bindings"),
                });
            }
        }
        registry.delete(name)
    }

    /// Binds a queue to an exchange.
    ///
    /// The default exchange is checked before the queue so `queue.bind` on it
    /// is always access-refused, matching RabbitMQ.
    pub async fn bind_queue(
        &self,
        exchange: impl AsRef<str>,
        queue: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<(), DomainError> {
        let exchange = exchange.as_ref();
        let queue = queue.as_ref();
        let routing_key = routing_key.as_ref();

        if exchange.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "bindings on the default exchange are implicit".to_string(),
            });
        }

        let slot = self.slot(queue)?;
        {
            let queue = self.lock_queue(&slot)?;
            queue.check_access(connection)?;
        }
        let mut router = self.router.write().map_err(|_| DomainError::BrokerClosed)?;
        router.registry_mut().bind(exchange, queue, routing_key)
    }

    /// Removes a queue binding, returning whether it existed.
    ///
    /// Like [`Self::bind_queue`], the default exchange is rejected first.
    pub async fn unbind_queue(
        &self,
        exchange: impl AsRef<str>,
        queue: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<bool, DomainError> {
        let exchange = exchange.as_ref();
        let queue = queue.as_ref();
        let routing_key = routing_key.as_ref();

        if exchange.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "bindings on the default exchange are implicit".to_string(),
            });
        }

        let slot = self.slot(queue)?;
        {
            let queue = self.lock_queue(&slot)?;
            queue.check_access(connection)?;
        }
        let mut router = self.router.write().map_err(|_| DomainError::BrokerClosed)?;
        router.registry_mut().unbind(exchange, queue, routing_key)
    }

    /// Declares a queue and returns its effective name and current counts.
    pub async fn declare_queue(
        &self,
        name: impl AsRef<str>,
        config: QueueConfig,
        connection: ConnectionId,
    ) -> Result<QueueDeclaration, DomainError> {
        let name = name.as_ref();

        let (slot, queue_name) = if name.is_empty() {
            validate_queue_arguments(&config.arguments)?;
            let mut queues = self.queues.lock().map_err(|_| DomainError::BrokerClosed)?;
            let queue_name = self.generate_server_name(&queues);
            let queue = Queue::new(queue_name.clone(), config, connection);
            let slot = Arc::new(QueueSlot::new(
                queue,
                Arc::clone(&self.dispatcher),
                self.queue_channel_capacity,
            ));
            queues.insert(queue_name.clone(), Arc::clone(&slot));
            drop(queues);
            slot.ensure_task();
            (slot, queue_name)
        } else {
            let queue_name = QueueName::new(name)?;
            let existing = self
                .queues
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?
                .get(name)
                .cloned();
            match existing {
                Some(slot) => {
                    let queue = self.lock_queue(&slot)?;
                    queue.check_access(connection)?;
                    validate_queue_arguments(&config.arguments)?;
                    if queue.config() != &config {
                        return Err(DomainError::PreconditionFailed {
                            message: format!(
                                "queue '{}' already exists with different properties",
                                queue_name.as_str()
                            ),
                        });
                    }
                    drop(queue);
                    (slot, queue_name)
                }
                None => {
                    validate_queue_arguments(&config.arguments)?;
                    let queue = Queue::new(queue_name.clone(), config, connection);
                    let slot = Arc::new(QueueSlot::new(
                        queue,
                        Arc::clone(&self.dispatcher),
                        self.queue_channel_capacity,
                    ));
                    self.queues
                        .lock()
                        .map_err(|_| DomainError::BrokerClosed)?
                        .insert(queue_name.clone(), Arc::clone(&slot));
                    slot.ensure_task();
                    (slot, queue_name)
                }
            }
        };

        let queue = self.lock_queue(&slot)?;
        Ok(QueueDeclaration {
            name: queue_name,
            message_count: count_to_u32(queue.message_count()),
            consumer_count: count_to_u32(queue.consumer_count()),
        })
    }

    /// Looks up a queue for a passive declaration and returns its counts.
    pub async fn declare_queue_passive(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<QueueDeclaration, DomainError> {
        let slot = self.slot(name.as_ref())?;
        let queue = self.lock_queue(&slot)?;
        queue.check_access(connection)?;
        Ok(QueueDeclaration {
            name: queue.name().clone(),
            message_count: count_to_u32(queue.message_count()),
            consumer_count: count_to_u32(queue.consumer_count()),
        })
    }

    /// Removes all messages from a queue, returning how many were discarded.
    pub async fn purge_queue(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<u32, DomainError> {
        let slot = self.slot(name.as_ref())?;
        let mut queue = self.lock_queue(&slot)?;
        queue.check_access(connection)?;
        Ok(count_to_u32(queue.clear_messages()))
    }

    /// Deletes a queue, enforcing the `if_unused` and `if_empty` conditions.
    ///
    /// Deleting a missing queue is idempotent and returns an empty outcome.
    pub async fn delete_queue(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<QueueDeleteOutcome, DomainError> {
        let name = name.as_ref();
        let Some(slot) = self.maybe_slot(name) else {
            return Ok(QueueDeleteOutcome::default());
        };

        let (messages, cancels) = {
            let mut queue = self.lock_queue(&slot)?;
            queue.check_access(connection)?;
            if if_unused && queue.consumer_count() != 0 {
                return Err(DomainError::PreconditionFailed {
                    message: format!("queue '{name}' still has consumers"),
                });
            }
            if if_empty && !queue.is_empty() {
                return Err(DomainError::PreconditionFailed {
                    message: format!("queue '{name}' is not empty"),
                });
            }
            let messages = queue.message_count();
            let cancels = queue
                .consumers()
                .map(|consumer| ConsumerCancel {
                    consumer,
                    reason: CancelReason::QueueDeleted,
                })
                .collect::<Vec<_>>();
            queue.clear_messages();
            (messages, cancels)
        };

        self.queues
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?
            .remove(name);

        if let Ok(mut dispatcher) = self.dispatcher.lock() {
            let consumers: Vec<ConsumerId> = cancels.iter().map(|cancel| cancel.consumer).collect();
            dispatcher.forget_consumers(&consumers);
        }

        Ok(QueueDeleteOutcome { messages, cancels })
    }

    fn remove_consumer_from_queue(
        &self,
        queue_name: &str,
        consumer: ConsumerId,
    ) -> Result<bool, DomainError> {
        let Some(slot) = self.maybe_slot(queue_name) else {
            return Ok(false);
        };
        let auto_delete = {
            let mut queue = self.lock_queue(&slot)?;
            let removed = queue.remove_consumer(consumer);
            queue.is_auto_delete() && removed && queue.consumer_count() == 0
        };
        if auto_delete {
            self.queues
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?
                .remove(queue_name);
        }
        Ok(auto_delete)
    }

    fn requeue_entries(&self, entries: Vec<(QueueName, Message)>) -> usize {
        let mut grouped: BTreeMap<QueueName, Vec<Message>> = BTreeMap::new();
        for (queue, message) in entries {
            grouped.entry(queue).or_default().push(message);
        }

        let mut requeued = 0;
        for (queue_name, messages) in grouped {
            let Some(slot) = self.maybe_slot(queue_name.as_str()) else {
                continue;
            };
            let Ok(mut queue) = self.lock_queue(&slot) else {
                continue;
            };
            for message in messages.into_iter().rev() {
                queue.push_front(message, true);
                requeued += 1;
            }
        }
        requeued
    }

    fn generate_server_name(&self, queues: &BTreeMap<QueueName, Arc<QueueSlot>>) -> QueueName {
        loop {
            let next = self.next_server_name.fetch_add(1, Ordering::Relaxed) + 1;
            let name =
                QueueName::new(format!("amq.gen-{next}")).expect("generated names are never empty");
            if !queues.contains_key(&name) {
                return name;
            }
        }
    }
}

/// The effective queue name and counts reported by a declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeclaration {
    /// The queue name, which may have been generated by the server.
    pub name: QueueName,
    /// Number of messages ready in the queue.
    pub message_count: u32,
    /// Number of consumers registered on the queue.
    pub consumer_count: u32,
}

fn count_to_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::ExchangeKind;

    fn vhost() -> Vhost {
        Vhost::new(VhostId::new(1), VhostName::new("/").expect("valid vhost"))
    }

    #[tokio::test]
    async fn passive_exchange_declare_requires_the_exchange_to_exist() {
        let vhost = vhost();
        let error = vhost
            .declare_exchange_passive("missing")
            .await
            .expect_err("missing exchange");
        assert!(matches!(error, DomainError::ExchangeNotFound { .. }));

        vhost
            .declare_exchange("events", ExchangeConfig::new(ExchangeKind::Topic))
            .await
            .expect("declare");
        vhost
            .declare_exchange_passive("events")
            .await
            .expect("passive declare ignores the type");
    }

    #[tokio::test]
    async fn exchange_delete_checks_reserved_names_and_if_unused() {
        let vhost = vhost();
        for name in ["", "amq.direct"] {
            let error = vhost
                .delete_exchange(name, false)
                .await
                .expect_err("reserved exchange");
            assert!(matches!(error, DomainError::AccessRefused { .. }));
        }

        vhost
            .declare_exchange("bound", ExchangeConfig::new(ExchangeKind::Direct))
            .await
            .expect("declare exchange");
        vhost
            .declare_queue("q", QueueConfig::default(), ConnectionId::new(1))
            .await
            .expect("declare queue");
        vhost
            .bind_queue("bound", "q", "rk", ConnectionId::new(1))
            .await
            .expect("bind");

        let error = vhost
            .delete_exchange("bound", true)
            .await
            .expect_err("bound exchange");
        assert!(matches!(error, DomainError::PreconditionFailed { .. }));
        assert!(vhost.delete_exchange("bound", false).await.expect("delete"));
        assert!(
            !vhost
                .delete_exchange("bound", false)
                .await
                .expect("idempotent")
        );
    }

    #[tokio::test]
    async fn default_exchange_bind_is_rejected_before_queue_lookup() {
        let vhost = vhost();
        let error = vhost
            .bind_queue("", "missing", "rk", ConnectionId::new(1))
            .await
            .expect_err("default exchange bind");
        assert!(matches!(error, DomainError::AccessRefused { .. }));
    }

    #[tokio::test]
    async fn queue_delete_is_idempotent_and_enforces_empty_and_unused() {
        let vhost = vhost();
        let connection = ConnectionId::new(1);
        let missing = vhost
            .delete_queue("missing", connection, true, true)
            .await
            .expect("missing delete is idempotent");
        assert_eq!(missing, QueueDeleteOutcome::default());

        vhost
            .declare_queue("q", QueueConfig::default(), connection)
            .await
            .expect("declare queue");
        vhost
            .publish(
                connection,
                PublishRequest {
                    exchange: "",
                    routing_key: "q",
                    mandatory: false,
                    message: crate::message::Message::new(bytes::Bytes::from_static(b"x")),
                    properties: (),
                },
            )
            .await
            .expect("publish");

        let error = vhost
            .delete_queue("q", connection, false, true)
            .await
            .expect_err("non-empty queue");
        assert!(matches!(error, DomainError::PreconditionFailed { .. }));

        let outcome = vhost
            .delete_queue("q", connection, true, false)
            .await
            .expect("delete");
        assert_eq!(outcome.messages, 1);
    }
}
