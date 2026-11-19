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
use crate::consumer::{
    ChannelRuntime, ConsumerOptions, QueueConsumer, QueueConsumerState, QueueUnacked, SettleOutcome,
};
use crate::delivery::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerId, CreditSignal, DeliverOutcome, Delivery,
    DeliverySink, DeliveryTag,
};
use crate::error::DomainError;
use crate::exchange::ExchangeConfig;
use crate::message::Message;
use crate::publish::{AckState, GetMessage, GetOutcome, PublishOutcome, PublishRequest, Returned};
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
    consumers: Arc<Mutex<QueueConsumerState>>,
    sender: mpsc::Sender<QueueCommand>,
    receiver: Mutex<Option<mpsc::Receiver<QueueCommand>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl QueueSlot {
    fn new(queue: Queue, capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            queue: Arc::new(Mutex::new(queue)),
            consumers: Arc::new(Mutex::new(QueueConsumerState::default())),
            sender,
            receiver: Mutex::new(Some(receiver)),
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
    consumers: Arc<Mutex<QueueConsumerState>>,
    sink: S,
) -> DispatchFuture {
    Box::pin(async move { deliver_ready(&queue, &consumers, sink).await })
}

async fn deliver_ready<S: DeliverySink>(
    queue: &Mutex<Queue>,
    consumers: &Mutex<QueueConsumerState>,
    mut sink: S,
) -> Result<usize, DomainError> {
    let mut delivered = 0;
    loop {
        let next = {
            let mut queue = queue.lock().map_err(|_| DomainError::BrokerClosed)?;
            if queue.is_empty() {
                break;
            }
            let mut consumers = consumers.lock().map_err(|_| DomainError::BrokerClosed)?;
            let Some(consumer) = consumers.reserve_next() else {
                break;
            };
            let (message, redelivered) = queue
                .take_front()
                .expect("a non-empty queue has a front message");
            let queue_name = queue.name().clone();
            let delivery_tag = consumers.record_delivery(&consumer, queue_name, message.clone());
            Some(Delivery {
                consumer: consumer.consumer,
                delivery_tag,
                message,
                redelivered,
            })
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
    queues: RwLock<BTreeMap<QueueName, Arc<QueueSlot>>>,
    channels: RwLock<BTreeMap<ChannelId, Arc<ChannelRuntime>>>,
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
        let slots: BTreeMap<QueueName, Arc<QueueSlot>> = queues
            .iter()
            .map(|(name, queue)| {
                (
                    name.clone(),
                    Arc::new(QueueSlot::new(queue.clone(), queue_channel_capacity)),
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
            queues: RwLock::new(slots),
            channels: RwLock::new(BTreeMap::new()),
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
            .read()
            .map_err(|_| DomainError::BrokerClosed)?
            .get(name)
            .cloned()
            .ok_or_else(|| DomainError::QueueNotFound {
                vhost: self.name.as_str().to_string(),
                queue: name.to_string(),
            })
    }

    fn maybe_slot(&self, name: &str) -> Option<Arc<QueueSlot>> {
        self.queues.read().ok()?.get(name).cloned()
    }

    fn lock_queue<'a>(
        &self,
        slot: &'a QueueSlot,
    ) -> Result<std::sync::MutexGuard<'a, Queue>, DomainError> {
        slot.queue.lock().map_err(|_| DomainError::BrokerClosed)
    }

    fn snapshot_slots(&self) -> Result<Vec<Arc<QueueSlot>>, DomainError> {
        let queues = self.queues.read().map_err(|_| DomainError::BrokerClosed)?;
        Ok(queues.values().cloned().collect())
    }

    fn ensure_runtime(
        &self,
        channel: ChannelId,
        connection: Option<ConnectionId>,
    ) -> Result<Arc<ChannelRuntime>, DomainError> {
        let mut channels = self
            .channels
            .write()
            .map_err(|_| DomainError::BrokerClosed)?;
        let runtime = channels
            .entry(channel)
            .or_insert_with(|| Arc::new(ChannelRuntime::new(connection)))
            .clone();
        if let Some(connection) = connection {
            runtime.set_connection(connection);
        }
        Ok(runtime)
    }

    fn consumer_registered(&self, consumer: ConsumerId) -> Result<bool, DomainError> {
        for slot in self.snapshot_slots()? {
            let consumers = slot
                .consumers
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            if consumers
                .consumers
                .iter()
                .any(|candidate| candidate.consumer == consumer)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn remove_slot_if_current(
        &self,
        name: &str,
        slot: &Arc<QueueSlot>,
    ) -> Result<bool, DomainError> {
        let mut queues = self.queues.write().map_err(|_| DomainError::BrokerClosed)?;
        if queues
            .get(name)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            queues.remove(name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn discard_slot_state(&self, slot: &QueueSlot) -> Result<(), DomainError> {
        let mut consumers = slot
            .consumers
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        for unacked in std::mem::take(&mut consumers.unacked).into_values() {
            for entry in unacked.into_values() {
                entry.runtime.release_channel_slot();
            }
        }
        consumers.consumers.clear();
        consumers.cursor = None;
        Ok(())
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
            let queues = self.queues.read().map_err(|_| DomainError::BrokerClosed)?;
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
        if self.consumer_registered(consumer)? {
            return Err(DomainError::InvalidArgument {
                message: format!("consumer {} is already registered", consumer.get()),
            });
        }

        {
            let mut queue = self.lock_queue(&slot)?;
            queue.check_access(connection)?;
            queue.add_consumer(connection, consumer);
        }

        let runtime = self.ensure_runtime(channel, Some(connection))?;
        let mut consumers = slot
            .consumers
            .lock()
            .map_err(|_| DomainError::BrokerClosed)?;
        if consumers
            .consumers
            .iter()
            .any(|candidate| candidate.consumer == consumer)
        {
            return Err(DomainError::InvalidArgument {
                message: format!("consumer {} is already registered", consumer.get()),
            });
        }
        consumers.consumers.push(QueueConsumer {
            consumer,
            connection,
            channel,
            no_ack: options.no_ack,
            inflight: 0,
            runtime,
        });
        Ok(())
    }

    /// Cancels a consumer, returning whether it was registered.
    pub async fn cancel(&self, consumer: ConsumerId) -> Result<bool, DomainError> {
        for slot in self.snapshot_slots()? {
            let removed = {
                let mut consumers = slot
                    .consumers
                    .lock()
                    .map_err(|_| DomainError::BrokerClosed)?;
                consumers.remove_consumer(consumer)
            };
            if removed.is_some() {
                self.remove_consumer_from_slot(&slot, consumer)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Applies a prefetch limit to a channel and returns credit signals.
    pub async fn set_prefetch(
        &self,
        channel: ChannelId,
        prefetch_count: u32,
        global: bool,
    ) -> Result<Vec<CreditSignal>, DomainError> {
        let runtime = self.ensure_runtime(channel, None)?;
        if global {
            runtime
                .channel_prefetch
                .store(prefetch_count, Ordering::Release);
            return Ok(vec![CreditSignal::Channel {
                channel,
                credit: runtime.channel_credit(),
            }]);
        }

        runtime
            .consumer_prefetch
            .store(prefetch_count, Ordering::Release);
        let mut inflight = BTreeMap::new();
        for slot in self.snapshot_slots()? {
            let consumers = slot
                .consumers
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            for consumer in &consumers.consumers {
                if consumer.channel == channel {
                    inflight.insert(consumer.consumer, consumer.inflight);
                }
            }
        }
        Ok(inflight
            .into_iter()
            .map(|(consumer, inflight)| CreditSignal::Consumer {
                consumer,
                credit: runtime.consumer_credit(inflight),
            })
            .collect())
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
        let runtime = self.ensure_runtime(channel, Some(connection))?;
        let mut queue = self.lock_queue(&slot)?;
        queue.check_access(connection)?;
        let Some((message, redelivered)) = queue.take_front() else {
            return Ok(GetOutcome::Empty);
        };

        let remaining = queue.message_count();
        let ack_state = if no_ack {
            AckState::NotRequired
        } else {
            AckState::Required
        };
        let delivery_tag = runtime.allocate_tag();
        if !no_ack {
            runtime.inflight.fetch_add(1, Ordering::AcqRel);
            let mut consumers = slot
                .consumers
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            consumers.unacked.entry(channel).or_default().insert(
                delivery_tag,
                QueueUnacked {
                    consumer: None,
                    queue: queue.name().clone(),
                    message: message.clone(),
                    runtime: Arc::clone(&runtime),
                },
            );
        }

        Ok(GetOutcome::Message(GetMessage {
            message,
            redelivered,
            ack_state,
            remaining,
            delivery_tag: Some(delivery_tag),
        }))
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

        let (outcome, _) = self.settle(channel, delivery_tag, multiple, false)?;
        Ok(outcome)
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
        let mut plan = Vec::new();
        let mut found = false;
        for slot in self.snapshot_slots()? {
            let consumers = slot
                .consumers
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            let Some(unacked) = consumers.unacked.get(&channel) else {
                continue;
            };
            if unacked.contains_key(&delivery_tag) {
                found = true;
            }
            if multiple {
                let tags: Vec<DeliveryTag> = unacked
                    .range(..=delivery_tag)
                    .map(|(tag, _)| *tag)
                    .collect();
                if !tags.is_empty() {
                    plan.push((Arc::clone(&slot), tags));
                }
            } else if unacked.contains_key(&delivery_tag) {
                plan.push((Arc::clone(&slot), vec![delivery_tag]));
            }
        }
        if !found {
            return Err(unknown_delivery_tag(channel, delivery_tag));
        }
        self.apply_plan(channel, plan, requeue)
    }

    fn apply_plan(
        &self,
        channel: ChannelId,
        plan: Vec<(Arc<QueueSlot>, Vec<DeliveryTag>)>,
        requeue: bool,
    ) -> Result<(SettleOutcome, Vec<(QueueName, Message)>), DomainError> {
        let mut settled = 0usize;
        let mut consumer_counts: BTreeMap<ConsumerId, u32> = BTreeMap::new();
        let mut entries = Vec::new();
        let mut runtime: Option<Arc<ChannelRuntime>> = None;

        for (slot, tags) in plan {
            let removed = {
                let mut consumers = slot
                    .consumers
                    .lock()
                    .map_err(|_| DomainError::BrokerClosed)?;
                let mut removed = Vec::new();
                if let Some(unacked) = consumers.unacked.get_mut(&channel) {
                    for tag in &tags {
                        if let Some(entry) = unacked.remove(tag) {
                            removed.push(entry);
                        }
                    }
                    if unacked.is_empty() {
                        consumers.unacked.remove(&channel);
                    }
                }
                for entry in &removed {
                    if let Some(consumer) = entry.consumer {
                        if let Some(record) = consumers
                            .consumers
                            .iter_mut()
                            .find(|candidate| candidate.consumer == consumer)
                        {
                            record.inflight = record.inflight.saturating_sub(1);
                        }
                    }
                }
                removed
            };

            for entry in removed {
                entry.runtime.release_channel_slot();
                settled += 1;
                runtime = Some(Arc::clone(&entry.runtime));
                if let Some(consumer) = entry.consumer {
                    *consumer_counts.entry(consumer).or_default() += 1;
                }
                if requeue {
                    entries.push((entry.queue, entry.message));
                }
            }
        }

        let credit = if settled == 0 {
            Vec::new()
        } else {
            let runtime = runtime.expect("settled deliveries carry a channel runtime");
            if runtime.channel_prefetch.load(Ordering::Acquire) != 0 {
                vec![CreditSignal::Channel {
                    channel,
                    credit: settled as u32,
                }]
            } else {
                consumer_counts
                    .into_iter()
                    .map(|(consumer, credit)| CreditSignal::Consumer { consumer, credit })
                    .collect()
            }
        };

        Ok((
            SettleOutcome {
                settled,
                requeued: 0,
                credit,
            },
            entries,
        ))
    }

    /// Requeues every unacknowledged delivery on a channel.
    pub async fn recover(&self, channel: ChannelId) -> Result<SettleOutcome, DomainError> {
        let mut plan = Vec::new();
        for slot in self.snapshot_slots()? {
            let consumers = slot
                .consumers
                .lock()
                .map_err(|_| DomainError::BrokerClosed)?;
            if let Some(unacked) = consumers.unacked.get(&channel) {
                let tags: Vec<DeliveryTag> = unacked.keys().copied().collect();
                if !tags.is_empty() {
                    plan.push((Arc::clone(&slot), tags));
                }
            }
        }

        let (mut outcome, entries) = self.apply_plan(channel, plan, true)?;
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
        let future = dispatch_future(Arc::clone(&slot.queue), Arc::clone(&slot.consumers), sink);
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
        let slots: Vec<Arc<QueueSlot>> = {
            let queues = self.queues.read().map_err(|_| DomainError::BrokerClosed)?;
            queues.values().cloned().collect()
        };

        let mut pending = Vec::with_capacity(slots.len());
        for slot in slots {
            slot.ensure_task();
            let future = dispatch_future(
                Arc::clone(&slot.queue),
                Arc::clone(&slot.consumers),
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
        let future = dispatch_future(Arc::clone(&slot.queue), Arc::clone(&slot.consumers), sink);
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
        let slots: Vec<Arc<QueueSlot>> = {
            let queues = self.queues.read().map_err(|_| DomainError::BrokerClosed)?;
            queues.values().cloned().collect()
        };

        for slot in slots {
            slot.ensure_task();
            let future = dispatch_future(
                Arc::clone(&slot.queue),
                Arc::clone(&slot.consumers),
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
        let mut outcome = SettleOutcome::default();
        let mut entries = Vec::new();
        let mut to_remove = Vec::new();

        for slot in self.snapshot_slots()? {
            let (removed, consumers) = {
                let mut consumers = slot
                    .consumers
                    .lock()
                    .map_err(|_| DomainError::BrokerClosed)?;
                let removed = consumers
                    .unacked
                    .remove(&channel)
                    .map(|unacked| unacked.into_values().collect::<Vec<_>>())
                    .unwrap_or_default();
                let removed_consumers: Vec<ConsumerId> = consumers
                    .consumers
                    .iter()
                    .filter(|consumer| consumer.channel == channel)
                    .map(|consumer| consumer.consumer)
                    .collect();
                consumers
                    .consumers
                    .retain(|consumer| consumer.channel != channel);
                if consumers
                    .cursor
                    .is_some_and(|cursor| removed_consumers.contains(&cursor))
                {
                    consumers.cursor = None;
                }
                (removed, removed_consumers)
            };

            for entry in removed {
                entry.runtime.release_channel_slot();
                outcome.settled += 1;
                entries.push((entry.queue, entry.message));
            }
            for consumer in consumers {
                to_remove.push((Arc::clone(&slot), consumer));
            }
        }

        if let Ok(mut channels) = self.channels.write() {
            channels.remove(&channel);
        }
        for (slot, consumer) in to_remove {
            self.remove_consumer_from_slot(&slot, consumer)?;
        }
        outcome.requeued = self.requeue_entries(entries);
        Ok(outcome)
    }

    /// Requeues unacknowledged deliveries for a closed connection and removes
    /// its consumers.
    pub async fn close_connection(&self, connection: ConnectionId) -> ConnectionCloseOutcome {
        let Ok(slots) = self.snapshot_slots() else {
            return ConnectionCloseOutcome::default();
        };

        let mut requeued = Vec::new();
        for slot in &slots {
            let Ok(mut consumers) = slot.consumers.lock() else {
                return ConnectionCloseOutcome::default();
            };
            let channels: Vec<ChannelId> = consumers
                .unacked
                .iter()
                .filter(|(_, unacked)| {
                    unacked
                        .values()
                        .any(|entry| entry.runtime.connection() == Some(connection))
                })
                .map(|(channel, _)| *channel)
                .collect();
            let mut removed = Vec::new();
            for channel in channels {
                if let Some(unacked) = consumers.unacked.remove(&channel) {
                    removed.extend(unacked.into_values());
                }
            }
            consumers
                .consumers
                .retain(|consumer| consumer.connection != connection);
            if consumers.cursor.is_some_and(|cursor| {
                consumers
                    .consumers
                    .iter()
                    .all(|consumer| consumer.consumer != cursor)
            }) {
                consumers.cursor = None;
            }
            drop(consumers);

            for entry in removed {
                entry.runtime.release_channel_slot();
                requeued.push((entry.queue, entry.message));
            }
        }

        if let Ok(mut channels) = self.channels.write() {
            channels.retain(|_, runtime| runtime.connection() != Some(connection));
        }
        self.requeue_entries(requeued);

        let names: Vec<QueueName> = self
            .queues
            .read()
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

            if remove
                && self
                    .remove_slot_if_current(name.as_str(), &slot)
                    .unwrap_or(false)
            {
                let _ = self.discard_slot_state(&slot);
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
            let (slot, queue_name) = {
                let mut queues = self.queues.write().map_err(|_| DomainError::BrokerClosed)?;
                let queue_name = self.generate_server_name(&queues);
                let queue = Queue::new(queue_name.clone(), config, connection);
                let slot = Arc::new(QueueSlot::new(queue, self.queue_channel_capacity));
                queues.insert(queue_name.clone(), Arc::clone(&slot));
                (slot, queue_name)
            };
            slot.ensure_task();
            (slot, queue_name)
        } else {
            let queue_name = QueueName::new(name)?;
            let existing = {
                let queues = self.queues.read().map_err(|_| DomainError::BrokerClosed)?;
                queues.get(name).cloned()
            };
            match existing {
                Some(slot) => {
                    self.validate_redeclared_queue(&slot, &queue_name, &config, connection)?;
                    (slot, queue_name)
                }
                None => {
                    let (slot, inserted) = {
                        let mut queues =
                            self.queues.write().map_err(|_| DomainError::BrokerClosed)?;
                        match queues.get(name).cloned() {
                            Some(winner) => (winner, false),
                            None => {
                                validate_queue_arguments(&config.arguments)?;
                                let queue =
                                    Queue::new(queue_name.clone(), config.clone(), connection);
                                let slot =
                                    Arc::new(QueueSlot::new(queue, self.queue_channel_capacity));
                                queues.insert(queue_name.clone(), Arc::clone(&slot));
                                (slot, true)
                            }
                        }
                    };
                    if inserted {
                        slot.ensure_task();
                    } else {
                        self.validate_redeclared_queue(&slot, &queue_name, &config, connection)?;
                    }
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

        let removed = self.remove_slot_if_current(name, &slot)?;
        let _ = self.discard_slot_state(&slot);

        if removed {
            Ok(QueueDeleteOutcome { messages, cancels })
        } else {
            Ok(QueueDeleteOutcome::default())
        }
    }

    fn remove_consumer_from_slot(
        &self,
        slot: &Arc<QueueSlot>,
        consumer: ConsumerId,
    ) -> Result<bool, DomainError> {
        let (auto_delete, name) = {
            let mut queue = self.lock_queue(slot)?;
            let removed = queue.remove_consumer(consumer);
            let name = queue.name().clone();
            (
                queue.is_auto_delete() && removed && queue.consumer_count() == 0,
                name,
            )
        };
        if auto_delete {
            let removed = self.remove_slot_if_current(name.as_str(), slot)?;
            if removed {
                self.discard_slot_state(slot)?;
            }
            return Ok(removed);
        }
        Ok(false)
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

    fn validate_redeclared_queue(
        &self,
        slot: &Arc<QueueSlot>,
        queue_name: &QueueName,
        config: &QueueConfig,
        connection: ConnectionId,
    ) -> Result<(), DomainError> {
        let queue = self.lock_queue(slot)?;
        queue.check_access(connection)?;
        validate_queue_arguments(&config.arguments)?;
        if queue.config() != config {
            return Err(DomainError::PreconditionFailed {
                message: format!(
                    "queue '{}' already exists with different properties",
                    queue_name.as_str()
                ),
            });
        }
        Ok(())
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

fn unknown_delivery_tag(channel: ChannelId, delivery_tag: DeliveryTag) -> DomainError {
    DomainError::PreconditionFailed {
        message: format!(
            "unknown delivery tag {} on channel {}",
            delivery_tag.get(),
            channel.get()
        ),
    }
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

    #[test]
    fn publish_does_not_take_the_exclusive_queue_directory_lock() {
        use std::sync::mpsc;
        use std::time::Duration;

        let vhost = Arc::new(vhost());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();

        let reader = {
            let vhost = Arc::clone(&vhost);
            std::thread::spawn(move || {
                let guard = vhost.queues.read().expect("queue directory read lock");
                entered_tx.send(()).expect("signal reader entered");
                let _ = release_rx.recv();
                drop(guard);
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("reader acquired the shared queue directory lock");

        let (result_tx, result_rx) = mpsc::channel();
        let publisher = {
            let vhost = Arc::clone(&vhost);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("current-thread runtime");
                let outcome = runtime.block_on(vhost.publish(
                    ConnectionId::new(1),
                    PublishRequest {
                        exchange: "",
                        routing_key: "unknown-queue",
                        mandatory: false,
                        message: crate::message::Message::new(bytes::Bytes::from_static(b"x")),
                        properties: (),
                    },
                ));
                let _ = result_tx.send(outcome);
            })
        };

        let outcome = result_rx.recv_timeout(Duration::from_secs(5));

        let _ = release_tx.send(());
        reader.join().expect("reader thread joins");
        publisher.join().expect("publisher thread joins");

        assert_eq!(
            outcome.expect("publish completes while the shared lock is held"),
            Ok(PublishOutcome::Dropped)
        );
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
