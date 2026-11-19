//! Consumer registration, prefetch/credit accounting and settlement.
//!
//! Consumers are dispatched round-robin per queue while respecting per-consumer
//! and channel-wide prefetch limits. Deliveries are tracked per channel so
//! acknowledgement, rejection, recovery and connection close can requeue
//! unacknowledged messages with the redelivered flag set.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::binding::QueueName;
use crate::delivery::{
    ChannelId, ConsumerId, CreditSignal, DeliverOutcome, Delivery, DeliverySink, DeliveryTag,
};
use crate::error::DomainError;
use crate::message::Message;
use crate::publish::{GetOutcome, get_message};
use crate::queue::{ConnectionCloseOutcome, ConnectionId, Queue, QueueRegistry};

/// Options that affect how a consumer receives messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsumerOptions {
    /// Whether deliveries are not acknowledged by the client.
    pub no_ack: bool,
}

#[derive(Debug, Clone)]
struct ConsumerState {
    connection: ConnectionId,
    channel: ChannelId,
    queue: QueueName,
    inflight: u32,
    no_ack: bool,
}

#[derive(Debug)]
struct ChannelState {
    connection: Option<ConnectionId>,
    consumers: BTreeSet<ConsumerId>,
    consumer_prefetch: u32,
    channel_prefetch: u32,
    next_tag: u64,
    inflight: u32,
    unacked: BTreeMap<DeliveryTag, Unacked>,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            connection: None,
            consumers: BTreeSet::new(),
            consumer_prefetch: 0,
            channel_prefetch: 0,
            next_tag: 1,
            inflight: 0,
            unacked: BTreeMap::new(),
        }
    }
}

impl ChannelState {
    fn allocate_tag(&mut self) -> DeliveryTag {
        let tag = DeliveryTag::new(self.next_tag);
        self.next_tag = self.next_tag.wrapping_add(1);
        if self.next_tag == 0 {
            self.next_tag = 1;
        }
        tag
    }
}

#[derive(Debug, Clone)]
struct Unacked {
    consumer: Option<ConsumerId>,
    queue: QueueName,
    message: Message,
}

/// The result of acknowledging, rejecting, recovering or requeueing deliveries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SettleOutcome {
    /// Number of unacknowledged deliveries removed from the channel.
    pub settled: usize,
    /// Number of messages returned to their queues.
    pub requeued: usize,
    /// Credit made available by settling the deliveries.
    pub credit: Vec<CreditSignal>,
}

/// Tracks consumers and dispatches queue messages to them.
#[derive(Debug, Default)]
pub struct ConsumerDispatcher {
    consumers: BTreeMap<ConsumerId, ConsumerState>,
    channels: BTreeMap<ChannelId, ChannelState>,
    cursors: BTreeMap<QueueName, ConsumerId>,
}

impl ConsumerDispatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a consumer on a queue using default (acknowledging) options.
    ///
    /// Queue membership remains owned by [`QueueRegistry`]; this only records
    /// the channel, connection and prefetch state needed for dispatch.
    pub fn register(
        &mut self,
        queues: &mut QueueRegistry,
        connection: ConnectionId,
        channel: ChannelId,
        consumer: ConsumerId,
        queue: impl AsRef<str>,
    ) -> Result<(), DomainError> {
        self.register_with(
            queues,
            connection,
            channel,
            consumer,
            queue,
            ConsumerOptions::default(),
        )
    }

    /// Registers a consumer with explicit acknowledgement options.
    pub fn register_with(
        &mut self,
        queues: &mut QueueRegistry,
        connection: ConnectionId,
        channel: ChannelId,
        consumer: ConsumerId,
        queue: impl AsRef<str>,
        options: ConsumerOptions,
    ) -> Result<(), DomainError> {
        if self.consumers.contains_key(&consumer) {
            return Err(DomainError::InvalidArgument {
                message: format!("consumer {} is already registered", consumer.get()),
            });
        }

        let queue = queues.declare_passive(queue.as_ref(), connection)?;
        let queue = queue.name().clone();
        queues.add_consumer(queue.as_str(), connection, consumer)?;

        self.consumers.insert(
            consumer,
            ConsumerState {
                connection,
                channel,
                queue,
                inflight: 0,
                no_ack: options.no_ack,
            },
        );
        let state = self.channels.entry(channel).or_default();
        state.connection = Some(connection);
        state.consumers.insert(consumer);
        Ok(())
    }

    /// Applies a prefetch limit and returns the resulting credit signals.
    ///
    /// A count of zero means unlimited. Consumer-scoped limits produce one
    /// signal per consumer on the channel; channel-scoped limits produce one
    /// aggregate signal.
    pub fn set_prefetch(
        &mut self,
        channel: ChannelId,
        prefetch_count: u32,
        global: bool,
    ) -> Vec<CreditSignal> {
        let consumers: Vec<ConsumerId> = self
            .channels
            .entry(channel)
            .or_default()
            .consumers
            .iter()
            .copied()
            .collect();

        if global {
            let state = self.channels.get_mut(&channel).expect("channel exists");
            state.channel_prefetch = prefetch_count;
            vec![CreditSignal::Channel {
                channel,
                credit: available_slots(prefetch_count, state.inflight),
            }]
        } else {
            self.channels
                .get_mut(&channel)
                .expect("channel exists")
                .consumer_prefetch = prefetch_count;
            consumers
                .into_iter()
                .map(|consumer| {
                    let inflight = self
                        .consumers
                        .get(&consumer)
                        .map_or(0, |state| state.inflight);
                    CreditSignal::Consumer {
                        consumer,
                        credit: available_slots(prefetch_count, inflight),
                    }
                })
                .collect()
        }
    }

    /// Delivers ready messages to consumers with available credit.
    ///
    /// Yields to the sink between deliveries and stops once the sink reports
    /// [`DeliverOutcome::Closed`]. Returns the number of messages delivered in
    /// this call.
    pub async fn dispatch<S: DeliverySink>(
        &mut self,
        queues: &mut QueueRegistry,
        queue: impl AsRef<str>,
        sink: &mut S,
    ) -> Result<usize, DomainError> {
        let name = queue.as_ref();
        let mut delivered = 0;

        loop {
            let queue = queues
                .queue(name)
                .ok_or_else(|| DomainError::QueueNotFound {
                    vhost: String::new(),
                    queue: name.to_string(),
                })?;
            if queue.is_empty() {
                break;
            }
            let Some(consumer) = self.next_consumer(name, queue) else {
                break;
            };
            let Some((message, redelivered)) = queues.take_message(name) else {
                break;
            };
            let delivery_tag = self.record_delivery(consumer, message.clone());
            let outcome = sink
                .deliver(Delivery {
                    consumer,
                    delivery_tag,
                    message,
                    redelivered,
                })
                .await;
            if outcome == DeliverOutcome::Closed {
                break;
            }
            delivered += 1;
        }

        Ok(delivered)
    }

    /// Fetches one message for `basic.get`, tracking it for acknowledgement
    /// unless `no_ack` is set.
    pub fn get(
        &mut self,
        queues: &mut QueueRegistry,
        connection: ConnectionId,
        channel: ChannelId,
        queue: impl AsRef<str>,
        no_ack: bool,
    ) -> Result<GetOutcome, DomainError> {
        let name = queue.as_ref();
        let outcome = get_message(queues, connection, name, no_ack)?;
        let GetOutcome::Message(mut got) = outcome else {
            return Ok(GetOutcome::Empty);
        };

        let queue_name = queues
            .queue(name)
            .map(|queue| queue.name().clone())
            .ok_or_else(|| DomainError::QueueNotFound {
                vhost: String::new(),
                queue: name.to_string(),
            })?;

        let state = self.channels.entry(channel).or_default();
        state.connection = Some(connection);
        let delivery_tag = state.allocate_tag();
        if !no_ack {
            state.inflight += 1;
            state.unacked.insert(
                delivery_tag,
                Unacked {
                    consumer: None,
                    queue: queue_name,
                    message: got.message.clone(),
                },
            );
        }
        got.delivery_tag = Some(delivery_tag);
        Ok(GetOutcome::Message(got))
    }

    /// Acknowledges one delivery or every delivery up to the tag.
    pub fn ack(
        &mut self,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
    ) -> Result<SettleOutcome, DomainError> {
        let tags = self.selected_tags(channel, delivery_tag, multiple)?;
        let removed = self.remove_unacked(channel, &tags);
        let settled = removed.len();
        let credit = self.credit_for_removed(channel, &removed);
        Ok(SettleOutcome {
            settled,
            requeued: 0,
            credit,
        })
    }

    /// Rejects one delivery or every delivery up to the tag.
    pub fn nack(
        &mut self,
        queues: &mut QueueRegistry,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
        requeue: bool,
    ) -> Result<SettleOutcome, DomainError> {
        self.settle(queues, channel, delivery_tag, multiple, requeue)
    }

    /// Rejects a single delivery.
    pub fn reject(
        &mut self,
        queues: &mut QueueRegistry,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        requeue: bool,
    ) -> Result<SettleOutcome, DomainError> {
        self.settle(queues, channel, delivery_tag, false, requeue)
    }

    /// Requeues every unacknowledged delivery on the channel.
    pub fn recover(
        &mut self,
        queues: &mut QueueRegistry,
        channel: ChannelId,
    ) -> Result<SettleOutcome, DomainError> {
        let tags: Vec<DeliveryTag> = self
            .channels
            .get(&channel)
            .map(|state| state.unacked.keys().copied().collect())
            .unwrap_or_default();
        if tags.is_empty() {
            return Ok(SettleOutcome::default());
        }

        let removed = self.remove_unacked(channel, &tags);
        let settled = removed.len();
        let credit = self.credit_for_removed(channel, &removed);
        let requeued = requeue_entries(queues, removed);
        Ok(SettleOutcome {
            settled,
            requeued,
            credit,
        })
    }

    /// Cancels a consumer, removing its queue membership.
    ///
    /// Unacknowledged deliveries are left on the channel so they can still be
    /// acknowledged or requeued when the channel or connection closes.
    /// Returns whether the consumer was registered.
    pub fn cancel(
        &mut self,
        queues: &mut QueueRegistry,
        consumer: ConsumerId,
    ) -> Result<bool, DomainError> {
        let Some(state) = self.consumers.remove(&consumer) else {
            return Ok(false);
        };
        if let Some(channel) = self.channels.get_mut(&state.channel) {
            channel.consumers.remove(&consumer);
        }
        self.cursors.retain(|_, current| *current != consumer);
        queues.remove_consumer(state.queue.as_str(), consumer)?;
        Ok(true)
    }

    /// Removes consumer registrations after their queues have been deleted.
    ///
    /// Unacknowledged deliveries belonging to those consumers are discarded
    /// because their queue no longer exists.
    pub fn forget_consumers(&mut self, consumers: &[ConsumerId]) {
        let mut per_channel: BTreeMap<ChannelId, Vec<DeliveryTag>> = BTreeMap::new();
        for consumer in consumers {
            let Some(state) = self.consumers.remove(consumer) else {
                continue;
            };
            if let Some(channel) = self.channels.get_mut(&state.channel) {
                channel.consumers.remove(consumer);
                for (tag, entry) in &channel.unacked {
                    if entry.consumer == Some(*consumer) {
                        per_channel.entry(state.channel).or_default().push(*tag);
                    }
                }
            }
        }
        self.cursors
            .retain(|_, consumer| !consumers.contains(consumer));
        for (channel, tags) in per_channel {
            self.remove_unacked(channel, &tags);
        }
    }

    /// Requeues unacknowledged deliveries for a connection, then cleans up.
    pub fn close_connection(
        &mut self,
        queues: &mut QueueRegistry,
        connection: ConnectionId,
    ) -> ConnectionCloseOutcome {
        let channels: Vec<ChannelId> = self
            .channels
            .iter()
            .filter(|(_, state)| state.connection == Some(connection))
            .map(|(channel, _)| *channel)
            .collect();

        let mut requeued = Vec::new();
        for channel in &channels {
            let tags: Vec<DeliveryTag> = self
                .channels
                .get(channel)
                .map(|state| state.unacked.keys().copied().collect())
                .unwrap_or_default();
            if !tags.is_empty() {
                requeued.extend(self.remove_unacked(*channel, &tags));
            }
        }

        let consumers: Vec<ConsumerId> = self
            .consumers
            .iter()
            .filter(|(_, state)| state.connection == connection)
            .map(|(consumer, _)| *consumer)
            .collect();

        for consumer in &consumers {
            if let Some(state) = self.consumers.remove(consumer) {
                if let Some(channel) = self.channels.get_mut(&state.channel) {
                    channel.consumers.remove(consumer);
                }
            }
        }
        self.cursors
            .retain(|_, consumer| !consumers.contains(consumer));

        for channel in channels {
            self.channels.remove(&channel);
        }

        requeue_entries(queues, requeued);
        queues.close_connection(connection)
    }

    /// Requeues unacknowledged deliveries on a closing channel, then removes
    /// its consumers and state.
    pub fn close_channel(
        &mut self,
        queues: &mut QueueRegistry,
        channel: ChannelId,
    ) -> Result<SettleOutcome, DomainError> {
        let Some(state) = self.channels.get(&channel) else {
            return Ok(SettleOutcome::default());
        };
        let consumers: Vec<ConsumerId> = state.consumers.iter().copied().collect();
        let tags: Vec<DeliveryTag> = state.unacked.keys().copied().collect();
        let consumers: Vec<(ConsumerId, QueueName)> = consumers
            .into_iter()
            .filter_map(|consumer| {
                self.consumers
                    .get(&consumer)
                    .map(|state| (consumer, state.queue.clone()))
            })
            .collect();

        let removed = self.remove_unacked(channel, &tags);
        let settled = removed.len();

        for (consumer, queue) in &consumers {
            self.consumers.remove(consumer);
            self.cursors.retain(|_, current| current != consumer);
            queues.remove_consumer(queue.as_str(), *consumer)?;
        }
        self.channels.remove(&channel);

        let requeued = requeue_entries(queues, removed);
        Ok(SettleOutcome {
            settled,
            requeued,
            credit: Vec::new(),
        })
    }

    /// Returns the number of unacknowledged deliveries on a channel.
    pub fn unacked_count(&self, channel: ChannelId) -> usize {
        self.channels
            .get(&channel)
            .map_or(0, |state| state.unacked.len())
    }

    fn next_consumer(&self, queue_name: &str, queue: &Queue) -> Option<ConsumerId> {
        let count = queue.consumer_count();
        if count == 0 {
            return None;
        }

        let start = self
            .cursors
            .get(queue_name)
            .and_then(|last| queue.consumers().position(|candidate| candidate == *last))
            .map_or(0, |position| (position + 1) % count);

        for offset in 0..count {
            let index = (start + offset) % count;
            let Some(consumer) = queue.consumers().nth(index) else {
                break;
            };
            if self.has_credit(consumer) {
                return Some(consumer);
            }
        }

        None
    }

    fn has_credit(&self, consumer: ConsumerId) -> bool {
        let Some(state) = self.consumers.get(&consumer) else {
            return false;
        };
        if state.no_ack {
            return true;
        }
        let Some(channel) = self.channels.get(&state.channel) else {
            return false;
        };
        let consumer_ok =
            channel.consumer_prefetch == 0 || state.inflight < channel.consumer_prefetch;
        let channel_ok =
            channel.channel_prefetch == 0 || channel.inflight < channel.channel_prefetch;
        consumer_ok && channel_ok
    }

    fn record_delivery(&mut self, consumer: ConsumerId, message: Message) -> DeliveryTag {
        let (channel, queue, no_ack) = {
            let state = self
                .consumers
                .get_mut(&consumer)
                .expect("selected consumer is registered");
            let no_ack = state.no_ack;
            if !no_ack {
                state.inflight += 1;
            }
            (state.channel, state.queue.clone(), no_ack)
        };

        let delivery_tag = {
            let state = self
                .channels
                .get_mut(&channel)
                .expect("consumer channel is registered");
            let delivery_tag = state.allocate_tag();
            if !no_ack {
                state.inflight += 1;
                state.unacked.insert(
                    delivery_tag,
                    Unacked {
                        consumer: Some(consumer),
                        queue: queue.clone(),
                        message,
                    },
                );
            }
            delivery_tag
        };

        self.cursors.insert(queue, consumer);
        delivery_tag
    }

    fn selected_tags(
        &self,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
    ) -> Result<Vec<DeliveryTag>, DomainError> {
        let state = self
            .channels
            .get(&channel)
            .ok_or_else(|| unknown_delivery_tag(channel, delivery_tag))?;
        if !state.unacked.contains_key(&delivery_tag) {
            return Err(unknown_delivery_tag(channel, delivery_tag));
        }

        if multiple {
            Ok(state
                .unacked
                .range(..=delivery_tag)
                .map(|(tag, _)| *tag)
                .collect())
        } else {
            Ok(vec![delivery_tag])
        }
    }

    fn remove_unacked(&mut self, channel: ChannelId, tags: &[DeliveryTag]) -> Vec<Unacked> {
        let mut removed = Vec::with_capacity(tags.len());
        {
            let Some(state) = self.channels.get_mut(&channel) else {
                return removed;
            };
            for tag in tags {
                if let Some(entry) = state.unacked.remove(tag) {
                    removed.push(entry);
                }
            }
            state.inflight = state.unacked.len() as u32;
        }

        for entry in &removed {
            if let Some(consumer) = entry.consumer {
                if let Some(state) = self.consumers.get_mut(&consumer) {
                    state.inflight = state.inflight.saturating_sub(1);
                }
            }
        }

        removed
    }

    fn credit_for_removed(&self, channel: ChannelId, removed: &[Unacked]) -> Vec<CreditSignal> {
        if removed.is_empty() {
            return Vec::new();
        }
        let Some(state) = self.channels.get(&channel) else {
            return Vec::new();
        };

        if state.channel_prefetch != 0 {
            return vec![CreditSignal::Channel {
                channel,
                credit: removed.len() as u32,
            }];
        }

        let mut counts: BTreeMap<ConsumerId, u32> = BTreeMap::new();
        for entry in removed {
            if let Some(consumer) = entry.consumer {
                *counts.entry(consumer).or_default() += 1;
            }
        }
        counts
            .into_iter()
            .map(|(consumer, credit)| CreditSignal::Consumer { consumer, credit })
            .collect()
    }

    fn settle(
        &mut self,
        queues: &mut QueueRegistry,
        channel: ChannelId,
        delivery_tag: DeliveryTag,
        multiple: bool,
        requeue: bool,
    ) -> Result<SettleOutcome, DomainError> {
        let tags = self.selected_tags(channel, delivery_tag, multiple)?;
        let removed = self.remove_unacked(channel, &tags);
        let settled = removed.len();
        let credit = self.credit_for_removed(channel, &removed);
        let requeued = if requeue {
            requeue_entries(queues, removed)
        } else {
            0
        };
        Ok(SettleOutcome {
            settled,
            requeued,
            credit,
        })
    }
}

/// Shared per-channel runtime used by queue-owned consumer state.
///
/// Delivery tags and prefetch accounting are channel-wide, while the consumers
/// and unacknowledged deliveries that use them are split across queue tasks.
#[derive(Debug)]
pub(crate) struct ChannelRuntime {
    connection: Mutex<Option<ConnectionId>>,
    pub(crate) consumer_prefetch: AtomicU32,
    pub(crate) channel_prefetch: AtomicU32,
    pub(crate) inflight: AtomicU32,
    next_tag: AtomicU64,
}

impl ChannelRuntime {
    pub(crate) fn new(connection: Option<ConnectionId>) -> Self {
        Self {
            connection: Mutex::new(connection),
            consumer_prefetch: AtomicU32::new(0),
            channel_prefetch: AtomicU32::new(0),
            inflight: AtomicU32::new(0),
            next_tag: AtomicU64::new(1),
        }
    }

    pub(crate) fn connection(&self) -> Option<ConnectionId> {
        self.connection
            .lock()
            .map(|connection| *connection)
            .unwrap_or(None)
    }

    pub(crate) fn set_connection(&self, connection: ConnectionId) {
        if let Ok(mut current) = self.connection.lock() {
            *current = Some(connection);
        }
    }

    pub(crate) fn allocate_tag(&self) -> DeliveryTag {
        let mut current = self.next_tag.load(Ordering::Relaxed);
        loop {
            let next = if current == u64::MAX { 1 } else { current + 1 };
            match self.next_tag.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return DeliveryTag::new(current),
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn reserve_channel_slot(&self) -> bool {
        let limit = self.channel_prefetch.load(Ordering::Acquire);
        loop {
            let current = self.inflight.load(Ordering::Acquire);
            if limit != 0 && current >= limit {
                return false;
            }
            match self.inflight.compare_exchange_weak(
                current,
                current.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    pub(crate) fn release_channel_slot(&self) {
        loop {
            let current = self.inflight.load(Ordering::Acquire);
            let next = current.saturating_sub(1);
            match self.inflight.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    pub(crate) fn consumer_credit(&self, inflight: u32) -> u32 {
        let limit = self.consumer_prefetch.load(Ordering::Acquire);
        available_slots(limit, inflight)
    }

    pub(crate) fn channel_credit(&self) -> u32 {
        let limit = self.channel_prefetch.load(Ordering::Acquire);
        available_slots(limit, self.inflight.load(Ordering::Acquire))
    }
}

/// A consumer registered on one queue, in queue registration order.
#[derive(Debug, Clone)]
pub(crate) struct QueueConsumer {
    pub(crate) consumer: ConsumerId,
    pub(crate) connection: ConnectionId,
    pub(crate) channel: ChannelId,
    pub(crate) no_ack: bool,
    pub(crate) inflight: u32,
    pub(crate) runtime: Arc<ChannelRuntime>,
}

/// An unacknowledged delivery owned by one queue.
#[derive(Debug, Clone)]
pub(crate) struct QueueUnacked {
    pub(crate) consumer: Option<ConsumerId>,
    pub(crate) queue: QueueName,
    pub(crate) message: Message,
    pub(crate) runtime: Arc<ChannelRuntime>,
}

/// Consumer and unacknowledged-delivery state owned by one queue task.
#[derive(Debug, Default)]
pub(crate) struct QueueConsumerState {
    pub(crate) consumers: Vec<QueueConsumer>,
    pub(crate) cursor: Option<ConsumerId>,
    pub(crate) unacked: BTreeMap<ChannelId, BTreeMap<DeliveryTag, QueueUnacked>>,
}

impl QueueConsumerState {
    pub(crate) fn remove_consumer(&mut self, consumer: ConsumerId) -> Option<QueueConsumer> {
        let index = self
            .consumers
            .iter()
            .position(|candidate| candidate.consumer == consumer)?;
        let removed = self.consumers.remove(index);
        if self.cursor == Some(consumer) {
            self.cursor = None;
        }
        Some(removed)
    }

    pub(crate) fn reserve_next(&mut self) -> Option<QueueConsumer> {
        let count = self.consumers.len();
        if count == 0 {
            return None;
        }

        let start = self
            .cursor
            .and_then(|last| {
                self.consumers
                    .iter()
                    .position(|candidate| candidate.consumer == last)
            })
            .map_or(0, |position| (position + 1) % count);

        for offset in 0..count {
            let index = (start + offset) % count;
            if self.consumers[index].no_ack {
                return Some(self.consumers[index].clone());
            }

            let runtime = Arc::clone(&self.consumers[index].runtime);
            let limit = runtime.consumer_prefetch.load(Ordering::Acquire);
            if limit != 0 && self.consumers[index].inflight >= limit {
                continue;
            }
            if !runtime.reserve_channel_slot() {
                continue;
            }

            let candidate = &mut self.consumers[index];
            candidate.inflight += 1;
            return Some(candidate.clone());
        }

        None
    }

    pub(crate) fn record_delivery(
        &mut self,
        consumer: &QueueConsumer,
        queue: QueueName,
        message: Message,
    ) -> DeliveryTag {
        let delivery_tag = consumer.runtime.allocate_tag();
        if !consumer.no_ack {
            self.unacked.entry(consumer.channel).or_default().insert(
                delivery_tag,
                QueueUnacked {
                    consumer: Some(consumer.consumer),
                    queue,
                    message,
                    runtime: Arc::clone(&consumer.runtime),
                },
            );
        }
        self.cursor = Some(consumer.consumer);
        delivery_tag
    }
}

fn available_slots(limit: u32, inflight: u32) -> u32 {
    if limit == 0 {
        0
    } else {
        limit.saturating_sub(inflight)
    }
}

fn requeue_entries(queues: &mut QueueRegistry, entries: Vec<Unacked>) -> usize {
    let mut grouped: BTreeMap<QueueName, Vec<Message>> = BTreeMap::new();
    for entry in entries {
        grouped.entry(entry.queue).or_default().push(entry.message);
    }

    let mut requeued = 0;
    for (queue, messages) in grouped {
        for message in messages.into_iter().rev() {
            if queues.requeue_message(queue.as_str(), message, true) {
                requeued += 1;
            }
        }
    }
    requeued
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
