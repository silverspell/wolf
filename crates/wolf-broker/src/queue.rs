//! Queue declarations, configuration, message storage and lifecycle rules.
//!
//! Queues are protocol-neutral: this module knows about names, declarations,
//! stored messages and consumer memberships, but nothing about wire methods.
//! Lifecycle rules follow RabbitMQ semantics for classic queues:
//!
//! - An empty declaration name asks the broker for a server-generated,
//!   collision-free queue name.
//! - Exclusive queues belong to the connection that declared them; other
//!   connections cannot access them and they are removed when that connection
//!   closes.
//! - Auto-delete queues are removed once their last tracked consumer leaves.
//! - Redeclaring an existing queue requires an identical configuration.

use std::collections::{BTreeMap, VecDeque, btree_map};
use std::fmt;

use crate::binding::QueueName;
use crate::delivery::{CancelReason, ConsumerCancel, ConsumerId};
use crate::error::DomainError;
use crate::message::Message;
use crate::validation::{QueueArguments, validate_queue_arguments};

/// An opaque client connection identifier.
///
/// The queue domain only uses it to decide ownership of exclusive queues and to
/// clean up consumers when a connection closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Creates a connection identifier from its raw value.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw numeric identifier.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The declaration properties of a queue.
///
/// [`PartialEq`] is used to detect conflicting redeclarations: any difference
/// yields [`DomainError::PreconditionFailed`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueConfig {
    /// Whether the declaration asked for a durable queue.
    ///
    /// Durability is accepted for compatibility but queues are kept in memory
    /// at this stage.
    pub durable: bool,
    /// Whether only the declaring connection may access the queue.
    pub exclusive: bool,
    /// Whether the queue is removed when its last consumer leaves.
    pub auto_delete: bool,
    /// Protocol-neutral declaration arguments.
    pub arguments: QueueArguments,
}

impl QueueConfig {
    /// Creates a non-durable, non-exclusive, non-auto-delete queue config.
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsumerEntry {
    consumer: ConsumerId,
    connection: ConnectionId,
}

#[derive(Debug, Clone)]
struct QueuedMessage {
    message: Message,
    redelivered: bool,
}

/// A declared queue with its stored messages and consumers.
#[derive(Debug, Clone)]
pub struct Queue {
    name: QueueName,
    config: QueueConfig,
    owner: ConnectionId,
    messages: VecDeque<QueuedMessage>,
    consumers: Vec<ConsumerEntry>,
}

impl Queue {
    fn new(name: QueueName, config: QueueConfig, owner: ConnectionId) -> Self {
        Self {
            name,
            config,
            owner,
            messages: VecDeque::new(),
            consumers: Vec::new(),
        }
    }

    /// Returns the queue name.
    pub fn name(&self) -> &QueueName {
        &self.name
    }

    /// Returns the declaration configuration.
    pub fn config(&self) -> &QueueConfig {
        &self.config
    }

    /// Returns the connection that defined the queue.
    pub fn owner(&self) -> ConnectionId {
        self.owner
    }

    /// Returns whether the queue is exclusive to its owning connection.
    pub fn is_exclusive(&self) -> bool {
        self.config.exclusive
    }

    /// Returns whether the queue is auto-deleted when the last consumer leaves.
    pub fn is_auto_delete(&self) -> bool {
        self.config.auto_delete
    }

    /// Returns whether the queue was declared durable.
    pub fn is_durable(&self) -> bool {
        self.config.durable
    }

    /// Returns the number of stored messages.
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Returns whether the queue currently stores no messages.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Iterates over the stored messages in publish order.
    pub fn messages(&self) -> impl Iterator<Item = &Message> + '_ {
        self.messages.iter().map(|entry| &entry.message)
    }

    /// Returns the number of tracked consumers.
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// Iterates over the queue's consumers in registration order.
    pub fn consumers(&self) -> impl Iterator<Item = ConsumerId> + '_ {
        self.consumers.iter().map(|entry| entry.consumer)
    }

    fn check_access(&self, connection: ConnectionId) -> Result<(), DomainError> {
        if self.config.exclusive && self.owner != connection {
            return Err(DomainError::ResourceLocked {
                message: format!(
                    "queue '{}' is exclusive to another connection",
                    self.name.as_str()
                ),
            });
        }
        Ok(())
    }

    fn remove_consumers_for_connection(&mut self, connection: ConnectionId) -> Vec<ConsumerId> {
        let mut removed = Vec::new();
        self.consumers.retain(|entry| {
            if entry.connection == connection {
                removed.push(entry.consumer);
                false
            } else {
                true
            }
        });
        removed
    }
}

/// The result of deleting a queue.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueDeleteOutcome {
    /// Number of messages that were still stored when the queue was removed.
    pub messages: usize,
    /// Cancellations to send for the queue's consumers.
    pub cancels: Vec<ConsumerCancel>,
}

impl QueueDeleteOutcome {
    /// Returns the number of messages that were discarded.
    pub fn message_count(&self) -> usize {
        self.messages
    }
}

/// The result of cleaning up after a connection closes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectionCloseOutcome {
    /// Queues removed because they were exclusive to the closed connection or
    /// because a shared auto-delete queue lost its last consumer.
    pub deleted_queues: Vec<QueueName>,
    /// Cancellations triggered by exclusive queue removal.
    pub cancels: Vec<ConsumerCancel>,
}

/// The queues declared in one virtual host.
#[derive(Debug, Clone, Default)]
pub struct QueueRegistry {
    queues: BTreeMap<QueueName, Queue>,
    next_server_name: u64,
}

impl QueueRegistry {
    /// Creates an empty queue registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares a queue, returning the effective queue name.
    ///
    /// An empty `name` asks the server to generate a unique name. Existing
    /// queues are subject to the exclusive-access rule and must have an
    /// identical [`QueueConfig`], otherwise their redeclaration fails with
    /// [`DomainError::PreconditionFailed`]. Declaration arguments are always
    /// validated, including for redeclarations.
    pub fn declare(
        &mut self,
        name: impl AsRef<str>,
        config: QueueConfig,
        connection: ConnectionId,
    ) -> Result<QueueName, DomainError> {
        let name = name.as_ref();

        if name.is_empty() {
            validate_queue_arguments(&config.arguments)?;
            let name = self.generate_server_name();
            self.queues
                .insert(name.clone(), Queue::new(name.clone(), config, connection));
            return Ok(name);
        }

        let name = QueueName::new(name)?;
        match self.queues.get(&name) {
            Some(existing) => {
                existing.check_access(connection)?;
                validate_queue_arguments(&config.arguments)?;
                if existing.config != config {
                    return Err(DomainError::PreconditionFailed {
                        message: format!(
                            "queue '{}' already exists with different properties",
                            name.as_str()
                        ),
                    });
                }
                Ok(name)
            }
            None => {
                validate_queue_arguments(&config.arguments)?;
                self.queues
                    .insert(name.clone(), Queue::new(name.clone(), config, connection));
                Ok(name)
            }
        }
    }

    /// Looks up a queue for a passive declaration.
    ///
    /// Passive declarations only check existence and exclusive access; they do
    /// not compare declaration properties.
    pub fn declare_passive(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<&Queue, DomainError> {
        self.queue_for(name, connection)
    }

    /// Returns a queue by name without applying access checks.
    pub fn queue(&self, name: impl AsRef<str>) -> Option<&Queue> {
        let name = name.as_ref();
        self.queues.get(name)
    }

    /// Returns a queue by name, enforcing exclusive-queue access.
    pub fn queue_for(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<&Queue, DomainError> {
        let name = name.as_ref();
        let queue = self.queues.get(name).ok_or_else(|| queue_not_found(name))?;
        queue.check_access(connection)?;
        Ok(queue)
    }

    /// Returns whether a queue with this name exists.
    pub fn contains(&self, name: impl AsRef<str>) -> bool {
        let name = name.as_ref();
        self.queues.contains_key(name)
    }

    /// Returns the number of declared queues.
    pub fn len(&self) -> usize {
        self.queues.len()
    }

    /// Returns whether no queues are declared.
    pub fn is_empty(&self) -> bool {
        self.queues.is_empty()
    }

    /// Iterates over all declared queues in name order.
    pub fn iter(&self) -> btree_map::Iter<'_, QueueName, Queue> {
        self.queues.iter()
    }

    /// Publishes a message to a queue.
    pub fn publish(
        &mut self,
        name: impl AsRef<str>,
        connection: ConnectionId,
        message: Message,
    ) -> Result<(), DomainError> {
        let queue = self.queue_mut_for(name.as_ref(), connection)?;
        queue.messages.push_back(QueuedMessage {
            message,
            redelivered: false,
        });
        Ok(())
    }

    pub(crate) fn take_message(&mut self, name: &str) -> Option<(Message, bool)> {
        let queue = self.queues.get_mut(name)?;
        queue
            .messages
            .pop_front()
            .map(|entry| (entry.message, entry.redelivered))
    }

    pub(crate) fn requeue_message(
        &mut self,
        name: &str,
        message: Message,
        redelivered: bool,
    ) -> bool {
        let Some(queue) = self.queues.get_mut(name) else {
            return false;
        };
        queue.messages.push_front(QueuedMessage {
            message,
            redelivered,
        });
        true
    }

    /// Removes all messages from a queue, returning how many were discarded.
    pub fn purge(
        &mut self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<usize, DomainError> {
        let queue = self.queue_mut_for(name.as_ref(), connection)?;
        let removed = queue.messages.len();
        queue.messages.clear();
        Ok(removed)
    }

    /// Adds a consumer to a queue.
    ///
    /// Adding the same consumer twice is idempotent.
    pub fn add_consumer(
        &mut self,
        name: impl AsRef<str>,
        connection: ConnectionId,
        consumer: ConsumerId,
    ) -> Result<(), DomainError> {
        let queue = self.queue_mut_for(name.as_ref(), connection)?;
        if !queue
            .consumers
            .iter()
            .any(|entry| entry.consumer == consumer)
        {
            queue.consumers.push(ConsumerEntry {
                consumer,
                connection,
            });
        }
        Ok(())
    }

    /// Removes a consumer from a queue.
    ///
    /// Returns whether the queue was removed because it was auto-delete and the
    /// consumer was its last one. Removing a consumer from a missing queue is a
    /// no-op.
    pub fn remove_consumer(
        &mut self,
        name: impl AsRef<str>,
        consumer: ConsumerId,
    ) -> Result<bool, DomainError> {
        let name = name.as_ref();
        let Some(queue) = self.queues.get_mut(name) else {
            return Ok(false);
        };

        let before = queue.consumers.len();
        queue.consumers.retain(|entry| entry.consumer != consumer);
        let removed = queue.consumers.len() != before;
        let auto_delete = removed && queue.config.auto_delete && queue.consumers.is_empty();

        if auto_delete {
            self.queues.remove(name);
        }
        Ok(auto_delete)
    }

    /// Deletes a queue, returning its remaining messages and consumer cancels.
    ///
    /// Deleting a missing queue is idempotent and returns an empty outcome.
    pub fn delete(
        &mut self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<QueueDeleteOutcome, DomainError> {
        let name = name.as_ref();
        let Some(queue) = self.queues.get(name) else {
            return Ok(QueueDeleteOutcome::default());
        };
        queue.check_access(connection)?;

        let queue = self
            .queues
            .remove(name)
            .expect("queue was checked to exist");
        let cancels = queue
            .consumers
            .iter()
            .map(|entry| ConsumerCancel {
                consumer: entry.consumer,
                reason: CancelReason::QueueDeleted,
            })
            .collect();

        Ok(QueueDeleteOutcome {
            messages: queue.messages.len(),
            cancels,
        })
    }

    /// Deletes exclusive queues owned by a closed connection and removes that
    /// connection's consumers from shared queues.
    ///
    /// Shared auto-delete queues that lose their last consumer are removed too.
    /// Cancellations are emitted for consumers of deleted exclusive queues.
    pub fn close_connection(&mut self, connection: ConnectionId) -> ConnectionCloseOutcome {
        let mut outcome = ConnectionCloseOutcome::default();
        let names: Vec<QueueName> = self.queues.keys().cloned().collect();

        for name in names {
            let exclusive_owned = self
                .queues
                .get(&name)
                .is_some_and(|queue| queue.config.exclusive && queue.owner == connection);

            if exclusive_owned {
                if let Some(queue) = self.queues.remove(&name) {
                    outcome.deleted_queues.push(queue.name.clone());
                    outcome
                        .cancels
                        .extend(queue.consumers.iter().map(|entry| ConsumerCancel {
                            consumer: entry.consumer,
                            reason: CancelReason::QueueDeleted,
                        }));
                }
                continue;
            }

            let auto_deleted = match self.queues.get_mut(&name) {
                Some(queue) => {
                    let removed = queue.remove_consumers_for_connection(connection);
                    queue.config.auto_delete && !removed.is_empty() && queue.consumers.is_empty()
                }
                None => false,
            };

            if auto_deleted {
                if let Some(queue) = self.queues.remove(&name) {
                    outcome.deleted_queues.push(queue.name.clone());
                }
            }
        }

        outcome
    }

    fn queue_mut_for(
        &mut self,
        name: &str,
        connection: ConnectionId,
    ) -> Result<&mut Queue, DomainError> {
        let queue = self
            .queues
            .get_mut(name)
            .ok_or_else(|| queue_not_found(name))?;
        queue.check_access(connection)?;
        Ok(queue)
    }

    fn generate_server_name(&mut self) -> QueueName {
        loop {
            self.next_server_name = self.next_server_name.wrapping_add(1);
            let name = format!("amq.gen-{}", self.next_server_name);
            let name = QueueName::new(name).expect("generated names are never empty");
            if !self.queues.contains_key(&name) {
                return name;
            }
        }
    }
}

fn queue_not_found(queue: &str) -> DomainError {
    DomainError::QueueNotFound {
        vhost: String::new(),
        queue: queue.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(exclusive: bool, auto_delete: bool) -> QueueConfig {
        QueueConfig {
            exclusive,
            auto_delete,
            ..QueueConfig::default()
        }
    }

    fn message() -> Message {
        Message::new(bytes::Bytes::from_static(b"payload"))
    }

    #[test]
    fn server_generated_names_are_unique_and_registered() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);

        let mut names = Vec::new();
        for _ in 0..3 {
            let name = registry
                .declare("", QueueConfig::default(), connection)
                .expect("declare server-named queue");
            assert!(name.as_str().starts_with("amq.gen-"));
            assert!(!names.contains(&name));
            assert!(registry.contains(name.as_str()));
            names.push(name);
        }
        assert_eq!(registry.len(), 3);
    }

    #[test]
    fn auto_delete_only_fires_when_the_last_consumer_leaves() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);
        let name = registry
            .declare("auto", config(false, true), connection)
            .expect("declare");

        registry
            .add_consumer(name.as_str(), connection, ConsumerId::new(1))
            .expect("add one");
        registry
            .add_consumer(name.as_str(), connection, ConsumerId::new(2))
            .expect("add two");

        assert!(
            !registry
                .remove_consumer(name.as_str(), ConsumerId::new(1))
                .expect("remove one")
        );
        assert!(registry.contains(name.as_str()));

        assert!(
            registry
                .remove_consumer(name.as_str(), ConsumerId::new(2))
                .expect("remove two")
        );
        assert!(!registry.contains(name.as_str()));
    }

    #[test]
    fn passive_declare_ignores_configuration_differences() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);
        let name = registry
            .declare("stable", config(false, false), connection)
            .expect("declare");

        let queue = registry
            .declare_passive(name.as_str(), connection)
            .expect("passive");
        assert_eq!(queue.config(), &config(false, false));
        assert_eq!(queue.message_count(), 0);
    }

    #[test]
    fn exclusive_queues_are_owner_only() {
        let mut registry = QueueRegistry::new();
        let owner = ConnectionId::new(1);
        let other = ConnectionId::new(2);
        let name = registry
            .declare("private", config(true, false), owner)
            .expect("declare");

        for error in [
            registry.declare_passive(name.as_str(), other).map(|_| ()),
            registry
                .declare(name.as_str(), config(true, false), other)
                .map(|_| ()),
            registry.publish(name.as_str(), other, message()),
            registry.purge(name.as_str(), other).map(|_| ()),
            registry.add_consumer(name.as_str(), other, ConsumerId::new(9)),
        ] {
            assert!(matches!(error, Err(DomainError::ResourceLocked { .. })));
        }

        registry
            .publish(name.as_str(), owner, message())
            .expect("owner may publish");
        assert_eq!(
            registry
                .queue(name.as_str())
                .expect("queue")
                .message_count(),
            1
        );
    }

    #[test]
    fn redeclaration_requires_an_identical_configuration() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);
        let name = registry
            .declare("stable", QueueConfig::default(), connection)
            .expect("declare");

        registry
            .declare(name.as_str(), QueueConfig::default(), connection)
            .expect("identical redeclare");

        for different in [config(true, false), config(false, true)] {
            let error = registry
                .declare(name.as_str(), different, connection)
                .expect_err("different config");
            assert!(matches!(error, DomainError::PreconditionFailed { .. }));
        }
    }

    #[test]
    fn redeclare_validates_arguments_even_when_config_is_identical() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);
        let name = registry
            .declare("stable", QueueConfig::default(), connection)
            .expect("declare");

        let mut arguments = QueueArguments::new();
        arguments.insert(
            "x-queue-type".to_string(),
            crate::exchange::ArgumentValue::String("stream".to_string()),
        );
        let invalid = QueueConfig {
            arguments,
            ..QueueConfig::default()
        };

        let error = registry
            .declare(name.as_str(), invalid, connection)
            .expect_err("unsupported arguments");
        assert!(matches!(error, DomainError::PreconditionFailed { .. }));
    }

    #[test]
    fn deleting_a_queue_with_consumers_returns_one_cancel_each() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);
        let name = registry
            .declare("busy", QueueConfig::default(), connection)
            .expect("declare");
        registry
            .add_consumer(name.as_str(), connection, ConsumerId::new(3))
            .expect("add consumer");

        let outcome = registry.delete(name.as_str(), connection).expect("delete");

        assert_eq!(
            outcome.cancels,
            vec![ConsumerCancel {
                consumer: ConsumerId::new(3),
                reason: CancelReason::QueueDeleted,
            }]
        );
    }

    #[test]
    fn closing_a_connection_keeps_auto_delete_queues_that_never_had_consumers() {
        let mut registry = QueueRegistry::new();
        let connection = ConnectionId::new(1);
        let name = registry
            .declare("idle", config(false, true), connection)
            .expect("declare");

        let outcome = registry.close_connection(connection);

        assert!(outcome.deleted_queues.is_empty());
        assert!(registry.contains(name.as_str()));
    }

    #[test]
    fn closing_a_connection_removes_its_consumers_from_shared_queues() {
        let mut registry = QueueRegistry::new();
        let first = ConnectionId::new(1);
        let second = ConnectionId::new(2);
        let name = registry
            .declare("shared", config(false, true), first)
            .expect("declare");

        registry
            .add_consumer(name.as_str(), first, ConsumerId::new(1))
            .expect("first consumer");
        registry
            .add_consumer(name.as_str(), second, ConsumerId::new(2))
            .expect("second consumer");

        registry.close_connection(first);
        assert_eq!(
            registry
                .queue(name.as_str())
                .expect("queue")
                .consumers()
                .collect::<Vec<_>>(),
            vec![ConsumerId::new(2)]
        );

        let outcome = registry.close_connection(second);
        assert_eq!(outcome.deleted_queues, vec![name]);
    }
}
