//! Virtual hosts and their identifiers.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::binding::QueueName;
use crate::error::DomainError;
use crate::exchange::ExchangeConfig;
use crate::publish::{PublishOutcome, PublishRequest, publish};
use crate::queue::{ConnectionId, QueueConfig, QueueDeleteOutcome, QueueRegistry};
use crate::router::Router;

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

/// Protocol-neutral routing state owned by one virtual host.
#[derive(Debug)]
struct VhostState {
    router: Router,
    queues: QueueRegistry,
    destinations: Vec<QueueName>,
}

/// A virtual host: identity, name and its routing/queue state.
#[derive(Debug)]
pub struct Vhost {
    id: VhostId,
    name: VhostName,
    state: Mutex<VhostState>,
}

impl Vhost {
    pub fn new(id: VhostId, name: VhostName) -> Self {
        Self::with_routing(id, name, Router::default(), QueueRegistry::new())
    }

    /// Creates a virtual host with caller-supplied routing state.
    pub fn with_routing(
        id: VhostId,
        name: VhostName,
        router: Router,
        queues: QueueRegistry,
    ) -> Self {
        Self {
            id,
            name,
            state: Mutex::new(VhostState {
                router,
                queues,
                destinations: Vec::new(),
            }),
        }
    }

    pub fn id(&self) -> VhostId {
        self.id
    }

    pub fn name(&self) -> &VhostName {
        &self.name
    }

    /// Publishes a message using this virtual host's routing state.
    pub fn publish<P>(
        &self,
        connection: ConnectionId,
        request: PublishRequest<'_, P>,
    ) -> Result<PublishOutcome<P>, DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;
        let VhostState {
            router,
            queues,
            destinations,
        } = &mut *state;
        publish(router, queues, destinations, connection, request)
    }

    /// Returns the number of messages currently stored in `queue`.
    pub fn queue_message_count(&self, queue: impl AsRef<str>) -> usize {
        let queue = queue.as_ref();
        self.state
            .lock()
            .ok()
            .and_then(|state| state.queues.queue(queue).map(|queue| queue.message_count()))
            .unwrap_or(0)
    }

    /// Declares an exchange, enforcing reserved names and redeclaration rules.
    pub fn declare_exchange(
        &self,
        name: impl AsRef<str>,
        config: ExchangeConfig,
    ) -> Result<(), DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;
        state.router.registry_mut().declare(name.as_ref(), config)
    }

    /// Looks up an exchange for a passive declaration.
    pub fn declare_exchange_passive(&self, name: impl AsRef<str>) -> Result<(), DomainError> {
        let state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;
        state.router.registry().exchange(name.as_ref()).map(|_| ())
    }

    /// Deletes an exchange, returning whether it existed.
    ///
    /// The default and `amq.*` exchanges are reserved and cannot be deleted.
    /// With `if_unused`, an exchange that still has bindings is rejected.
    pub fn delete_exchange(
        &self,
        name: impl AsRef<str>,
        if_unused: bool,
    ) -> Result<bool, DomainError> {
        let name = name.as_ref();
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;

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

        let registry = state.router.registry_mut();
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
    pub fn bind_queue(
        &self,
        exchange: impl AsRef<str>,
        queue: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<(), DomainError> {
        let exchange = exchange.as_ref();
        let queue = queue.as_ref();
        let routing_key = routing_key.as_ref();
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;

        if exchange.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "bindings on the default exchange are implicit".to_string(),
            });
        }

        state.queues.queue_for(queue, connection)?;
        state
            .router
            .registry_mut()
            .bind(exchange, queue, routing_key)
    }

    /// Removes a queue binding, returning whether it existed.
    ///
    /// Like [`Self::bind_queue`], the default exchange is rejected first.
    pub fn unbind_queue(
        &self,
        exchange: impl AsRef<str>,
        queue: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<bool, DomainError> {
        let exchange = exchange.as_ref();
        let queue = queue.as_ref();
        let routing_key = routing_key.as_ref();
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;

        if exchange.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "bindings on the default exchange are implicit".to_string(),
            });
        }

        state.queues.queue_for(queue, connection)?;
        state
            .router
            .registry_mut()
            .unbind(exchange, queue, routing_key)
    }

    /// Declares a queue and returns its effective name and current counts.
    pub fn declare_queue(
        &self,
        name: impl AsRef<str>,
        config: QueueConfig,
        connection: ConnectionId,
    ) -> Result<QueueDeclaration, DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;
        let name = state.queues.declare(name, config, connection)?;
        let queue = state
            .queues
            .queue(name.as_str())
            .expect("a declared queue is registered");
        Ok(QueueDeclaration {
            name: name.clone(),
            message_count: count_to_u32(queue.message_count()),
            consumer_count: count_to_u32(queue.consumer_count()),
        })
    }

    /// Looks up a queue for a passive declaration and returns its counts.
    pub fn declare_queue_passive(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<QueueDeclaration, DomainError> {
        let state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;
        let queue = state.queues.declare_passive(name, connection)?;
        Ok(QueueDeclaration {
            name: queue.name().clone(),
            message_count: count_to_u32(queue.message_count()),
            consumer_count: count_to_u32(queue.consumer_count()),
        })
    }

    /// Removes all messages from a queue, returning how many were discarded.
    pub fn purge_queue(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
    ) -> Result<u32, DomainError> {
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;
        state.queues.purge(name, connection).map(count_to_u32)
    }

    /// Deletes a queue, enforcing the `if_unused` and `if_empty` conditions.
    ///
    /// Deleting a missing queue is idempotent and returns an empty outcome.
    pub fn delete_queue(
        &self,
        name: impl AsRef<str>,
        connection: ConnectionId,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<QueueDeleteOutcome, DomainError> {
        let name = name.as_ref();
        let mut state = self.state.lock().map_err(|_| DomainError::BrokerClosed)?;

        if !state.queues.contains(name) {
            return Ok(QueueDeleteOutcome::default());
        }

        {
            let queue = state.queues.queue_for(name, connection)?;
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
        }

        state.queues.delete(name, connection)
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

    #[test]
    fn passive_exchange_declare_requires_the_exchange_to_exist() {
        let vhost = vhost();
        let error = vhost
            .declare_exchange_passive("missing")
            .expect_err("missing exchange");
        assert!(matches!(error, DomainError::ExchangeNotFound { .. }));

        vhost
            .declare_exchange("events", ExchangeConfig::new(ExchangeKind::Topic))
            .expect("declare");
        vhost
            .declare_exchange_passive("events")
            .expect("passive declare ignores the type");
    }

    #[test]
    fn exchange_delete_checks_reserved_names_and_if_unused() {
        let vhost = vhost();
        for name in ["", "amq.direct"] {
            let error = vhost
                .delete_exchange(name, false)
                .expect_err("reserved exchange");
            assert!(matches!(error, DomainError::AccessRefused { .. }));
        }

        vhost
            .declare_exchange("bound", ExchangeConfig::new(ExchangeKind::Direct))
            .expect("declare exchange");
        vhost
            .declare_queue("q", QueueConfig::default(), ConnectionId::new(1))
            .expect("declare queue");
        vhost
            .bind_queue("bound", "q", "rk", ConnectionId::new(1))
            .expect("bind");

        let error = vhost
            .delete_exchange("bound", true)
            .expect_err("bound exchange");
        assert!(matches!(error, DomainError::PreconditionFailed { .. }));
        assert!(vhost.delete_exchange("bound", false).expect("delete"));
        assert!(!vhost.delete_exchange("bound", false).expect("idempotent"));
    }

    #[test]
    fn default_exchange_bind_is_rejected_before_queue_lookup() {
        let vhost = vhost();
        let error = vhost
            .bind_queue("", "missing", "rk", ConnectionId::new(1))
            .expect_err("default exchange bind");
        assert!(matches!(error, DomainError::AccessRefused { .. }));
    }

    #[test]
    fn queue_delete_is_idempotent_and_enforces_empty_and_unused() {
        let vhost = vhost();
        let connection = ConnectionId::new(1);
        let missing = vhost
            .delete_queue("missing", connection, true, true)
            .expect("missing delete is idempotent");
        assert_eq!(missing, QueueDeleteOutcome::default());

        vhost
            .declare_queue("q", QueueConfig::default(), connection)
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
            .expect("publish");

        let error = vhost
            .delete_queue("q", connection, false, true)
            .expect_err("non-empty queue");
        assert!(matches!(error, DomainError::PreconditionFailed { .. }));

        let outcome = vhost
            .delete_queue("q", connection, true, false)
            .expect("delete");
        assert_eq!(outcome.messages, 1);
    }
}
