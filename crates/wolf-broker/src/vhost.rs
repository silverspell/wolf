//! Virtual hosts and their identifiers.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::binding::QueueName;
use crate::error::DomainError;
use crate::publish::{PublishOutcome, PublishRequest, publish};
use crate::queue::{ConnectionId, QueueRegistry};
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
}
