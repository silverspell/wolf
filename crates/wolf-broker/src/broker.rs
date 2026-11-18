//! In-memory broker skeleton: virtual host registry and lookups.

use std::collections::BTreeMap;

use crate::error::DomainError;
use crate::publish::{PublishOutcome, PublishRequest};
use crate::queue::ConnectionId;
use crate::vhost::{Vhost, VhostName};

/// The broker domain root.
///
/// Each virtual host owns its exchanges, bindings, queues and routing state.
#[derive(Debug, Default)]
pub struct Broker {
    vhosts: BTreeMap<VhostName, Vhost>,
}

impl Broker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_vhost(&mut self, vhost: Vhost) -> Result<(), DomainError> {
        let name = vhost.name().clone();
        if self.vhosts.contains_key(&name) {
            return Err(DomainError::AlreadyExists {
                kind: crate::error::ResourceKind::Vhost,
                name: name.as_str().to_string(),
            });
        }
        self.vhosts.insert(name, vhost);
        Ok(())
    }

    pub fn vhost(&self, name: &VhostName) -> Result<&Vhost, DomainError> {
        self.vhosts
            .get(name)
            .ok_or_else(|| DomainError::VhostNotFound {
                vhost: name.as_str().to_string(),
            })
    }

    /// Publishes a message through the named virtual host.
    pub async fn publish<P>(
        &self,
        vhost: &VhostName,
        connection: ConnectionId,
        request: PublishRequest<'_, P>,
    ) -> Result<PublishOutcome<P>, DomainError> {
        self.vhost(vhost)?.publish(connection, request).await
    }

    /// Verifies the virtual host and then reports that the queue is unknown.
    ///
    /// There is no queue registry yet, so any existing vhost yields
    /// [`DomainError::QueueNotFound`]; this is an intentional skeleton.
    pub fn lookup_queue(&self, vhost: &VhostName, queue: &str) -> Result<(), DomainError> {
        self.vhost(vhost)?;
        Err(DomainError::QueueNotFound {
            vhost: vhost.as_str().to_string(),
            queue: queue.to_string(),
        })
    }
}
