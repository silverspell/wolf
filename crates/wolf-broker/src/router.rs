//! Message routing to destination queues.
//!
//! Routing is protocol-neutral and returns only queue names; it never touches
//! message bodies. The caller reuses its destination buffer via
//! [`Router::route_into`], so publishing does not allocate per message.

use crate::binding::{QueueName, push_unique};
use crate::error::DomainError;
use crate::exchange::{ExchangeKind, ExchangeRegistry};

/// Routes routing keys through the exchange registry.
#[derive(Debug, Clone)]
pub struct Router {
    registry: ExchangeRegistry,
}

impl Router {
    pub fn new(registry: ExchangeRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &ExchangeRegistry {
        &self.registry
    }

    pub fn registry_mut(&mut self) -> &mut ExchangeRegistry {
        &mut self.registry
    }

    /// Returns the queues a routing key targets, if the exchange exists.
    pub fn route(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
    ) -> Result<Vec<QueueName>, DomainError> {
        let mut destinations = Vec::new();
        self.route_into(exchange, routing_key, &mut destinations)?;
        Ok(destinations)
    }

    /// Appends destinations to `out`, preserving its existing capacity.
    ///
    /// The default exchange routes implicitly: its routing key is the queue
    /// name. No queue registry is consulted yet, so the name is returned as-is.
    pub fn route_into(
        &self,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        out: &mut Vec<QueueName>,
    ) -> Result<(), DomainError> {
        let exchange = exchange.as_ref();
        let routing_key = routing_key.as_ref();
        if exchange.is_empty() {
            if !routing_key.is_empty() {
                push_unique(out, QueueName::from(routing_key));
            }
            return Ok(());
        }

        let exchange = self.registry.exchange(exchange)?;
        let bindings = exchange.bindings();
        match exchange.config().kind {
            ExchangeKind::Direct => bindings.route_direct(routing_key, out),
            ExchangeKind::Fanout => bindings.route_fanout(out),
            ExchangeKind::Topic => bindings.route_topic(routing_key, out),
        }
        Ok(())
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new(ExchangeRegistry::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{ExchangeConfig, ExchangeKind};

    fn queue(name: &str) -> QueueName {
        QueueName::new(name).expect("valid queue")
    }

    #[test]
    fn default_exchange_routes_to_the_routing_key_queue() {
        let router = Router::default();
        assert_eq!(
            router.route("", "jobs").expect("route"),
            vec![queue("jobs")]
        );
        assert!(router.route("", "").expect("empty route").is_empty());
    }

    #[test]
    fn route_into_reuses_the_destination_buffer() {
        let mut registry = ExchangeRegistry::new();
        registry
            .declare("broadcast", ExchangeConfig::new(ExchangeKind::Fanout))
            .expect("declare");
        registry.bind("broadcast", "q", "").expect("bind");

        let router = Router::new(registry);
        let mut out = Vec::with_capacity(8);
        let capacity = out.capacity();
        router
            .route_into("broadcast", "ignored", &mut out)
            .expect("route");
        assert_eq!(out, vec![queue("q")]);
        assert_eq!(out.capacity(), capacity);
    }

    #[test]
    fn unknown_exchange_is_reported() {
        let router = Router::default();
        let error = router.route("missing", "x").expect_err("missing exchange");
        assert_eq!(
            error,
            DomainError::ExchangeNotFound {
                vhost: String::new(),
                exchange: "missing".to_string(),
            }
        );
    }
}
