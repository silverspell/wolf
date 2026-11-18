//! Exchanges, their configuration and the exchange registry.
//!
//! Routing itself lives in [`crate::router`]; this module owns exchange
//! declaration rules, the default exchange (`""`) and the predefined
//! `amq.direct`, `amq.fanout` and `amq.topic` exchanges.

use std::borrow::Borrow;
use std::collections::{BTreeMap, btree_map};
use std::fmt;
use std::sync::Arc;

use crate::binding::{Binding, BindingTable, QueueName};
use crate::error::DomainError;
use crate::topic::RoutingKey;

/// The name of an exchange. The empty name denotes the default exchange.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExchangeName(Arc<str>);

impl ExchangeName {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ExchangeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for ExchangeName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExchangeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ExchangeName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ExchangeName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&String> for ExchangeName {
    fn from(value: &String) -> Self {
        Self::new(value.as_str())
    }
}

/// The exchange types supported by this broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ExchangeKind {
    #[default]
    Direct,
    Fanout,
    Topic,
}

impl ExchangeKind {
    /// Parses the wire-level exchange type name.
    pub fn parse(value: &str) -> Result<Self, DomainError> {
        match value {
            "direct" => Ok(Self::Direct),
            "fanout" => Ok(Self::Fanout),
            "topic" => Ok(Self::Topic),
            "headers" => Err(DomainError::CommandInvalid {
                message: "exchange type 'headers' is not supported".to_string(),
            }),
            other => Err(DomainError::CommandInvalid {
                message: format!("unknown exchange type '{other}'"),
            }),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Fanout => "fanout",
            Self::Topic => "topic",
        }
    }
}

impl fmt::Display for ExchangeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A protocol-neutral exchange argument value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentValue {
    Bool(bool),
    Integer(i64),
    String(String),
    Bytes(Vec<u8>),
}

/// Protocol-neutral exchange arguments.
pub type ExchangeArguments = BTreeMap<String, ArgumentValue>;

/// The declaration properties of an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeConfig {
    pub kind: ExchangeKind,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: ExchangeArguments,
}

impl ExchangeConfig {
    pub fn new(kind: ExchangeKind) -> Self {
        Self {
            kind,
            ..Self::default()
        }
    }
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            kind: ExchangeKind::Direct,
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: ExchangeArguments::new(),
        }
    }
}

/// An exchange and the bindings that target queues.
#[derive(Debug, Clone)]
pub struct Exchange {
    name: ExchangeName,
    config: ExchangeConfig,
    bindings: BindingTable,
}

impl Exchange {
    pub fn new(name: ExchangeName, config: ExchangeConfig) -> Self {
        let bindings = BindingTable::new(config.kind);
        Self {
            name,
            config,
            bindings,
        }
    }

    pub fn name(&self) -> &ExchangeName {
        &self.name
    }

    pub fn config(&self) -> &ExchangeConfig {
        &self.config
    }

    pub fn bindings(&self) -> &BindingTable {
        &self.bindings
    }
}

/// The exchanges known to the broker, including the reserved predefined ones.
#[derive(Debug, Clone)]
pub struct ExchangeRegistry {
    exchanges: BTreeMap<ExchangeName, Exchange>,
}

impl ExchangeRegistry {
    /// Builds a registry containing the default exchange and the `amq.*`
    /// exchanges defined by the ticket.
    pub fn new() -> Self {
        let mut registry = Self {
            exchanges: BTreeMap::new(),
        };
        registry.install(ExchangeName::new(""), predefined(ExchangeKind::Direct));
        registry.install(
            ExchangeName::new("amq.direct"),
            predefined(ExchangeKind::Direct),
        );
        registry.install(
            ExchangeName::new("amq.fanout"),
            predefined(ExchangeKind::Fanout),
        );
        registry.install(
            ExchangeName::new("amq.topic"),
            predefined(ExchangeKind::Topic),
        );
        registry
    }

    fn install(&mut self, name: ExchangeName, config: ExchangeConfig) {
        let exchange = Exchange::new(name.clone(), config);
        self.exchanges.insert(name, exchange);
    }

    /// Declares an exchange, enforcing reserved-name and redeclaration rules.
    pub fn declare(
        &mut self,
        name: impl Into<ExchangeName>,
        config: ExchangeConfig,
    ) -> Result<(), DomainError> {
        let name = name.into();
        let raw = name.as_str();

        if raw.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "the default exchange cannot be declared".to_string(),
            });
        }
        if raw.starts_with("amq.") {
            return Err(DomainError::AccessRefused {
                message: format!("exchange name '{raw}' is reserved"),
            });
        }

        match self.exchanges.get(raw) {
            Some(existing) if existing.config == config => Ok(()),
            Some(_) => Err(DomainError::PreconditionFailed {
                message: format!("exchange '{raw}' already exists with different properties"),
            }),
            None => {
                self.install(name, config);
                Ok(())
            }
        }
    }

    /// Deletes an exchange. The default exchange cannot be deleted.
    pub fn delete(&mut self, name: impl AsRef<str>) -> Result<bool, DomainError> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "the default exchange cannot be deleted".to_string(),
            });
        }
        Ok(self.exchanges.remove(name).is_some())
    }

    /// Binds a queue to an exchange. The default exchange has implicit
    /// bindings and cannot be bound explicitly.
    pub fn bind(
        &mut self,
        source: impl AsRef<str>,
        queue: impl Into<QueueName>,
        routing_key: impl Into<RoutingKey>,
    ) -> Result<(), DomainError> {
        let source = source.as_ref();
        if source.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "bindings on the default exchange are implicit".to_string(),
            });
        }

        let queue = queue.into();
        let routing_key = routing_key.into();
        let exchange = self
            .exchanges
            .get_mut(source)
            .ok_or_else(|| exchange_not_found(source))?;
        let binding = Binding {
            source: exchange.name.clone(),
            queue,
            routing_key,
        };
        exchange.bindings.insert(binding);
        Ok(())
    }

    /// Removes a binding, returning `false` when it did not exist.
    pub fn unbind(
        &mut self,
        source: impl AsRef<str>,
        queue: impl Into<QueueName>,
        routing_key: impl Into<RoutingKey>,
    ) -> Result<bool, DomainError> {
        let source = source.as_ref();
        if source.is_empty() {
            return Err(DomainError::AccessRefused {
                message: "bindings on the default exchange are implicit".to_string(),
            });
        }

        let queue = queue.into();
        let routing_key = routing_key.into();
        let exchange = self
            .exchanges
            .get_mut(source)
            .ok_or_else(|| exchange_not_found(source))?;
        let binding = Binding {
            source: exchange.name.clone(),
            queue,
            routing_key,
        };
        Ok(exchange.bindings.remove(&binding))
    }

    pub fn exchange(&self, name: impl AsRef<str>) -> Result<&Exchange, DomainError> {
        let name = name.as_ref();
        self.exchanges
            .get(name)
            .ok_or_else(|| exchange_not_found(name))
    }

    pub fn contains(&self, name: impl AsRef<str>) -> bool {
        self.exchanges.contains_key(name.as_ref())
    }

    pub fn len(&self) -> usize {
        self.exchanges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.exchanges.is_empty()
    }

    pub fn iter(&self) -> btree_map::Iter<'_, ExchangeName, Exchange> {
        self.exchanges.iter()
    }
}

impl Default for ExchangeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn predefined(kind: ExchangeKind) -> ExchangeConfig {
    ExchangeConfig {
        durable: true,
        ..ExchangeConfig::new(kind)
    }
}

fn exchange_not_found(name: &str) -> DomainError {
    DomainError::ExchangeNotFound {
        vhost: String::new(),
        exchange: name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_seeds_the_predefined_exchanges() {
        let registry = ExchangeRegistry::new();
        assert_eq!(registry.len(), 4);
        assert_eq!(
            registry.exchange("").expect("default").config().kind,
            ExchangeKind::Direct
        );
        assert_eq!(
            registry
                .exchange("amq.direct")
                .expect("direct")
                .config()
                .kind,
            ExchangeKind::Direct
        );
        assert_eq!(
            registry
                .exchange("amq.fanout")
                .expect("fanout")
                .config()
                .kind,
            ExchangeKind::Fanout
        );
        assert_eq!(
            registry.exchange("amq.topic").expect("topic").config().kind,
            ExchangeKind::Topic
        );
    }

    #[test]
    fn reserved_names_are_access_refused() {
        let mut registry = ExchangeRegistry::new();
        for name in ["", "amq.direct", "amq.anything"] {
            let error = registry
                .declare(name, ExchangeConfig::new(ExchangeKind::Direct))
                .expect_err("reserved name");
            assert!(matches!(error, DomainError::AccessRefused { .. }));
        }

        assert!(matches!(
            registry.delete(""),
            Err(DomainError::AccessRefused { .. })
        ));
        assert!(matches!(
            registry.bind("", "q", "q"),
            Err(DomainError::AccessRefused { .. })
        ));
        assert!(matches!(
            registry.unbind("", "q", "q"),
            Err(DomainError::AccessRefused { .. })
        ));
    }

    #[test]
    fn redeclaration_requires_identical_properties() {
        let mut registry = ExchangeRegistry::new();
        let config = ExchangeConfig::new(ExchangeKind::Topic);
        registry.declare("events", config.clone()).expect("declare");
        registry
            .declare("events", config)
            .expect("idempotent redeclare");

        let error = registry
            .declare("events", ExchangeConfig::new(ExchangeKind::Direct))
            .expect_err("different kind");
        assert!(matches!(error, DomainError::PreconditionFailed { .. }));
    }

    #[test]
    fn exchange_type_parsing_rejects_unsupported_types() {
        assert_eq!(
            ExchangeKind::parse("direct").expect("direct"),
            ExchangeKind::Direct
        );
        for kind in ["headers", "x-custom", ""] {
            let error = ExchangeKind::parse(kind).expect_err("unsupported");
            assert!(matches!(error, DomainError::CommandInvalid { .. }));
        }
    }

    #[test]
    fn deletion_is_idempotent_and_drops_bindings() {
        let mut registry = ExchangeRegistry::new();
        assert!(!registry.delete("missing").expect("missing delete"));

        registry
            .declare("events", ExchangeConfig::new(ExchangeKind::Direct))
            .expect("declare");
        registry.bind("events", "q", "rk").expect("bind");
        assert!(registry.delete("events").expect("delete"));
        assert!(!registry.delete("events").expect("delete again"));
    }
}
