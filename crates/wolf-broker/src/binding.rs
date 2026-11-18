//! Bindings between exchanges and queues, with per-kind lookup indexes.
//!
//! A [`BindingTable`] belongs to one exchange. Direct bindings are indexed by
//! routing key, fanout bindings form a set of queues, and topic bindings are
//! stored in a [`TopicTrie`]. Insertion and removal are idempotent.

use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use crate::error::DomainError;
use crate::exchange::{ExchangeKind, ExchangeName};
use crate::topic::{RoutingKey, TopicPattern, TopicTrie};

/// A validated queue name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueName(Arc<str>);

impl QueueName {
    pub fn new(name: impl Into<Arc<str>>) -> Result<Self, DomainError> {
        let name = name.into();
        if name.is_empty() {
            return Err(DomainError::InvalidArgument {
                message: "queue name must not be empty".to_string(),
            });
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for QueueName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for QueueName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for QueueName {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<String> for QueueName {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

impl From<&String> for QueueName {
    fn from(value: &String) -> Self {
        Self(Arc::from(value.as_str()))
    }
}

/// A queue bound to an exchange with a routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub source: ExchangeName,
    pub queue: QueueName,
    pub routing_key: RoutingKey,
}

impl Binding {
    pub fn new(source: ExchangeName, queue: QueueName, routing_key: impl Into<RoutingKey>) -> Self {
        Self {
            source,
            queue,
            routing_key: routing_key.into(),
        }
    }
}

/// Indexes for the bindings of a single exchange.
#[derive(Debug, Clone)]
pub struct BindingTable {
    kind: ExchangeKind,
    direct: HashMap<RoutingKey, Vec<QueueName>>,
    fanout: BTreeSet<QueueName>,
    topic: TopicTrie,
}

impl BindingTable {
    pub fn new(kind: ExchangeKind) -> Self {
        Self {
            kind,
            direct: HashMap::new(),
            fanout: BTreeSet::new(),
            topic: TopicTrie::new(),
        }
    }

    pub fn kind(&self) -> ExchangeKind {
        self.kind
    }

    /// Inserts a binding, returning `false` when it already exists.
    pub fn insert(&mut self, binding: Binding) -> bool {
        match self.kind {
            ExchangeKind::Direct => match self.direct.get_mut(binding.routing_key.as_str()) {
                Some(queues) => {
                    if queues.contains(&binding.queue) {
                        false
                    } else {
                        queues.push(binding.queue);
                        true
                    }
                }
                None => {
                    self.direct.insert(binding.routing_key, vec![binding.queue]);
                    true
                }
            },
            ExchangeKind::Fanout => self.fanout.insert(binding.queue),
            ExchangeKind::Topic => self.topic.insert(
                &TopicPattern::new(binding.routing_key.as_str()),
                binding.queue,
            ),
        }
    }

    /// Removes a binding, returning `false` when it was not present.
    pub fn remove(&mut self, binding: &Binding) -> bool {
        match self.kind {
            ExchangeKind::Direct => {
                let key = binding.routing_key.as_str();
                let mut remove_key = false;
                let removed = match self.direct.get_mut(key) {
                    Some(queues) => {
                        let before = queues.len();
                        queues.retain(|queue| queue != &binding.queue);
                        remove_key = queues.is_empty();
                        queues.len() != before
                    }
                    None => false,
                };
                if remove_key {
                    self.direct.remove(key);
                }
                removed
            }
            ExchangeKind::Fanout => self.fanout.remove(&binding.queue),
            ExchangeKind::Topic => self.topic.remove(
                &TopicPattern::new(binding.routing_key.as_str()),
                &binding.queue,
            ),
        }
    }

    pub fn route_into(&self, routing_key: &str, out: &mut Vec<QueueName>) {
        match self.kind {
            ExchangeKind::Direct => self.route_direct(routing_key, out),
            ExchangeKind::Fanout => self.route_fanout(out),
            ExchangeKind::Topic => self.route_topic(routing_key, out),
        }
    }

    pub fn route_direct(&self, routing_key: &str, out: &mut Vec<QueueName>) {
        if let Some(queues) = self.direct.get(routing_key) {
            for queue in queues {
                push_unique(out, queue.clone());
            }
        }
    }

    pub fn route_fanout(&self, out: &mut Vec<QueueName>) {
        for queue in &self.fanout {
            push_unique(out, queue.clone());
        }
    }

    pub fn route_topic(&self, routing_key: &str, out: &mut Vec<QueueName>) {
        self.topic.match_into(routing_key, out);
    }

    pub fn len(&self) -> usize {
        match self.kind {
            ExchangeKind::Direct => self.direct.values().map(Vec::len).sum(),
            ExchangeKind::Fanout => self.fanout.len(),
            ExchangeKind::Topic => self.topic.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(crate) fn push_unique(out: &mut Vec<QueueName>, queue: QueueName) {
    if !out.contains(&queue) {
        out.push(queue);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(name: &str) -> QueueName {
        QueueName::new(name).expect("valid queue")
    }

    fn binding(kind: ExchangeKind, queue_name: &str, routing_key: &str) -> Binding {
        Binding::new(
            ExchangeName::new(match kind {
                ExchangeKind::Direct => "direct",
                ExchangeKind::Fanout => "fanout",
                ExchangeKind::Topic => "topic",
            }),
            queue(queue_name),
            routing_key,
        )
    }

    #[test]
    fn direct_bindings_are_idempotent_and_removable() {
        let mut table = BindingTable::new(ExchangeKind::Direct);
        assert!(table.insert(binding(ExchangeKind::Direct, "q", "rk")));
        assert!(!table.insert(binding(ExchangeKind::Direct, "q", "rk")));
        assert_eq!(table.len(), 1);

        let mut out = Vec::new();
        table.route_into("rk", &mut out);
        assert_eq!(out, vec![queue("q")]);

        assert!(table.remove(&binding(ExchangeKind::Direct, "q", "rk")));
        assert!(!table.remove(&binding(ExchangeKind::Direct, "q", "rk")));
        assert!(table.is_empty());
    }

    #[test]
    fn fanout_ignores_routing_keys_and_deduplicates_queues() {
        let mut table = BindingTable::new(ExchangeKind::Fanout);
        assert!(table.insert(binding(ExchangeKind::Fanout, "q", "one")));
        assert!(!table.insert(binding(ExchangeKind::Fanout, "q", "two")));
        assert!(table.insert(binding(ExchangeKind::Fanout, "r", "three")));

        let mut out = Vec::new();
        table.route_into("anything", &mut out);
        assert_eq!(out, vec![queue("q"), queue("r")]);
    }

    #[test]
    fn topic_bindings_use_parsed_patterns() {
        let mut table = BindingTable::new(ExchangeKind::Topic);
        assert!(table.insert(binding(ExchangeKind::Topic, "q", "a.*")));
        assert!(!table.insert(binding(ExchangeKind::Topic, "q", "a.*")));
        assert_eq!(table.len(), 1);

        let mut out = Vec::new();
        table.route_into("a.b", &mut out);
        assert_eq!(out, vec![queue("q")]);

        assert!(table.remove(&binding(ExchangeKind::Topic, "q", "a.*")));
        assert!(table.is_empty());
    }
}
