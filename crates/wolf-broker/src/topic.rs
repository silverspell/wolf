//! Topic routing keys, binding patterns and a trie matcher.
//!
//! Topic patterns are tokenized when a binding is created, so publishing only
//! walks the trie and never allocates per message. `*` matches exactly one
//! segment and `#` matches zero or more segments. Empty segments produced by
//! consecutive dots are significant, matching RabbitMQ's topic semantics.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::binding::QueueName;

/// A message routing key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoutingKey(Arc<str>);

impl RoutingKey {
    pub fn new(key: impl Into<Arc<str>>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RoutingKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for RoutingKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for RoutingKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for RoutingKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&String> for RoutingKey {
    fn from(value: &String) -> Self {
        Self::new(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TopicToken {
    Literal(Arc<str>),
    Star,
    Hash,
}

/// A parsed topic binding pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPattern {
    raw: Arc<str>,
    tokens: Vec<TopicToken>,
}

impl TopicPattern {
    pub fn new(pattern: impl Into<Arc<str>>) -> Self {
        let raw = pattern.into();
        let tokens = tokenize(&raw);
        Self { raw, tokens }
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Matches a routing key using a direct, token-by-token reference matcher.
    ///
    /// The trie is used on the routing hot path; this helper is useful for
    /// small checks and as the reference implementation in tests.
    pub fn matches(&self, routing_key: &str) -> bool {
        let segments: Vec<&str> = routing_key.split('.').collect();
        matches_tokens(&self.tokens, &segments)
    }
}

impl AsRef<str> for TopicPattern {
    fn as_ref(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for TopicPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl From<&str> for TopicPattern {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TopicPattern {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&String> for TopicPattern {
    fn from(value: &String) -> Self {
        Self::new(value.as_str())
    }
}

fn tokenize(pattern: &str) -> Vec<TopicToken> {
    pattern
        .split('.')
        .map(|segment| match segment {
            "*" => TopicToken::Star,
            "#" => TopicToken::Hash,
            literal => TopicToken::Literal(Arc::from(literal)),
        })
        .collect()
}

fn matches_tokens(tokens: &[TopicToken], segments: &[&str]) -> bool {
    match tokens.first() {
        None => segments.is_empty(),
        Some(TopicToken::Hash) => {
            matches_tokens(&tokens[1..], segments)
                || (!segments.is_empty() && matches_tokens(tokens, &segments[1..]))
        }
        Some(TopicToken::Star) => {
            !segments.is_empty() && matches_tokens(&tokens[1..], &segments[1..])
        }
        Some(TopicToken::Literal(literal)) => {
            !segments.is_empty()
                && literal.as_ref() == segments[0]
                && matches_tokens(&tokens[1..], &segments[1..])
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Node {
    literals: HashMap<Arc<str>, Node>,
    star: Option<Box<Node>>,
    hash: Option<Box<Node>>,
    destinations: Vec<QueueName>,
}

impl Node {
    fn insert(&mut self, tokens: &[TopicToken], queue: QueueName) -> bool {
        let mut node = self;
        for token in tokens {
            node = match token {
                TopicToken::Literal(literal) => node.literals.entry(literal.clone()).or_default(),
                TopicToken::Star => node.star.get_or_insert_with(|| Box::new(Self::default())),
                TopicToken::Hash => node.hash.get_or_insert_with(|| Box::new(Self::default())),
            };
        }
        if node.destinations.contains(&queue) {
            false
        } else {
            node.destinations.push(queue);
            true
        }
    }

    fn remove(&mut self, tokens: &[TopicToken], queue: &QueueName) -> bool {
        let mut node = self;
        for token in tokens {
            node = match token {
                TopicToken::Literal(literal) => match node.literals.get_mut(literal) {
                    Some(child) => child,
                    None => return false,
                },
                TopicToken::Star => match node.star.as_deref_mut() {
                    Some(child) => child,
                    None => return false,
                },
                TopicToken::Hash => match node.hash.as_deref_mut() {
                    Some(child) => child,
                    None => return false,
                },
            };
        }
        match node.destinations.iter().position(|queued| queued == queue) {
            Some(index) => {
                node.destinations.remove(index);
                true
            }
            None => false,
        }
    }

    fn match_segments(&self, segments: &mut std::str::Split<'_, char>, out: &mut Vec<QueueName>) {
        if let Some(hash) = &self.hash {
            let mut remaining = segments.clone();
            loop {
                let mut rest = remaining.clone();
                hash.match_segments(&mut rest, out);
                if remaining.next().is_none() {
                    break;
                }
            }
        }

        match segments.next() {
            Some(segment) => {
                if let Some(child) = self.literals.get(segment) {
                    let mut rest = segments.clone();
                    child.match_segments(&mut rest, out);
                }
                if let Some(star) = &self.star {
                    let mut rest = segments.clone();
                    star.match_segments(&mut rest, out);
                }
            }
            None => {
                for queue in &self.destinations {
                    push_unique(out, queue.clone());
                }
            }
        }
    }
}

fn push_unique(out: &mut Vec<QueueName>, queue: QueueName) {
    if !out.contains(&queue) {
        out.push(queue);
    }
}

/// A trie indexing topic bindings by their parsed patterns.
#[derive(Debug, Clone, Default)]
pub struct TopicTrie {
    root: Node,
    len: usize,
}

impl TopicTrie {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a binding, returning `false` when it already exists.
    pub fn insert(&mut self, pattern: &TopicPattern, queue: QueueName) -> bool {
        if self.root.insert(&pattern.tokens, queue) {
            self.len += 1;
            true
        } else {
            false
        }
    }

    /// Removes a binding, returning `false` when it was not present.
    pub fn remove(&mut self, pattern: &TopicPattern, queue: &QueueName) -> bool {
        if self.root.remove(&pattern.tokens, queue) {
            self.len -= 1;
            true
        } else {
            false
        }
    }

    /// Collects every queue matching `routing_key`, without duplicates.
    pub fn match_into(&self, routing_key: &str, out: &mut Vec<QueueName>) {
        let mut segments = routing_key.split('.');
        self.root.match_segments(&mut segments, out);
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(name: &str) -> QueueName {
        QueueName::from(name)
    }

    fn matched(trie: &TopicTrie, routing_key: &str) -> Vec<String> {
        let mut out = Vec::new();
        trie.match_into(routing_key, &mut out);
        let mut names: Vec<String> = out
            .into_iter()
            .map(|queue| queue.as_str().to_string())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn star_matches_exactly_one_segment() {
        let pattern = TopicPattern::new("stock.*.price");
        assert!(pattern.matches("stock.usd.price"));
        assert!(!pattern.matches("stock.price"));
        assert!(!pattern.matches("stock.usd.eur.price"));
    }

    #[test]
    fn hash_matches_zero_or_more_segments() {
        let pattern = TopicPattern::new("a.#");
        assert!(pattern.matches("a"));
        assert!(pattern.matches("a.b"));
        assert!(pattern.matches("a.b.c"));

        let middle = TopicPattern::new("a.#.z");
        assert!(middle.matches("a.z"));
        assert!(middle.matches("a.b.c.z"));
        assert!(!middle.matches("a.b.c"));
    }

    #[test]
    fn multiple_hashes_and_empty_segments() {
        assert!(TopicPattern::new("#.#").matches(""));
        assert!(TopicPattern::new("#.#").matches("a.b.c"));
        assert!(TopicPattern::new("a..b").matches("a..b"));
        assert!(!TopicPattern::new("a..b").matches("a.b"));
        assert!(TopicPattern::new("").matches(""));
        assert!(!TopicPattern::new("").matches("a"));
    }

    #[test]
    fn trie_agrees_with_reference_matcher() {
        let patterns = [
            "*", "#", "#.#", "a.*", "a.#", "a.#.b", "a..b", "", "A", "a.*.#",
        ];
        let keys = [
            "", "a", "a.b", "a.b.c", "a..b", "A", "a.x.y.b", "a.x.b.z", "b.a",
        ];

        for pattern in patterns {
            let parsed = TopicPattern::new(pattern);
            let mut trie = TopicTrie::new();
            trie.insert(&parsed, queue("q"));

            for key in keys {
                let expected = parsed.matches(key);
                let mut out = Vec::new();
                trie.match_into(key, &mut out);
                assert_eq!(
                    !out.is_empty(),
                    expected,
                    "pattern {pattern:?} against {key:?}"
                );
            }
        }
    }

    #[test]
    fn trie_deduplicates_queues_across_patterns() {
        let mut trie = TopicTrie::new();
        trie.insert(&TopicPattern::new("a.*"), queue("q"));
        trie.insert(&TopicPattern::new("a.#"), queue("q"));
        trie.insert(&TopicPattern::new("#"), queue("q"));

        assert_eq!(matched(&trie, "a.b"), vec!["q".to_string()]);
        assert_eq!(trie.len(), 3);
    }

    #[test]
    fn trie_removes_bindings() {
        let mut trie = TopicTrie::new();
        let pattern = TopicPattern::new("a.#");
        trie.insert(&pattern, queue("q"));
        assert!(trie.remove(&pattern, &queue("q")));
        assert!(!trie.remove(&pattern, &queue("q")));
        assert!(trie.is_empty());
        assert!(matched(&trie, "a.b").is_empty());
    }
}
