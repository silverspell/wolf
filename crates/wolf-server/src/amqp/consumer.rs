//! Connection-local mapping from broker consumer identifiers to wire identity.
//!
//! Queue deletions produce protocol-neutral [`ConsumerCancel`] values. The
//! connection needs the channel and consumer tag to turn one into a
//! `basic.cancel` frame; this directory holds that mapping. `basic.consume`
//! populates it and `basic.cancel`, channel close and connection close drain it.

use std::collections::BTreeMap;

use wolf_amqp_codec::ShortString;
use wolf_broker::{ConsumerCancel, ConsumerId};

/// The channel and consumer tag registered for one broker consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerEntry {
    /// Channel that owns the consumer.
    pub channel: u16,
    /// Consumer tag advertised to the client.
    pub tag: ShortString,
}

/// All consumers registered on one connection.
#[derive(Debug, Clone, Default)]
pub struct ConsumerDirectory {
    entries: BTreeMap<ConsumerId, ConsumerEntry>,
}

impl ConsumerDirectory {
    /// Creates an empty consumer directory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a consumer, returning the entry it replaced.
    pub fn register(
        &mut self,
        consumer: ConsumerId,
        channel: u16,
        tag: ShortString,
    ) -> Option<ConsumerEntry> {
        self.entries
            .insert(consumer, ConsumerEntry { channel, tag })
    }

    /// Resolves a cancellation into the registered wire identity.
    pub fn resolve(&self, cancel: &ConsumerCancel) -> Option<&ConsumerEntry> {
        self.entries.get(&cancel.consumer)
    }

    /// Removes and returns a consumer registration.
    pub fn remove(&mut self, consumer: ConsumerId) -> Option<ConsumerEntry> {
        self.entries.remove(&consumer)
    }

    /// Looks up a consumer by its channel and client-visible tag.
    pub fn find_by_tag(&self, channel: u16, tag: &ShortString) -> Option<ConsumerId> {
        self.entries
            .iter()
            .find(|(_, entry)| entry.channel == channel && &entry.tag == tag)
            .map(|(consumer, _)| *consumer)
    }

    /// Returns whether a channel already uses a consumer tag.
    pub fn contains_tag(&self, channel: u16, tag: &ShortString) -> bool {
        self.find_by_tag(channel, tag).is_some()
    }

    /// Removes a consumer by its channel and tag.
    pub fn remove_by_tag(
        &mut self,
        channel: u16,
        tag: &ShortString,
    ) -> Option<(ConsumerId, ConsumerEntry)> {
        let consumer = self.find_by_tag(channel, tag)?;
        self.entries
            .remove(&consumer)
            .map(|entry| (consumer, entry))
    }

    /// Removes every consumer registered on a channel.
    pub fn remove_channel(&mut self, channel: u16) -> Vec<(ConsumerId, ConsumerEntry)> {
        let consumers: Vec<ConsumerId> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.channel == channel)
            .map(|(consumer, _)| *consumer)
            .collect();
        consumers
            .into_iter()
            .filter_map(|consumer| {
                self.entries
                    .remove(&consumer)
                    .map(|entry| (consumer, entry))
            })
            .collect()
    }

    /// Removes all consumer registrations, returning them.
    pub fn drain(&mut self) -> Vec<(ConsumerId, ConsumerEntry)> {
        std::mem::take(&mut self.entries).into_iter().collect()
    }

    /// Returns the registration for a consumer.
    pub fn get(&self, consumer: ConsumerId) -> Option<&ConsumerEntry> {
        self.entries.get(&consumer)
    }

    /// Returns the number of registered consumers.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no consumers are registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use wolf_broker::CancelReason;

    use super::*;

    #[test]
    fn registering_and_removing_consumers_preserves_wire_identity() {
        let mut directory = ConsumerDirectory::new();
        let consumer = ConsumerId::new(7);

        assert!(
            directory
                .register(consumer, 3, ShortString::new("tag"))
                .is_none()
        );
        assert_eq!(directory.len(), 1);
        assert_eq!(
            directory.get(consumer),
            Some(&ConsumerEntry {
                channel: 3,
                tag: ShortString::new("tag"),
            })
        );
        assert_eq!(
            directory
                .resolve(&ConsumerCancel {
                    consumer,
                    reason: CancelReason::QueueDeleted,
                })
                .map(|entry| entry.channel),
            Some(3)
        );

        let removed = directory.remove(consumer).expect("registered");
        assert_eq!(removed.tag.as_str(), "tag");
        assert!(directory.is_empty());
    }

    #[test]
    fn tags_are_scoped_to_a_channel() {
        let mut directory = ConsumerDirectory::new();
        directory.register(ConsumerId::new(1), 1, ShortString::new("tag"));
        directory.register(ConsumerId::new(2), 2, ShortString::new("tag"));

        assert_eq!(
            directory.find_by_tag(1, &ShortString::new("tag")),
            Some(ConsumerId::new(1))
        );
        assert_eq!(
            directory.find_by_tag(2, &ShortString::new("tag")),
            Some(ConsumerId::new(2))
        );
        assert!(directory.contains_tag(1, &ShortString::new("tag")));
        assert!(!directory.contains_tag(3, &ShortString::new("tag")));

        let (consumer, entry) = directory
            .remove_by_tag(1, &ShortString::new("tag"))
            .expect("removed");
        assert_eq!(consumer, ConsumerId::new(1));
        assert_eq!(entry.channel, 1);
        assert_eq!(directory.len(), 1);
    }

    #[test]
    fn removing_a_channel_or_draining_returns_every_entry() {
        let mut directory = ConsumerDirectory::new();
        directory.register(ConsumerId::new(1), 1, ShortString::new("a"));
        directory.register(ConsumerId::new(2), 1, ShortString::new("b"));
        directory.register(ConsumerId::new(3), 2, ShortString::new("c"));

        let removed = directory.remove_channel(1);
        assert_eq!(removed.len(), 2);
        assert_eq!(directory.len(), 1);

        let drained = directory.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, ConsumerId::new(3));
        assert!(directory.is_empty());
    }
}
