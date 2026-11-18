//! Connection-local mapping from broker consumer identifiers to wire identity.
//!
//! Queue deletions produce protocol-neutral [`ConsumerCancel`] values. The
//! connection needs the channel and consumer tag to turn one into a
//! `basic.cancel` frame; this directory holds that mapping. It will be
//! populated by the future `basic.consume` handler.

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
}
