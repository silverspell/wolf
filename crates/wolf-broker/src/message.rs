//! Messages with reference-counted bodies shared across clones.

use std::sync::Arc;

use bytes::Bytes;

/// Protocol-neutral message metadata preserved for consumer delivery.
///
/// Exchange and routing key identify how the message was published. The
/// properties are the opaque encoded content-header property bytes, so the
/// broker never interprets wire-protocol property layouts. Metadata is shared
/// behind an [`Arc`] when messages are cloned into multiple queues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMetadata {
    /// The exchange the message was published to (empty for the default).
    pub exchange: String,
    /// The routing key used for the publish.
    pub routing_key: String,
    /// Opaque encoded content-header property bytes.
    pub properties: Bytes,
}

impl MessageMetadata {
    /// Creates message metadata from an exchange, routing key and properties.
    pub fn new(
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
        properties: Bytes,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            properties,
        }
    }
}

/// A broker message.
///
/// The body is a [`Bytes`] handle, so cloning a `Message` shares the same
/// allocation instead of copying the payload. Optional metadata is likewise
/// shared across clones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    body: Bytes,
    metadata: Option<Arc<MessageMetadata>>,
}

impl Message {
    pub fn new(body: Bytes) -> Self {
        Self {
            body,
            metadata: None,
        }
    }

    /// Creates a message carrying publish metadata for deliveries and gets.
    pub fn with_metadata(body: Bytes, metadata: MessageMetadata) -> Self {
        Self {
            body,
            metadata: Some(Arc::new(metadata)),
        }
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn into_body(self) -> Bytes {
        self.body
    }

    /// Returns the shared publish metadata, if the message has any.
    pub fn metadata(&self) -> Option<&MessageMetadata> {
        self.metadata.as_deref()
    }
}
