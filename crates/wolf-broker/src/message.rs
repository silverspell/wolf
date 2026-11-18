//! Messages with reference-counted bodies shared across clones.

use bytes::Bytes;

/// A broker message.
///
/// The body is a [`Bytes`] handle, so cloning a `Message` shares the same
/// allocation instead of copying the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    body: Bytes,
}

impl Message {
    pub fn new(body: Bytes) -> Self {
        Self { body }
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn into_body(self) -> Bytes {
        self.body
    }
}
