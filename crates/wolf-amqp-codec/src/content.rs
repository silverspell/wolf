//! AMQP content body splitting, reassembly and content-header conversion.
//!
//! A message body may span several content-body frames. Splitting shares the
//! original [`Bytes`] allocation so no body bytes are copied per chunk, and
//! reassembly validates the total size against the content header's
//! `body_size`.

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::frame::{ContentHeader, FRAME_OVERHEAD, MAX_FRAME_SIZE};
use crate::methods::basic::CLASS_ID;
use crate::properties::BasicProperties;

/// Returns the maximum body payload that fits in one frame for `frame_max`.
///
/// `frame_max` is the total negotiated frame size including [`FRAME_OVERHEAD`].
/// A value of zero means no negotiated limit, so [`MAX_FRAME_SIZE`] is used.
pub fn content_body_payload_max(frame_max: u32) -> Result<usize> {
    if frame_max == 0 {
        return Ok(MAX_FRAME_SIZE);
    }
    let total = frame_max as usize;
    if total <= FRAME_OVERHEAD {
        return Err(Error::FrameMaxTooSmall { frame_max });
    }
    Ok((total - FRAME_OVERHEAD).min(MAX_FRAME_SIZE))
}

/// Splits a message body into zero-copy content-body frame payloads.
#[derive(Debug, Clone)]
pub struct ContentBodySplitter {
    body: Bytes,
    offset: usize,
    max_payload: usize,
}

impl ContentBodySplitter {
    /// Creates a splitter using the payload limit implied by `frame_max`.
    pub fn new(body: Bytes, frame_max: u32) -> Result<Self> {
        Ok(Self {
            body,
            offset: 0,
            max_payload: content_body_payload_max(frame_max)?,
        })
    }

    /// Returns the maximum payload size of each produced chunk.
    pub fn max_payload(&self) -> usize {
        self.max_payload
    }
}

impl Iterator for ContentBodySplitter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.body.len() {
            return None;
        }
        let end = (self.offset + self.max_payload).min(self.body.len());
        let chunk = self.body.slice(self.offset..end);
        self.offset = end;
        Some(chunk)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.body.len() - self.offset;
        let chunks = remaining.div_ceil(self.max_payload);
        (chunks, Some(chunks))
    }
}

/// Splits `body` into zero-copy chunks that fit the negotiated `frame_max`.
pub fn split_content_body(body: Bytes, frame_max: u32) -> Result<ContentBodySplitter> {
    ContentBodySplitter::new(body, frame_max)
}

/// Reassembles content-body chunks and validates the declared body size.
#[derive(Debug)]
pub struct ContentBodyAssembler {
    expected: u64,
    received: u64,
    chunks: Vec<Bytes>,
}

impl ContentBodyAssembler {
    /// Creates an assembler for the body size declared by `header`.
    pub fn new(header: &ContentHeader) -> Self {
        Self {
            expected: header.body_size,
            received: 0,
            chunks: Vec::new(),
        }
    }

    /// Adds a received chunk, rejecting data beyond the declared body size.
    pub fn push(&mut self, chunk: Bytes) -> Result<()> {
        let received = self.received.saturating_add(chunk.len() as u64);
        if received > self.expected {
            return Err(Error::BodySizeMismatch {
                expected: self.expected,
                actual: received,
            });
        }
        self.received = received;
        if !chunk.is_empty() {
            self.chunks.push(chunk);
        }
        Ok(())
    }

    /// Finishes assembly, rejecting a body shorter than the declared size.
    pub fn finish(self) -> Result<Bytes> {
        if self.received != self.expected {
            return Err(Error::BodySizeMismatch {
                expected: self.expected,
                actual: self.received,
            });
        }

        match self.chunks.len() {
            0 => Ok(Bytes::new()),
            1 => Ok(self
                .chunks
                .into_iter()
                .next()
                .expect("one chunk is present")),
            _ => {
                let capacity = self.chunks.iter().map(Bytes::len).sum();
                let mut buffer = BytesMut::with_capacity(capacity);
                for chunk in self.chunks {
                    buffer.extend_from_slice(&chunk);
                }
                Ok(buffer.freeze())
            }
        }
    }
}

/// Reassembles `chunks` into one body, validating `header.body_size`.
pub fn assemble_content_body(
    header: &ContentHeader,
    chunks: impl IntoIterator<Item = Bytes>,
) -> Result<Bytes> {
    let mut assembler = ContentBodyAssembler::new(header);
    for chunk in chunks {
        assembler.push(chunk)?;
    }
    assembler.finish()
}

impl ContentHeader {
    /// Builds a basic-class content header for `body_size` and `properties`.
    pub fn from_basic_properties(body_size: u64, properties: &BasicProperties) -> Result<Self> {
        Ok(Self {
            class_id: CLASS_ID,
            weight: 0,
            body_size,
            properties: Bytes::from(properties.encode()?),
        })
    }

    /// Decodes the header's raw property bytes into [`BasicProperties`].
    pub fn basic_properties(&self) -> Result<BasicProperties> {
        BasicProperties::decode(&self.properties)
    }
}
