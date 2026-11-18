//! AMQP 0-9-1 protocol header and frame encoding/decoding.

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::methods::Method;
use crate::types::Reader;

/// The frame terminator octet.
pub const FRAME_END: u8 = 0xCE;

/// The fixed frame overhead: a seven-byte header plus the frame-end octet.
pub const FRAME_OVERHEAD: usize = 8;

/// Maximum accepted frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// The AMQP 0-9-1 protocol header sent before any frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolHeader;

impl ProtocolHeader {
    /// The eight protocol header bytes for AMQP 0-9-1.
    pub const BYTES: [u8; 8] = *b"AMQP\0\0\x09\x01";

    /// Returns the protocol header bytes.
    pub const fn encode(self) -> [u8; 8] {
        Self::BYTES
    }

    /// Validates an AMQP 0-9-1 protocol header.
    ///
    /// Any bytes after the first eight are left for the caller to frame.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::BYTES.len() {
            return Err(Error::Incomplete {
                needed: Self::BYTES.len(),
                available: bytes.len(),
            });
        }
        if bytes[..Self::BYTES.len()] != Self::BYTES {
            let mut found = [0u8; 8];
            found.copy_from_slice(&bytes[..Self::BYTES.len()]);
            return Err(Error::ProtocolHeaderMismatch { found });
        }
        Ok(Self)
    }
}

/// AMQP 0-9-1 frame types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    /// Method frame.
    Method = 1,
    /// Content header frame.
    ContentHeader = 2,
    /// Content body frame.
    ContentBody = 3,
    /// Heartbeat frame.
    Heartbeat = 8,
}

impl TryFrom<u8> for FrameType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Method),
            2 => Ok(Self::ContentHeader),
            3 => Ok(Self::ContentBody),
            8 => Ok(Self::Heartbeat),
            other => Err(Error::UnknownFrameType { frame_type: other }),
        }
    }
}

impl FrameType {
    /// Returns the wire representation of the frame type.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// The AMQP content header carried before a message body.
///
/// `properties` holds the raw property flags and property value bytes that
/// follow the fixed header fields, so the codec is independent of individual
/// basic-property layouts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentHeader {
    /// Message class id.
    pub class_id: u16,
    /// Reserved weight field (always zero).
    pub weight: u16,
    /// Body size in bytes.
    pub body_size: u64,
    /// Raw property flags and values.
    pub properties: Bytes,
}

impl ContentHeader {
    /// Appends the content-header payload to `buffer`.
    pub fn encode_into(&self, buffer: &mut BytesMut) -> Result<()> {
        buffer.extend_from_slice(&self.class_id.to_be_bytes());
        buffer.extend_from_slice(&self.weight.to_be_bytes());
        buffer.extend_from_slice(&self.body_size.to_be_bytes());
        buffer.extend_from_slice(&self.properties);
        Ok(())
    }
}

/// The three wire segments of one encoded frame.
///
/// The header and tail are fixed-size inline arrays. For content-body frames
/// the payload is a clone of the source [`Bytes`] and therefore shares its
/// allocation, so writing a delivery does not copy the message body.
#[derive(Debug, Clone)]
pub struct FrameSegments {
    header: [u8; 7],
    payload: Bytes,
    tail: [u8; 1],
}

impl FrameSegments {
    /// Returns the seven-byte frame header.
    pub fn header(&self) -> &[u8; 7] {
        &self.header
    }

    /// Returns the frame payload.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Returns the content-body payload when this is a content-body frame.
    pub fn body(&self) -> Option<&Bytes> {
        (self.header[0] == FrameType::ContentBody.as_u8()).then_some(&self.payload)
    }

    /// Returns the frame-end octet.
    pub fn tail(&self) -> &[u8; 1] {
        &self.tail
    }

    /// Returns the total encoded frame length.
    pub fn len(&self) -> usize {
        self.header.len() + self.payload.len() + self.tail.len()
    }

    /// Returns `true` when the frame has no payload (heartbeat frames).
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

/// A decoded AMQP frame.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// A method frame.
    Method {
        /// Channel number.
        channel: u16,
        /// Decoded method.
        method: Method,
    },
    /// A content header frame.
    ContentHeader {
        /// Channel number.
        channel: u16,
        /// Decoded content header.
        header: ContentHeader,
    },
    /// A content body frame.
    ContentBody {
        /// Channel number.
        channel: u16,
        /// Zero-copy slice of the input buffer.
        body: Bytes,
    },
    /// A heartbeat frame.
    Heartbeat {
        /// Channel number (normally zero).
        channel: u16,
    },
}

/// Decodes one frame from the start of `input`.
///
/// Returns the frame and the number of bytes consumed, allowing the caller to
/// feed a buffer containing several frames.
pub fn decode_frame(input: &Bytes) -> Result<(Frame, usize)> {
    const HEADER_LEN: usize = 7;
    if input.len() < HEADER_LEN {
        return Err(Error::Incomplete {
            needed: HEADER_LEN,
            available: input.len(),
        });
    }

    let frame_type = FrameType::try_from(input[0])?;
    let channel = u16::from_be_bytes([input[1], input[2]]);
    let size = u32::from_be_bytes([input[3], input[4], input[5], input[6]]) as usize;
    if size > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge {
            size,
            max: MAX_FRAME_SIZE,
        });
    }
    let total = HEADER_LEN
        .checked_add(size)
        .and_then(|value| value.checked_add(1))
        .ok_or(Error::FrameTooLarge {
            size,
            max: MAX_FRAME_SIZE,
        })?;
    if input.len() < total {
        return Err(Error::Incomplete {
            needed: total,
            available: input.len(),
        });
    }
    let frame_end = input[total - 1];
    if frame_end != FRAME_END {
        return Err(Error::BadFrameEnd { found: frame_end });
    }
    let payload = input.slice(HEADER_LEN..HEADER_LEN + size);

    let frame = match frame_type {
        FrameType::Method => Frame::Method {
            channel,
            method: Method::decode(&payload)?,
        },
        FrameType::ContentHeader => {
            let mut reader = Reader::new(&payload);
            let class_id = reader.read_u16()?;
            let weight = reader.read_u16()?;
            let body_size = u64::from_be_bytes(reader.read_array::<8>()?);
            let properties = payload.slice(reader.position()..);
            Frame::ContentHeader {
                channel,
                header: ContentHeader {
                    class_id,
                    weight,
                    body_size,
                    properties,
                },
            }
        }
        FrameType::ContentBody => Frame::ContentBody {
            channel,
            body: payload,
        },
        FrameType::Heartbeat => {
            if !payload.is_empty() {
                return Err(Error::MalformedFrame {
                    reason: "heartbeat frame must not carry a payload",
                });
            }
            Frame::Heartbeat { channel }
        }
    };

    Ok((frame, total))
}

impl Frame {
    /// Encodes the frame into its wire representation.
    ///
    /// # Panics
    ///
    /// Panics if the frame exceeds AMQP encoding limits. Use
    /// [`Frame::try_encode`] when encoding untrusted values.
    pub fn encode(&self) -> Vec<u8> {
        self.try_encode()
            .expect("frame exceeds AMQP encoding limits")
    }

    /// Encodes the frame, returning an error for values that exceed AMQP limits.
    pub fn try_encode(&self) -> Result<Vec<u8>> {
        let segments = self.encode_segments()?;
        let mut output = Vec::with_capacity(segments.len());
        output.extend_from_slice(segments.header());
        output.extend_from_slice(segments.payload());
        output.extend_from_slice(segments.tail());
        Ok(output)
    }

    /// Splits the frame into header, payload and tail wire segments.
    ///
    /// Content-body payloads share the source [`Bytes`] allocation instead of
    /// copying the body.
    pub fn encode_segments(&self) -> Result<FrameSegments> {
        let (frame_type, channel, payload) = match self {
            Self::Method { channel, method } => (
                FrameType::Method,
                *channel,
                Bytes::from(method.try_encode()?),
            ),
            Self::ContentHeader { channel, header } => {
                let mut buffer = BytesMut::with_capacity(16 + header.properties.len());
                header.encode_into(&mut buffer)?;
                (FrameType::ContentHeader, *channel, buffer.freeze())
            }
            Self::ContentBody { channel, body } => (FrameType::ContentBody, *channel, body.clone()),
            Self::Heartbeat { channel } => (FrameType::Heartbeat, *channel, Bytes::new()),
        };

        let header = frame_header(frame_type, channel, payload.len())?;
        Ok(FrameSegments {
            header,
            payload,
            tail: [FRAME_END],
        })
    }

    /// Appends the encoded frame to `buffer`.
    ///
    /// The buffer is left unchanged when encoding fails. Content bodies are
    /// copied here; use [`Frame::encode_segments`] for zero-copy body writes.
    pub fn encode_into(&self, buffer: &mut BytesMut) -> Result<()> {
        let start = buffer.len();
        let result = (|| {
            let segments = self.encode_segments()?;
            buffer.extend_from_slice(segments.header());
            buffer.extend_from_slice(segments.payload());
            buffer.extend_from_slice(segments.tail());
            Ok(())
        })();
        if result.is_err() {
            buffer.truncate(start);
        }
        result
    }
}

fn frame_header(frame_type: FrameType, channel: u16, size: usize) -> Result<[u8; 7]> {
    if size > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge {
            size,
            max: MAX_FRAME_SIZE,
        });
    }
    let size = u32::try_from(size).map_err(|_| Error::FrameTooLarge {
        size,
        max: u32::MAX as usize,
    })?;

    let mut header = [0u8; 7];
    header[0] = frame_type.as_u8();
    header[1..3].copy_from_slice(&channel.to_be_bytes());
    header[3..7].copy_from_slice(&size.to_be_bytes());
    Ok(header)
}
