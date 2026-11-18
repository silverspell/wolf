//! AMQP 0-9-1 protocol header and frame encoding/decoding.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::methods::Method;
use crate::types::{Reader, Writer};

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
        if let Self::ContentBody { channel, body } = self {
            if body.len() > MAX_FRAME_SIZE {
                return Err(Error::FrameTooLarge {
                    size: body.len(),
                    max: MAX_FRAME_SIZE,
                });
            }
            let size = u32::try_from(body.len()).map_err(|_| Error::FrameTooLarge {
                size: body.len(),
                max: u32::MAX as usize,
            })?;

            let mut output = Vec::with_capacity(FRAME_OVERHEAD + body.len());
            output.push(FrameType::ContentBody.as_u8());
            output.extend_from_slice(&channel.to_be_bytes());
            output.extend_from_slice(&size.to_be_bytes());
            output.extend_from_slice(body);
            output.push(FRAME_END);
            return Ok(output);
        }

        let (frame_type, channel, payload) = match self {
            Self::Method { channel, method } => (FrameType::Method, *channel, method.try_encode()?),
            Self::ContentHeader { channel, header } => {
                let mut writer = Writer::new();
                writer.write_u16(header.class_id);
                writer.write_u16(header.weight);
                writer.write_u64(header.body_size);
                writer.write_bytes(&header.properties);
                (FrameType::ContentHeader, *channel, writer.into_inner())
            }
            Self::ContentBody { .. } => {
                unreachable!("content body frames are encoded without a copy above")
            }
            Self::Heartbeat { channel } => (FrameType::Heartbeat, *channel, Vec::new()),
        };

        if payload.len() > MAX_FRAME_SIZE {
            return Err(Error::FrameTooLarge {
                size: payload.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        let size = u32::try_from(payload.len()).map_err(|_| Error::FrameTooLarge {
            size: payload.len(),
            max: u32::MAX as usize,
        })?;

        let mut output = Vec::with_capacity(FRAME_OVERHEAD + payload.len());
        output.push(frame_type.as_u8());
        output.extend_from_slice(&channel.to_be_bytes());
        output.extend_from_slice(&size.to_be_bytes());
        output.extend_from_slice(&payload);
        output.push(FRAME_END);
        Ok(output)
    }
}
