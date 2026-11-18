//! Errors returned while decoding AMQP frames, methods and field tables.

use std::fmt;

/// Result type used throughout the codec.
pub type Result<T> = std::result::Result<T, Error>;

/// A codec error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The input ended before a complete value could be read.
    ///
    /// `needed` is the total number of bytes required and `available` is the
    /// number of bytes that were present.
    Incomplete { needed: usize, available: usize },
    /// A value was decoded but bytes remained in the input.
    TrailingBytes { remaining: usize },
    /// The eight-byte AMQP protocol header was not recognised.
    ProtocolHeaderMismatch { found: [u8; 8] },
    /// The frame type octet is not one of the AMQP 0-9-1 frame types.
    UnknownFrameType { frame_type: u8 },
    /// The frame did not terminate with the `0xCE` frame-end marker.
    BadFrameEnd { found: u8 },
    /// The frame payload was structurally invalid.
    MalformedFrame { reason: &'static str },
    /// The method belongs to a class this codec does not implement yet.
    UnsupportedClass { class_id: u16 },
    /// The method id is not-known inside the given class.
    UnknownMethod { class_id: u16, method_id: u16 },
    /// A field-table value used an unrecognised AMQP type code.
    UnknownFieldType { field_type: u8 },
    /// A short string was not valid UTF-8.
    InvalidUtf8,
    /// A short string exceeded the 255-byte protocol limit.
    ShortStringTooLong { len: usize },
    /// A field table's declared length or contents were malformed.
    MalformedTable,
    /// A field table or field array exceeded the configured nesting limit.
    TableNestingTooDeep { depth: usize },
    /// A frame payload exceeded the decoder's maximum frame size.
    FrameTooLarge { size: usize, max: usize },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { needed, available } => {
                write!(
                    f,
                    "incomplete input: needed {needed} bytes, have {available}"
                )
            }
            Self::TrailingBytes { remaining } => {
                write!(f, "trailing bytes after value: {remaining}")
            }
            Self::ProtocolHeaderMismatch { found } => {
                write!(f, "unexpected AMQP protocol header: {found:02x?}")
            }
            Self::UnknownFrameType { frame_type } => {
                write!(f, "unknown frame type {frame_type}")
            }
            Self::BadFrameEnd { found } => {
                write!(f, "bad frame end 0x{found:02x}")
            }
            Self::MalformedFrame { reason } => write!(f, "malformed frame: {reason}"),
            Self::UnsupportedClass { class_id } => {
                write!(f, "unsupported AMQP class {class_id}")
            }
            Self::UnknownMethod {
                class_id,
                method_id,
            } => write!(f, "unknown method {method_id} in class {class_id}"),
            Self::UnknownFieldType { field_type } => {
                write!(f, "unknown field type 0x{field_type:02x}")
            }
            Self::InvalidUtf8 => write!(f, "short string is not valid UTF-8"),
            Self::ShortStringTooLong { len } => {
                write!(f, "short string length {len} exceeds 255")
            }
            Self::MalformedTable => write!(f, "malformed field table"),
            Self::TableNestingTooDeep { depth } => {
                write!(f, "field table nesting depth {depth} exceeds limit")
            }
            Self::FrameTooLarge { size, max } => {
                write!(f, "frame size {size} exceeds maximum {max}")
            }
        }
    }
}

impl std::error::Error for Error {}
