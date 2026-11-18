//! AMQP primitive types and low-level encoding helpers.

use bytes::Bytes;
use std::fmt;

use crate::error::{Error, Result};

/// An AMQP short string: at most 255 bytes of UTF-8.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShortString(String);

impl ShortString {
    /// Creates a short string without checking its encoded length.
    ///
    /// Encoding rejects values longer than 255 bytes with
    /// [`Error::ShortStringTooLong`].
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the string contents.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the value and returns the underlying string.
    pub fn into_string(self) -> String {
        self.0
    }

    /// Returns the length in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when the string has no bytes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<String> for ShortString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ShortString {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl fmt::Display for ShortString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An AMQP long string, stored as shared bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LongString(Bytes);

impl LongString {
    /// Creates a long string from any byte container.
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self(value.into())
    }

    /// Returns the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the value and returns the shared bytes.
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Returns the length in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when the value has no bytes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Bytes> for LongString {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<Vec<u8>> for LongString {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<&'static [u8]> for LongString {
    fn from(value: &'static [u8]) -> Self {
        Self(Bytes::from_static(value))
    }
}

impl fmt::Display for LongString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} bytes", self.0.len())
    }
}

pub use crate::field_table::{FieldTable, FieldValue, MAX_TABLE_DEPTH};

#[derive(Debug)]
pub(crate) struct Reader<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) position: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn position(&self) -> usize {
        self.position
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn require(&self, count: usize) -> Result<()> {
        if self.remaining() < count {
            Err(Error::Incomplete {
                needed: self.position + count,
                available: self.bytes.len(),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8> {
        self.require(1)?;
        let value = self.bytes[self.position];
        self.position += 1;
        Ok(value)
    }

    pub(crate) fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.read_array::<2>()?))
    }

    pub(crate) fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.read_array::<4>()?))
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.read_array::<8>()?))
    }

    pub(crate) fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.require(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.position..self.position + N]);
        self.position += N;
        Ok(out)
    }

    pub(crate) fn read_bytes(&mut self, count: usize) -> Result<&'a [u8]> {
        self.require(count)?;
        let start = self.position;
        self.position += count;
        Ok(&self.bytes[start..self.position])
    }

    pub(crate) fn read_short_string(&mut self) -> Result<ShortString> {
        let len = self.read_u8()? as usize;
        let bytes = self.read_bytes(len)?;
        let value = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
        Ok(ShortString::new(value))
    }

    pub(crate) fn read_long_string(&mut self) -> Result<LongString> {
        Ok(LongString::new(self.read_long_bytes()?))
    }

    pub(crate) fn read_long_bytes(&mut self) -> Result<Bytes> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(Bytes::copy_from_slice(bytes))
    }
}

#[derive(Debug)]
pub(crate) struct Writer {
    pub(crate) bytes: Vec<u8>,
}

impl Writer {
    pub(crate) fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn write_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(crate) fn write_short_string(&mut self, value: &str) -> Result<()> {
        let len = value.len();
        if len > u8::MAX as usize {
            return Err(Error::ShortStringTooLong { len });
        }
        self.write_u8(len as u8);
        self.write_bytes(value.as_bytes());
        Ok(())
    }

    pub(crate) fn write_long_string(&mut self, value: &[u8]) -> Result<()> {
        let len = u32::try_from(value.len()).map_err(|_| Error::MalformedFrame {
            reason: "long string too long",
        })?;
        self.write_u32(len);
        self.write_bytes(value);
        Ok(())
    }
}
