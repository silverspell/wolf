//! AMQP field tables and field values, including nested tables and arrays.
//!
//! A field table is a length-prefixed sequence of short-string keys and typed
//! values. Values use the RabbitMQ AMQP 0-9-1 type-byte mapping and may nest
//! tables and arrays up to [`MAX_TABLE_DEPTH`].

use bytes::{BufMut, Bytes};

use crate::error::{Error, Result};
use crate::types::{LongString, Reader, ShortString, Writer};

/// Maximum nesting depth accepted for field tables and field arrays.
pub const MAX_TABLE_DEPTH: usize = 32;

/// A value stored inside an AMQP field table or field array.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// `t` — boolean.
    Bool(bool),
    /// `b` — signed 8-bit integer.
    I8(i8),
    /// `B` — unsigned 8-bit integer.
    U8(u8),
    /// `U` — signed 16-bit integer.
    I16(i16),
    /// `u` — unsigned 16-bit integer.
    U16(u16),
    /// `I` — signed 32-bit integer.
    I32(i32),
    /// `i` — unsigned 32-bit integer.
    U32(u32),
    /// `L` — signed 64-bit integer.
    I64(i64),
    /// `l` — unsigned 64-bit integer.
    U64(u64),
    /// `f` — 32-bit floating point.
    Float(f32),
    /// `d` — 64-bit floating point.
    Double(f64),
    /// `D` — decimal value with scale and signed 32-bit value.
    Decimal { scale: u8, value: i32 },
    /// `s` — short string.
    ShortString(ShortString),
    /// `S` — long string.
    LongString(LongString),
    /// `A` — field array.
    FieldArray(Vec<FieldValue>),
    /// `T` — 64-bit POSIX timestamp.
    Timestamp(u64),
    /// `F` — nested field table.
    FieldTable(FieldTable),
    /// `V` — void / no value.
    Void,
    /// `x` — byte array (RabbitMQ extension).
    ByteArray(Bytes),
}

impl FieldValue {
    fn encode<B: BufMut>(&self, writer: &mut Writer<B>, depth: usize) -> Result<()> {
        match self {
            Self::Bool(value) => {
                writer.write_u8(b't');
                writer.write_u8(u8::from(*value));
            }
            Self::I8(value) => {
                writer.write_u8(b'b');
                writer.write_u8(*value as u8);
            }
            Self::U8(value) => {
                writer.write_u8(b'B');
                writer.write_u8(*value);
            }
            Self::I16(value) => {
                writer.write_u8(b'U');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::U16(value) => {
                writer.write_u8(b'u');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::I32(value) => {
                writer.write_u8(b'I');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::U32(value) => {
                writer.write_u8(b'i');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::I64(value) => {
                writer.write_u8(b'L');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::U64(value) => {
                writer.write_u8(b'l');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::Float(value) => {
                writer.write_u8(b'f');
                writer.write_bytes(&value.to_bits().to_be_bytes());
            }
            Self::Double(value) => {
                writer.write_u8(b'd');
                writer.write_bytes(&value.to_bits().to_be_bytes());
            }
            Self::Decimal { scale, value } => {
                writer.write_u8(b'D');
                writer.write_u8(*scale);
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::ShortString(value) => {
                writer.write_u8(b's');
                writer.write_short_string(value.as_str())?;
            }
            Self::LongString(value) => {
                writer.write_u8(b'S');
                writer.write_long_string(value.as_bytes())?;
            }
            Self::FieldArray(values) => {
                writer.write_u8(b'A');
                writer.write_field_array(values, depth + 1)?;
            }
            Self::Timestamp(value) => {
                writer.write_u8(b'T');
                writer.write_bytes(&value.to_be_bytes());
            }
            Self::FieldTable(value) => {
                writer.write_u8(b'F');
                writer.write_field_table(value, depth + 1)?;
            }
            Self::Void => writer.write_u8(b'V'),
            Self::ByteArray(value) => {
                writer.write_u8(b'x');
                writer.write_long_string(value)?;
            }
        }
        Ok(())
    }

    fn decode(reader: &mut Reader<'_>, depth: usize) -> Result<Self> {
        let field_type = reader.read_u8()?;
        let value = match field_type {
            b't' => Self::Bool(reader.read_u8()? != 0),
            b'b' => Self::I8(reader.read_u8()? as i8),
            b'B' => Self::U8(reader.read_u8()?),
            b'U' => Self::I16(i16::from_be_bytes(reader.read_array::<2>()?)),
            b'u' => Self::U16(u16::from_be_bytes(reader.read_array::<2>()?)),
            b'I' => Self::I32(i32::from_be_bytes(reader.read_array::<4>()?)),
            b'i' => Self::U32(u32::from_be_bytes(reader.read_array::<4>()?)),
            b'L' => Self::I64(i64::from_be_bytes(reader.read_array::<8>()?)),
            b'l' => Self::U64(u64::from_be_bytes(reader.read_array::<8>()?)),
            b'f' => Self::Float(f32::from_bits(u32::from_be_bytes(
                reader.read_array::<4>()?,
            ))),
            b'd' => Self::Double(f64::from_bits(u64::from_be_bytes(
                reader.read_array::<8>()?,
            ))),
            b'D' => Self::Decimal {
                scale: reader.read_u8()?,
                value: i32::from_be_bytes(reader.read_array::<4>()?),
            },
            b's' => Self::ShortString(reader.read_short_string()?),
            b'S' => Self::LongString(reader.read_long_string()?),
            b'A' => Self::FieldArray(reader.read_field_array(depth + 1)?),
            b'T' => Self::Timestamp(u64::from_be_bytes(reader.read_array::<8>()?)),
            b'F' => Self::FieldTable(reader.read_field_table(depth + 1)?),
            b'V' => Self::Void,
            b'x' => Self::ByteArray(reader.read_long_bytes()?),
            other => return Err(Error::UnknownFieldType { field_type: other }),
        };
        Ok(value)
    }
}

/// An ordered AMQP field table.
///
/// Entries retain insertion order so clients observe the same ordering on the
/// wire that the server wrote.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable {
    entries: Vec<(ShortString, FieldValue)>,
}

impl FieldTable {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an entry, replacing an existing value for the same key.
    pub fn insert(&mut self, key: impl Into<ShortString>, value: FieldValue) -> Option<FieldValue> {
        let key = key.into();
        if let Some((_, existing)) = self
            .entries
            .iter_mut()
            .find(|(candidate, _)| candidate == &key)
        {
            Some(std::mem::replace(existing, value))
        } else {
            self.entries.push((key, value));
            None
        }
    }

    /// Returns the value for `key`, if present.
    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.entries
            .iter()
            .find(|(candidate, _)| candidate.as_str() == key)
            .map(|(_, value)| value)
    }

    /// Returns `true` when `key` is present.
    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Iterates over entries in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&ShortString, &FieldValue)> {
        self.entries.iter().map(|(key, value)| (key, value))
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` when the table has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Encodes the table, including its four-byte length prefix.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.write_field_table(self, 0)?;
        Ok(writer.into_inner())
    }

    /// Decodes a length-prefixed table and returns it with bytes consumed.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        let mut reader = Reader::new(bytes);
        let table = reader.read_field_table(0)?;
        Ok((table, reader.position()))
    }
}

impl FromIterator<(ShortString, FieldValue)> for FieldTable {
    fn from_iter<T: IntoIterator<Item = (ShortString, FieldValue)>>(iter: T) -> Self {
        Self {
            entries: iter.into_iter().collect(),
        }
    }
}

impl Extend<(ShortString, FieldValue)> for FieldTable {
    fn extend<T: IntoIterator<Item = (ShortString, FieldValue)>>(&mut self, iter: T) {
        self.entries.extend(iter);
    }
}

impl Reader<'_> {
    pub(crate) fn read_field_table(&mut self, depth: usize) -> Result<FieldTable> {
        if depth > MAX_TABLE_DEPTH {
            return Err(Error::TableNestingTooDeep { depth });
        }
        let len = self.read_u32()? as usize;
        let body = self.read_bytes(len)?;
        let mut reader = Reader::new(body);
        let mut table = FieldTable::new();
        while reader.remaining() > 0 {
            let key = reader.read_short_string()?;
            let value = FieldValue::decode(&mut reader, depth)?;
            table.entries.push((key, value));
        }
        Ok(table)
    }

    fn read_field_array(&mut self, depth: usize) -> Result<Vec<FieldValue>> {
        if depth > MAX_TABLE_DEPTH {
            return Err(Error::TableNestingTooDeep { depth });
        }
        let len = self.read_u32()? as usize;
        let body = self.read_bytes(len)?;
        let mut reader = Reader::new(body);
        let mut values = Vec::new();
        while reader.remaining() > 0 {
            values.push(FieldValue::decode(&mut reader, depth)?);
        }
        Ok(values)
    }
}

impl<B: BufMut> Writer<B> {
    pub(crate) fn write_field_table(&mut self, table: &FieldTable, depth: usize) -> Result<()> {
        if depth > MAX_TABLE_DEPTH {
            return Err(Error::TableNestingTooDeep { depth });
        }
        let mut body = Writer::new();
        for (key, value) in &table.entries {
            body.write_short_string(key.as_str())?;
            value.encode(&mut body, depth)?;
        }
        let len = u32::try_from(body.len()).map_err(|_| Error::MalformedTable)?;
        let body = body.into_inner();
        self.write_u32(len);
        self.write_bytes(&body);
        Ok(())
    }

    fn write_field_array(&mut self, values: &[FieldValue], depth: usize) -> Result<()> {
        if depth > MAX_TABLE_DEPTH {
            return Err(Error::TableNestingTooDeep { depth });
        }
        let mut body = Writer::new();
        for value in values {
            value.encode(&mut body, depth)?;
        }
        let len = u32::try_from(body.len()).map_err(|_| Error::MalformedTable)?;
        let body = body.into_inner();
        self.write_u32(len);
        self.write_bytes(&body);
        Ok(())
    }
}
