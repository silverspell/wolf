//! Conversion of AMQP `FieldTable` values into broker declaration arguments.
//!
//! The broker accepts a deliberately small set of scalar argument types.
//! Anything else is rejected with [`DomainError::InvalidArgument`] so that an
//! unsupported declaration fails loudly instead of being silently dropped.

use std::collections::BTreeMap;

use wolf_amqp_codec::{FieldTable, FieldValue};
use wolf_broker::{ArgumentValue, DomainError};

/// Converts a wire-level field table into protocol-neutral arguments.
///
/// Integers wider than `i64` and values the broker cannot represent (floats,
/// decimals, timestamps, void, nested tables and arrays) are rejected.
pub fn convert(table: &FieldTable) -> Result<BTreeMap<String, ArgumentValue>, DomainError> {
    table
        .iter()
        .map(|(key, value)| {
            let value = convert_value(value).map_err(|message| DomainError::InvalidArgument {
                message: format!("argument '{}': {message}", key.as_str()),
            })?;
            Ok((key.as_str().to_string(), value))
        })
        .collect()
}

fn convert_value(value: &FieldValue) -> Result<ArgumentValue, String> {
    match value {
        FieldValue::Bool(value) => Ok(ArgumentValue::Bool(*value)),
        FieldValue::I8(value) => Ok(ArgumentValue::Integer(i64::from(*value))),
        FieldValue::U8(value) => Ok(ArgumentValue::Integer(i64::from(*value))),
        FieldValue::I16(value) => Ok(ArgumentValue::Integer(i64::from(*value))),
        FieldValue::U16(value) => Ok(ArgumentValue::Integer(i64::from(*value))),
        FieldValue::I32(value) => Ok(ArgumentValue::Integer(i64::from(*value))),
        FieldValue::U32(value) => Ok(ArgumentValue::Integer(i64::from(*value))),
        FieldValue::I64(value) => Ok(ArgumentValue::Integer(*value)),
        FieldValue::U64(value) => i64::try_from(*value)
            .map(ArgumentValue::Integer)
            .map_err(|_| {
                format!("unsigned integer {value} does not fit in a signed 64-bit integer")
            }),
        FieldValue::ShortString(value) => Ok(ArgumentValue::String(value.as_str().to_string())),
        FieldValue::LongString(value) => std::str::from_utf8(value.as_bytes())
            .map(|value| ArgumentValue::String(value.to_string()))
            .map_err(|_| "long string is not valid UTF-8".to_string()),
        FieldValue::ByteArray(value) => Ok(ArgumentValue::Bytes(value.to_vec())),
        other => Err(format!("unsupported value {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use wolf_amqp_codec::{LongString, ShortString};

    use super::*;

    fn table(entries: &[(&str, FieldValue)]) -> FieldTable {
        let mut table = FieldTable::new();
        for (key, value) in entries {
            table.insert(*key, value.clone());
        }
        table
    }

    #[test]
    fn scalar_values_map_onto_protocol_neutral_arguments() {
        let converted = convert(&table(&[
            ("bool", FieldValue::Bool(true)),
            ("i8", FieldValue::I8(-8)),
            ("u8", FieldValue::U8(8)),
            ("i16", FieldValue::I16(-16)),
            ("u16", FieldValue::U16(16)),
            ("i32", FieldValue::I32(-32)),
            ("u32", FieldValue::U32(32)),
            ("i64", FieldValue::I64(-64)),
            ("u64", FieldValue::U64(64)),
            ("short", FieldValue::ShortString(ShortString::new("text"))),
            ("long", FieldValue::LongString(LongString::new("long-text"))),
            ("bytes", FieldValue::ByteArray(Bytes::from_static(b"raw"))),
        ]))
        .expect("scalars convert");

        assert_eq!(converted.get("bool"), Some(&ArgumentValue::Bool(true)));
        assert_eq!(converted.get("i64"), Some(&ArgumentValue::Integer(-64)));
        assert_eq!(
            converted.get("short"),
            Some(&ArgumentValue::String("text".into()))
        );
        assert_eq!(
            converted.get("long"),
            Some(&ArgumentValue::String("long-text".into()))
        );
        assert_eq!(
            converted.get("bytes"),
            Some(&ArgumentValue::Bytes(b"raw".to_vec()))
        );
    }

    #[test]
    fn unsigned_64_bit_overflow_is_invalid_argument() {
        let error = convert(&table(&[("too-big", FieldValue::U64(i64::MAX as u64 + 1))]))
            .expect_err("overflow");

        assert!(matches!(error, DomainError::InvalidArgument { .. }));
    }

    #[test]
    fn non_utf8_long_strings_are_invalid_argument() {
        let error = convert(&table(&[(
            "invalid",
            FieldValue::LongString(LongString::from(vec![0xff, 0xfe])),
        )]))
        .expect_err("invalid UTF-8");

        assert!(matches!(error, DomainError::InvalidArgument { .. }));
    }

    #[test]
    fn unsupported_values_are_invalid_argument() {
        for value in [
            FieldValue::Float(1.5),
            FieldValue::Double(2.5),
            FieldValue::Decimal { scale: 2, value: 5 },
            FieldValue::Timestamp(10),
            FieldValue::Void,
            FieldValue::FieldArray(vec![FieldValue::Bool(true)]),
            FieldValue::FieldTable(FieldTable::new()),
        ] {
            let error = convert(&table(&[("unsupported", value)]))
                .expect_err("unsupported value must fail");
            assert!(matches!(error, DomainError::InvalidArgument { .. }));
        }
    }
}
