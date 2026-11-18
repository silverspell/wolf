use bytes::Bytes;
use wolf_amqp_codec::{Error, FieldTable, FieldValue, LongString, MAX_TABLE_DEPTH, ShortString};

#[test]
fn field_table_encodes_spec_golden_bytes() {
    let mut table = FieldTable::new();
    table.insert("x", FieldValue::Bool(true));

    let encoded = table.encode().unwrap();
    assert_eq!(encoded, vec![0, 0, 0, 4, 1, b'x', b't', 1]);

    let (decoded, consumed) = FieldTable::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded, table);
}

#[test]
fn field_table_preserves_insertion_order() {
    let mut table = FieldTable::new();
    table.insert("b", FieldValue::U8(1));
    table.insert("a", FieldValue::U8(2));

    let encoded = table.encode().unwrap();
    assert_eq!(
        encoded,
        vec![0, 0, 0, 8, 1, b'b', b'B', 1, 1, b'a', b'B', 2]
    );

    let (decoded, _) = FieldTable::decode(&encoded).unwrap();
    let keys: Vec<&str> = decoded.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["b", "a"]);
}

#[test]
fn field_table_round_trips_all_field_types() {
    let mut nested = FieldTable::new();
    nested.insert("k", FieldValue::I32(7));

    let mut table = FieldTable::new();
    table.insert("bool", FieldValue::Bool(true));
    table.insert("i8", FieldValue::I8(-8));
    table.insert("u8", FieldValue::U8(8));
    table.insert("i16", FieldValue::I16(-16));
    table.insert("u16", FieldValue::U16(16));
    table.insert("i32", FieldValue::I32(-32));
    table.insert("u32", FieldValue::U32(32));
    table.insert("i64", FieldValue::I64(-64));
    table.insert("u64", FieldValue::U64(64));
    table.insert("float", FieldValue::Float(1.5));
    table.insert("double", FieldValue::Double(2.25));
    table.insert(
        "decimal",
        FieldValue::Decimal {
            scale: 2,
            value: 1234,
        },
    );
    table.insert(
        "short",
        FieldValue::ShortString(ShortString::new("short value")),
    );
    table.insert(
        "long",
        FieldValue::LongString(LongString::from(b"long value".to_vec())),
    );
    table.insert(
        "array",
        FieldValue::FieldArray(vec![
            FieldValue::Bool(false),
            FieldValue::LongString(LongString::from(b"array item".to_vec())),
        ]),
    );
    table.insert("timestamp", FieldValue::Timestamp(1_700_000_000));
    table.insert("table", FieldValue::FieldTable(nested));
    table.insert("void", FieldValue::Void);
    table.insert(
        "bytes",
        FieldValue::ByteArray(Bytes::from_static(&[1, 2, 3])),
    );

    let encoded = table.encode().unwrap();
    let (decoded, consumed) = FieldTable::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded, table);
    assert_eq!(decoded.get("i32"), Some(&FieldValue::I32(-32)));
}

#[test]
fn unknown_field_type_is_rejected() {
    let bytes = [0, 0, 0, 3, 1, b'z', b'Z'];
    let error = FieldTable::decode(&bytes).unwrap_err();
    assert_eq!(error, Error::UnknownFieldType { field_type: b'Z' });
}

#[test]
fn truncated_field_table_is_incomplete() {
    let bytes = [0, 0, 0, 4, 1, b'x'];
    let error = FieldTable::decode(&bytes).unwrap_err();
    assert!(matches!(error, Error::Incomplete { .. }));
}

#[test]
fn invalid_utf8_short_string_is_rejected() {
    let bytes = [0, 0, 0, 2, 1, 0xFF];
    let error = FieldTable::decode(&bytes).unwrap_err();
    assert_eq!(error, Error::InvalidUtf8);
}

#[test]
fn short_string_longer_than_255_bytes_is_rejected() {
    let long_key = "a".repeat(256);
    let mut table = FieldTable::new();
    table.insert(long_key, FieldValue::Bool(true));

    let error = table.encode().unwrap_err();
    assert_eq!(error, Error::ShortStringTooLong { len: 256 });
}

#[test]
fn deeply_nested_table_is_rejected() {
    fn nested_table(levels: usize) -> FieldTable {
        let mut table = FieldTable::new();
        for _ in 0..levels {
            let mut outer = FieldTable::new();
            outer.insert("n", FieldValue::FieldTable(table));
            table = outer;
        }
        table
    }

    let table = nested_table(MAX_TABLE_DEPTH + 8);
    let error = table.encode().unwrap_err();
    assert!(matches!(error, Error::TableNestingTooDeep { .. }));
}
