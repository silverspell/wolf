use std::panic;

use bytes::Bytes;
use wolf_amqp_codec::field_table::{
    FieldTable as ModuleFieldTable, FieldValue as ModuleFieldValue,
    MAX_TABLE_DEPTH as MODULE_MAX_TABLE_DEPTH,
};
use wolf_amqp_codec::types::{
    FieldTable as TypesFieldTable, FieldValue as TypesFieldValue,
    MAX_TABLE_DEPTH as TYPES_MAX_TABLE_DEPTH,
};
use wolf_amqp_codec::{Error, FieldTable, FieldValue, LongString, ShortString};

fn round_trip(value: FieldValue) -> FieldValue {
    let mut table = FieldTable::new();
    table.insert("value", value);
    let encoded = table.encode().unwrap();
    let (decoded, consumed) = FieldTable::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());
    decoded.get("value").cloned().unwrap()
}

#[test]
fn every_field_value_variant_round_trips_with_type_and_value() {
    let cases = [
        FieldValue::Bool(false),
        FieldValue::Bool(true),
        FieldValue::I8(i8::MIN),
        FieldValue::I8(i8::MAX),
        FieldValue::U8(u8::MIN),
        FieldValue::U8(u8::MAX),
        FieldValue::I16(i16::MIN),
        FieldValue::I16(i16::MAX),
        FieldValue::U16(u16::MIN),
        FieldValue::U16(u16::MAX),
        FieldValue::I32(i32::MIN),
        FieldValue::I32(i32::MAX),
        FieldValue::U32(u32::MIN),
        FieldValue::U32(u32::MAX),
        FieldValue::I64(i64::MIN),
        FieldValue::I64(i64::MAX),
        FieldValue::U64(u64::MIN),
        FieldValue::U64(u64::MAX),
        FieldValue::Float(f32::INFINITY),
        FieldValue::Float(f32::NEG_INFINITY),
        FieldValue::Float(-0.0),
        FieldValue::Double(f64::INFINITY),
        FieldValue::Double(f64::NEG_INFINITY),
        FieldValue::Double(-0.0),
        FieldValue::Decimal {
            scale: 3,
            value: -1234,
        },
        FieldValue::Decimal {
            scale: 0,
            value: i32::MIN,
        },
        FieldValue::ShortString(ShortString::new("")),
        FieldValue::ShortString(ShortString::new("héllo-世界")),
        FieldValue::LongString(LongString::from(vec![0xFF, 0x00, 0x80])),
        FieldValue::LongString(LongString::new(Bytes::new())),
        FieldValue::FieldArray(vec![]),
        FieldValue::FieldArray(vec![
            FieldValue::U8(1),
            FieldValue::ShortString(ShortString::new("item")),
        ]),
        FieldValue::Timestamp(u64::MIN),
        FieldValue::Timestamp(u64::MAX),
        FieldValue::FieldTable(FieldTable::new()),
        FieldValue::Void,
        FieldValue::ByteArray(Bytes::from_static(&[0xFF, 0x00, 0x80])),
        FieldValue::ByteArray(Bytes::new()),
    ];

    for value in cases {
        let decoded = round_trip(value.clone());
        assert_eq!(decoded, value);
    }
}

#[test]
fn negative_zero_keeps_its_bit_pattern() {
    match round_trip(FieldValue::Float(-0.0)) {
        FieldValue::Float(value) => assert_eq!(value.to_bits(), (-0.0f32).to_bits()),
        other => panic!("expected float, got {other:?}"),
    }

    match round_trip(FieldValue::Double(-0.0)) {
        FieldValue::Double(value) => assert_eq!(value.to_bits(), (-0.0f64).to_bits()),
        other => panic!("expected double, got {other:?}"),
    }
}

#[test]
fn nested_table_in_array_and_array_in_table_round_trip() {
    let mut inner = FieldTable::new();
    inner.insert("answer", FieldValue::I32(42));
    inner.insert(
        "tags",
        FieldValue::FieldArray(vec![
            FieldValue::ShortString(ShortString::new("a")),
            FieldValue::U64(7),
        ]),
    );

    let array = FieldValue::FieldArray(vec![
        FieldValue::FieldTable(inner.clone()),
        FieldValue::Void,
        FieldValue::Bool(true),
    ]);

    let mut outer = FieldTable::new();
    outer.insert("items", array);
    outer.insert("nested", FieldValue::FieldTable(inner));

    let decoded = round_trip(FieldValue::FieldTable(outer));
    let decoded = match decoded {
        FieldValue::FieldTable(table) => table,
        other => panic!("expected table, got {other:?}"),
    };

    let items = match decoded.get("items") {
        Some(FieldValue::FieldArray(items)) => items,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[1], FieldValue::Void);
    assert_eq!(items[2], FieldValue::Bool(true));

    let table_in_array = match &items[0] {
        FieldValue::FieldTable(table) => table,
        other => panic!("expected table in array, got {other:?}"),
    };
    assert_eq!(table_in_array.get("answer"), Some(&FieldValue::I32(42)));
    let tags = match table_in_array.get("tags") {
        Some(FieldValue::FieldArray(tags)) => tags,
        other => panic!("expected array in table in array, got {other:?}"),
    };
    assert_eq!(
        tags,
        &vec![
            FieldValue::ShortString(ShortString::new("a")),
            FieldValue::U64(7),
        ]
    );

    let nested = match decoded.get("nested") {
        Some(FieldValue::FieldTable(table)) => table,
        other => panic!("expected nested table, got {other:?}"),
    };
    assert_eq!(nested, table_in_array);
    assert_eq!(
        nested.get("tags"),
        Some(&FieldValue::FieldArray(tags.clone()))
    );
}

#[test]
fn declared_length_overflow_is_incomplete_not_a_panic() {
    let top_level = FieldTable::decode(&[0, 0, 0xFF, 0xFF, 0x00]).unwrap_err();
    assert!(matches!(top_level, Error::Incomplete { .. }));

    let nested_table = [0, 0, 0, 7, 1, b'n', b'F', 0xFF, 0xFF, 0xFF, 0xFF];
    let error = FieldTable::decode(&nested_table).unwrap_err();
    assert!(matches!(error, Error::Incomplete { .. }));

    let nested_array = [0, 0, 0, 7, 1, b'a', b'A', 0xFF, 0xFF, 0xFF, 0xFF];
    let error = FieldTable::decode(&nested_array).unwrap_err();
    assert!(matches!(error, Error::Incomplete { .. }));
}

#[test]
fn unknown_type_byte_is_rejected_at_top_level_and_nested() {
    let top_level = FieldTable::decode(&[0, 0, 0, 3, 1, b'z', b'Z']).unwrap_err();
    assert_eq!(top_level, Error::UnknownFieldType { field_type: b'Z' });

    let nested_table = [0, 0, 0, 10, 1, b'n', b'F', 0, 0, 0, 3, 1, b'z', b'Z'];
    assert_eq!(
        FieldTable::decode(&nested_table).unwrap_err(),
        Error::UnknownFieldType { field_type: b'Z' }
    );

    let nested_array = [0, 0, 0, 8, 1, b'a', b'A', 0, 0, 0, 1, b'Z'];
    assert_eq!(
        FieldTable::decode(&nested_array).unwrap_err(),
        Error::UnknownFieldType { field_type: b'Z' }
    );
}

#[test]
fn malformed_and_truncated_inputs_return_errors_without_panicking() {
    let corpus: &[&[u8]] = &[
        &[],
        &[0],
        &[0, 0],
        &[0, 0, 0],
        &[0, 0, 0, 1],
        &[0, 0, 0, 1, 0],
        &[0, 0, 0, 2, 5],
        &[0, 0, 0, 2, 1, b'x'],
        &[0, 0, 0, 4, 1, b'x', b'S'],
        &[0, 0, 0, 6, 1, b'x', b'S', 0, 0],
        &[0, 0, 0, 7, 1, b'x', b'S', 0, 0, 0, 9],
        &[0, 0, 0, 3, 1, b'x', b'z'],
        &[0xFF, 0xFF, 0xFF, 0xFF],
    ];

    for bytes in corpus {
        let result = panic::catch_unwind(|| FieldTable::decode(bytes));
        assert!(result.is_ok(), "decode panicked for {bytes:?}");
        assert!(result.unwrap().is_err(), "expected error for {bytes:?}");
    }
}

#[test]
fn every_type_byte_decodes_without_panicking() {
    for type_byte in 0u8..=u8::MAX {
        let bytes = [0, 0, 0, 3, 1, b'k', type_byte];
        let result = panic::catch_unwind(|| FieldTable::decode(&bytes));
        assert!(
            result.is_ok(),
            "decode panicked for type byte {type_byte:#04x}"
        );
    }
}

#[test]
fn field_table_module_and_types_compatibility_paths_resolve() {
    assert_eq!(MODULE_MAX_TABLE_DEPTH, TYPES_MAX_TABLE_DEPTH);
    assert_eq!(MODULE_MAX_TABLE_DEPTH, wolf_amqp_codec::MAX_TABLE_DEPTH);

    let mut module_table = ModuleFieldTable::new();
    module_table.insert("k", ModuleFieldValue::U8(1));

    let mut types_table = TypesFieldTable::new();
    types_table.insert("k", TypesFieldValue::U8(1));

    assert_eq!(module_table, types_table);

    let encoded = module_table.encode().unwrap();
    assert_eq!(TypesFieldTable::decode(&encoded).unwrap().0, types_table);
}
