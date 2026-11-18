use wolf_amqp_codec::properties::{
    APP_ID, CLUSTER_ID, CONTENT_ENCODING, CONTENT_TYPE, CORRELATION_ID, DELIVERY_MODE, EXPIRATION,
    HEADERS, MESSAGE_ID, PRIORITY, REPLY_TO, RESERVED_FLAGS, TIMESTAMP, TYPE, USER_ID,
};
use wolf_amqp_codec::{BasicProperties, Error, FieldTable, FieldValue, ShortString};

fn short(value: &str) -> Option<ShortString> {
    Some(ShortString::new(value))
}

fn headers_table() -> FieldTable {
    let mut headers = FieldTable::new();
    headers.insert(
        "content-type",
        FieldValue::ShortString(ShortString::new("text/plain")),
    );
    headers.insert("x-number", FieldValue::I64(-7));
    headers
}

fn all_properties() -> BasicProperties {
    BasicProperties {
        content_type: short("text/plain"),
        content_encoding: short("utf-8"),
        headers: Some(headers_table()),
        delivery_mode: Some(2),
        priority: Some(9),
        correlation_id: short("corr-1"),
        reply_to: short("reply.queue"),
        expiration: short("60000"),
        message_id: short("msg-1"),
        timestamp: Some(1_700_000_000),
        r#type: short("event"),
        user_id: short("guest"),
        app_id: short("wolf"),
        cluster_id: short("cluster-a"),
    }
}

#[test]
fn empty_properties_encode_to_zero_flags() {
    let properties = BasicProperties::default();

    assert_eq!(properties.flags(), 0);
    assert_eq!(properties.encode().unwrap(), vec![0x00, 0x00]);

    let decoded = BasicProperties::decode(&[0x00, 0x00]).unwrap();
    assert_eq!(decoded, properties);
}

#[test]
fn all_properties_round_trip_preserving_values() {
    let properties = all_properties();

    assert_eq!(properties.flags(), 0xFFFC);
    let encoded = properties.encode().unwrap();
    assert_eq!(&encoded[..2], &[0xFF, 0xFC]);

    let decoded = BasicProperties::decode(&encoded).unwrap();
    assert_eq!(decoded, properties);
    assert_eq!(decoded.flags(), 0xFFFC);
    assert_eq!(decoded.encode().unwrap(), encoded);
}

#[test]
fn property_flags_follow_amqp_bit_order() {
    let cases: Vec<(u16, BasicProperties, Vec<u8>)> = vec![
        (
            CONTENT_TYPE,
            BasicProperties {
                content_type: short("ct"),
                ..BasicProperties::default()
            },
            vec![0x80, 0x00, 2, b'c', b't'],
        ),
        (
            CONTENT_ENCODING,
            BasicProperties {
                content_encoding: short("ce"),
                ..BasicProperties::default()
            },
            vec![0x40, 0x00, 2, b'c', b'e'],
        ),
        (
            HEADERS,
            BasicProperties {
                headers: Some(FieldTable::new()),
                ..BasicProperties::default()
            },
            vec![0x20, 0x00, 0, 0, 0, 0],
        ),
        (
            DELIVERY_MODE,
            BasicProperties {
                delivery_mode: Some(1),
                ..BasicProperties::default()
            },
            vec![0x10, 0x00, 1],
        ),
        (
            PRIORITY,
            BasicProperties {
                priority: Some(2),
                ..BasicProperties::default()
            },
            vec![0x08, 0x00, 2],
        ),
        (
            CORRELATION_ID,
            BasicProperties {
                correlation_id: short("ci"),
                ..BasicProperties::default()
            },
            vec![0x04, 0x00, 2, b'c', b'i'],
        ),
        (
            REPLY_TO,
            BasicProperties {
                reply_to: short("rt"),
                ..BasicProperties::default()
            },
            vec![0x02, 0x00, 2, b'r', b't'],
        ),
        (
            EXPIRATION,
            BasicProperties {
                expiration: short("ex"),
                ..BasicProperties::default()
            },
            vec![0x01, 0x00, 2, b'e', b'x'],
        ),
        (
            MESSAGE_ID,
            BasicProperties {
                message_id: short("mi"),
                ..BasicProperties::default()
            },
            vec![0x00, 0x80, 2, b'm', b'i'],
        ),
        (
            TIMESTAMP,
            BasicProperties {
                timestamp: Some(1),
                ..BasicProperties::default()
            },
            vec![0x00, 0x40, 0, 0, 0, 0, 0, 0, 0, 1],
        ),
        (
            TYPE,
            BasicProperties {
                r#type: short("ty"),
                ..BasicProperties::default()
            },
            vec![0x00, 0x20, 2, b't', b'y'],
        ),
        (
            USER_ID,
            BasicProperties {
                user_id: short("ui"),
                ..BasicProperties::default()
            },
            vec![0x00, 0x10, 2, b'u', b'i'],
        ),
        (
            APP_ID,
            BasicProperties {
                app_id: short("ai"),
                ..BasicProperties::default()
            },
            vec![0x00, 0x08, 2, b'a', b'i'],
        ),
        (
            CLUSTER_ID,
            BasicProperties {
                cluster_id: short("cl"),
                ..BasicProperties::default()
            },
            vec![0x00, 0x04, 2, b'c', b'l'],
        ),
    ];

    assert_eq!(cases.len(), 14);
    for (expected_flag, properties, expected_bytes) in cases {
        assert_eq!(properties.flags(), expected_flag);
        let encoded = properties.encode().unwrap();
        assert_eq!(encoded, expected_bytes);
        assert_eq!(&encoded[..2], &expected_flag.to_be_bytes());
        assert_eq!(BasicProperties::decode(&encoded).unwrap(), properties);
    }
}

#[test]
fn mixed_set_order_preserves_canonical_flags_and_values() {
    let properties = BasicProperties {
        cluster_id: short("cluster-a"),
        timestamp: Some(1_700_000_000),
        content_type: short("text/plain"),
        user_id: short("guest"),
        headers: Some(headers_table()),
        priority: Some(9),
        message_id: short("msg-1"),
        content_encoding: short("utf-8"),
        expiration: short("60000"),
        delivery_mode: Some(2),
        app_id: short("wolf"),
        reply_to: short("reply.queue"),
        correlation_id: short("corr-1"),
        r#type: short("event"),
    };

    assert_eq!(properties.flags(), 0xFFFC);
    let encoded = properties.encode().unwrap();
    assert_eq!(&encoded[..2], &[0xFF, 0xFC]);

    let decoded = BasicProperties::decode(&encoded).unwrap();
    assert_eq!(decoded, properties);
    assert_eq!(decoded.flags(), 0xFFFC);
}

#[test]
fn missing_property_differs_from_empty_short_string() {
    let missing = BasicProperties::decode(&[0x00, 0x00]).unwrap();
    let empty = BasicProperties::decode(&[0x80, 0x00, 0x00]).unwrap();

    assert_eq!(missing.content_type, None);
    assert_eq!(empty.content_type, Some(ShortString::new("")));
    assert_ne!(missing.content_type, empty.content_type);
    assert_eq!(missing.flags(), 0);
    assert_eq!(empty.flags(), CONTENT_TYPE);
}

#[test]
fn missing_headers_differs_from_empty_headers_table() {
    let missing = BasicProperties::decode(&[0x00, 0x00]).unwrap();
    let empty = BasicProperties::decode(&[0x20, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap();

    assert_eq!(missing.headers, None);
    assert_eq!(empty.headers, Some(FieldTable::new()));
    assert_ne!(missing.headers, empty.headers);
    assert_eq!(empty.flags(), HEADERS);
}

#[test]
fn zero_valued_delivery_mode_and_priority_are_preserved() {
    let properties = BasicProperties {
        delivery_mode: Some(0),
        priority: Some(0),
        ..BasicProperties::default()
    };

    let encoded = properties.encode().unwrap();
    assert_eq!(encoded, vec![0x18, 0x00, 0x00, 0x00]);

    let decoded = BasicProperties::decode(&encoded).unwrap();
    assert_eq!(decoded.delivery_mode, Some(0));
    assert_eq!(decoded.priority, Some(0));
    assert_eq!(decoded, properties);
}

#[test]
fn reserved_flag_bits_are_rejected() {
    for bits in [0x0001_u16, 0x0002, 0x0003] {
        assert_eq!(
            BasicProperties::decode(&bits.to_be_bytes()).unwrap_err(),
            Error::MalformedFrame {
                reason: "reserved basic property flag bits set",
            }
        );
    }

    assert_eq!(RESERVED_FLAGS, 0x0003);
}

#[test]
fn trailing_bytes_are_rejected() {
    assert_eq!(
        BasicProperties::decode(&[0x00, 0x00, 0xFF]).unwrap_err(),
        Error::TrailingBytes { remaining: 1 }
    );

    assert_eq!(
        BasicProperties::decode(&[0x80, 0x00, 0x01, b'a', 0xFF]).unwrap_err(),
        Error::TrailingBytes { remaining: 1 }
    );
}

#[test]
fn truncated_property_value_is_incomplete() {
    let cases = vec![
        (
            vec![0x80, 0x00],
            Error::Incomplete {
                needed: 3,
                available: 2,
            },
        ),
        (
            vec![0x80, 0x00, 0x05, b'a'],
            Error::Incomplete {
                needed: 8,
                available: 4,
            },
        ),
        (
            vec![0x10, 0x00],
            Error::Incomplete {
                needed: 3,
                available: 2,
            },
        ),
        (
            vec![0x00, 0x40, 0x00],
            Error::Incomplete {
                needed: 10,
                available: 3,
            },
        ),
        (
            vec![0x20, 0x00, 0x00, 0x00, 0x00, 0x05],
            Error::Incomplete {
                needed: 11,
                available: 6,
            },
        ),
    ];

    for (bytes, expected) in cases {
        assert_eq!(BasicProperties::decode(&bytes).unwrap_err(), expected);
    }
}
