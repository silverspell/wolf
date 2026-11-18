use bytes::Bytes;
use wolf_amqp_codec::{
    Error, ExchangeDeclare, ExchangeDeclareOk, ExchangeDelete, ExchangeDeleteOk, ExchangeMethod,
    FieldTable, FieldValue, Method, ShortString, exchange,
};

fn sample_methods() -> Vec<(ExchangeMethod, u16)> {
    let mut arguments = FieldTable::new();
    arguments.insert("x-match", FieldValue::ShortString(ShortString::new("all")));

    vec![
        (
            ExchangeMethod::Declare(ExchangeDeclare {
                ticket: 1,
                exchange: ShortString::new("events"),
                exchange_type: ShortString::new("topic"),
                passive: false,
                durable: true,
                auto_delete: false,
                internal: true,
                nowait: false,
                arguments,
            }),
            exchange::DECLARE,
        ),
        (
            ExchangeMethod::DeclareOk(ExchangeDeclareOk),
            exchange::DECLARE_OK,
        ),
        (
            ExchangeMethod::Delete(ExchangeDelete {
                ticket: 2,
                exchange: ShortString::new("events"),
                if_unused: true,
                nowait: false,
            }),
            exchange::DELETE,
        ),
        (
            ExchangeMethod::DeleteOk(ExchangeDeleteOk),
            exchange::DELETE_OK,
        ),
    ]
}

fn sample_arguments() -> FieldTable {
    let mut nested = FieldTable::new();
    nested.insert("enabled", FieldValue::Bool(true));
    nested.insert("limit", FieldValue::I32(42));

    let mut arguments = FieldTable::new();
    arguments.insert("x-message-ttl", FieldValue::I64(60_000));
    arguments.insert(
        "x-dead-letter-exchange",
        FieldValue::ShortString(ShortString::new("dlx")),
    );
    arguments.insert("nested", FieldValue::FieldTable(nested));
    arguments.insert(
        "codes",
        FieldValue::FieldArray(vec![FieldValue::U8(1), FieldValue::U8(2)]),
    );
    arguments
}

fn declared_with(bits: u8) -> ExchangeDeclare {
    ExchangeDeclare {
        ticket: 0,
        exchange: ShortString::new("ex"),
        exchange_type: ShortString::new("direct"),
        passive: bits & 0x01 != 0,
        durable: bits & 0x02 != 0,
        auto_delete: bits & 0x04 != 0,
        internal: bits & 0x08 != 0,
        nowait: bits & 0x10 != 0,
        arguments: FieldTable::new(),
    }
}

fn deleted_with(bits: u8) -> ExchangeDelete {
    ExchangeDelete {
        ticket: 0,
        exchange: ShortString::new("ex"),
        if_unused: bits & 0x01 != 0,
        nowait: bits & 0x02 != 0,
    }
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        (
            exchange::CLASS_ID,
            exchange::DECLARE,
            exchange::DECLARE_OK,
            exchange::DELETE,
            exchange::DELETE_OK,
        ),
        (40, 10, 11, 20, 21)
    );
}

#[test]
fn declare_encodes_exact_wire_bytes() {
    let method = Method::Exchange(ExchangeMethod::Declare(ExchangeDeclare {
        ticket: 1,
        exchange: ShortString::new("ex"),
        exchange_type: ShortString::new("direct"),
        passive: true,
        durable: false,
        auto_delete: true,
        internal: false,
        nowait: true,
        arguments: FieldTable::new(),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x28, 0x00, 0x0A, // class 40, method 10
            0x00, 0x01, // ticket
            0x02, b'e', b'x', // exchange
            0x06, b'd', b'i', b'r', b'e', b'c', b't', // type
            0x15, // passive | auto-delete | nowait
            0x00, 0x00, 0x00, 0x00, // empty arguments
        ]
    );
}

#[test]
fn delete_encodes_exact_wire_bytes_without_arguments() {
    let method = Method::Exchange(ExchangeMethod::Delete(ExchangeDelete {
        ticket: 7,
        exchange: ShortString::new("ex"),
        if_unused: true,
        nowait: false,
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x28, 0x00, 0x14, 0x00, 0x07, 0x02, b'e', b'x', 0x01]
    );
}

#[test]
fn declare_ok_and_delete_ok_encode_empty_bodies() {
    assert_eq!(
        Method::Exchange(ExchangeMethod::DeclareOk(ExchangeDeclareOk)).encode(),
        vec![0x00, 0x28, 0x00, 0x0B]
    );
    assert_eq!(
        Method::Exchange(ExchangeMethod::DeleteOk(ExchangeDeleteOk)).encode(),
        vec![0x00, 0x28, 0x00, 0x15]
    );
}

#[test]
fn declare_bit_octet_preserves_every_mask() {
    for mask in 0..=0x1Fu8 {
        let declare = declared_with(mask);
        let encoded = Method::Exchange(ExchangeMethod::Declare(declare.clone())).encode();
        assert_eq!(encoded[16], mask, "packed octet for mask {mask:#07b}");

        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        let expected = Method::Exchange(ExchangeMethod::Declare(declared_with(mask)));
        assert_eq!(decoded, expected);

        let Method::Exchange(ExchangeMethod::Declare(decoded_declare)) = decoded else {
            panic!("expected exchange.declare");
        };
        assert_eq!(decoded_declare.passive, mask & 0x01 != 0);
        assert_eq!(decoded_declare.durable, mask & 0x02 != 0);
        assert_eq!(decoded_declare.auto_delete, mask & 0x04 != 0);
        assert_eq!(decoded_declare.internal, mask & 0x08 != 0);
        assert_eq!(decoded_declare.nowait, mask & 0x10 != 0);
    }
}

#[test]
fn delete_bit_octet_preserves_every_mask() {
    for mask in 0..=0x03u8 {
        let delete = deleted_with(mask);
        let encoded = Method::Exchange(ExchangeMethod::Delete(delete.clone())).encode();
        assert_eq!(encoded[9], mask, "packed octet for mask {mask:#04b}");

        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        let expected = Method::Exchange(ExchangeMethod::Delete(deleted_with(mask)));
        assert_eq!(decoded, expected);

        let Method::Exchange(ExchangeMethod::Delete(decoded_delete)) = decoded else {
            panic!("expected exchange.delete");
        };
        assert_eq!(decoded_delete.if_unused, mask & 0x01 != 0);
        assert_eq!(decoded_delete.nowait, mask & 0x02 != 0);
    }
}

#[test]
fn declare_arguments_round_trip_as_field_table() {
    let method = Method::Exchange(ExchangeMethod::Declare(ExchangeDeclare {
        ticket: 0,
        exchange: ShortString::new("events"),
        exchange_type: ShortString::new("headers"),
        passive: false,
        durable: true,
        auto_delete: false,
        internal: false,
        nowait: false,
        arguments: sample_arguments(),
    }));

    let encoded = method.encode();
    let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
    assert_eq!(decoded, method);

    let Method::Exchange(ExchangeMethod::Declare(ref declare)) = decoded else {
        panic!("expected exchange.declare");
    };
    assert_eq!(declare.arguments, sample_arguments());
    assert_eq!(
        declare.arguments.get("x-message-ttl"),
        Some(&FieldValue::I64(60_000))
    );
    assert_eq!(decoded.try_encode().unwrap(), encoded);
}

#[test]
fn all_exchange_methods_round_trip_with_all_fields() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), exchange::CLASS_ID);
        assert_eq!(expected.method_id(), method_id);

        let encoded = expected.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.try_encode().unwrap(), encoded);
    }
}

#[test]
fn unknown_method_id_is_an_error() {
    let error = Method::decode(&Bytes::from_static(&[0x00, 0x28, 0x00, 0x63])).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 40,
            method_id: 99,
        }
    );
}
