use bytes::Bytes;
use wolf_amqp_codec::{
    Error, FieldTable, FieldValue, Method, QueueBind, QueueBindOk, QueueDeclare, QueueDeclareOk,
    QueueDelete, QueueDeleteOk, QueueMethod, QueuePurge, QueuePurgeOk, QueueUnbind, QueueUnbindOk,
    ShortString, queue,
};

fn sample_methods() -> Vec<(QueueMethod, u16)> {
    let mut arguments = FieldTable::new();
    arguments.insert("x-max-priority", FieldValue::I32(10));

    vec![
        (
            QueueMethod::Declare(QueueDeclare {
                ticket: 0,
                queue: ShortString::new("work"),
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: arguments.clone(),
            }),
            queue::DECLARE,
        ),
        (
            QueueMethod::DeclareOk(QueueDeclareOk {
                queue: ShortString::new("work"),
                message_count: 3,
                consumer_count: 1,
            }),
            queue::DECLARE_OK,
        ),
        (
            QueueMethod::Bind(QueueBind {
                ticket: 0,
                queue: ShortString::new("work"),
                exchange: ShortString::new("events"),
                routing_key: ShortString::new("jobs"),
                nowait: true,
                arguments: arguments.clone(),
            }),
            queue::BIND,
        ),
        (QueueMethod::BindOk(QueueBindOk), queue::BIND_OK),
        (
            QueueMethod::Purge(QueuePurge {
                ticket: 0,
                queue: ShortString::new("work"),
                nowait: false,
            }),
            queue::PURGE,
        ),
        (
            QueueMethod::PurgeOk(QueuePurgeOk { message_count: 5 }),
            queue::PURGE_OK,
        ),
        (
            QueueMethod::Delete(QueueDelete {
                ticket: 0,
                queue: ShortString::new("work"),
                if_unused: true,
                if_empty: false,
                nowait: true,
            }),
            queue::DELETE,
        ),
        (
            QueueMethod::DeleteOk(QueueDeleteOk { message_count: 2 }),
            queue::DELETE_OK,
        ),
        (
            QueueMethod::Unbind(QueueUnbind {
                ticket: 0,
                queue: ShortString::new("work"),
                exchange: ShortString::new("events"),
                routing_key: ShortString::new("jobs"),
                arguments,
            }),
            queue::UNBIND,
        ),
        (QueueMethod::UnbindOk(QueueUnbindOk), queue::UNBIND_OK),
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

fn declared_with(bits: u8) -> QueueDeclare {
    QueueDeclare {
        ticket: 0,
        queue: ShortString::new("q"),
        passive: bits & 0x01 != 0,
        durable: bits & 0x02 != 0,
        exclusive: bits & 0x04 != 0,
        auto_delete: bits & 0x08 != 0,
        nowait: bits & 0x10 != 0,
        arguments: FieldTable::new(),
    }
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        (
            queue::CLASS_ID,
            queue::DECLARE,
            queue::DECLARE_OK,
            queue::BIND,
            queue::BIND_OK,
            queue::PURGE,
            queue::PURGE_OK,
            queue::DELETE,
            queue::DELETE_OK,
            queue::UNBIND,
            queue::UNBIND_OK,
        ),
        (50, 10, 11, 20, 21, 30, 31, 40, 41, 50, 51)
    );
}

#[test]
fn declare_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::Declare(QueueDeclare {
        ticket: 1,
        queue: ShortString::new("q"),
        passive: true,
        durable: false,
        exclusive: true,
        auto_delete: false,
        nowait: true,
        arguments: FieldTable::new(),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x32, 0x00, 0x0A, // class 50, method 10
            0x00, 0x01, // ticket
            0x01, b'q', // queue
            0x15, // passive | exclusive | nowait
            0x00, 0x00, 0x00, 0x00, // empty arguments
        ]
    );
}

#[test]
fn declare_ok_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::DeclareOk(QueueDeclareOk {
        queue: ShortString::new("q"),
        message_count: 7,
        consumer_count: 2,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x32, 0x00, 0x0B, // class 50, method 11
            0x01, b'q', // queue
            0x00, 0x00, 0x00, 0x07, // message count
            0x00, 0x00, 0x00, 0x02, // consumer count
        ]
    );
}

#[test]
fn bind_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::Bind(QueueBind {
        ticket: 1,
        queue: ShortString::new("q"),
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        nowait: true,
        arguments: FieldTable::new(),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x32, 0x00, 0x14, // class 50, method 20
            0x00, 0x01, // ticket
            0x01, b'q', // queue
            0x02, b'e', b'x', // exchange
            0x02, b'r', b'k', // routing key
            0x01, // nowait
            0x00, 0x00, 0x00, 0x00, // empty arguments
        ]
    );
}

#[test]
fn bind_ok_encodes_exact_wire_bytes() {
    assert_eq!(
        Method::Queue(QueueMethod::BindOk(QueueBindOk)).encode(),
        vec![0x00, 0x32, 0x00, 0x15]
    );
}

#[test]
fn purge_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::Purge(QueuePurge {
        ticket: 1,
        queue: ShortString::new("q"),
        nowait: true,
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x32, 0x00, 0x1E, 0x00, 0x01, 0x01, b'q', 0x01]
    );
}

#[test]
fn purge_ok_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::PurgeOk(QueuePurgeOk { message_count: 9 }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x32, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x09]
    );
}

#[test]
fn delete_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::Delete(QueueDelete {
        ticket: 1,
        queue: ShortString::new("q"),
        if_unused: true,
        if_empty: false,
        nowait: true,
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x32, 0x00, 0x28, 0x00, 0x01, 0x01, b'q', 0x05]
    );
}

#[test]
fn delete_ok_encodes_exact_wire_bytes() {
    let method = Method::Queue(QueueMethod::DeleteOk(QueueDeleteOk { message_count: 4 }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x32, 0x00, 0x29, 0x00, 0x00, 0x00, 0x04]
    );
}

#[test]
fn unbind_encodes_exact_wire_bytes_without_nowait() {
    let method = Method::Queue(QueueMethod::Unbind(QueueUnbind {
        ticket: 1,
        queue: ShortString::new("q"),
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        arguments: FieldTable::new(),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x32, 0x00, 0x32, // class 50, method 50
            0x00, 0x01, // ticket
            0x01, b'q', // queue
            0x02, b'e', b'x', // exchange
            0x02, b'r', b'k', // routing key
            0x00, 0x00, 0x00, 0x00, // empty arguments, no nowait bit
        ]
    );
}

#[test]
fn unbind_ok_encodes_exact_wire_bytes() {
    assert_eq!(
        Method::Queue(QueueMethod::UnbindOk(QueueUnbindOk)).encode(),
        vec![0x00, 0x32, 0x00, 0x33]
    );
}

#[test]
fn declare_bit_octet_preserves_every_mask() {
    for mask in 0..=0x1Fu8 {
        let declare = declared_with(mask);
        let encoded = Method::Queue(QueueMethod::Declare(declare.clone())).encode();
        assert_eq!(encoded[8], mask, "packed octet for mask {mask:#07b}");

        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(
            decoded,
            Method::Queue(QueueMethod::Declare(declared_with(mask)))
        );

        let Method::Queue(QueueMethod::Declare(decoded_declare)) = decoded else {
            panic!("expected queue.declare");
        };
        assert_eq!(decoded_declare.passive, mask & 0x01 != 0);
        assert_eq!(decoded_declare.durable, mask & 0x02 != 0);
        assert_eq!(decoded_declare.exclusive, mask & 0x04 != 0);
        assert_eq!(decoded_declare.auto_delete, mask & 0x08 != 0);
        assert_eq!(decoded_declare.nowait, mask & 0x10 != 0);
    }
}

#[test]
fn bind_purge_delete_bit_octets_preserve_values() {
    for nowait in [false, true] {
        let bind = QueueBind {
            ticket: 0,
            queue: ShortString::new("q"),
            exchange: ShortString::new("ex"),
            routing_key: ShortString::new("rk"),
            nowait,
            arguments: FieldTable::new(),
        };
        let encoded = Method::Queue(QueueMethod::Bind(bind.clone())).encode();
        assert_eq!(encoded[14], u8::from(nowait));
        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Queue(QueueMethod::Bind(bind)));

        let purge = QueuePurge {
            ticket: 0,
            queue: ShortString::new("q"),
            nowait,
        };
        let encoded = Method::Queue(QueueMethod::Purge(purge.clone())).encode();
        assert_eq!(encoded[8], u8::from(nowait));
        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Queue(QueueMethod::Purge(purge)));
    }

    for mask in 0..=0x07u8 {
        let delete = QueueDelete {
            ticket: 0,
            queue: ShortString::new("q"),
            if_unused: mask & 0x01 != 0,
            if_empty: mask & 0x02 != 0,
            nowait: mask & 0x04 != 0,
        };
        let encoded = Method::Queue(QueueMethod::Delete(delete.clone())).encode();
        assert_eq!(encoded[8], mask, "packed octet for mask {mask:#05b}");
        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Queue(QueueMethod::Delete(delete)));
    }
}

#[test]
fn arguments_round_trip_as_field_table() {
    let declare = Method::Queue(QueueMethod::Declare(QueueDeclare {
        ticket: 0,
        queue: ShortString::new("q"),
        passive: false,
        durable: true,
        exclusive: false,
        auto_delete: false,
        nowait: false,
        arguments: sample_arguments(),
    }));
    let bind = Method::Queue(QueueMethod::Bind(QueueBind {
        ticket: 0,
        queue: ShortString::new("q"),
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        nowait: false,
        arguments: sample_arguments(),
    }));
    let unbind = Method::Queue(QueueMethod::Unbind(QueueUnbind {
        ticket: 0,
        queue: ShortString::new("q"),
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        arguments: sample_arguments(),
    }));

    for method in [declare, bind, unbind] {
        let encoded = method.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, method);
        assert_eq!(decoded.try_encode().unwrap(), encoded);

        let arguments = match &decoded {
            Method::Queue(QueueMethod::Declare(method)) => &method.arguments,
            Method::Queue(QueueMethod::Bind(method)) => &method.arguments,
            Method::Queue(QueueMethod::Unbind(method)) => &method.arguments,
            other => panic!("unexpected method {other:?}"),
        };
        assert_eq!(arguments, &sample_arguments());
        assert_eq!(
            arguments.get("x-message-ttl"),
            Some(&FieldValue::I64(60_000))
        );
    }
}

#[test]
fn all_queue_methods_round_trip_with_all_fields() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), queue::CLASS_ID);
        assert_eq!(expected.method_id(), method_id);

        let encoded = expected.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.try_encode().unwrap(), encoded);
    }
}

#[test]
fn unknown_method_id_is_an_error() {
    let error = Method::decode(&Bytes::from_static(&[0x00, 0x32, 0x00, 0x63])).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 50,
            method_id: 99,
        }
    );
}
