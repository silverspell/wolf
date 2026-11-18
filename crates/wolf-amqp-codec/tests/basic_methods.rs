use bytes::Bytes;
use wolf_amqp_codec::{
    BasicAck, BasicCancel, BasicCancelOk, BasicConsume, BasicConsumeOk, BasicDeliver, BasicGet,
    BasicGetEmpty, BasicGetOk, BasicMethod, BasicNack, BasicPublish, BasicQos, BasicQosOk,
    BasicRecover, BasicRecoverAsync, BasicRecoverOk, BasicReject, BasicReturn, Error, FieldTable,
    FieldValue, Method, ShortString, basic, basic_ack, basic_consume, basic_publish, basic_recover,
};

fn sample_arguments() -> FieldTable {
    let mut nested = FieldTable::new();
    nested.insert("exclusive", FieldValue::Bool(true));

    let mut arguments = FieldTable::new();
    arguments.insert("x-priority", FieldValue::I32(5));
    arguments.insert(
        "x-dead-letter-routing-key",
        FieldValue::ShortString(ShortString::new("dead")),
    );
    arguments.insert("nested", FieldValue::FieldTable(nested));
    arguments
}

fn consume_with(bits: u8) -> BasicConsume {
    BasicConsume {
        ticket: 0,
        queue: ShortString::new("q"),
        consumer_tag: ShortString::new("ct"),
        no_local: bits & 0x01 != 0,
        no_ack: bits & 0x02 != 0,
        exclusive: bits & 0x04 != 0,
        nowait: bits & 0x08 != 0,
        arguments: FieldTable::new(),
    }
}

fn nack_with(bits: u8) -> BasicNack {
    BasicNack {
        delivery_tag: 0x0102_0304_0506_0708,
        multiple: bits & 0x01 != 0,
        requeue: bits & 0x02 != 0,
    }
}

fn sample_methods() -> Vec<(BasicMethod, u16)> {
    let delivery_tag = 0x0102_0304_0506_0708;

    vec![
        (
            BasicMethod::Qos(BasicQos {
                prefetch_size: 1024,
                prefetch_count: 20,
                global: true,
            }),
            basic_consume::QOS,
        ),
        (BasicMethod::QosOk(BasicQosOk), basic_consume::QOS_OK),
        (
            BasicMethod::Consume(BasicConsume {
                ticket: 7,
                queue: ShortString::new("work"),
                consumer_tag: ShortString::new("ctag"),
                no_local: true,
                no_ack: true,
                exclusive: true,
                nowait: true,
                arguments: sample_arguments(),
            }),
            basic_consume::CONSUME,
        ),
        (
            BasicMethod::ConsumeOk(BasicConsumeOk {
                consumer_tag: ShortString::new("ctag"),
            }),
            basic_consume::CONSUME_OK,
        ),
        (
            BasicMethod::Cancel(BasicCancel {
                consumer_tag: ShortString::new("ctag"),
                nowait: true,
            }),
            basic_consume::CANCEL,
        ),
        (
            BasicMethod::CancelOk(BasicCancelOk {
                consumer_tag: ShortString::new("ctag"),
            }),
            basic_consume::CANCEL_OK,
        ),
        (
            BasicMethod::Publish(BasicPublish {
                ticket: 7,
                exchange: ShortString::new("events"),
                routing_key: ShortString::new("jobs"),
                mandatory: true,
                immediate: true,
            }),
            basic_publish::PUBLISH,
        ),
        (
            BasicMethod::Return(BasicReturn {
                reply_code: 312,
                reply_text: ShortString::new("NO_ROUTE"),
                exchange: ShortString::new("events"),
                routing_key: ShortString::new("jobs"),
            }),
            basic_publish::RETURN,
        ),
        (
            BasicMethod::Deliver(BasicDeliver {
                consumer_tag: ShortString::new("ctag"),
                delivery_tag,
                redelivered: true,
                exchange: ShortString::new("events"),
                routing_key: ShortString::new("jobs"),
            }),
            basic_consume::DELIVER,
        ),
        (
            BasicMethod::Get(BasicGet {
                ticket: 7,
                queue: ShortString::new("work"),
                no_ack: true,
            }),
            basic_consume::GET,
        ),
        (
            BasicMethod::GetOk(BasicGetOk {
                delivery_tag,
                redelivered: true,
                exchange: ShortString::new("events"),
                routing_key: ShortString::new("jobs"),
                message_count: 3,
            }),
            basic_consume::GET_OK,
        ),
        (
            BasicMethod::GetEmpty(BasicGetEmpty {
                cluster_id: ShortString::new("cluster"),
            }),
            basic_consume::GET_EMPTY,
        ),
        (
            BasicMethod::Ack(BasicAck {
                delivery_tag,
                multiple: true,
            }),
            basic_ack::ACK,
        ),
        (
            BasicMethod::Reject(BasicReject {
                delivery_tag,
                requeue: true,
            }),
            basic_ack::REJECT,
        ),
        (
            BasicMethod::Nack(BasicNack {
                delivery_tag,
                multiple: true,
                requeue: true,
            }),
            basic_ack::NACK,
        ),
    ]
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        [
            basic::CLASS_ID,
            basic_consume::QOS,
            basic_consume::QOS_OK,
            basic_consume::CONSUME,
            basic_consume::CONSUME_OK,
            basic_consume::CANCEL,
            basic_consume::CANCEL_OK,
            basic_consume::DELIVER,
            basic_consume::GET,
            basic_consume::GET_OK,
            basic_consume::GET_EMPTY,
            basic_ack::ACK,
            basic_ack::REJECT,
            basic_ack::NACK,
            basic_publish::PUBLISH,
            basic_publish::RETURN,
        ],
        [
            60, 10, 11, 20, 21, 30, 31, 60, 70, 71, 72, 80, 90, 120, 40, 50
        ]
    );
}

#[test]
fn qos_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Qos(BasicQos {
        prefetch_size: 0x0000_0100,
        prefetch_count: 10,
        global: true,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x0A, // class 60, method 10
            0x00, 0x00, 0x01, 0x00, // prefetch size
            0x00, 0x0A, // prefetch count
            0x01, // global
        ]
    );
}

#[test]
fn qos_ok_encodes_exact_wire_bytes() {
    assert_eq!(
        Method::Basic(BasicMethod::QosOk(BasicQosOk)).encode(),
        vec![0x00, 0x3C, 0x00, 0x0B]
    );
}

#[test]
fn consume_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Consume(BasicConsume {
        ticket: 1,
        queue: ShortString::new("q"),
        consumer_tag: ShortString::new("ct"),
        no_local: true,
        no_ack: false,
        exclusive: true,
        nowait: false,
        arguments: FieldTable::new(),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x14, // class 60, method 20
            0x00, 0x01, // ticket
            0x01, b'q', // queue
            0x02, b'c', b't', // consumer tag
            0x05, // no-local | exclusive
            0x00, 0x00, 0x00, 0x00, // empty arguments
        ]
    );
}

#[test]
fn consume_ok_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::ConsumeOk(BasicConsumeOk {
        consumer_tag: ShortString::new("ct"),
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x3C, 0x00, 0x15, 0x02, b'c', b't']
    );
}

#[test]
fn cancel_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Cancel(BasicCancel {
        consumer_tag: ShortString::new("ct"),
        nowait: true,
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x3C, 0x00, 0x1E, 0x02, b'c', b't', 0x01]
    );
}

#[test]
fn cancel_ok_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::CancelOk(BasicCancelOk {
        consumer_tag: ShortString::new("ct"),
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x3C, 0x00, 0x1F, 0x02, b'c', b't']
    );
}

#[test]
fn publish_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Publish(BasicPublish {
        ticket: 1,
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        mandatory: true,
        immediate: false,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x28, // class 60, method 40
            0x00, 0x01, // ticket
            0x02, b'e', b'x', // exchange
            0x02, b'r', b'k', // routing key
            0x01, // mandatory
        ]
    );
}

#[test]
fn publish_flag_octet_preserves_every_mask() {
    for mask in 0..=0x03u8 {
        let publish = BasicPublish {
            ticket: 1,
            exchange: ShortString::new("ex"),
            routing_key: ShortString::new("rk"),
            mandatory: mask & 0x01 != 0,
            immediate: mask & 0x02 != 0,
        };
        let encoded = Method::Basic(BasicMethod::Publish(publish.clone())).encode();
        assert_eq!(encoded[12], mask, "packed octet for mask {mask:#04b}");

        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Basic(BasicMethod::Publish(publish)));

        let Method::Basic(BasicMethod::Publish(decoded_publish)) = decoded else {
            panic!("expected basic.publish");
        };
        assert_eq!(decoded_publish.mandatory, mask & 0x01 != 0);
        assert_eq!(decoded_publish.immediate, mask & 0x02 != 0);
    }
}

#[test]
fn return_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Return(BasicReturn {
        reply_code: 312,
        reply_text: ShortString::new("NO_ROUTE"),
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x32, // class 60, method 50
            0x01, 0x38, // reply code 312
            0x08, b'N', b'O', b'_', b'R', b'O', b'U', b'T', b'E', // reply text
            0x02, b'e', b'x', // exchange
            0x02, b'r', b'k', // routing key
        ]
    );
}

#[test]
fn deliver_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Deliver(BasicDeliver {
        consumer_tag: ShortString::new("ct"),
        delivery_tag: 0x0102_0304_0506_0708,
        redelivered: true,
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x3C, // class 60, method 60
            0x02, b'c', b't', // consumer tag
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // delivery tag
            0x01, // redelivered
            0x02, b'e', b'x', // exchange
            0x02, b'r', b'k', // routing key
        ]
    );
}

#[test]
fn get_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Get(BasicGet {
        ticket: 1,
        queue: ShortString::new("q"),
        no_ack: true,
    }));
    assert_eq!(
        method.encode(),
        vec![0x00, 0x3C, 0x00, 0x46, 0x00, 0x01, 0x01, b'q', 0x01]
    );
}

#[test]
fn get_ok_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::GetOk(BasicGetOk {
        delivery_tag: 0x0102_0304_0506_0708,
        redelivered: true,
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        message_count: 3,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x47, // class 60, method 71
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // delivery tag
            0x01, // redelivered
            0x02, b'e', b'x', // exchange
            0x02, b'r', b'k', // routing key
            0x00, 0x00, 0x00, 0x03, // message count
        ]
    );
}

#[test]
fn get_empty_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::GetEmpty(BasicGetEmpty {
        cluster_id: ShortString::new("cluster"),
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x48, // class 60, method 72
            0x07, b'c', b'l', b'u', b's', b't', b'e', b'r', // cluster id
        ]
    );
}

#[test]
fn ack_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Ack(BasicAck {
        delivery_tag: 0x0102_0304_0506_0708,
        multiple: true,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x50, // class 60, method 80
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // delivery tag
            0x01, // multiple
        ]
    );
}

#[test]
fn reject_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Reject(BasicReject {
        delivery_tag: 0x0102_0304_0506_0708,
        requeue: true,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x5A, // class 60, method 90
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // delivery tag
            0x01, // requeue
        ]
    );
}

#[test]
fn nack_encodes_exact_wire_bytes() {
    let method = Method::Basic(BasicMethod::Nack(BasicNack {
        delivery_tag: 0x0102_0304_0506_0708,
        multiple: true,
        requeue: true,
    }));
    assert_eq!(
        method.encode(),
        vec![
            0x00, 0x3C, 0x00, 0x78, // class 60, method 120
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // delivery tag
            0x03, // multiple | requeue
        ]
    );
}

#[test]
fn qos_global_bit_preserves_both_values() {
    for global in [false, true] {
        let qos = BasicQos {
            prefetch_size: 0,
            prefetch_count: 0,
            global,
        };
        let encoded = Method::Basic(BasicMethod::Qos(qos.clone())).encode();
        assert_eq!(encoded[10], u8::from(global));
        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Basic(BasicMethod::Qos(qos)));
    }
}

#[test]
fn consume_bit_octet_preserves_every_mask() {
    for mask in 0..=0x0Fu8 {
        let consume = consume_with(mask);
        let encoded = Method::Basic(BasicMethod::Consume(consume.clone())).encode();
        assert_eq!(encoded[11], mask, "packed octet for mask {mask:#06b}");

        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Basic(BasicMethod::Consume(consume)));

        let Method::Basic(BasicMethod::Consume(decoded_consume)) = decoded else {
            panic!("expected basic.consume");
        };
        assert_eq!(decoded_consume.no_local, mask & 0x01 != 0);
        assert_eq!(decoded_consume.no_ack, mask & 0x02 != 0);
        assert_eq!(decoded_consume.exclusive, mask & 0x04 != 0);
        assert_eq!(decoded_consume.nowait, mask & 0x08 != 0);
    }
}

#[test]
fn cancel_deliver_get_and_get_ok_bits_preserve_values() {
    for nowait in [false, true] {
        let cancel = BasicCancel {
            consumer_tag: ShortString::new("ct"),
            nowait,
        };
        let encoded = Method::Basic(BasicMethod::Cancel(cancel.clone())).encode();
        assert_eq!(encoded[7], u8::from(nowait));
        assert_eq!(
            Method::decode(&Bytes::from(encoded)).unwrap(),
            Method::Basic(BasicMethod::Cancel(cancel))
        );

        let get = BasicGet {
            ticket: 0,
            queue: ShortString::new("q"),
            no_ack: nowait,
        };
        let encoded = Method::Basic(BasicMethod::Get(get.clone())).encode();
        assert_eq!(encoded[8], u8::from(nowait));
        assert_eq!(
            Method::decode(&Bytes::from(encoded)).unwrap(),
            Method::Basic(BasicMethod::Get(get))
        );

        let deliver = BasicDeliver {
            consumer_tag: ShortString::new("ct"),
            delivery_tag: 1,
            redelivered: nowait,
            exchange: ShortString::new("ex"),
            routing_key: ShortString::new("rk"),
        };
        let encoded = Method::Basic(BasicMethod::Deliver(deliver.clone())).encode();
        assert_eq!(encoded[15], u8::from(nowait));
        assert_eq!(
            Method::decode(&Bytes::from(encoded)).unwrap(),
            Method::Basic(BasicMethod::Deliver(deliver))
        );

        let get_ok = BasicGetOk {
            delivery_tag: 1,
            redelivered: nowait,
            exchange: ShortString::new("ex"),
            routing_key: ShortString::new("rk"),
            message_count: 1,
        };
        let encoded = Method::Basic(BasicMethod::GetOk(get_ok.clone())).encode();
        assert_eq!(encoded[12], u8::from(nowait));
        assert_eq!(
            Method::decode(&Bytes::from(encoded)).unwrap(),
            Method::Basic(BasicMethod::GetOk(get_ok))
        );
    }
}

#[test]
fn ack_and_reject_bits_preserve_values() {
    for bit in [false, true] {
        let ack = BasicAck {
            delivery_tag: 1,
            multiple: bit,
        };
        let encoded = Method::Basic(BasicMethod::Ack(ack.clone())).encode();
        assert_eq!(encoded[12], u8::from(bit));
        assert_eq!(
            Method::decode(&Bytes::from(encoded)).unwrap(),
            Method::Basic(BasicMethod::Ack(ack))
        );

        let reject = BasicReject {
            delivery_tag: 1,
            requeue: bit,
        };
        let encoded = Method::Basic(BasicMethod::Reject(reject.clone())).encode();
        assert_eq!(encoded[12], u8::from(bit));
        assert_eq!(
            Method::decode(&Bytes::from(encoded)).unwrap(),
            Method::Basic(BasicMethod::Reject(reject))
        );
    }
}

#[test]
fn nack_bit_octet_preserves_every_mask() {
    for mask in 0..=0x03u8 {
        let nack = nack_with(mask);
        let encoded = Method::Basic(BasicMethod::Nack(nack.clone())).encode();
        assert_eq!(encoded[12], mask, "packed octet for mask {mask:#04b}");

        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();
        assert_eq!(decoded, Method::Basic(BasicMethod::Nack(nack)));

        let Method::Basic(BasicMethod::Nack(decoded_nack)) = decoded else {
            panic!("expected basic.nack");
        };
        assert_eq!(decoded_nack.multiple, mask & 0x01 != 0);
        assert_eq!(decoded_nack.requeue, mask & 0x02 != 0);
    }
}

#[test]
fn all_basic_methods_round_trip_with_all_fields() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), basic::CLASS_ID);
        assert_eq!(expected.method_id(), method_id);

        let encoded = expected.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.try_encode().unwrap(), encoded);
    }
}

#[test]
fn consume_arguments_round_trip_as_field_table() {
    let method = Method::Basic(BasicMethod::Consume(BasicConsume {
        ticket: 0,
        queue: ShortString::new("q"),
        consumer_tag: ShortString::new("ct"),
        no_local: false,
        no_ack: false,
        exclusive: false,
        nowait: false,
        arguments: sample_arguments(),
    }));

    let encoded = method.encode();
    let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
    assert_eq!(decoded, method);
    assert_eq!(decoded.try_encode().unwrap(), encoded);

    let Method::Basic(BasicMethod::Consume(consume)) = decoded else {
        panic!("expected basic.consume");
    };
    assert_eq!(consume.arguments, sample_arguments());
    assert_eq!(
        consume.arguments.get("x-priority"),
        Some(&FieldValue::I32(5))
    );
}

#[test]
fn get_ok_and_get_empty_decode_to_distinct_variants() {
    let get_ok = Method::Basic(BasicMethod::GetOk(BasicGetOk {
        delivery_tag: 9,
        redelivered: false,
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        message_count: 0,
    }));
    let get_empty = Method::Basic(BasicMethod::GetEmpty(BasicGetEmpty {
        cluster_id: ShortString::new(""),
    }));

    assert_ne!(get_ok, get_empty);
    assert_eq!(
        Method::decode(&Bytes::from(get_ok.encode())).unwrap(),
        get_ok
    );
    assert_eq!(
        Method::decode(&Bytes::from(get_empty.encode())).unwrap(),
        get_empty
    );
}

#[test]
fn truncated_get_empty_is_incomplete_without_panicking() {
    let error =
        Method::decode(&Bytes::from_static(&[0x00, 0x3C, 0x00, 0x48, 0x05, b'c'])).unwrap_err();
    assert!(matches!(error, Error::Incomplete { .. }), "got {error:?}");
}

#[test]
fn unknown_method_id_is_an_error() {
    let error = Method::decode(&Bytes::from_static(&[0x00, 0x3C, 0x00, 0x63])).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 60,
            method_id: 99,
        }
    );
}

#[test]
fn recover_method_ids_and_exact_wire_bytes() {
    assert_eq!(
        [
            basic_recover::RECOVER_ASYNC,
            basic_recover::RECOVER,
            basic_recover::RECOVER_OK,
        ],
        [100, 110, 111]
    );

    assert_eq!(
        Method::Basic(BasicMethod::RecoverAsync(BasicRecoverAsync {
            requeue: true,
        }))
        .encode(),
        vec![0x00, 0x3C, 0x00, 0x64, 0x01]
    );
    assert_eq!(
        Method::Basic(BasicMethod::Recover(BasicRecover { requeue: true })).encode(),
        vec![0x00, 0x3C, 0x00, 0x6E, 0x01]
    );
    assert_eq!(
        Method::Basic(BasicMethod::RecoverOk(BasicRecoverOk)).encode(),
        vec![0x00, 0x3C, 0x00, 0x6F]
    );
}

#[test]
fn recover_methods_round_trip_every_requeue_value() {
    for requeue in [false, true] {
        let async_method = Method::Basic(BasicMethod::RecoverAsync(BasicRecoverAsync { requeue }));
        let encoded = async_method.encode();
        assert_eq!(encoded[4], u8::from(requeue));
        assert_eq!(Method::decode(&Bytes::from(encoded)).unwrap(), async_method);

        let sync_method = Method::Basic(BasicMethod::Recover(BasicRecover { requeue }));
        let encoded = sync_method.encode();
        assert_eq!(encoded[4], u8::from(requeue));
        assert_eq!(Method::decode(&Bytes::from(encoded)).unwrap(), sync_method);
    }

    let ok = Method::Basic(BasicMethod::RecoverOk(BasicRecoverOk));
    assert_eq!(Method::decode(&Bytes::from(ok.encode())).unwrap(), ok);
}
