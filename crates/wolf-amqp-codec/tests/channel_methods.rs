use bytes::Bytes;
use wolf_amqp_codec::{
    ChannelClose, ChannelCloseOk, ChannelMethod, ChannelOpen, ChannelOpenOk, Error, LongString,
    Method, ShortString, channel,
};

fn sample_methods() -> Vec<(ChannelMethod, u16)> {
    vec![
        (
            ChannelMethod::Open(ChannelOpen {
                out_of_band: ShortString::new(""),
            }),
            channel::OPEN,
        ),
        (
            ChannelMethod::OpenOk(ChannelOpenOk {
                reserved: LongString::from(b"hb".to_vec()),
            }),
            channel::OPEN_OK,
        ),
        (
            ChannelMethod::Close(ChannelClose {
                reply_code: 320,
                reply_text: ShortString::new("channel forced"),
                class_id: 20,
                method_id: 40,
            }),
            channel::CLOSE,
        ),
        (ChannelMethod::CloseOk(ChannelCloseOk), channel::CLOSE_OK),
    ]
}

fn variant_name(method: &ChannelMethod) -> &'static str {
    match method {
        ChannelMethod::Open(_) => "open",
        ChannelMethod::OpenOk(_) => "open-ok",
        ChannelMethod::Close(_) => "close",
        ChannelMethod::CloseOk(_) => "close-ok",
    }
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        (
            channel::CLASS_ID,
            channel::OPEN,
            channel::OPEN_OK,
            channel::CLOSE,
            channel::CLOSE_OK,
        ),
        (20, 10, 11, 40, 41)
    );
}

#[test]
fn methods_encode_exact_wire_bytes() {
    let cases = [
        (
            Method::Channel(ChannelMethod::Open(ChannelOpen {
                out_of_band: ShortString::new("x"),
            })),
            vec![0x00, 0x14, 0x00, 0x0A, 0x01, b'x'],
        ),
        (
            Method::Channel(ChannelMethod::OpenOk(ChannelOpenOk {
                reserved: LongString::from(b"hb".to_vec()),
            })),
            vec![0x00, 0x14, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x02, b'h', b'b'],
        ),
        (
            Method::Channel(ChannelMethod::Close(ChannelClose {
                reply_code: 320,
                reply_text: ShortString::new("bye"),
                class_id: 20,
                method_id: 40,
            })),
            vec![
                0x00, 0x14, 0x00, 0x28, 0x01, 0x40, 0x03, b'b', b'y', b'e', 0x00, 0x14, 0x00, 0x28,
            ],
        ),
        (
            Method::Channel(ChannelMethod::CloseOk(ChannelCloseOk)),
            vec![0x00, 0x14, 0x00, 0x29],
        ),
    ];
    for (method, expected) in cases {
        assert_eq!(method.encode(), expected);
    }
}

#[test]
fn all_channel_methods_round_trip_with_all_fields() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), channel::CLASS_ID);
        assert_eq!(expected.method_id(), method_id);

        let encoded = expected.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.try_encode().unwrap(), encoded);
    }
}

#[test]
fn dispatch_selects_the_right_variant() {
    for (method, _) in sample_methods() {
        let encoded = Method::from(method.clone()).encode();
        let decoded = Method::decode(&Bytes::from(encoded)).unwrap();

        let Method::Channel(decoded_method) = decoded else {
            panic!("expected a channel method, got {decoded:?}");
        };
        assert_eq!(variant_name(&decoded_method), variant_name(&method));
    }
}

#[test]
fn unknown_method_id_is_an_error() {
    let error = Method::decode(&Bytes::from_static(&[0x00, 0x14, 0x00, 0x63])).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 20,
            method_id: 99,
        }
    );
}
