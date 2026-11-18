mod support;

use bytes::Bytes;
use wolf_amqp_codec::{
    ConnectionMethod, FieldTable, Frame, LongString, Method, Open, OpenOk, ProtocolHeader,
    ShortString, Start, StartOk, Tune, TuneOk, decode_frame,
};

#[test]
fn client_protocol_header_and_start_ok_decode() {
    let mut handshake = support::PROTOCOL_HEADER.to_vec();
    handshake.extend_from_slice(&support::start_ok_frame());

    assert_eq!(ProtocolHeader::decode(&handshake[..8]), Ok(ProtocolHeader));

    let wire = Bytes::copy_from_slice(&handshake[8..]);
    let (frame, consumed) = decode_frame(&wire).unwrap();
    assert_eq!(consumed, wire.len());

    match frame {
        Frame::Method {
            channel: 0,
            method: Method::Connection(ConnectionMethod::StartOk(start_ok)),
        } => {
            assert_eq!(start_ok.client_properties, FieldTable::new());
            assert_eq!(start_ok.mechanism.as_str(), "PLAIN");
            assert_eq!(start_ok.response.as_bytes(), b"\0guest\0guest");
            assert_eq!(start_ok.locale.as_str(), "en_US");
        }
        other => panic!("expected connection.start-ok, got {other:?}"),
    }
}

#[test]
fn server_start_reply_encodes_spec_golden_bytes() {
    let start = Start {
        version_major: 0,
        version_minor: 9,
        server_properties: FieldTable::new(),
        mechanisms: LongString::from(b"PLAIN AMQPLAIN".to_vec()),
        locales: LongString::from(b"en_US".to_vec()),
    };
    let frame = Frame::Method {
        channel: 0,
        method: Method::from(ConnectionMethod::Start(start)),
    };

    let mut payload = vec![0x00, 0x0A, 0x00, 0x0A, 0x00, 0x09, 0, 0, 0, 0];
    payload.extend_from_slice(&[0, 0, 0, 14]);
    payload.extend_from_slice(b"PLAIN AMQPLAIN");
    payload.extend_from_slice(&[0, 0, 0, 5]);
    payload.extend_from_slice(b"en_US");

    let expected = support::frame_bytes(1, 0, &payload);
    assert_eq!(frame.encode(), expected);
}

#[test]
fn first_handshake_slice_round_trips() {
    let mut wire = support::start_ok_frame();
    let methods = [
        ConnectionMethod::Tune(Tune {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }),
        ConnectionMethod::TuneOk(TuneOk {
            channel_max: 2047,
            frame_max: 131_072,
            heartbeat: 60,
        }),
        ConnectionMethod::Open(Open {
            virtual_host: ShortString::new("/"),
            capabilities: ShortString::new(""),
            insist: false,
        }),
        ConnectionMethod::OpenOk(OpenOk {
            known_hosts: ShortString::new(""),
        }),
    ];
    for method in methods {
        wire.extend_from_slice(
            &Frame::Method {
                channel: 0,
                method: Method::from(method),
            }
            .encode(),
        );
    }

    let wire = Bytes::from(wire);
    let mut offset = 0;
    let mut method_ids = Vec::new();
    while offset < wire.len() {
        let (frame, consumed) = decode_frame(&wire.slice(offset..)).unwrap();
        let original = &wire[offset..offset + consumed];
        assert_eq!(frame.encode().as_slice(), original);
        if let Frame::Method { method, .. } = &frame {
            method_ids.push(method.method_id());
        }
        offset += consumed;
    }

    assert_eq!(offset, wire.len());
    assert_eq!(method_ids, [11, 30, 31, 40, 41]);
}

#[test]
fn start_ok_created_by_client_round_trips() {
    let start_ok = StartOk {
        client_properties: FieldTable::new(),
        mechanism: ShortString::new("PLAIN"),
        response: LongString::from(b"\0guest\0guest".to_vec()),
        locale: ShortString::new("en_US"),
    };
    let method = Method::from(ConnectionMethod::StartOk(start_ok));
    let decoded = Method::decode(&Bytes::from(method.encode())).unwrap();
    assert_eq!(decoded, method);
}
