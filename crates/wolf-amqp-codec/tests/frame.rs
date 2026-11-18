mod support;

use bytes::Bytes;
use wolf_amqp_codec::{CloseOk, ConnectionMethod, ContentHeader, Frame, Method, decode_frame};

#[test]
fn heartbeat_has_spec_golden_bytes_and_round_trips() {
    let frame = Frame::Heartbeat { channel: 0 };
    let encoded = frame.encode();
    assert_eq!(encoded, vec![8, 0, 0, 0, 0, 0, 0, 0xCE]);

    let wire = Bytes::from(encoded);
    let (decoded, consumed) = decode_frame(&wire).unwrap();
    assert_eq!(consumed, 8);
    assert_eq!(decoded, frame);
}

#[test]
fn method_frame_re_encodes_byte_for_byte() {
    let raw = support::frame_bytes(1, 3, &[0x00, 0x0A, 0x00, 0x33]);
    let wire = Bytes::from(raw.clone());
    let (frame, consumed) = decode_frame(&wire).unwrap();

    assert_eq!(consumed, raw.len());
    assert_eq!(
        frame,
        Frame::Method {
            channel: 3,
            method: Method::from(ConnectionMethod::CloseOk(CloseOk)),
        }
    );
    assert_eq!(frame.encode(), raw);
}

#[test]
fn content_header_round_trips_all_fields() {
    let frame = Frame::ContentHeader {
        channel: 9,
        header: ContentHeader {
            class_id: 60,
            weight: 0,
            body_size: 1_234,
            properties: Bytes::from_static(&[0x80, 0x00]),
        },
    };

    let encoded = frame.encode();
    let (decoded, consumed) = decode_frame(&Bytes::from(encoded.clone())).unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded, frame);
    assert_eq!(decoded.encode(), encoded);
}

#[test]
fn content_body_is_zero_copy_from_the_input_buffer() {
    let frame = Frame::ContentBody {
        channel: 5,
        body: Bytes::from_static(b"hello body"),
    };
    let wire = Bytes::from(frame.encode());
    let expected_pointer = wire[7..].as_ptr();

    let (decoded, consumed) = decode_frame(&wire).unwrap();
    assert_eq!(consumed, wire.len());
    match decoded {
        Frame::ContentBody { channel, body } => {
            assert_eq!(channel, 5);
            assert_eq!(&body[..], b"hello body");
            assert_eq!(body.as_ptr(), expected_pointer);
        }
        other => panic!("expected content body frame, got {other:?}"),
    }
}

#[test]
fn decode_reports_consumed_bytes_for_trailing_frames() {
    let mut wire = support::frame_bytes(8, 0, &[]);
    wire.extend_from_slice(&support::frame_bytes(8, 7, &[]));
    let wire = Bytes::from(wire);

    let (first, consumed) = decode_frame(&wire).unwrap();
    assert_eq!(consumed, 8);
    assert_eq!(first, Frame::Heartbeat { channel: 0 });

    let (second, second_consumed) = decode_frame(&wire.slice(consumed..)).unwrap();
    assert_eq!(second_consumed, 8);
    assert_eq!(second, Frame::Heartbeat { channel: 7 });
}

#[test]
fn channel_number_is_preserved() {
    let wire = Bytes::from(support::frame_bytes(1, 42, &[0x00, 0x0A, 0x00, 0x33]));
    let (decoded, _) = decode_frame(&wire).unwrap();
    match decoded {
        Frame::Method { channel, .. } => assert_eq!(channel, 42),
        other => panic!("expected method frame, got {other:?}"),
    }
}
