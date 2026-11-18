mod support;

use bytes::Bytes;
use wolf_amqp_codec::{Error, FieldTable, Frame, MAX_FRAME_SIZE, Method, decode_frame};

#[test]
fn unknown_frame_type_is_an_error() {
    let wire = Bytes::from(support::frame_bytes(9, 0, &[]));
    let error = decode_frame(&wire).unwrap_err();
    assert_eq!(error, Error::UnknownFrameType { frame_type: 9 });
}

#[test]
fn unknown_method_id_is_an_error() {
    let wire = Bytes::from(support::frame_bytes(1, 0, &[0x00, 0x0A, 0x00, 0x63]));
    let error = decode_frame(&wire).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 10,
            method_id: 99
        }
    );
}

#[test]
fn unsupported_class_is_an_error() {
    let wire = Bytes::from(support::frame_bytes(1, 0, &[0x00, 0x3C, 0x00, 0x0A]));
    let error = decode_frame(&wire).unwrap_err();
    assert_eq!(error, Error::UnsupportedClass { class_id: 60 });
}

#[test]
fn truncated_frame_is_incomplete() {
    let mut bytes = vec![1, 0, 0, 0, 0, 0, 10];
    bytes.extend_from_slice(&[1, 2, 3]);
    let error = decode_frame(&Bytes::from(bytes)).unwrap_err();
    assert_eq!(
        error,
        Error::Incomplete {
            needed: 18,
            available: 10
        }
    );
}

#[test]
fn bad_frame_end_is_an_error() {
    let wire = Bytes::from_static(&[8, 0, 0, 0, 0, 0, 0, 0x00]);
    let error = decode_frame(&wire).unwrap_err();
    assert_eq!(error, Error::BadFrameEnd { found: 0x00 });
}

#[test]
fn oversized_frame_is_rejected_before_slicing() {
    let size = u32::try_from(MAX_FRAME_SIZE + 1).unwrap();
    let mut bytes = vec![1, 0, 0];
    bytes.extend_from_slice(&size.to_be_bytes());
    let error = decode_frame(&Bytes::from(bytes)).unwrap_err();
    assert_eq!(
        error,
        Error::FrameTooLarge {
            size: MAX_FRAME_SIZE + 1,
            max: MAX_FRAME_SIZE
        }
    );
}

#[test]
fn heartbeat_with_payload_is_rejected() {
    let wire = Bytes::from(support::frame_bytes(8, 0, &[1]));
    let error = decode_frame(&wire).unwrap_err();
    assert_eq!(
        error,
        Error::MalformedFrame {
            reason: "heartbeat frame must not carry a payload"
        }
    );
}

#[test]
fn trailing_method_payload_is_rejected() {
    let wire = Bytes::from(support::frame_bytes(1, 0, &[0, 10, 0, 51, 0xAA]));
    let error = decode_frame(&wire).unwrap_err();
    assert_eq!(error, Error::TrailingBytes { remaining: 1 });
}

#[test]
fn deeply_nested_decoded_table_is_rejected() {
    fn nested_table_bytes(levels: usize) -> Vec<u8> {
        let mut table = vec![0, 0, 0, 0];
        for _ in 0..levels {
            let mut body = vec![1, b'n', b'F'];
            body.extend_from_slice(&table);
            let len = u32::try_from(body.len()).unwrap();
            let mut outer = Vec::new();
            outer.extend_from_slice(&len.to_be_bytes());
            outer.extend_from_slice(&body);
            table = outer;
        }
        table
    }

    let bytes = nested_table_bytes(wolf_amqp_codec::types::MAX_TABLE_DEPTH + 8);
    let error = FieldTable::decode(&bytes).unwrap_err();
    assert!(matches!(error, Error::TableNestingTooDeep { .. }));
}

#[test]
fn method_decode_does_not_panic_on_short_input() {
    for payload in [vec![], vec![0], vec![0, 10], vec![0, 10, 0]] {
        let error = Method::decode(&Bytes::from(payload));
        assert!(error.is_err());
    }
}

#[test]
fn frame_encoding_rejects_oversized_body() {
    let frame = Frame::ContentBody {
        channel: 0,
        body: Bytes::from(vec![0u8; MAX_FRAME_SIZE + 1]),
    };
    assert!(matches!(
        frame.try_encode(),
        Err(Error::FrameTooLarge { .. })
    ));
}
