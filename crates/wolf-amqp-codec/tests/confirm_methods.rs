use bytes::Bytes;
use wolf_amqp_codec::{ConfirmMethod, ConfirmSelect, ConfirmSelectOk, Error, Method, confirm};

fn sample_methods() -> Vec<(ConfirmMethod, u16)> {
    vec![
        (
            ConfirmMethod::Select(ConfirmSelect { nowait: false }),
            confirm::SELECT,
        ),
        (
            ConfirmMethod::Select(ConfirmSelect { nowait: true }),
            confirm::SELECT,
        ),
        (ConfirmMethod::SelectOk(ConfirmSelectOk), confirm::SELECT_OK),
    ]
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        (confirm::CLASS_ID, confirm::SELECT, confirm::SELECT_OK),
        (85, 10, 11)
    );
}

#[test]
fn select_encodes_exact_wire_bytes() {
    let nowait_false = Method::Confirm(ConfirmMethod::Select(ConfirmSelect { nowait: false }));
    assert_eq!(nowait_false.encode(), [0x00, 0x55, 0x00, 0x0A, 0x00]);

    let nowait_true = Method::Confirm(ConfirmMethod::Select(ConfirmSelect { nowait: true }));
    assert_eq!(nowait_true.encode(), [0x00, 0x55, 0x00, 0x0A, 0x01]);

    let select_ok = Method::Confirm(ConfirmMethod::SelectOk(ConfirmSelectOk));
    assert_eq!(select_ok.encode(), [0x00, 0x55, 0x00, 0x0B]);
}

#[test]
fn select_decode_preserves_nowait() {
    let nowait_false =
        Method::decode(&Bytes::from_static(&[0x00, 0x55, 0x00, 0x0A, 0x00])).unwrap();
    assert_eq!(
        nowait_false,
        Method::Confirm(ConfirmMethod::Select(ConfirmSelect { nowait: false }))
    );

    let nowait_true = Method::decode(&Bytes::from_static(&[0x00, 0x55, 0x00, 0x0A, 0x01])).unwrap();
    assert_eq!(
        nowait_true,
        Method::Confirm(ConfirmMethod::Select(ConfirmSelect { nowait: true }))
    );
}

#[test]
fn all_confirm_methods_round_trip() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), confirm::CLASS_ID);
        assert_eq!(expected.method_id(), method_id);

        let encoded = expected.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.try_encode().unwrap(), encoded);
    }
}

#[test]
fn unknown_method_id_is_an_error() {
    let error = Method::decode(&Bytes::from_static(&[0x00, 0x55, 0x00, 0x63])).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 85,
            method_id: 99,
        }
    );
}
