use bytes::Bytes;
use wolf_amqp_codec::{
    Close, CloseOk, ConnectionMethod, FieldTable, FieldValue, LongString, Method, Open, OpenOk,
    ShortString, Start, StartOk, Tune, TuneOk, connection,
};

fn sample_methods() -> Vec<(ConnectionMethod, u16)> {
    let mut capabilities = FieldTable::new();
    capabilities.insert("publisher_confirms", FieldValue::Bool(true));

    let mut server_properties = FieldTable::new();
    server_properties.insert("product", FieldValue::ShortString(ShortString::new("wolf")));
    server_properties.insert("capabilities", FieldValue::FieldTable(capabilities));

    let mut client_properties = FieldTable::new();
    client_properties.insert(
        "product",
        FieldValue::ShortString(ShortString::new("wolf-test-client")),
    );

    vec![
        (
            ConnectionMethod::Start(Start {
                version_major: 0,
                version_minor: 9,
                server_properties,
                mechanisms: LongString::from(b"PLAIN AMQPLAIN".to_vec()),
                locales: LongString::from(b"en_US".to_vec()),
            }),
            connection::START,
        ),
        (
            ConnectionMethod::StartOk(StartOk {
                client_properties,
                mechanism: ShortString::new("PLAIN"),
                response: LongString::from(b"\0guest\0guest".to_vec()),
                locale: ShortString::new("en_US"),
            }),
            connection::START_OK,
        ),
        (
            ConnectionMethod::Tune(Tune {
                channel_max: 2047,
                frame_max: 131_072,
                heartbeat: 60,
            }),
            connection::TUNE,
        ),
        (
            ConnectionMethod::TuneOk(TuneOk {
                channel_max: 1024,
                frame_max: 65_536,
                heartbeat: 30,
            }),
            connection::TUNE_OK,
        ),
        (
            ConnectionMethod::Open(Open {
                virtual_host: ShortString::new("/"),
                capabilities: ShortString::new(""),
                insist: true,
            }),
            connection::OPEN,
        ),
        (
            ConnectionMethod::OpenOk(OpenOk {
                known_hosts: ShortString::new(""),
            }),
            connection::OPEN_OK,
        ),
        (
            ConnectionMethod::Close(Close {
                reply_code: 320,
                reply_text: ShortString::new("connection forced"),
                class_id: 10,
                method_id: 50,
            }),
            connection::CLOSE,
        ),
        (ConnectionMethod::CloseOk(CloseOk), connection::CLOSE_OK),
    ]
}

fn variant_name(method: &ConnectionMethod) -> &'static str {
    match method {
        ConnectionMethod::Start(_) => "start",
        ConnectionMethod::StartOk(_) => "start-ok",
        ConnectionMethod::Tune(_) => "tune",
        ConnectionMethod::TuneOk(_) => "tune-ok",
        ConnectionMethod::Open(_) => "open",
        ConnectionMethod::OpenOk(_) => "open-ok",
        ConnectionMethod::Close(_) => "close",
        ConnectionMethod::CloseOk(_) => "close-ok",
    }
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        (
            connection::CLASS_ID,
            connection::START,
            connection::START_OK,
            connection::TUNE,
            connection::TUNE_OK,
            connection::OPEN,
            connection::OPEN_OK,
            connection::CLOSE,
            connection::CLOSE_OK,
        ),
        (10, 10, 11, 30, 31, 40, 41, 50, 51)
    );
}

#[test]
fn all_connection_methods_round_trip_with_all_fields() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), connection::CLASS_ID);
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

        let Method::Connection(decoded_method) = decoded;
        assert_eq!(variant_name(&decoded_method), variant_name(&method));
    }
}
