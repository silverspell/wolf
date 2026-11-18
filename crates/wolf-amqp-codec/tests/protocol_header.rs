mod support;

use wolf_amqp_codec::{Error, ProtocolHeader};

#[test]
fn protocol_header_matches_spec_bytes() {
    assert_eq!(ProtocolHeader::BYTES, support::PROTOCOL_HEADER);
    assert_eq!(
        ProtocolHeader::BYTES,
        [0x41, 0x4D, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01]
    );
}

#[test]
fn protocol_header_round_trips_byte_for_byte() {
    let encoded = ProtocolHeader.encode();
    assert_eq!(encoded, ProtocolHeader::BYTES);
    assert_eq!(ProtocolHeader::decode(&encoded), Ok(ProtocolHeader));
}

#[test]
fn protocol_header_allows_trailing_frames() {
    let mut input = ProtocolHeader::BYTES.to_vec();
    input.push(8);
    assert_eq!(ProtocolHeader::decode(&input), Ok(ProtocolHeader));
}

#[test]
fn truncated_protocol_header_is_incomplete() {
    let error = ProtocolHeader::decode(&ProtocolHeader::BYTES[..7]).unwrap_err();
    assert_eq!(
        error,
        Error::Incomplete {
            needed: 8,
            available: 7
        }
    );
}

#[test]
fn wrong_protocol_header_is_rejected() {
    let mut input = ProtocolHeader::BYTES;
    input[7] = 0;
    let error = ProtocolHeader::decode(&input).unwrap_err();
    assert_eq!(error, Error::ProtocolHeaderMismatch { found: input });
}
