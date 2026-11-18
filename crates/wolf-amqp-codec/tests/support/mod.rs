#![allow(dead_code)]

pub const PROTOCOL_HEADER: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];

pub fn frame_bytes(frame_type: u8, channel: u16, payload: &[u8]) -> Vec<u8> {
    let size = u32::try_from(payload.len()).expect("test frame fits in u32");
    let mut bytes = Vec::with_capacity(7 + payload.len() + 1);
    bytes.push(frame_type);
    bytes.extend_from_slice(&channel.to_be_bytes());
    bytes.extend_from_slice(&size.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.push(0xCE);
    bytes
}

pub fn start_ok_payload() -> Vec<u8> {
    let mut payload = vec![0x00, 0x0A, 0x00, 0x0B];
    payload.extend_from_slice(&[0, 0, 0, 0]);
    payload.push(5);
    payload.extend_from_slice(b"PLAIN");
    payload.extend_from_slice(&[0, 0, 0, 12]);
    payload.extend_from_slice(b"\0guest\0guest");
    payload.push(5);
    payload.extend_from_slice(b"en_US");
    payload
}

pub fn start_ok_frame() -> Vec<u8> {
    frame_bytes(1, 0, &start_ok_payload())
}
