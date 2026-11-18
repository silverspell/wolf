use bytes::Bytes;
use wolf_amqp_codec::{
    BasicMethod, BasicProperties, BasicPublish, BasicReturn, ContentHeader, Error, FRAME_OVERHEAD,
    Frame, MAX_FRAME_SIZE, Method, ShortString, assemble_content_body, content_body_payload_max,
    decode_frame, split_content_body,
};

fn header(body_size: u64) -> ContentHeader {
    ContentHeader {
        class_id: 60,
        weight: 0,
        body_size,
        properties: Bytes::new(),
    }
}

fn frame_max_with_payload(payload: usize) -> u32 {
    u32::try_from(FRAME_OVERHEAD + payload).unwrap()
}

fn within(outer: &Bytes, inner: &Bytes) -> bool {
    let start = outer.as_ptr() as usize;
    let end = start + outer.len();
    let inner_start = inner.as_ptr() as usize;
    inner_start >= start && inner_start + inner.len() <= end
}

fn decode_frames(wire: &Bytes) -> Vec<Frame> {
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset < wire.len() {
        let (frame, consumed) = decode_frame(&wire.slice(offset..)).unwrap();
        offset += consumed;
        frames.push(frame);
    }
    assert_eq!(offset, wire.len());
    frames
}

fn sample_properties() -> BasicProperties {
    BasicProperties {
        content_type: Some(ShortString::new("text/plain")),
        delivery_mode: Some(2),
        priority: Some(5),
        correlation_id: Some(ShortString::new("corr")),
        timestamp: Some(1_700_000_000),
        ..BasicProperties::default()
    }
}

#[test]
fn split_zero_copy_chunks_preserve_body() {
    let original = Bytes::from((0..=255u8).cycle().take(37).collect::<Vec<_>>());
    let chunks: Vec<Bytes> = split_content_body(original.clone(), frame_max_with_payload(5))
        .unwrap()
        .collect();

    assert_eq!(chunks.len(), 37usize.div_ceil(5));
    for chunk in &chunks {
        assert!(
            within(&original, chunk),
            "chunk must be a slice of the original allocation"
        );
    }
    assert_eq!(
        assemble_content_body(&header(original.len() as u64), chunks).unwrap(),
        original
    );
}

#[test]
fn split_empty_body_yields_no_chunks() {
    let chunks: Vec<Bytes> = split_content_body(Bytes::new(), frame_max_with_payload(4))
        .unwrap()
        .collect();
    assert!(chunks.is_empty());
}

#[test]
fn split_exact_multiple_has_no_trailing_empty_chunk() {
    let body = Bytes::from_static(b"abcdefgh");
    let chunks: Vec<Bytes> = split_content_body(body, frame_max_with_payload(4))
        .unwrap()
        .collect();

    assert_eq!(chunks.len(), 2);
    assert!(chunks.iter().all(|chunk| chunk.len() == 4));
}

#[test]
fn frame_max_zero_is_treated_as_unlimited() {
    let splitter = split_content_body(Bytes::from_static(b"abc"), 0).unwrap();
    assert_eq!(splitter.max_payload(), MAX_FRAME_SIZE);
    assert_eq!(splitter.count(), 1);
}

#[test]
fn frame_max_at_or_below_overhead_is_rejected() {
    for frame_max in 1..=FRAME_OVERHEAD as u32 {
        assert_eq!(
            content_body_payload_max(frame_max),
            Err(Error::FrameMaxTooSmall { frame_max })
        );
    }
}

#[test]
fn frame_max_just_above_overhead_splits_into_single_bytes() {
    let splitter =
        split_content_body(Bytes::from_static(b"abc"), frame_max_with_payload(1)).unwrap();
    assert_eq!(splitter.max_payload(), 1);

    let chunks: Vec<Bytes> =
        split_content_body(Bytes::from_static(b"abc"), frame_max_with_payload(1))
            .unwrap()
            .collect();
    assert_eq!(
        chunks,
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ]
    );
}

#[test]
fn assembler_reassembly_preserves_bytes() {
    let body = Bytes::from((0..=255u8).cycle().take(29).collect::<Vec<_>>());
    let chunks: Vec<Bytes> = split_content_body(body.clone(), frame_max_with_payload(4))
        .unwrap()
        .collect();
    let mut assembler = wolf_amqp_codec::ContentBodyAssembler::new(&header(body.len() as u64));
    for chunk in chunks {
        assembler.push(chunk).unwrap();
    }
    assert_eq!(assembler.finish().unwrap(), body);
}

#[test]
fn assembler_single_chunk_is_copy_free() {
    let chunk = Bytes::from_static(b"single chunk");
    let mut assembler = wolf_amqp_codec::ContentBodyAssembler::new(&header(chunk.len() as u64));
    assembler.push(chunk.clone()).unwrap();
    let reassembled = assembler.finish().unwrap();

    assert_eq!(reassembled.as_ptr(), chunk.as_ptr());
    assert_eq!(reassembled, chunk);
}

#[test]
fn assembler_rejects_body_shorter_than_header_body_size() {
    let mut assembler = wolf_amqp_codec::ContentBodyAssembler::new(&header(10));
    assembler.push(Bytes::from_static(b"abc")).unwrap();
    assert_eq!(
        assembler.finish(),
        Err(Error::BodySizeMismatch {
            expected: 10,
            actual: 3
        })
    );
}

#[test]
fn assembler_rejects_body_longer_than_header_body_size() {
    let mut assembler = wolf_amqp_codec::ContentBodyAssembler::new(&header(4));
    assert_eq!(
        assembler.push(Bytes::from_static(b"abcde")),
        Err(Error::BodySizeMismatch {
            expected: 4,
            actual: 5
        })
    );
}

#[test]
fn content_header_basic_properties_round_trip() {
    let properties = sample_properties();
    let content_header = ContentHeader::from_basic_properties(42, &properties).unwrap();

    assert_eq!(content_header.class_id, 60);
    assert_eq!(content_header.weight, 0);
    assert_eq!(content_header.body_size, 42);
    assert_eq!(content_header.basic_properties().unwrap(), properties);
}

#[test]
fn publish_content_header_and_split_body_round_trip() {
    let channel = 7;
    let body = Bytes::from_static(b"hello content body");
    let properties = sample_properties();
    let method = Method::Basic(BasicMethod::Publish(BasicPublish {
        ticket: 1,
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
        mandatory: true,
        immediate: false,
    }));
    let content_header =
        ContentHeader::from_basic_properties(body.len() as u64, &properties).unwrap();
    let frame_max = frame_max_with_payload(4);

    let mut wire = Vec::new();
    wire.extend_from_slice(
        &Frame::Method {
            channel,
            method: method.clone(),
        }
        .encode(),
    );
    wire.extend_from_slice(
        &Frame::ContentHeader {
            channel,
            header: content_header.clone(),
        }
        .encode(),
    );
    for chunk in split_content_body(body.clone(), frame_max).unwrap() {
        wire.extend_from_slice(
            &Frame::ContentBody {
                channel,
                body: chunk,
            }
            .encode(),
        );
    }

    let frames = decode_frames(&Bytes::from(wire));
    assert_eq!(frames.len(), 2 + body.len().div_ceil(4));
    assert_eq!(frames[0], Frame::Method { channel, method });

    let Frame::ContentHeader {
        channel: decoded_channel,
        header: decoded_header,
    } = &frames[1]
    else {
        panic!("expected content header frame, got {:?}", frames[1]);
    };
    assert_eq!(*decoded_channel, channel);
    assert_eq!(decoded_header.class_id, 60);
    assert_eq!(decoded_header.weight, 0);
    assert_eq!(decoded_header.body_size, body.len() as u64);
    assert_eq!(decoded_header.basic_properties().unwrap(), properties);

    let mut chunks = Vec::new();
    for frame in &frames[2..] {
        let Frame::ContentBody {
            channel: body_channel,
            body: chunk,
        } = frame
        else {
            panic!("expected content body frame, got {frame:?}");
        };
        assert_eq!(*body_channel, channel);
        chunks.push(chunk.clone());
    }
    assert_eq!(
        assemble_content_body(&content_header, chunks).unwrap(),
        body
    );
}

#[test]
fn return_content_header_and_split_body_round_trip() {
    let channel = 9;
    let body = Bytes::from_static(b"returned message body");
    let properties = sample_properties();
    let method = Method::Basic(BasicMethod::Return(BasicReturn {
        reply_code: 312,
        reply_text: ShortString::new("NO_ROUTE"),
        exchange: ShortString::new("ex"),
        routing_key: ShortString::new("rk"),
    }));
    let content_header =
        ContentHeader::from_basic_properties(body.len() as u64, &properties).unwrap();
    let frame_max = frame_max_with_payload(3);

    let mut wire = Vec::new();
    wire.extend_from_slice(
        &Frame::Method {
            channel,
            method: method.clone(),
        }
        .encode(),
    );
    wire.extend_from_slice(
        &Frame::ContentHeader {
            channel,
            header: content_header.clone(),
        }
        .encode(),
    );
    for chunk in split_content_body(body.clone(), frame_max).unwrap() {
        wire.extend_from_slice(
            &Frame::ContentBody {
                channel,
                body: chunk,
            }
            .encode(),
        );
    }

    let frames = decode_frames(&Bytes::from(wire));
    assert_eq!(frames.len(), 2 + body.len().div_ceil(3));
    assert_eq!(frames[0], Frame::Method { channel, method });

    let Frame::ContentHeader {
        channel: decoded_channel,
        header: decoded_header,
    } = &frames[1]
    else {
        panic!("expected content header frame, got {:?}", frames[1]);
    };
    assert_eq!(*decoded_channel, channel);
    assert_eq!(decoded_header.class_id, 60);
    assert_eq!(decoded_header.body_size, body.len() as u64);
    assert_eq!(decoded_header.basic_properties().unwrap(), properties);

    let mut chunks = Vec::new();
    for frame in &frames[2..] {
        let Frame::ContentBody {
            channel: body_channel,
            body: chunk,
        } = frame
        else {
            panic!("expected content body frame, got {frame:?}");
        };
        assert_eq!(*body_channel, channel);
        chunks.push(chunk.clone());
    }
    assert_eq!(
        assemble_content_body(&content_header, chunks).unwrap(),
        body
    );
}
