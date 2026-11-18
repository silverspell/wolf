//! Frame write coalescing contract for connection listeners.

use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::channel;
use wolf_amqp_codec::{CloseOk, ConnectionMethod, ContentHeader, Frame, Method};
use wolf_broker::{ConsumerId, Delivery, DeliveryTag, Message};
use wolf_server::delivery::ServerEvent;
use wolf_server::writer::{DrainOutcome, drain_outbox, write_frames};

#[derive(Debug, Default)]
struct CountingWriter {
    bytes: Vec<u8>,
    writes: usize,
    vectored_writes: usize,
}

impl CountingWriter {
    fn new() -> Self {
        Self::default()
    }

    fn write_calls(&self) -> usize {
        self.writes + self.vectored_writes
    }
}

impl AsyncWrite for CountingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.writes += 1;
        self.bytes.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.vectored_writes += 1;
        let mut written = 0;
        for buf in bufs {
            self.bytes.extend_from_slice(buf);
            written += buf.len();
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn delivery_event(consumer: u64) -> ServerEvent {
    ServerEvent::Delivery(Delivery {
        consumer: ConsumerId::new(consumer),
        delivery_tag: DeliveryTag::new(consumer),
        message: Message::new(Bytes::from_static(b"payload")),
        redelivered: false,
    })
}

#[tokio::test]
async fn single_write_per_delivery() {
    let frames = vec![
        Frame::Method {
            channel: 3,
            method: Method::from(ConnectionMethod::CloseOk(CloseOk)),
        },
        Frame::ContentHeader {
            channel: 3,
            header: ContentHeader {
                class_id: 60,
                weight: 0,
                body_size: 9,
                properties: Bytes::from_static(&[0x80, 0x00]),
            },
        },
        Frame::ContentBody {
            channel: 3,
            body: Bytes::from_static(b"hello body"),
        },
    ];
    let expected: Vec<u8> = frames
        .iter()
        .flat_map(|frame| frame.try_encode().expect("frame encodes"))
        .collect();
    let mut writer = CountingWriter::new();

    write_frames(&mut writer, &frames)
        .await
        .expect("frames are written");

    assert_eq!(writer.write_calls(), 1, "delivery must use one write call");
    assert_eq!(writer.bytes, expected);
}

#[tokio::test]
async fn drain_batches_pending_deliveries() {
    let (sender, mut outbox) = channel(2);
    sender
        .send(delivery_event(1))
        .await
        .expect("first event queues");
    sender
        .send(delivery_event(2))
        .await
        .expect("second event queues");
    let first = outbox.recv().await.expect("first event");
    let mut writer = CountingWriter::new();

    let outcome = drain_outbox(&mut writer, first, &mut outbox, |_event| {
        Ok(vec![Frame::Method {
            channel: 1,
            method: Method::from(ConnectionMethod::CloseOk(CloseOk)),
        }])
    })
    .await;

    assert_eq!(outcome, DrainOutcome::Continue);
    assert_eq!(
        writer.write_calls(),
        1,
        "pending deliveries must share one write turn"
    );
    let frame_len = Frame::Method {
        channel: 1,
        method: Method::from(ConnectionMethod::CloseOk(CloseOk)),
    }
    .try_encode()
    .expect("frame encodes")
    .len();
    assert_eq!(
        writer.bytes.len(),
        2 * frame_len,
        "both deliveries are written"
    );
    assert!(
        outbox.try_recv().is_err(),
        "all pending deliveries must be drained"
    );
}
