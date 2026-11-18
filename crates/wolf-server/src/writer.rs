//! Coalesced frame writing and outbox draining for connection sessions.
//!
//! Frames are encoded into fixed header, shared payload and tail segments so a
//! whole delivery can be written with one vectored write. Writers that do not
//! support vectored writes receive one contiguous buffered write instead.

use std::io;
use std::io::IoSlice;

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TryRecvError;
use wolf_amqp_codec::{Frame, FrameSegments};

use crate::amqp::error::AmqpError;
use crate::delivery::ServerEvent;

/// Maximum number of outbox events drained into one write turn.
pub const MAX_DRAIN_BATCH: usize = 64;

/// The result of one outbox drain turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The session may continue processing.
    Continue,
    /// The peer or outbox disconnected; the session must stop.
    Disconnected,
    /// Rendering an event failed and a close frame was written.
    ProtocolError,
}

impl DrainOutcome {
    /// Returns `true` when the session must stop after this outcome.
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Continue)
    }
}

/// Writes one frame to `writer`.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &Frame) -> io::Result<()> {
    write_frames(writer, std::slice::from_ref(frame)).await
}

/// Writes every frame in `frames` using one write call when possible.
pub async fn write_frames<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frames: &[Frame],
) -> io::Result<()> {
    if frames.is_empty() {
        return Ok(());
    }

    let segments = frames
        .iter()
        .map(Frame::encode_segments)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::other(error.to_string()))?;

    if writer.is_write_vectored() {
        let mut buffers = Vec::with_capacity(segments.len() * 3);
        for segment in &segments {
            buffers.push(IoSlice::new(segment.header()));
            buffers.push(IoSlice::new(segment.payload()));
            buffers.push(IoSlice::new(segment.tail()));
        }
        write_all_vectored(writer, &mut buffers).await
    } else {
        let capacity = segments.iter().map(FrameSegments::len).sum();
        let mut buffer = BytesMut::with_capacity(capacity);
        for segment in &segments {
            buffer.extend_from_slice(segment.header());
            buffer.extend_from_slice(segment.payload());
            buffer.extend_from_slice(segment.tail());
        }
        writer.write_all(&buffer).await
    }
}

/// Renders the first event, drains pending events up to
/// [`MAX_DRAIN_BATCH`] and writes the rendered frames in one write turn.
pub async fn drain_outbox<W, F>(
    writer: &mut W,
    first: ServerEvent,
    outbox: &mut Receiver<ServerEvent>,
    mut render: F,
) -> DrainOutcome
where
    W: AsyncWrite + Unpin,
    F: FnMut(ServerEvent) -> Result<Vec<Frame>, AmqpError>,
{
    let mut frames = Vec::new();
    let mut pending = Some(first);
    let mut disconnected = false;

    for _ in 0..MAX_DRAIN_BATCH {
        let Some(event) = pending.take() else {
            break;
        };
        match render(event) {
            Ok(mut rendered) => frames.append(&mut rendered),
            Err(error) => {
                let close = error.to_frame(0);
                let _ = write_frame(writer, &close).await;
                return DrainOutcome::ProtocolError;
            }
        }
        match outbox.try_recv() {
            Ok(event) => pending = Some(event),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                disconnected = true;
                break;
            }
        }
    }

    if !frames.is_empty() && write_frames(writer, &frames).await.is_err() {
        return DrainOutcome::Disconnected;
    }

    if disconnected {
        DrainOutcome::Disconnected
    } else {
        DrainOutcome::Continue
    }
}

async fn write_all_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    buffers: &mut [IoSlice<'_>],
) -> io::Result<()> {
    let total: usize = buffers.iter().map(|buffer| buffer.len()).sum();
    let mut written = 0;
    let mut remaining: &mut [IoSlice<'_>] = buffers;

    while written < total {
        let count = writer.write_vectored(remaining).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write frame",
            ));
        }
        written += count;
        if written < total {
            IoSlice::advance_slices(&mut remaining, count);
        }
    }
    Ok(())
}
