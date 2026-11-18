//! TCP listener, accepted sessions and graceful shutdown.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, watch};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::{MissedTickBehavior, sleep, timeout};
use wolf_amqp_codec::{Error as CodecError, Frame, ProtocolHeader, decode_frame};
use wolf_broker::{Broker, Vhost, VhostId, VhostName};

use crate::config::ServerConfig;
use crate::delivery::DeliveryHub;
use crate::session::{ConnectionId, Session};

const RETRY_DELAY: Duration = Duration::from_millis(10);
const HEARTBEAT_TICK: Duration = Duration::from_millis(250);

/// Failure while waiting for the server to shut down.
#[derive(Debug)]
pub enum ShutdownError {
    /// The server did not stop within the configured timeout.
    TimedOut,
    /// The listener task failed.
    Join(JoinError),
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut => write!(f, "server shutdown timed out"),
            Self::Join(error) => write!(f, "server task failed: {error}"),
        }
    }
}

impl std::error::Error for ShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TimedOut => None,
            Self::Join(error) => Some(error),
        }
    }
}

/// Handle used to stop a running server.
#[derive(Debug)]
pub struct ShutdownHandle {
    sender: watch::Sender<bool>,
    active_sessions: Arc<AtomicUsize>,
    timeout: Duration,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl ShutdownHandle {
    /// Returns the number of sessions currently active.
    pub fn active_sessions(&self) -> usize {
        self.active_sessions.load(Ordering::Acquire)
    }

    /// Signals the listener and all sessions to stop, then waits for them.
    pub async fn shutdown(&self) -> Result<(), ShutdownError> {
        let _ = self.sender.send(true);

        let mut slot = self.task.lock().await;
        let result = {
            let Some(task) = slot.as_mut() else {
                return Ok(());
            };
            match timeout(self.timeout, &mut *task).await {
                Ok(joined) => joined.map_err(ShutdownError::Join),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    Err(ShutdownError::TimedOut)
                }
            }
        };
        slot.take();
        result
    }
}

/// Binds the AMQP listener and starts accepting connections.
pub async fn start(config: ServerConfig) -> io::Result<(SocketAddr, ShutdownHandle)> {
    let broker = default_broker()?;
    let listener = TcpListener::bind(config.amqp_addr).await?;
    let local_addr = listener.local_addr()?;
    let (sender, receiver) = watch::channel(false);
    let active_sessions = Arc::new(AtomicUsize::new(0));
    let session_count = Arc::clone(&active_sessions);
    let hub = Arc::new(DeliveryHub::new());

    let task = tokio::spawn(accept_loop(listener, receiver, session_count, broker, hub));

    let handle = ShutdownHandle {
        sender,
        active_sessions,
        timeout: config.shutdown_timeout,
        task: Mutex::new(Some(task)),
    };

    Ok((local_addr, handle))
}

fn default_broker() -> io::Result<Arc<Broker>> {
    let mut broker = Broker::new();
    let name = VhostName::new("/")
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    broker
        .add_vhost(Vhost::new(VhostId::new(1), name))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    Ok(Arc::new(broker))
}

async fn accept_loop(
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
    active_sessions: Arc<AtomicUsize>,
    broker: Arc<Broker>,
    hub: Arc<DeliveryHub>,
) {
    let mut sessions = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let guard = SessionGuard::new(Arc::clone(&active_sessions));
                    let session_shutdown = shutdown.clone();
                    let broker = Arc::clone(&broker);
                    let hub = Arc::clone(&hub);
                    sessions.spawn(run_session(
                        stream,
                        peer,
                        session_shutdown,
                        guard,
                        broker,
                        hub,
                    ));
                }
                Err(_) => sleep(RETRY_DELAY).await,
            },
            Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }

    drop(listener);
    while sessions.join_next().await.is_some() {}
}

async fn run_session(
    mut stream: TcpStream,
    peer: SocketAddr,
    mut shutdown: watch::Receiver<bool>,
    _guard: SessionGuard,
    broker: Arc<Broker>,
    hub: Arc<DeliveryHub>,
) {
    let (outbox, mut outbox_rx) = hub.subscribe();
    let mut session = Session::with_delivery(
        ConnectionId::generate(),
        peer,
        broker,
        Arc::clone(&hub),
        outbox,
    );
    let mut buffer = BytesMut::with_capacity(1024);
    let mut read_buffer = [0u8; 4096];
    let mut header_done = false;
    let mut heartbeat = tokio::time::interval(HEARTBEAT_TICK);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_heartbeat = std::time::Instant::now();

    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => break,
            _ = heartbeat.tick() => {
                let interval = session.heartbeat_interval();
                if !interval.is_zero() && last_heartbeat.elapsed() >= interval / 2 {
                    if write_frame(&mut stream, &Frame::Heartbeat { channel: 0 }).await.is_err() {
                        break;
                    }
                    last_heartbeat = std::time::Instant::now();
                }
            },
            event = outbox_rx.recv() => match event {
                Some(event) => match session.delivery_event(event) {
                    Ok(frames) => {
                        if write_frames(&mut stream, &frames).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let close = error.to_frame(0);
                        let _ = write_frame(&mut stream, &close).await;
                        break;
                    }
                },
                None => break,
            },
            read = stream.read(&mut read_buffer) => match read {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    buffer.extend_from_slice(&read_buffer[..read]);
                    if !header_done {
                        if buffer.len() < ProtocolHeader::BYTES.len() {
                            continue;
                        }
                        if ProtocolHeader::decode(&buffer[..ProtocolHeader::BYTES.len()]).is_err() {
                            let _ = stream.write_all(&ProtocolHeader::BYTES).await;
                            break;
                        }
                        buffer.advance(ProtocolHeader::BYTES.len());
                        header_done = true;
                        if write_frame(&mut stream, &session.start_frame()).await.is_err() {
                            break;
                        }
                    }

                    match process_buffer(&mut session, &mut stream, &mut buffer).await {
                        Ok(ProcessOutcome::Continue) => {}
                        Ok(ProcessOutcome::Close) => break,
                        Ok(ProcessOutcome::IoError) | Err(()) => break,
                    }
                }
            },
        }
    }

    session.shutdown();
    drain_before_close(&mut stream).await;
    let _ = stream.shutdown().await;
}

async fn drain_before_close(stream: &mut TcpStream) {
    let mut buffer = [0u8; 1024];
    let _ = timeout(Duration::from_millis(250), async {
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
}

enum ProcessOutcome {
    Continue,
    Close,
    IoError,
}

async fn process_buffer(
    session: &mut Session,
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<ProcessOutcome, ()> {
    let mut data = buffer.split().freeze();
    loop {
        if data.is_empty() {
            return Ok(ProcessOutcome::Continue);
        }
        match decode_frame(&data) {
            Ok((frame, used)) => {
                data.advance(used);
                match session.dispatch(frame) {
                    Ok(frames) => {
                        if write_frames(stream, &frames).await.is_err() {
                            return Ok(ProcessOutcome::IoError);
                        }
                        if session.is_closed() {
                            return Ok(ProcessOutcome::Close);
                        }
                    }
                    Err(error) => {
                        let close = error.to_frame(0);
                        if write_frame(stream, &close).await.is_err() {
                            return Ok(ProcessOutcome::IoError);
                        }
                        return Ok(ProcessOutcome::Close);
                    }
                }
            }
            Err(CodecError::Incomplete { .. }) => {
                buffer.extend_from_slice(&data);
                return Ok(ProcessOutcome::Continue);
            }
            Err(_) => return Err(()),
        }
    }
}

async fn write_frames(stream: &mut TcpStream, frames: &[Frame]) -> io::Result<()> {
    for frame in frames {
        write_frame(stream, frame).await?;
    }
    Ok(())
}

async fn write_frame(stream: &mut TcpStream, frame: &Frame) -> io::Result<()> {
    let bytes = frame
        .try_encode()
        .map_err(|error| io::Error::other(error.to_string()))?;
    stream.write_all(&bytes).await
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

struct SessionGuard {
    active_sessions: Arc<AtomicUsize>,
}

impl SessionGuard {
    fn new(active_sessions: Arc<AtomicUsize>) -> Self {
        active_sessions.fetch_add(1, Ordering::AcqRel);
        Self { active_sessions }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.active_sessions.fetch_sub(1, Ordering::AcqRel);
    }
}
