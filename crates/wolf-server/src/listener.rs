//! TCP listener, accepted sessions and graceful shutdown.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, watch};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};

use crate::amqp::MethodKey;
use crate::config::ServerConfig;
use crate::session::{ConnectionId, Session};

const RETRY_DELAY: Duration = Duration::from_millis(10);

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
    let listener = TcpListener::bind(config.amqp_addr).await?;
    let local_addr = listener.local_addr()?;
    let (sender, receiver) = watch::channel(false);
    let active_sessions = Arc::new(AtomicUsize::new(0));
    let session_count = Arc::clone(&active_sessions);

    let task = tokio::spawn(accept_loop(listener, receiver, session_count));

    let handle = ShutdownHandle {
        sender,
        active_sessions,
        timeout: config.shutdown_timeout,
        task: Mutex::new(Some(task)),
    };

    Ok((local_addr, handle))
}

async fn accept_loop(
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
    active_sessions: Arc<AtomicUsize>,
) {
    let mut sessions = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => {
                    let guard = SessionGuard::new(Arc::clone(&active_sessions));
                    let session_shutdown = shutdown.clone();
                    sessions.spawn(run_session(stream, session_shutdown, guard));
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
    mut shutdown: watch::Receiver<bool>,
    _guard: SessionGuard,
) {
    let mut session = Session::new(ConnectionId::generate());
    let mut buffer = [0u8; 1024];

    loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => break,
            read = stream.read(&mut buffer) => match read {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let _ = session.dispatch(MethodKey::new(0, 0));
                }
            },
        }
    }
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
