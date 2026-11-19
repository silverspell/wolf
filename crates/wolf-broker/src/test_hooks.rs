//! Deterministic pause points for integration tests.
//!
//! Compiled only when the default-off `test-hooks` feature is enabled. The
//! hook is inert unless explicitly installed on a [`crate::Vhost`] with
//! [`crate::Vhost::with_ack_pause_hook`].

use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

/// The broker-side half of an ack pause hook.
#[derive(Default)]
pub struct AckPauseHook {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl fmt::Debug for AckPauseHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AckPauseHook").finish_non_exhaustive()
    }
}

impl AckPauseHook {
    /// Creates an unarmed hook and the test handle that drives it.
    pub fn pair() -> (Arc<Self>, AckPauseHandle) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        (
            Arc::new(Self {
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(Some(release_rx)),
            }),
            AckPauseHandle {
                entered: entered_rx,
                release: Some(release_tx),
            },
        )
    }

    pub(crate) async fn pause(&self) {
        let entered = match self.entered.lock() {
            Ok(mut guard) => guard.take(),
            Err(_) => None,
        };
        let Some(entered) = entered else {
            return;
        };

        let release = match self.release.lock() {
            Ok(mut guard) => guard.take(),
            Err(_) => None,
        };
        let Some(release) = release else {
            let _ = entered.send(());
            return;
        };

        let _ = entered.send(());
        let _ = release.await;
    }
}

/// The test half of an ack pause hook.
pub struct AckPauseHandle {
    entered: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for AckPauseHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AckPauseHandle").finish_non_exhaustive()
    }
}

impl AckPauseHandle {
    /// Waits until the hooked ack reaches the pause point.
    pub async fn wait_entered(&mut self) -> bool {
        (&mut self.entered).await.is_ok()
    }

    /// Releases a paused ack, reporting whether a hook was waiting.
    pub fn release(&mut self) -> bool {
        self.release
            .take()
            .is_some_and(|sender| sender.send(()).is_ok())
    }
}
