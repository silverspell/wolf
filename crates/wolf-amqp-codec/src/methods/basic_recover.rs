//! AMQP Basic class recover methods.

/// `basic.recover-async` method id.
pub const RECOVER_ASYNC: u16 = 100;
/// `basic.recover` method id.
pub const RECOVER: u16 = 110;
/// `basic.recover-ok` method id.
pub const RECOVER_OK: u16 = 111;

/// `basic.recover-async` — requeue unacknowledged deliveries without a reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasicRecoverAsync {
    /// Whether unacknowledged messages must be requeued.
    pub requeue: bool,
}

/// `basic.recover` — requeue unacknowledged deliveries and reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasicRecover {
    /// Whether unacknowledged messages must be requeued.
    pub requeue: bool,
}

/// `basic.recover-ok` — acknowledgement of `basic.recover`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasicRecoverOk;
