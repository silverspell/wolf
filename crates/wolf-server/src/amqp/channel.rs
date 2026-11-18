//! Per-channel AMQP state owned by a connection.

use wolf_broker::ChannelId;

use crate::amqp::basic_publish::PendingPublish;
use crate::amqp::confirm::ConfirmState;

/// State for a single AMQP channel.
#[derive(Debug)]
pub struct Channel {
    /// The channel number.
    pub number: u16,
    /// Process-local broker channel identifier used by the domain dispatcher.
    pub broker_id: ChannelId,
    /// Whether content flow is currently active.
    pub flow_active: bool,
    /// The publish currently being assembled, if any.
    pub pending_publish: Option<PendingPublish>,
    /// Publisher-confirm state for this channel.
    pub confirm: ConfirmState,
    closing: bool,
}

impl Channel {
    /// Creates an open channel that starts with content flow active.
    pub fn new(number: u16) -> Self {
        Self::with_broker_id(number, ChannelId::generate())
    }

    /// Creates an open channel with an explicit broker channel identifier.
    pub fn with_broker_id(number: u16, broker_id: ChannelId) -> Self {
        Self {
            number,
            broker_id,
            flow_active: true,
            pending_publish: None,
            confirm: ConfirmState::new(),
            closing: false,
        }
    }

    /// Returns the channel number.
    pub const fn number(&self) -> u16 {
        self.number
    }

    /// Returns the process-local broker channel identifier.
    pub const fn broker_id(&self) -> ChannelId {
        self.broker_id
    }

    /// Returns whether content flow is currently active.
    pub const fn flow_active(&self) -> bool {
        self.flow_active
    }

    /// Returns whether the server has sent `channel.close` for this channel.
    pub const fn is_closing(&self) -> bool {
        self.closing
    }

    /// Updates the content-flow state.
    pub fn set_flow_active(&mut self, active: bool) {
        self.flow_active = active;
    }

    /// Marks the channel as closing and discards any partial publish.
    pub fn begin_close(&mut self) {
        self.closing = true;
        self.pending_publish = None;
    }
}
