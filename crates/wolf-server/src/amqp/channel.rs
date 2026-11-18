//! Per-channel AMQP state owned by a connection.

/// State for a single AMQP channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Channel {
    /// The channel number.
    pub number: u16,
    /// Whether content flow is currently active.
    pub flow_active: bool,
}

impl Channel {
    /// Creates an open channel that starts with content flow active.
    pub const fn new(number: u16) -> Self {
        Self {
            number,
            flow_active: true,
        }
    }

    /// Returns the channel number.
    pub const fn number(&self) -> u16 {
        self.number
    }

    /// Returns whether content flow is currently active.
    pub const fn flow_active(&self) -> bool {
        self.flow_active
    }

    /// Updates the content-flow state.
    pub fn set_flow_active(&mut self, active: bool) {
        self.flow_active = active;
    }
}
