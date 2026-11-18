//! Publisher-confirm state and per-publish acknowledgement encoding.
//!
//! Confirm mode is enabled per channel by `confirm.select`. Every completed
//! publish then receives exactly one `basic.ack` carrying a monotonically
//! increasing delivery tag starting at 1. Selecting confirm mode again is
//! idempotent and does not reset the sequence.

use wolf_amqp_codec::{
    BasicAck, BasicMethod, ConfirmMethod, ConfirmSelectOk, Frame, Method, basic, confirm,
};

/// Publisher-confirm state for one channel.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfirmState {
    enabled: bool,
    next_publish_sequence: u64,
}

impl ConfirmState {
    /// Creates a channel state with confirm mode disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether confirm mode is enabled.
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enables confirm mode, returning an optional `confirm.select-ok` frame.
    ///
    /// The first selection resets the publish sequence; later selections are
    /// idempotent and leave the sequence untouched.
    pub fn select(&mut self, channel: u16, nowait: bool) -> Option<Frame> {
        if !self.enabled {
            self.enabled = true;
            self.next_publish_sequence = 0;
        }
        if nowait {
            None
        } else {
            Some(Self::select_ok_frame(channel))
        }
    }

    /// Advances the publish sequence and returns the next delivery tag.
    ///
    /// Returns `None` when confirm mode is disabled.
    pub fn next_publish_sequence(&mut self) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        self.next_publish_sequence += 1;
        Some(self.next_publish_sequence)
    }

    /// Builds the `confirm.select-ok` frame for a channel.
    pub fn select_ok_frame(channel: u16) -> Frame {
        Frame::Method {
            channel,
            method: Method::Confirm(ConfirmMethod::SelectOk(ConfirmSelectOk)),
        }
    }

    /// Builds a single-message `basic.ack` for a completed publish.
    pub fn ack_frame(channel: u16, delivery_tag: u64) -> Frame {
        Frame::Method {
            channel,
            method: Method::Basic(BasicMethod::Ack(BasicAck {
                delivery_tag,
                multiple: false,
            })),
        }
    }

    /// Returns the class and method ids used by confirm errors.
    pub const fn select_key() -> (u16, u16) {
        (confirm::CLASS_ID, confirm::SELECT)
    }

    /// Returns the class and method ids used by publish acknowledgements.
    pub const fn ack_key() -> (u16, u16) {
        (basic::CLASS_ID, basic::ACK)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack_tag(frame: &Frame) -> u64 {
        let Frame::Method {
            method: Method::Basic(BasicMethod::Ack(ack)),
            ..
        } = frame
        else {
            panic!("expected basic.ack, got {frame:?}");
        };
        assert!(!ack.multiple);
        ack.delivery_tag
    }

    #[test]
    fn first_select_resets_the_sequence_and_replies_unless_nowait() {
        let mut state = ConfirmState::new();
        assert!(!state.is_enabled());

        let frame = state.select(2, false).expect("select-ok");
        assert!(state.is_enabled());
        let Frame::Method { channel, method } = frame else {
            panic!("expected method frame");
        };
        assert_eq!(channel, 2);
        assert_eq!(
            method,
            Method::Confirm(ConfirmMethod::SelectOk(ConfirmSelectOk))
        );
        assert_eq!(state.next_publish_sequence(), Some(1));
    }

    #[test]
    fn nowait_select_suppresses_select_ok() {
        let mut state = ConfirmState::new();

        assert!(state.select(1, true).is_none());
        assert!(state.is_enabled());
    }

    #[test]
    fn repeated_select_is_idempotent_and_does_not_reset_the_sequence() {
        let mut state = ConfirmState::new();
        state.select(1, false);
        assert_eq!(state.next_publish_sequence(), Some(1));
        assert_eq!(state.next_publish_sequence(), Some(2));

        let frame = state.select(1, false).expect("select-ok again");
        assert!(matches!(
            frame,
            Frame::Method {
                method: Method::Confirm(ConfirmMethod::SelectOk(_)),
                ..
            }
        ));
        assert_eq!(state.next_publish_sequence(), Some(3));
    }

    #[test]
    fn sequence_is_monotonic_and_disabled_channels_get_no_sequence() {
        let mut state = ConfirmState::new();
        assert_eq!(state.next_publish_sequence(), None);

        state.select(1, true);
        let tags: Vec<u64> = (0..4)
            .filter_map(|_| state.next_publish_sequence())
            .collect();
        assert_eq!(tags, vec![1, 2, 3, 4]);
    }

    #[test]
    fn ack_frames_are_single_message_acks() {
        let frame = ConfirmState::ack_frame(7, 42);
        let Frame::Method { channel, .. } = frame else {
            panic!("expected method frame");
        };
        assert_eq!(channel, 7);
        assert_eq!(ack_tag(&frame), 42);
    }

    #[test]
    fn select_and_ack_keys_match_the_codec() {
        assert_eq!(ConfirmState::select_key(), (85, 10));
        assert_eq!(ConfirmState::ack_key(), (60, 80));
    }
}
