//! AMQP connection state machine: handshake, tune, vhost and channels.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use wolf_amqp_codec::{
    ChannelCloseOk, ChannelFlowOk, ChannelMethod, ChannelOpenOk, CloseOk, ConnectionMethod,
    FieldTable, FieldValue, Frame, LongString, Method, OpenOk, ShortString, Start, StartOk, Tune,
    TuneOk, channel, connection,
};
use wolf_broker::{Broker, VhostName};

use crate::amqp::auth::{self, AuthError};
use crate::amqp::channel::Channel;
use crate::amqp::error::{AmqpError, ReplyCode};

/// Default maximum number of channels advertised in `connection.tune`.
pub const DEFAULT_CHANNEL_MAX: u16 = 2047;
/// Default maximum frame size advertised in `connection.tune`.
pub const DEFAULT_FRAME_MAX: u32 = 131_072;
/// Default heartbeat interval advertised in `connection.tune`.
pub const DEFAULT_HEARTBEAT: u16 = 60;

/// The connection handshake state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Waiting for the client's `connection.start-ok`.
    AwaitingStartOk,
    /// Waiting for the client's `connection.tune-ok`.
    AwaitingTuneOk,
    /// Waiting for the client's `connection.open`.
    AwaitingOpen,
    /// The connection and its vhost are open.
    Open,
    /// A close handshake is in progress.
    Closing,
    /// The connection is closed.
    Closed,
}

/// Alias for the connection state machine state.
pub type ConnectionState = State;

/// State for a single client connection.
#[derive(Debug)]
pub struct Connection {
    state: State,
    peer: SocketAddr,
    broker: Arc<Broker>,
    channels: BTreeMap<u16, Channel>,
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,
    vhost: Option<VhostName>,
}

impl Connection {
    /// Creates a connection in the initial handshake state.
    pub fn new(peer: SocketAddr, broker: Arc<Broker>) -> Self {
        Self {
            state: State::AwaitingStartOk,
            peer,
            broker,
            channels: BTreeMap::new(),
            channel_max: DEFAULT_CHANNEL_MAX,
            frame_max: DEFAULT_FRAME_MAX,
            heartbeat: DEFAULT_HEARTBEAT,
            vhost: None,
        }
    }

    /// Returns the current handshake state.
    pub const fn state(&self) -> State {
        self.state
    }

    /// Returns whether the connection has completed its close handshake.
    pub const fn is_closed(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    /// Returns the negotiated heartbeat interval in seconds.
    pub const fn heartbeat(&self) -> u16 {
        self.heartbeat
    }

    /// Returns the negotiated heartbeat interval in seconds.
    pub const fn heartbeat_interval(&self) -> u16 {
        self.heartbeat
    }

    /// Returns the negotiated maximum channel number.
    pub const fn channel_max(&self) -> u16 {
        self.channel_max
    }

    /// Returns the negotiated maximum frame size.
    pub const fn frame_max(&self) -> u32 {
        self.frame_max
    }

    /// Returns the opened virtual host, if the connection has been opened.
    pub fn vhost(&self) -> Option<&VhostName> {
        self.vhost.as_ref()
    }

    /// Returns the currently open channels.
    pub fn channels(&self) -> impl Iterator<Item = &Channel> {
        self.channels.values()
    }

    /// Returns the peer address of this connection.
    pub const fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Builds the server's `connection.start` frame.
    pub fn start_frame(&self) -> Frame {
        Frame::Method {
            channel: 0,
            method: Method::Connection(ConnectionMethod::Start(Start {
                version_major: 0,
                version_minor: 9,
                server_properties: server_properties(),
                mechanisms: LongString::from(b"PLAIN".to_vec()),
                locales: LongString::from(b"en_US".to_vec()),
            })),
        }
    }

    /// Handles one decoded client frame, returning frames to write back.
    pub fn handle_frame(&mut self, frame: Frame) -> Result<Vec<Frame>, AmqpError> {
        match frame {
            Frame::Heartbeat { .. } => Ok(Vec::new()),
            Frame::Method { channel, method } => self.handle_method(channel, method),
            Frame::ContentHeader { .. } | Frame::ContentBody { .. } => Err(self.command_invalid(
                0,
                0,
                "content frames are not valid during connection setup",
            )),
        }
    }

    fn handle_method(
        &mut self,
        frame_channel: u16,
        method: Method,
    ) -> Result<Vec<Frame>, AmqpError> {
        if self.state == State::Closing || self.state == State::Closed {
            if let Method::Connection(ConnectionMethod::CloseOk(_)) = method {
                self.state = State::Closed;
            }
            return Ok(Vec::new());
        }

        if method.class_id() == connection::CLASS_ID {
            if frame_channel != 0 {
                return Err(self.command_invalid(
                    method.class_id(),
                    method.method_id(),
                    "connection methods must use channel 0",
                ));
            }
            return self.handle_connection_method(method);
        }

        if frame_channel == 0 {
            return Err(self.command_invalid(
                method.class_id(),
                method.method_id(),
                "channel methods must use a non-zero channel",
            ));
        }

        self.handle_channel_method(frame_channel, method)
    }

    fn handle_connection_method(&mut self, method: Method) -> Result<Vec<Frame>, AmqpError> {
        let Method::Connection(method) = method else {
            return Err(self.command_invalid(0, 0, "expected a connection method"));
        };

        match (self.state, method) {
            (State::AwaitingStartOk, ConnectionMethod::StartOk(start_ok)) => {
                self.handle_start_ok(start_ok)
            }
            (State::AwaitingTuneOk, ConnectionMethod::TuneOk(tune_ok)) => {
                self.handle_tune_ok(tune_ok)
            }
            (State::AwaitingOpen, ConnectionMethod::Open(open)) => {
                self.handle_open(open.virtual_host, open.insist)
            }
            (State::Open, ConnectionMethod::Close(_)) => {
                self.state = State::Closed;
                Ok(vec![Frame::Method {
                    channel: 0,
                    method: Method::Connection(ConnectionMethod::CloseOk(CloseOk)),
                }])
            }
            (_, ConnectionMethod::Close(_)) => {
                self.state = State::Closed;
                Ok(vec![Frame::Method {
                    channel: 0,
                    method: Method::Connection(ConnectionMethod::CloseOk(CloseOk)),
                }])
            }
            (_, ConnectionMethod::CloseOk(_)) => Ok(Vec::new()),
            (_, method) => Err(self.command_invalid(
                connection::CLASS_ID,
                method.method_id(),
                "method is not valid in the current connection state",
            )),
        }
    }

    fn handle_start_ok(&mut self, start_ok: StartOk) -> Result<Vec<Frame>, AmqpError> {
        auth::authenticate_long_string(start_ok.mechanism.as_str(), &start_ok.response, self.peer)
            .map_err(AuthError::into_amqp)?;
        self.state = State::AwaitingTuneOk;
        Ok(vec![self.tune_frame()])
    }

    fn tune_frame(&self) -> Frame {
        Frame::Method {
            channel: 0,
            method: Method::Connection(ConnectionMethod::Tune(Tune {
                channel_max: self.channel_max,
                frame_max: self.frame_max,
                heartbeat: self.heartbeat,
            })),
        }
    }

    fn handle_tune_ok(&mut self, tune_ok: TuneOk) -> Result<Vec<Frame>, AmqpError> {
        self.channel_max = negotiate_limit_u16(self.channel_max, tune_ok.channel_max);
        self.frame_max = negotiate_limit_u32(self.frame_max, tune_ok.frame_max);
        self.heartbeat = negotiate_heartbeat(self.heartbeat, tune_ok.heartbeat);
        self.state = State::AwaitingOpen;
        Ok(Vec::new())
    }

    fn handle_open(
        &mut self,
        virtual_host: ShortString,
        insist: bool,
    ) -> Result<Vec<Frame>, AmqpError> {
        let _ = insist;
        let vhost_name = VhostName::new(virtual_host.as_str()).map_err(|error| {
            let mapped = AmqpError::from_domain(&error);
            AmqpError::connection(mapped.reply_code, mapped.text)
                .with_offending(connection::CLASS_ID, connection::OPEN)
        })?;
        match self.broker.vhost(&vhost_name) {
            Ok(vhost) => {
                self.vhost = Some(vhost.name().clone());
                self.state = State::Open;
                Ok(vec![Frame::Method {
                    channel: 0,
                    method: Method::Connection(ConnectionMethod::OpenOk(OpenOk {
                        known_hosts: ShortString::new(""),
                    })),
                }])
            }
            Err(error) => {
                let mapped = AmqpError::from_domain(&error);
                Err(AmqpError::connection(mapped.reply_code, mapped.text)
                    .with_offending(connection::CLASS_ID, connection::OPEN))
            }
        }
    }

    fn handle_channel_method(
        &mut self,
        frame_channel: u16,
        method: Method,
    ) -> Result<Vec<Frame>, AmqpError> {
        if self.state != State::Open {
            return Err(self.command_invalid(
                method.class_id(),
                method.method_id(),
                "channel methods require an open connection",
            ));
        }

        let Method::Channel(method) = method else {
            return Err(self.command_invalid(0, 0, "expected a channel method"));
        };

        match method {
            ChannelMethod::Open(_) => self.open_channel(frame_channel),
            ChannelMethod::Close(_) => self.close_channel(frame_channel),
            ChannelMethod::Flow(flow) => self.channel_flow(frame_channel, flow.active),
            ChannelMethod::OpenOk(_) | ChannelMethod::FlowOk(_) | ChannelMethod::CloseOk(_) => {
                Err(self.command_invalid(
                    channel::CLASS_ID,
                    method.method_id(),
                    "client sent a server-only channel method",
                ))
            }
        }
    }

    fn open_channel(&mut self, frame_channel: u16) -> Result<Vec<Frame>, AmqpError> {
        if self.channel_max != 0 && frame_channel > self.channel_max {
            return Err(self.command_invalid(
                channel::CLASS_ID,
                channel::OPEN,
                "channel number exceeds the negotiated channel_max",
            ));
        }
        if self.channels.contains_key(&frame_channel) {
            return Err(self.command_invalid(
                channel::CLASS_ID,
                channel::OPEN,
                "channel is already open",
            ));
        }
        self.channels
            .insert(frame_channel, Channel::new(frame_channel));
        Ok(vec![Frame::Method {
            channel: frame_channel,
            method: Method::Channel(ChannelMethod::OpenOk(ChannelOpenOk {
                reserved: LongString::from(Vec::new()),
            })),
        }])
    }

    fn close_channel(&mut self, frame_channel: u16) -> Result<Vec<Frame>, AmqpError> {
        if self.channels.remove(&frame_channel).is_none() {
            return Err(self.command_invalid(
                channel::CLASS_ID,
                channel::CLOSE,
                "channel is not open",
            ));
        }
        Ok(vec![Frame::Method {
            channel: frame_channel,
            method: Method::Channel(ChannelMethod::CloseOk(ChannelCloseOk)),
        }])
    }

    fn channel_flow(&mut self, frame_channel: u16, active: bool) -> Result<Vec<Frame>, AmqpError> {
        let Some(channel) = self.channels.get_mut(&frame_channel) else {
            return Err(self.command_invalid(
                channel::CLASS_ID,
                channel::FLOW,
                "channel is not open",
            ));
        };
        if !active {
            return Err(AmqpError::connection(
                ReplyCode::NotImplemented,
                "channel.flow with active=false is not supported",
            )
            .with_offending(channel::CLASS_ID, channel::FLOW));
        }
        channel.set_flow_active(true);
        Ok(vec![Frame::Method {
            channel: frame_channel,
            method: Method::Channel(ChannelMethod::FlowOk(ChannelFlowOk { active: true })),
        }])
    }

    fn command_invalid(&self, class_id: u16, method_id: u16, text: impl Into<String>) -> AmqpError {
        AmqpError::connection(ReplyCode::CommandInvalid, text).with_offending(class_id, method_id)
    }
}

fn negotiate_limit_u16(server: u16, client: u16) -> u16 {
    match (server, client) {
        (0, client) => client,
        (server, 0) => server,
        (server, client) => server.min(client),
    }
}

fn negotiate_limit_u32(server: u32, client: u32) -> u32 {
    match (server, client) {
        (0, client) => client,
        (server, 0) => server,
        (server, client) => server.min(client),
    }
}

fn negotiate_heartbeat(server: u16, client: u16) -> u16 {
    negotiate_limit_u16(server, client)
}

/// Builds the server properties advertised in `connection.start`.
pub fn server_properties() -> FieldTable {
    let mut properties = FieldTable::new();
    properties.insert("product", FieldValue::LongString(LongString::new("wolf")));
    properties.insert(
        "version",
        FieldValue::LongString(LongString::new(env!("CARGO_PKG_VERSION"))),
    );
    properties.insert(
        "platform",
        FieldValue::LongString(LongString::new(std::env::consts::OS)),
    );
    properties.insert("capabilities", FieldValue::FieldTable(capabilities()));
    properties
}

/// Builds the M1 capability set advertised to clients.
pub fn capabilities() -> FieldTable {
    let mut capabilities = FieldTable::new();
    for capability in [
        "publisher_confirms",
        "basic.nack",
        "consumer_cancel_notify",
        "per_consumer_qos",
        "authentication_failure_close",
    ] {
        capabilities.insert(capability, FieldValue::Bool(true));
    }
    capabilities
}

#[cfg(test)]
mod tests {
    use wolf_amqp_codec::{ChannelOpen, FieldTable, FieldValue, ShortString};
    use wolf_broker::{Vhost, VhostId, VhostName};

    use super::*;

    fn test_broker() -> Arc<Broker> {
        let mut broker = Broker::new();
        let name = VhostName::new("/").expect("valid vhost");
        broker
            .add_vhost(Vhost::new(VhostId::new(1), name))
            .expect("add vhost");
        Arc::new(broker)
    }

    fn connection() -> Connection {
        Connection::new(
            "127.0.0.1:1234".parse().expect("valid address"),
            test_broker(),
        )
    }

    fn start_ok() -> Frame {
        Frame::Method {
            channel: 0,
            method: Method::Connection(ConnectionMethod::StartOk(StartOk {
                client_properties: FieldTable::new(),
                mechanism: ShortString::new("PLAIN"),
                response: LongString::from(b"\0guest\0guest".to_vec()),
                locale: ShortString::new("en_US"),
            })),
        }
    }

    #[test]
    fn capabilities_are_exactly_the_five_m1_entries() {
        let capabilities = capabilities();
        assert_eq!(capabilities.len(), 5);
        for required in [
            "publisher_confirms",
            "basic.nack",
            "consumer_cancel_notify",
            "per_consumer_qos",
            "authentication_failure_close",
        ] {
            assert_eq!(
                capabilities.get(required),
                Some(&FieldValue::Bool(true)),
                "{required} must be advertised"
            );
        }
        for forbidden in ["connection.blocked", "connection.unblocked", "channel.flow"] {
            assert!(!capabilities.contains_key(forbidden));
        }
    }

    #[test]
    fn heartbeats_use_the_non_zero_then_smaller_rule() {
        assert_eq!(negotiate_heartbeat(60, 30), 30);
        assert_eq!(negotiate_heartbeat(60, 0), 60);
        assert_eq!(negotiate_heartbeat(0, 30), 30);
        assert_eq!(negotiate_heartbeat(0, 0), 0);
        assert_eq!(negotiate_heartbeat(60, 120), 60);
    }

    #[test]
    fn tune_ok_negotiates_channel_frame_and_heartbeat_values() {
        let mut connection = connection();
        connection.handle_frame(start_ok()).expect("start-ok");
        assert_eq!(connection.state(), State::AwaitingTuneOk);
        connection
            .handle_frame(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::TuneOk(TuneOk {
                    channel_max: 100,
                    frame_max: 65_536,
                    heartbeat: 0,
                })),
            })
            .expect("tune-ok");
        assert_eq!(connection.channel_max(), 100);
        assert_eq!(connection.frame_max(), 65_536);
        assert_eq!(connection.heartbeat(), 60);
    }

    #[test]
    fn channel_open_and_flow_follow_the_negotiated_state() {
        let mut connection = connection();
        connection.handle_frame(start_ok()).expect("start-ok");
        connection
            .handle_frame(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::TuneOk(TuneOk {
                    channel_max: 2,
                    frame_max: 0,
                    heartbeat: 0,
                })),
            })
            .expect("tune-ok");
        connection
            .handle_frame(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::Open(wolf_amqp_codec::Open {
                    virtual_host: ShortString::new("/"),
                    capabilities: ShortString::new(""),
                    insist: false,
                })),
            })
            .expect("open");

        let opened = connection
            .handle_frame(Frame::Method {
                channel: 1,
                method: Method::Channel(ChannelMethod::Open(ChannelOpen {
                    out_of_band: ShortString::new(""),
                })),
            })
            .expect("channel open");
        assert!(matches!(
            opened.as_slice(),
            [Frame::Method {
                channel: 1,
                method: Method::Channel(ChannelMethod::OpenOk(_))
            }]
        ));

        let error = connection
            .handle_frame(Frame::Method {
                channel: 1,
                method: Method::Channel(ChannelMethod::Flow(wolf_amqp_codec::ChannelFlow {
                    active: false,
                })),
            })
            .expect_err("flow false is unsupported");
        assert_eq!(error.reply_code, ReplyCode::NotImplemented);
    }
}
