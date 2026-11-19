//! AMQP connection state machine: handshake, tune, vhost and channels.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use wolf_amqp_codec::{
    BasicAck, BasicCancel, BasicConsume, BasicGet, BasicMethod, BasicNack, BasicPublish, BasicQos,
    BasicRecover, BasicRecoverAsync, BasicReject, Blocked, ChannelCloseOk, ChannelFlowOk,
    ChannelMethod, ChannelOpenOk, CloseOk, ConfirmMethod, ConnectionMethod, ContentHeader,
    ExchangeMethod, FieldTable, FieldValue, Frame, LongString, Method, OpenOk, QueueMethod,
    ShortString, Start, StartOk, Tune, TuneOk, Unblocked, basic, basic_ack, basic_consume,
    basic_recover, channel, confirm, connection, exchange, queue,
};
use wolf_broker::{
    Broker, ChannelId, ConnectionId as BrokerConnectionId, Message, MessageMetadata,
    PublishOutcome, PublishRequest, Vhost, VhostName,
};

use crate::amqp::auth::{self, Account, AuthError};
use crate::amqp::basic_publish::{CompletedPublish, PendingPublish};
use crate::amqp::channel::Channel;
use crate::amqp::confirm::ConfirmState;
use crate::amqp::consumer::ConsumerDirectory;
use crate::amqp::error::{AmqpError, CloseLevel, ReplyCode};
use crate::amqp::{
    basic_ack as basic_ack_handler, basic_consume as basic_consume_handler,
    basic_get as basic_get_handler, exchange as exchange_handler, queue as queue_handler,
    unsupported,
};
use crate::delivery::{DeliveryHub, ServerEvent};

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
    account: Arc<Account>,
    broker_connection: BrokerConnectionId,
    channels: BTreeMap<u16, Channel>,
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,
    vhost: Option<VhostName>,
    consumers: ConsumerDirectory,
    hub: Arc<DeliveryHub>,
    outbox: Sender<ServerEvent>,
    connection_blocked_capability: bool,
    memory_alarm: bool,
    blocked_sent: bool,
    deferred_publish: Option<(u16, CompletedPublish)>,
    cleaned: bool,
}

impl Connection {
    /// Creates a connection in the initial handshake state.
    pub fn new(peer: SocketAddr, broker: Arc<Broker>, account: Arc<Account>) -> Self {
        Self::with_connection_id(peer, broker, account, BrokerConnectionId::new(0))
    }

    /// Creates a connection with the broker-side connection identifier used
    /// for exclusive-queue ownership and publish access checks.
    pub fn with_connection_id(
        peer: SocketAddr,
        broker: Arc<Broker>,
        account: Arc<Account>,
        broker_connection: BrokerConnectionId,
    ) -> Self {
        let hub = Arc::new(DeliveryHub::new());
        let (outbox, _receiver) = hub.subscribe();
        Self::with_delivery(peer, broker, account, broker_connection, hub, outbox)
    }

    /// Creates a connection that routes deliveries through a shared hub.
    pub fn with_delivery(
        peer: SocketAddr,
        broker: Arc<Broker>,
        account: Arc<Account>,
        broker_connection: BrokerConnectionId,
        hub: Arc<DeliveryHub>,
        outbox: Sender<ServerEvent>,
    ) -> Self {
        Self {
            state: State::AwaitingStartOk,
            peer,
            broker,
            account,
            broker_connection,
            channels: BTreeMap::new(),
            channel_max: DEFAULT_CHANNEL_MAX,
            frame_max: DEFAULT_FRAME_MAX,
            heartbeat: DEFAULT_HEARTBEAT,
            vhost: None,
            consumers: ConsumerDirectory::new(),
            hub,
            outbox,
            connection_blocked_capability: false,
            memory_alarm: false,
            blocked_sent: false,
            deferred_publish: None,
            cleaned: false,
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

    /// Returns whether the client advertised the `connection.blocked`
    /// capability in `connection.start-ok`.
    pub const fn connection_blocked_capability(&self) -> bool {
        self.connection_blocked_capability
    }

    /// Returns whether the memory alarm is currently active.
    pub const fn is_memory_alarmed(&self) -> bool {
        self.memory_alarm
    }

    /// Returns whether socket reads must pause to bound deferred publishes.
    ///
    /// At most one completed publish may wait for the alarm to clear. Once it
    /// is deferred, another publish would exceed that bound, so the session
    /// retains the undecoded bytes and applies TCP backpressure.
    pub const fn read_paused(&self) -> bool {
        self.memory_alarm && self.deferred_publish.is_some()
    }

    /// Applies a memory-alarm transition, returning frames to write.
    ///
    /// Raising the alarm notifies capable open connections with
    /// `connection.blocked`. Clearing it emits `connection.unblocked` only when
    /// a block was actually sent, then drains the deferred publish through the
    /// normal routing and confirm path.
    pub async fn set_memory_alarm(&mut self, active: bool) -> Result<Vec<Frame>, AmqpError> {
        self.memory_alarm = active;
        if active {
            let mut frames = Vec::new();
            self.push_blocked_frame(&mut frames);
            return Ok(frames);
        }

        let mut frames = Vec::new();
        if self.blocked_sent {
            self.blocked_sent = false;
            frames.push(unblocked_frame());
        }
        if let Some((channel, completed)) = self.deferred_publish.take() {
            let mut routed = self.route_completed_publish(channel, completed).await?;
            frames.append(&mut routed);
        }
        Ok(frames)
    }

    fn push_blocked_frame(&mut self, frames: &mut Vec<Frame>) {
        if self.memory_alarm
            && self.connection_blocked_capability
            && self.state == State::Open
            && !self.blocked_sent
        {
            self.blocked_sent = true;
            frames.push(blocked_frame());
        }
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
    pub async fn handle_frame(&mut self, frame: Frame) -> Result<Vec<Frame>, AmqpError> {
        match frame {
            Frame::Heartbeat { .. } => Ok(Vec::new()),
            Frame::Method { channel, method } => self.handle_method(channel, method).await,
            Frame::ContentHeader { channel, header } => {
                let result = self.handle_content_header(channel, header).await;
                self.finish_channel_result(channel, result).await
            }
            Frame::ContentBody { channel, body } => {
                let result = self.handle_content_body(channel, body).await;
                self.finish_channel_result(channel, result).await
            }
        }
    }

    async fn handle_method(
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
            return self.handle_connection_method(method).await;
        }

        if frame_channel == 0 {
            return Err(self.command_invalid(
                method.class_id(),
                method.method_id(),
                "channel methods must use a non-zero channel",
            ));
        }

        let result = self.handle_channel_method(frame_channel, method).await;
        self.finish_channel_result(frame_channel, result).await
    }

    async fn finish_channel_result(
        &mut self,
        frame_channel: u16,
        result: Result<Vec<Frame>, AmqpError>,
    ) -> Result<Vec<Frame>, AmqpError> {
        match result {
            Ok(frames) => Ok(frames),
            Err(error) if error.close_level == CloseLevel::Channel => {
                if let Some(broker_channel) = self
                    .channels
                    .get(&frame_channel)
                    .map(|channel| channel.broker_id)
                {
                    self.cleanup_channel(frame_channel, broker_channel).await;
                }
                if let Some(channel) = self.channels.get_mut(&frame_channel) {
                    channel.begin_close();
                }
                Ok(vec![error.to_frame(frame_channel)])
            }
            Err(error) => Err(error),
        }
    }

    async fn handle_connection_method(&mut self, method: Method) -> Result<Vec<Frame>, AmqpError> {
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
                self.cleanup_connection().await;
                self.state = State::Closed;
                Ok(vec![Frame::Method {
                    channel: 0,
                    method: Method::Connection(ConnectionMethod::CloseOk(CloseOk)),
                }])
            }
            (_, ConnectionMethod::Close(_)) => {
                self.cleanup_connection().await;
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
        auth::authenticate_long_string(
            start_ok.mechanism.as_str(),
            &start_ok.response,
            self.peer,
            &self.account,
        )
        .map_err(AuthError::into_amqp)?;
        self.connection_blocked_capability =
            supports_connection_blocked(&start_ok.client_properties);
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
                let mut frames = vec![Frame::Method {
                    channel: 0,
                    method: Method::Connection(ConnectionMethod::OpenOk(OpenOk {
                        known_hosts: ShortString::new(""),
                    })),
                }];
                self.push_blocked_frame(&mut frames);
                Ok(frames)
            }
            Err(error) => {
                let mapped = AmqpError::from_domain(&error);
                Err(AmqpError::connection(mapped.reply_code, mapped.text)
                    .with_offending(connection::CLASS_ID, connection::OPEN))
            }
        }
    }

    async fn handle_channel_method(
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

        if let Some(channel) = self.channels.get(&frame_channel) {
            if channel.is_closing() {
                if matches!(method, Method::Channel(ChannelMethod::CloseOk(_))) {
                    self.channels.remove(&frame_channel);
                }
                return Ok(Vec::new());
            }
        }

        match method {
            Method::Channel(ChannelMethod::Open(_)) => self.open_channel(frame_channel).await,
            Method::Channel(ChannelMethod::Close(_)) => self.close_channel(frame_channel).await,
            Method::Channel(ChannelMethod::Flow(flow)) => {
                self.channel_flow(frame_channel, flow.active).await
            }
            Method::Channel(method) => Err(self.command_invalid(
                channel::CLASS_ID,
                method.method_id(),
                "client sent a server-only channel method",
            )),
            Method::Confirm(ConfirmMethod::Select(select)) => {
                self.confirm_select(frame_channel, select).await
            }
            Method::Confirm(ConfirmMethod::SelectOk(_)) => Err(self.command_invalid(
                confirm::CLASS_ID,
                confirm::SELECT_OK,
                "client sent a server-only confirm method",
            )),
            Method::Basic(BasicMethod::Publish(publish)) => {
                self.basic_publish(frame_channel, publish).await
            }
            Method::Basic(BasicMethod::Qos(qos)) => self.basic_qos(frame_channel, qos).await,
            Method::Basic(BasicMethod::Consume(consume)) => {
                self.basic_consume(frame_channel, consume).await
            }
            Method::Basic(BasicMethod::Cancel(cancel)) => {
                self.basic_cancel(frame_channel, cancel).await
            }
            Method::Basic(BasicMethod::Get(get)) => self.basic_get(frame_channel, get).await,
            Method::Basic(BasicMethod::Ack(ack)) => self.basic_ack(frame_channel, ack).await,
            Method::Basic(BasicMethod::Nack(nack)) => self.basic_nack(frame_channel, nack).await,
            Method::Basic(BasicMethod::Reject(reject)) => {
                self.basic_reject(frame_channel, reject).await
            }
            Method::Basic(BasicMethod::Recover(recover)) => {
                self.basic_recover(frame_channel, recover).await
            }
            Method::Basic(BasicMethod::RecoverAsync(recover)) => {
                self.basic_recover_async(frame_channel, recover).await
            }
            Method::Exchange(ExchangeMethod::Declare(declare)) => {
                self.exchange_declare(frame_channel, declare).await
            }
            Method::Exchange(ExchangeMethod::Delete(delete)) => {
                self.exchange_delete(frame_channel, delete).await
            }
            Method::Queue(QueueMethod::Declare(declare)) => {
                self.queue_declare(frame_channel, declare).await
            }
            Method::Queue(QueueMethod::Bind(bind)) => self.queue_bind(frame_channel, bind).await,
            Method::Queue(QueueMethod::Unbind(unbind)) => {
                self.queue_unbind(frame_channel, unbind).await
            }
            Method::Queue(QueueMethod::Purge(purge)) => {
                self.queue_purge(frame_channel, purge).await
            }
            Method::Queue(QueueMethod::Delete(delete)) => {
                self.queue_delete(frame_channel, delete).await
            }
            Method::Basic(method) => Err(self.command_invalid(
                basic::CLASS_ID,
                method.method_id(),
                "client sent a server-only basic method",
            )),
            Method::Exchange(method) => Err(self.command_invalid(
                exchange::CLASS_ID,
                method.method_id(),
                "client sent a server-only exchange method",
            )),
            Method::Queue(method) => Err(self.command_invalid(
                queue::CLASS_ID,
                method.method_id(),
                "client sent a server-only queue method",
            )),
            Method::Tx(method) => unsupported::handle_tx(frame_channel, method),
            method => Err(self.command_invalid(
                method.class_id(),
                method.method_id(),
                "method is not supported",
            )),
        }
    }

    async fn exchange_declare(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::ExchangeDeclare,
    ) -> Result<Vec<Frame>, AmqpError> {
        let vhost = self.resolved_vhost(exchange::CLASS_ID, exchange::DECLARE)?;
        exchange_handler::declare(vhost, frame_channel, request).await
    }

    async fn exchange_delete(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::ExchangeDelete,
    ) -> Result<Vec<Frame>, AmqpError> {
        let vhost = self.resolved_vhost(exchange::CLASS_ID, exchange::DELETE)?;
        exchange_handler::delete(vhost, frame_channel, request).await
    }

    async fn queue_declare(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::QueueDeclare,
    ) -> Result<Vec<Frame>, AmqpError> {
        let vhost = self.resolved_vhost(queue::CLASS_ID, queue::DECLARE)?;
        queue_handler::declare(vhost, frame_channel, self.broker_connection, request).await
    }

    async fn queue_bind(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::QueueBind,
    ) -> Result<Vec<Frame>, AmqpError> {
        let vhost = self.resolved_vhost(queue::CLASS_ID, queue::BIND)?;
        queue_handler::bind(vhost, frame_channel, self.broker_connection, request).await
    }

    async fn queue_unbind(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::QueueUnbind,
    ) -> Result<Vec<Frame>, AmqpError> {
        let vhost = self.resolved_vhost(queue::CLASS_ID, queue::UNBIND)?;
        queue_handler::unbind(vhost, frame_channel, self.broker_connection, request).await
    }

    async fn queue_purge(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::QueuePurge,
    ) -> Result<Vec<Frame>, AmqpError> {
        let vhost = self.resolved_vhost(queue::CLASS_ID, queue::PURGE)?;
        queue_handler::purge(vhost, frame_channel, self.broker_connection, request).await
    }

    async fn queue_delete(
        &mut self,
        frame_channel: u16,
        request: wolf_amqp_codec::QueueDelete,
    ) -> Result<Vec<Frame>, AmqpError> {
        let Some(name) = self.vhost.clone() else {
            return Err(self.command_invalid(
                queue::CLASS_ID,
                queue::DELETE,
                "connection has no open vhost",
            ));
        };
        let broker = self.broker.clone();
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(queue::CLASS_ID, queue::DELETE)
        })?;
        queue_handler::delete_with_hub(
            vhost,
            frame_channel,
            self.broker_connection,
            request,
            &mut self.consumers,
            &self.hub,
        )
        .await
    }

    fn resolved_vhost(&self, class_id: u16, method_id: u16) -> Result<&Vhost, AmqpError> {
        let Some(name) = self.vhost.as_ref() else {
            return Err(self.command_invalid(class_id, method_id, "connection has no open vhost"));
        };
        self.broker
            .vhost(name)
            .map_err(|error| AmqpError::from_domain(&error).with_offending(class_id, method_id))
    }

    async fn confirm_select(
        &mut self,
        frame_channel: u16,
        select: wolf_amqp_codec::ConfirmSelect,
    ) -> Result<Vec<Frame>, AmqpError> {
        let Some(channel) = self.channels.get_mut(&frame_channel) else {
            return Err(self.command_invalid(
                confirm::CLASS_ID,
                confirm::SELECT,
                "channel is not open",
            ));
        };
        Ok(channel
            .confirm
            .select(frame_channel, select.nowait)
            .into_iter()
            .collect())
    }

    async fn basic_publish(
        &mut self,
        frame_channel: u16,
        publish: BasicPublish,
    ) -> Result<Vec<Frame>, AmqpError> {
        let Some(channel) = self.channels.get_mut(&frame_channel) else {
            return Err(self.command_invalid(
                basic::CLASS_ID,
                basic::PUBLISH,
                "channel is not open",
            ));
        };
        if channel.is_closing() {
            return Ok(Vec::new());
        }
        if channel.pending_publish.is_some() {
            return Err(unexpected_frame(
                "basic.publish arrived before the previous content finished",
            ));
        }
        channel.pending_publish = Some(PendingPublish::begin(publish)?);
        Ok(Vec::new())
    }

    async fn handle_content_header(
        &mut self,
        frame_channel: u16,
        header: ContentHeader,
    ) -> Result<Vec<Frame>, AmqpError> {
        if self.state != State::Open {
            return Err(self.command_invalid(0, 0, "content frames require an open connection"));
        }

        let completed = {
            let Some(channel) = self.channels.get_mut(&frame_channel) else {
                return Err(unexpected_frame("content header on an unopened channel"));
            };
            if channel.is_closing() {
                return Ok(Vec::new());
            }
            let Some(mut pending) = channel.pending_publish.take() else {
                return Err(unexpected_frame("content header without basic.publish"));
            };
            let completed = pending.accept_header(header)?;
            if completed.is_none() {
                channel.pending_publish = Some(pending);
            }
            completed
        };

        match completed {
            Some(completed) => self.complete_publish(frame_channel, completed).await,
            None => Ok(Vec::new()),
        }
    }

    async fn handle_content_body(
        &mut self,
        frame_channel: u16,
        body: bytes::Bytes,
    ) -> Result<Vec<Frame>, AmqpError> {
        if self.state != State::Open {
            return Err(self.command_invalid(0, 0, "content frames require an open connection"));
        }

        let completed = {
            let Some(channel) = self.channels.get_mut(&frame_channel) else {
                return Err(unexpected_frame("content body on an unopened channel"));
            };
            if channel.is_closing() {
                return Ok(Vec::new());
            }
            let Some(pending) = channel.pending_publish.as_mut() else {
                return Err(unexpected_frame("content body without a content header"));
            };
            pending.accept_body(body)?
        };

        match completed {
            Some(completed) => self.complete_publish(frame_channel, completed).await,
            None => Ok(Vec::new()),
        }
    }

    async fn complete_publish(
        &mut self,
        frame_channel: u16,
        completed: CompletedPublish,
    ) -> Result<Vec<Frame>, AmqpError> {
        if let Some(channel) = self.channels.get_mut(&frame_channel) {
            channel.pending_publish = None;
        }

        if self.memory_alarm {
            if self.deferred_publish.is_some() {
                return Err(unexpected_frame(
                    "only one publish may wait for the memory alarm",
                ));
            }
            self.deferred_publish = Some((frame_channel, completed));
            return Ok(Vec::new());
        }

        self.route_completed_publish(frame_channel, completed).await
    }

    async fn route_completed_publish(
        &mut self,
        frame_channel: u16,
        completed: CompletedPublish,
    ) -> Result<Vec<Frame>, AmqpError> {
        let Some(vhost_name) = self.vhost.clone() else {
            return Err(self.command_invalid(0, 0, "connection has no open vhost"));
        };
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&vhost_name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic::PUBLISH)
        })?;
        let metadata = MessageMetadata::new(
            completed.method().exchange.as_str(),
            completed.method().routing_key.as_str(),
            completed.property_bytes(),
        );
        let request = PublishRequest {
            exchange: completed.method().exchange.as_str(),
            routing_key: completed.method().routing_key.as_str(),
            mandatory: completed.method().mandatory,
            message: Message::with_metadata(completed.body().clone(), metadata),
            properties: completed.properties().clone(),
        };

        let outcome = broker
            .publish(&vhost_name, self.broker_connection, request)
            .await
            .map_err(|error| {
                AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic::PUBLISH)
            })?;

        let mut frames = match outcome {
            PublishOutcome::Returned(_) => {
                completed.return_frames(frame_channel, self.frame_max)?
            }
            PublishOutcome::Routed { .. } | PublishOutcome::Dropped => Vec::new(),
        };

        vhost
            .dispatch_all_detached(self.hub.sink())
            .map_err(|error| {
                AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic::PUBLISH)
            })?;

        if let Some(channel) = self.channels.get_mut(&frame_channel) {
            if let Some(sequence) = channel.confirm.next_publish_sequence() {
                frames.push(ConfirmState::ack_frame(frame_channel, sequence));
            }
        }

        Ok(frames)
    }

    fn vhost_name(&self, class_id: u16, method_id: u16) -> Result<VhostName, AmqpError> {
        self.vhost.clone().ok_or_else(|| {
            self.command_invalid(class_id, method_id, "connection has no open vhost")
        })
    }

    fn channel_broker_id(
        &self,
        frame_channel: u16,
        class_id: u16,
        method_id: u16,
    ) -> Result<ChannelId, AmqpError> {
        self.channels
            .get(&frame_channel)
            .map(|channel| channel.broker_id)
            .ok_or_else(|| self.command_invalid(class_id, method_id, "channel is not open"))
    }

    async fn basic_qos(
        &mut self,
        frame_channel: u16,
        request: BasicQos,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_consume::QOS)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_consume::QOS)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_consume::QOS)
        })?;
        basic_consume_handler::qos(vhost, broker_channel, &self.hub, frame_channel, request).await
    }

    async fn basic_consume(
        &mut self,
        frame_channel: u16,
        request: BasicConsume,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_consume::CONSUME)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_consume::CONSUME)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_consume::CONSUME)
        })?;
        basic_consume_handler::consume(
            vhost,
            &mut self.consumers,
            &self.hub,
            &self.outbox,
            frame_channel,
            broker_channel,
            self.broker_connection,
            request,
        )
        .await
    }

    async fn basic_cancel(
        &mut self,
        frame_channel: u16,
        request: BasicCancel,
    ) -> Result<Vec<Frame>, AmqpError> {
        let name = self.vhost_name(basic::CLASS_ID, basic_consume::CANCEL)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_consume::CANCEL)
        })?;
        basic_consume_handler::cancel(
            vhost,
            &mut self.consumers,
            &self.hub,
            frame_channel,
            request,
        )
        .await
    }

    async fn basic_get(
        &mut self,
        frame_channel: u16,
        request: BasicGet,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_consume::GET)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_consume::GET)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_consume::GET)
        })?;
        basic_get_handler::get(
            vhost,
            frame_channel,
            broker_channel,
            self.broker_connection,
            self.frame_max,
            request,
        )
        .await
    }

    async fn basic_ack(
        &mut self,
        frame_channel: u16,
        request: BasicAck,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_ack::ACK)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_ack::ACK)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_ack::ACK)
        })?;
        basic_ack_handler::ack(vhost, broker_channel, &self.hub, request).await
    }

    async fn basic_nack(
        &mut self,
        frame_channel: u16,
        request: BasicNack,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_ack::NACK)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_ack::NACK)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_ack::NACK)
        })?;
        basic_ack_handler::nack(vhost, broker_channel, &self.hub, request).await
    }

    async fn basic_reject(
        &mut self,
        frame_channel: u16,
        request: BasicReject,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_ack::REJECT)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_ack::REJECT)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_ack::REJECT)
        })?;
        basic_ack_handler::reject(vhost, broker_channel, &self.hub, request).await
    }

    async fn basic_recover(
        &mut self,
        frame_channel: u16,
        request: BasicRecover,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_recover::RECOVER)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_recover::RECOVER)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error).with_offending(basic::CLASS_ID, basic_recover::RECOVER)
        })?;
        basic_ack_handler::recover(vhost, broker_channel, &self.hub, frame_channel, request).await
    }

    async fn basic_recover_async(
        &mut self,
        frame_channel: u16,
        request: BasicRecoverAsync,
    ) -> Result<Vec<Frame>, AmqpError> {
        let broker_channel =
            self.channel_broker_id(frame_channel, basic::CLASS_ID, basic_recover::RECOVER_ASYNC)?;
        let name = self.vhost_name(basic::CLASS_ID, basic_recover::RECOVER_ASYNC)?;
        let broker = Arc::clone(&self.broker);
        let vhost = broker.vhost(&name).map_err(|error| {
            AmqpError::from_domain(&error)
                .with_offending(basic::CLASS_ID, basic_recover::RECOVER_ASYNC)
        })?;
        basic_ack_handler::recover_async(vhost, broker_channel, &self.hub, request).await
    }

    /// Handles a delivery or cancellation routed to this connection.
    pub fn delivery_event(&mut self, event: ServerEvent) -> Result<Vec<Frame>, AmqpError> {
        match event {
            ServerEvent::Delivery(delivery) => {
                let frame_max = self.frame_max;
                let Some(entry) = self.consumers.get(delivery.consumer) else {
                    return Ok(Vec::new());
                };
                basic_consume_handler::delivery_frames(
                    entry.channel,
                    frame_max,
                    &entry.tag,
                    &delivery,
                )
            }
            ServerEvent::Cancel(cancel) => {
                self.hub.unregister(cancel.consumer);
                Ok(self
                    .consumers
                    .remove(cancel.consumer)
                    .map(|entry| {
                        vec![basic_consume_handler::cancel_frame(
                            entry.channel,
                            entry.tag,
                        )]
                    })
                    .unwrap_or_default())
            }
        }
    }

    /// Cleans up broker consumer state and unacked deliveries for one channel.
    async fn cleanup_channel(&mut self, frame_channel: u16, broker_channel: ChannelId) {
        let Some(name) = self.vhost.clone() else {
            for (consumer, _) in self.consumers.remove_channel(frame_channel) {
                self.hub.unregister(consumer);
            }
            return;
        };
        let broker = Arc::clone(&self.broker);
        if let Ok(vhost) = broker.vhost(&name) {
            let _ = vhost.close_channel(broker_channel).await;
            for (consumer, _) in self.consumers.remove_channel(frame_channel) {
                self.hub.unregister(consumer);
            }
            let _ = vhost.dispatch_all_detached(self.hub.sink());
        } else {
            for (consumer, _) in self.consumers.remove_channel(frame_channel) {
                self.hub.unregister(consumer);
            }
        }
    }

    /// Cleans up all broker state owned by this connection. Idempotent.
    pub async fn cleanup_connection(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;

        if let Some(name) = self.vhost.clone() {
            let broker = Arc::clone(&self.broker);
            if let Ok(vhost) = broker.vhost(&name) {
                let _ = vhost.close_connection(self.broker_connection).await;
                let _ = vhost.dispatch_all_detached(self.hub.sink());
            }
        }
        for (consumer, _) in self.consumers.drain() {
            self.hub.unregister(consumer);
        }
        self.channels.clear();
    }

    async fn open_channel(&mut self, frame_channel: u16) -> Result<Vec<Frame>, AmqpError> {
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

    async fn close_channel(&mut self, frame_channel: u16) -> Result<Vec<Frame>, AmqpError> {
        let Some(broker_channel) = self
            .channels
            .get(&frame_channel)
            .map(|channel| channel.broker_id)
        else {
            return Err(self.command_invalid(
                channel::CLASS_ID,
                channel::CLOSE,
                "channel is not open",
            ));
        };
        self.cleanup_channel(frame_channel, broker_channel).await;
        self.channels.remove(&frame_channel);
        Ok(vec![Frame::Method {
            channel: frame_channel,
            method: Method::Channel(ChannelMethod::CloseOk(ChannelCloseOk)),
        }])
    }

    async fn channel_flow(
        &mut self,
        frame_channel: u16,
        active: bool,
    ) -> Result<Vec<Frame>, AmqpError> {
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

fn unexpected_frame(text: impl Into<String>) -> AmqpError {
    AmqpError::connection(ReplyCode::UnexpectedFrame, text)
}

fn supports_connection_blocked(properties: &FieldTable) -> bool {
    let Some(FieldValue::FieldTable(capabilities)) = properties.get("capabilities") else {
        return false;
    };
    matches!(
        capabilities.get("connection.blocked"),
        Some(FieldValue::Bool(true))
    )
}

fn blocked_frame() -> Frame {
    Frame::Method {
        channel: 0,
        method: Method::Connection(ConnectionMethod::Blocked(Blocked {
            reason: ShortString::new("low on memory"),
        })),
    }
}

fn unblocked_frame() -> Frame {
    Frame::Method {
        channel: 0,
        method: Method::Connection(ConnectionMethod::Unblocked(Unblocked)),
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
        "connection.blocked",
    ] {
        capabilities.insert(capability, FieldValue::Bool(true));
    }
    capabilities
}

#[cfg(test)]
mod tests {
    use wolf_amqp_codec::{ChannelOpen, FieldTable, FieldValue, Open, QueueDeclare, ShortString};
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
            Arc::new(Account::guest()),
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
    fn capabilities_are_exactly_the_six_m1_entries() {
        let capabilities = capabilities();
        assert_eq!(capabilities.len(), 6);
        for required in [
            "publisher_confirms",
            "basic.nack",
            "consumer_cancel_notify",
            "per_consumer_qos",
            "authentication_failure_close",
            "connection.blocked",
        ] {
            assert_eq!(
                capabilities.get(required),
                Some(&FieldValue::Bool(true)),
                "{required} must be advertised"
            );
        }
        for forbidden in ["connection.unblocked", "channel.flow"] {
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

    #[tokio::test]
    async fn tune_ok_negotiates_channel_frame_and_heartbeat_values() {
        let mut connection = connection();
        connection.handle_frame(start_ok()).await.expect("start-ok");
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
            .await
            .expect("tune-ok");
        assert_eq!(connection.channel_max(), 100);
        assert_eq!(connection.frame_max(), 65_536);
        assert_eq!(connection.heartbeat(), 60);
    }

    #[tokio::test]
    async fn channel_open_and_flow_follow_the_negotiated_state() {
        let mut connection = connection();
        connection.handle_frame(start_ok()).await.expect("start-ok");
        connection
            .handle_frame(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::TuneOk(TuneOk {
                    channel_max: 2,
                    frame_max: 0,
                    heartbeat: 0,
                })),
            })
            .await
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
            .await
            .expect("open");

        let opened = connection
            .handle_frame(Frame::Method {
                channel: 1,
                method: Method::Channel(ChannelMethod::Open(ChannelOpen {
                    out_of_band: ShortString::new(""),
                })),
            })
            .await
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
            .await
            .expect_err("flow false is unsupported");
        assert_eq!(error.reply_code, ReplyCode::NotImplemented);
    }

    #[tokio::test]
    async fn cleanup_connection_unregisters_captured_outbox_routes() {
        let hub = Arc::new(DeliveryHub::new());
        let (outbox, _receiver) = hub.subscribe();
        let mut connection = Connection::with_delivery(
            "127.0.0.1:1234".parse().expect("valid address"),
            test_broker(),
            Arc::new(Account::guest()),
            BrokerConnectionId::new(1),
            Arc::clone(&hub),
            outbox,
        );

        connection.handle_frame(start_ok()).await.expect("start-ok");
        connection
            .handle_frame(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::TuneOk(TuneOk {
                    channel_max: 0,
                    frame_max: 0,
                    heartbeat: 0,
                })),
            })
            .await
            .expect("tune-ok");
        connection
            .handle_frame(Frame::Method {
                channel: 0,
                method: Method::Connection(ConnectionMethod::Open(Open {
                    virtual_host: ShortString::new("/"),
                    capabilities: ShortString::new(""),
                    insist: false,
                })),
            })
            .await
            .expect("open");
        connection
            .handle_frame(Frame::Method {
                channel: 1,
                method: Method::Channel(ChannelMethod::Open(ChannelOpen {
                    out_of_band: ShortString::new(""),
                })),
            })
            .await
            .expect("channel open");
        connection
            .handle_frame(Frame::Method {
                channel: 1,
                method: Method::Queue(QueueMethod::Declare(QueueDeclare {
                    ticket: 0,
                    queue: ShortString::new("jobs"),
                    passive: false,
                    durable: false,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                    arguments: FieldTable::new(),
                })),
            })
            .await
            .expect("queue declare");
        connection
            .handle_frame(Frame::Method {
                channel: 1,
                method: Method::Basic(BasicMethod::Consume(BasicConsume {
                    ticket: 0,
                    queue: ShortString::new("jobs"),
                    consumer_tag: ShortString::new("ctag"),
                    no_local: false,
                    no_ack: false,
                    exclusive: false,
                    nowait: false,
                    arguments: FieldTable::new(),
                })),
            })
            .await
            .expect("basic.consume");

        assert_eq!(hub.route_count(), 1);

        connection.cleanup_connection().await;

        assert_eq!(hub.route_count(), 0);
    }
}
