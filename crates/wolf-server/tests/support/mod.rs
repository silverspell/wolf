#![allow(dead_code)]

//! Raw AMQP test client helpers shared by the server integration tests.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use wolf_amqp_codec::{
    ChannelClose, ChannelMethod, ChannelOpen, ChannelOpenOk, Close, ConnectionMethod, FieldTable,
    Frame, LongString, Method, Open, ProtocolHeader, ShortString, Start, StartOk, Tune, TuneOk,
};
use wolf_server::{ServerConfig, ShutdownHandle, start};

/// Timeout used for individual frame reads.
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Builds a loopback-only server configuration.
pub fn config() -> ServerConfig {
    ServerConfig {
        amqp_addr: "127.0.0.1:0".parse().expect("valid loopback address"),
        shutdown_timeout: Duration::from_secs(5),
        ..ServerConfig::default()
    }
}

/// Starts a test server on an ephemeral loopback port.
pub async fn start_server() -> (SocketAddr, ShutdownHandle) {
    start(config()).await.expect("server starts")
}

/// A minimal raw AMQP client used to inspect the exact wire behaviour.
#[derive(Debug)]
pub struct TestClient {
    stream: TcpStream,
    buffer: BytesMut,
}

impl TestClient {
    /// Connects to the server without sending the protocol header yet.
    pub async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect to server");
        Self {
            stream,
            buffer: BytesMut::new(),
        }
    }

    /// Sends the AMQP 0-9-1 protocol header.
    pub async fn send_protocol_header(&mut self) {
        self.stream
            .write_all(&ProtocolHeader::BYTES)
            .await
            .expect("write protocol header");
    }

    /// Encodes and sends one frame.
    pub async fn send_frame(&mut self, frame: &Frame) {
        let bytes = frame.try_encode().expect("frame encodes");
        self.stream.write_all(&bytes).await.expect("write frame");
    }

    /// Sends a method frame on the given channel.
    pub async fn send_method(&mut self, channel: u16, method: Method) {
        self.send_frame(&Frame::Method { channel, method }).await;
    }

    /// Sends a heartbeat frame on channel 0.
    pub async fn send_heartbeat(&mut self) {
        self.send_frame(&Frame::Heartbeat { channel: 0 }).await;
    }

    async fn fill(&mut self) -> bool {
        let mut buffer = [0u8; 4096];
        match self.stream.read(&mut buffer).await {
            Ok(0) | Err(_) => false,
            Ok(read) => {
                self.buffer.extend_from_slice(&buffer[..read]);
                true
            }
        }
    }

    /// Reads the next complete frame, waiting up to [`IO_TIMEOUT`].
    pub async fn read_frame(&mut self) -> Frame {
        timeout(IO_TIMEOUT, async {
            loop {
                let bytes = self.buffer.clone().freeze();
                match wolf_amqp_codec::decode_frame(&bytes) {
                    Ok((frame, used)) => {
                        self.buffer.advance(used);
                        return frame;
                    }
                    Err(wolf_amqp_codec::Error::Incomplete { .. }) => {
                        if !self.fill().await {
                            panic!("connection closed before a full frame arrived");
                        }
                    }
                    Err(error) => panic!("invalid frame from server: {error}"),
                }
            }
        })
        .await
        .expect("timed out waiting for a frame")
    }

    /// Reads the next method frame, skipping heartbeats.
    pub async fn read_method(&mut self) -> (u16, Method) {
        loop {
            if let Frame::Method { channel, method } = self.read_frame().await {
                return (channel, method);
            }
        }
    }

    /// Reads the next method frame on a specific channel.
    pub async fn read_method_on(&mut self, expected_channel: u16) -> Method {
        loop {
            let (channel, method) = self.read_method().await;
            if channel == expected_channel {
                return method;
            }
        }
    }

    /// Reads the next connection-class method on channel 0.
    pub async fn read_connection_method(&mut self) -> ConnectionMethod {
        loop {
            let (channel, method) = self.read_method().await;
            if channel == 0 {
                if let Method::Connection(method) = method {
                    return method;
                }
            }
        }
    }

    /// Sends the protocol header and reads the server's `connection.start`.
    pub async fn begin(&mut self) -> Start {
        self.send_protocol_header().await;
        match self.read_connection_method().await {
            ConnectionMethod::Start(start) => start,
            other => panic!("expected connection.start, got {other:?}"),
        }
    }

    /// Sends `connection.start-ok` with the given credentials.
    pub async fn send_start_ok(&mut self, username: &str, password: &str) {
        let mut response = Vec::new();
        response.push(0);
        response.extend_from_slice(username.as_bytes());
        response.push(0);
        response.extend_from_slice(password.as_bytes());
        self.send_method(
            0,
            Method::Connection(ConnectionMethod::StartOk(StartOk {
                client_properties: FieldTable::new(),
                mechanism: ShortString::new("PLAIN"),
                response: LongString::from(response),
                locale: ShortString::new("en_US"),
            })),
        )
        .await;
    }

    /// Reads the server's `connection.tune`.
    pub async fn read_tune(&mut self) -> Tune {
        match self.read_connection_method().await {
            ConnectionMethod::Tune(tune) => tune,
            other => panic!("expected connection.tune, got {other:?}"),
        }
    }

    /// Sends `connection.tune-ok` with the requested values.
    pub async fn send_tune_ok(&mut self, channel_max: u16, frame_max: u32, heartbeat: u16) {
        self.send_method(
            0,
            Method::Connection(ConnectionMethod::TuneOk(TuneOk {
                channel_max,
                frame_max,
                heartbeat,
            })),
        )
        .await;
    }

    /// Sends `connection.open` for a virtual host.
    pub async fn send_open(&mut self, virtual_host: &str) {
        self.send_method(
            0,
            Method::Connection(ConnectionMethod::Open(Open {
                virtual_host: ShortString::new(virtual_host),
                capabilities: ShortString::new(""),
                insist: false,
            })),
        )
        .await;
    }

    /// Reads `connection.open-ok`.
    pub async fn read_open_ok(&mut self) {
        match self.read_connection_method().await {
            ConnectionMethod::OpenOk(_) => {}
            other => panic!("expected connection.open-ok, got {other:?}"),
        }
    }

    /// Completes a full handshake with zero tune-ok values.
    pub async fn handshake(&mut self) -> Tune {
        self.handshake_with(0, 0, 0).await
    }

    /// Completes a full handshake with explicit tune-ok values.
    pub async fn handshake_with(
        &mut self,
        channel_max: u16,
        frame_max: u32,
        heartbeat: u16,
    ) -> Tune {
        let _ = self.begin().await;
        self.send_start_ok("guest", "guest").await;
        let tune = self.read_tune().await;
        self.send_tune_ok(channel_max, frame_max, heartbeat).await;
        self.send_open("/").await;
        self.read_open_ok().await;
        tune
    }

    /// Opens a channel and reads `channel.open-ok`.
    pub async fn open_channel(&mut self, channel: u16) -> ChannelOpenOk {
        self.send_method(
            channel,
            Method::Channel(ChannelMethod::Open(ChannelOpen {
                out_of_band: ShortString::new(""),
            })),
        )
        .await;
        match self.read_method_on(channel).await {
            Method::Channel(ChannelMethod::OpenOk(open_ok)) => open_ok,
            other => panic!("expected channel.open-ok, got {other:?}"),
        }
    }

    /// Closes a channel and reads `channel.close-ok`.
    pub async fn close_channel(&mut self, channel: u16) {
        self.send_method(
            channel,
            Method::Channel(ChannelMethod::Close(ChannelClose {
                reply_code: 200,
                reply_text: ShortString::new("OK"),
                class_id: 0,
                method_id: 0,
            })),
        )
        .await;
        match self.read_method_on(channel).await {
            Method::Channel(ChannelMethod::CloseOk(_)) => {}
            other => panic!("expected channel.close-ok, got {other:?}"),
        }
    }

    /// Reads a `connection.close` and returns its payload.
    pub async fn expect_connection_close(&mut self) -> Close {
        match self.read_connection_method().await {
            ConnectionMethod::Close(close) => close,
            other => panic!("expected connection.close, got {other:?}"),
        }
    }
}
