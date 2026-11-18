//! Server-side assembly of `basic.publish` content and mandatory returns.
//!
//! A publish arrives as three frames: the method, one content header and zero
//! or more content bodies. [`PendingPublish`] tracks that sequence per channel
//! and produces a [`CompletedPublish`] once the declared body size has been
//! received. Bodies stay as [`Bytes`] handles, so single-chunk payloads and
//! fan-out share the original allocation.

use bytes::Bytes;
use wolf_amqp_codec::{
    BasicMethod, BasicProperties, BasicPublish, BasicReturn, ContentBodyAssembler, ContentHeader,
    Frame, Method, ShortString, basic, split_content_body,
};

use crate::amqp::error::{AmqpError, ReplyCode};

/// A publish whose method frame has been received but whose content is not
/// complete yet.
#[derive(Debug)]
pub struct PendingPublish {
    method: BasicPublish,
    header: Option<ContentHeader>,
    properties: Option<BasicProperties>,
    assembler: Option<ContentBodyAssembler>,
    remaining: u64,
}

/// A fully assembled publish ready to be routed.
#[derive(Debug)]
pub struct CompletedPublish {
    method: BasicPublish,
    header: ContentHeader,
    properties: BasicProperties,
    body: Bytes,
}

impl PendingPublish {
    /// Starts a publish, rejecting the unsupported `immediate` flag.
    pub fn begin(method: BasicPublish) -> Result<Self, AmqpError> {
        if method.immediate {
            return Err(AmqpError::connection(
                ReplyCode::NotImplemented,
                "basic.publish with immediate=true is not supported",
            )
            .with_offending(basic::CLASS_ID, basic::PUBLISH));
        }
        Ok(Self {
            method,
            header: None,
            properties: None,
            assembler: None,
            remaining: 0,
        })
    }

    /// Accepts the content header, completing zero-length bodies immediately.
    pub fn accept_header(
        &mut self,
        header: ContentHeader,
    ) -> Result<Option<CompletedPublish>, AmqpError> {
        if self.header.is_some() {
            return Err(unexpected_frame("content header received twice"));
        }
        if header.class_id != basic::CLASS_ID || header.weight != 0 {
            return Err(unexpected_frame(
                "content header class or weight does not match basic.publish",
            ));
        }

        let properties = header
            .basic_properties()
            .map_err(|error| unexpected_frame(format!("invalid basic properties: {error}")))?;
        if let Some(expiration) = &properties.expiration {
            if !expiration.as_str().is_empty() {
                return Err(AmqpError::new(
                    ReplyCode::PreconditionFailed,
                    "per-message expiration is not supported",
                )
                .with_offending(basic::CLASS_ID, basic::PUBLISH));
            }
        }

        self.remaining = header.body_size;
        self.assembler = Some(ContentBodyAssembler::new(&header));
        self.properties = Some(properties);
        self.header = Some(header);

        if self.remaining == 0 {
            self.complete()
        } else {
            Ok(None)
        }
    }

    /// Accepts one body chunk, completing the publish when the declared size
    /// has been received.
    pub fn accept_body(&mut self, body: Bytes) -> Result<Option<CompletedPublish>, AmqpError> {
        let Some(assembler) = self.assembler.as_mut() else {
            return Err(unexpected_frame("content body without a content header"));
        };
        assembler
            .push(body.clone())
            .map_err(|error| unexpected_frame(format!("invalid content body: {error}")))?;
        self.remaining = self.remaining.saturating_sub(body.len() as u64);

        if self.remaining == 0 {
            self.complete()
        } else {
            Ok(None)
        }
    }

    fn complete(&mut self) -> Result<Option<CompletedPublish>, AmqpError> {
        let header = self
            .header
            .take()
            .ok_or_else(|| unexpected_frame("content body without a content header"))?;
        let properties = self
            .properties
            .take()
            .ok_or_else(|| unexpected_frame("content properties are missing"))?;
        let assembler = self
            .assembler
            .take()
            .ok_or_else(|| unexpected_frame("content body assembler is missing"))?;
        let body = assembler
            .finish()
            .map_err(|error| unexpected_frame(format!("invalid content body: {error}")))?;

        Ok(Some(CompletedPublish {
            method: self.method.clone(),
            header,
            properties,
            body,
        }))
    }
}

impl CompletedPublish {
    /// Returns the publish method.
    pub fn method(&self) -> &BasicPublish {
        &self.method
    }

    /// Returns the decoded basic properties.
    pub fn properties(&self) -> &BasicProperties {
        &self.properties
    }

    /// Returns the assembled body, sharing the original allocation when it
    /// arrived in a single chunk.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Builds the `basic.return` method, content header and body frames.
    ///
    /// The returned body chunks are zero-copy slices of the assembled body and
    /// the content header preserves the original property bytes.
    pub fn return_frames(&self, channel: u16, frame_max: u32) -> Result<Vec<Frame>, AmqpError> {
        let method = Method::Basic(BasicMethod::Return(BasicReturn {
            reply_code: 312,
            reply_text: ShortString::new("NO_ROUTE"),
            exchange: self.method.exchange.clone(),
            routing_key: self.method.routing_key.clone(),
        }));
        let header = ContentHeader {
            class_id: basic::CLASS_ID,
            weight: 0,
            body_size: self.body.len() as u64,
            properties: self.header.properties.clone(),
        };

        let mut frames = vec![
            Frame::Method { channel, method },
            Frame::ContentHeader { channel, header },
        ];
        for chunk in split_content_body(self.body.clone(), frame_max)
            .map_err(|error| unexpected_frame(format!("cannot split returned body: {error}")))?
        {
            frames.push(Frame::ContentBody {
                channel,
                body: chunk,
            });
        }
        Ok(frames)
    }
}

fn unexpected_frame(text: impl Into<String>) -> AmqpError {
    AmqpError::connection(ReplyCode::UnexpectedFrame, text)
        .with_offending(basic::CLASS_ID, basic::PUBLISH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wolf_amqp_codec::{FieldTable, ShortString};

    fn publish(mandatory: bool, immediate: bool) -> BasicPublish {
        BasicPublish {
            ticket: 0,
            exchange: ShortString::new(""),
            routing_key: ShortString::new("missing"),
            mandatory,
            immediate,
        }
    }

    fn header(body_size: u64, properties: &BasicProperties) -> ContentHeader {
        ContentHeader::from_basic_properties(body_size, properties).expect("header encodes")
    }

    #[test]
    fn immediate_publish_is_rejected_at_connection_level() {
        let error = PendingPublish::begin(publish(false, true)).expect_err("immediate");

        assert_eq!(error.reply_code, ReplyCode::NotImplemented);
        assert_eq!(error.reply_code.code(), 540);
        assert_eq!(
            error.close_level,
            crate::amqp::error::CloseLevel::Connection
        );
        assert_eq!(
            (error.offending_class_id, error.offending_method_id),
            (60, 40)
        );
    }

    #[test]
    fn zero_length_body_completes_on_the_content_header() {
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");
        let completed = pending
            .accept_header(header(0, &BasicProperties::default()))
            .expect("header")
            .expect("zero-length publish completes");

        assert!(completed.body().is_empty());
    }

    #[test]
    fn single_chunk_body_is_not_copied() {
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");
        assert!(
            pending
                .accept_header(header(7, &BasicProperties::default()))
                .expect("header")
                .is_none()
        );
        let body = Bytes::from_static(b"payload");
        let pointer = body.as_ptr();
        let completed = pending
            .accept_body(body)
            .expect("body")
            .expect("publish completes");

        assert_eq!(completed.body().as_ptr(), pointer);
        assert_eq!(completed.body().as_ref(), b"payload");
    }

    #[test]
    fn non_empty_expiration_is_rejected_at_channel_level() {
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");
        let properties = BasicProperties {
            expiration: Some(ShortString::new("1000")),
            ..BasicProperties::default()
        };

        let error = pending
            .accept_header(header(0, &properties))
            .expect_err("expiration");

        assert_eq!(error.reply_code, ReplyCode::PreconditionFailed);
        assert_eq!(error.reply_code.code(), 406);
        assert_eq!(error.close_level, crate::amqp::error::CloseLevel::Channel);
        assert_eq!(
            (error.offending_class_id, error.offending_method_id),
            (60, 40)
        );
    }

    #[test]
    fn empty_expiration_is_accepted() {
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");
        let properties = BasicProperties {
            expiration: Some(ShortString::new("")),
            ..BasicProperties::default()
        };

        let completed = pending
            .accept_header(header(0, &properties))
            .expect("empty expiration")
            .expect("publish completes");

        assert_eq!(
            completed.properties().expiration,
            Some(ShortString::new(""))
        );
    }

    #[test]
    fn body_without_header_is_unexpected() {
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");

        let error = pending
            .accept_body(Bytes::from_static(b"orphan"))
            .expect_err("body without header");

        assert_eq!(error.reply_code, ReplyCode::UnexpectedFrame);
        assert_eq!(error.reply_code.code(), 505);
        assert_eq!(
            error.close_level,
            crate::amqp::error::CloseLevel::Connection
        );
    }

    #[test]
    fn oversized_body_is_unexpected() {
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");
        pending
            .accept_header(header(2, &BasicProperties::default()))
            .expect("header");

        let error = pending
            .accept_body(Bytes::from_static(b"too-long"))
            .expect_err("oversized body");

        assert_eq!(error.reply_code, ReplyCode::UnexpectedFrame);
        assert_eq!(
            error.close_level,
            crate::amqp::error::CloseLevel::Connection
        );
    }

    #[test]
    fn content_header_class_mismatch_is_unexpected() {
        let mut header =
            ContentHeader::from_basic_properties(0, &BasicProperties::default()).expect("header");
        header.class_id = 50;
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");

        let error = pending
            .accept_header(header)
            .expect_err("mismatched content header");

        assert_eq!(error.reply_code, ReplyCode::UnexpectedFrame);
    }

    #[test]
    fn content_header_weight_mismatch_is_unexpected() {
        let mut header =
            ContentHeader::from_basic_properties(0, &BasicProperties::default()).expect("header");
        header.weight = 1;
        let mut pending = PendingPublish::begin(publish(false, false)).expect("begin");

        let error = pending
            .accept_header(header)
            .expect_err("mismatched content header");

        assert_eq!(error.reply_code, ReplyCode::UnexpectedFrame);
    }

    #[test]
    fn returned_frames_preserve_method_properties_and_body_chunks() {
        let mut pending = PendingPublish::begin(publish(true, false)).expect("begin");
        let properties = BasicProperties {
            content_type: Some(ShortString::new("text/plain")),
            headers: Some(FieldTable::new()),
            ..BasicProperties::default()
        };
        let body = Bytes::from_static(b"return-me");
        let pointer = body.as_ptr();
        let body_len = body.len();
        pending
            .accept_header(header(body_len as u64, &properties))
            .expect("header");
        let completed = pending
            .accept_body(body)
            .expect("body")
            .expect("publish completes");

        let frames = completed.return_frames(3, 16).expect("return frames");

        let Frame::Method {
            channel,
            method: Method::Basic(BasicMethod::Return(returned)),
        } = &frames[0]
        else {
            panic!("expected basic.return first, got {:?}", frames[0]);
        };
        assert_eq!(*channel, 3);
        assert_eq!(returned.reply_code, 312);
        assert_eq!(returned.reply_text.as_str(), "NO_ROUTE");
        assert_eq!(returned.exchange.as_str(), "");
        assert_eq!(returned.routing_key.as_str(), "missing");

        let Frame::ContentHeader { channel, header } = &frames[1] else {
            panic!("expected content header second, got {:?}", frames[1]);
        };
        assert_eq!(*channel, 3);
        assert_eq!(header.body_size, body_len as u64);
        assert_eq!(header.basic_properties().expect("properties"), properties);

        let mut chunks = Vec::new();
        for frame in &frames[2..] {
            let Frame::ContentBody { channel, body } = frame else {
                panic!("expected content body, got {frame:?}");
            };
            assert_eq!(*channel, 3);
            chunks.push(body.clone());
        }
        assert_eq!(chunks.concat(), b"return-me");
        assert_eq!(chunks[0].as_ptr(), pointer);
    }
}
