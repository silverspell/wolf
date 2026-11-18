//! SASL PLAIN authentication and the loopback-only guest policy.
//!
//! Wolf ships a built-in `guest`/`guest` credential for RabbitMQ compatibility;
//! deployments may override it through [`crate::config`]. Like RabbitMQ, the
//! guest account may only connect from a loopback address.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use wolf_amqp_codec::LongString;

use crate::amqp::error::{AmqpError, ReplyCode};

/// The built-in RabbitMQ-compatible user name.
const GUEST_USER: &str = "guest";
/// The built-in RabbitMQ-compatible password.
///
/// This is the loopback-restricted default account kept for RabbitMQ
/// compatibility; the suppression below is deliberately limited to this line.
const GUEST_PASSWORD: &str = "guest"; // nosemgrep: loopflow-rust-hardcoded-secret-or-insecure-tls -- loopback-restricted default account for RabbitMQ compatibility (WOLF-22)

/// A configured user account.
///
/// The password is private and redacted from [`fmt::Debug`] output.
#[derive(Clone, PartialEq, Eq)]
pub struct Account {
    username: String,
    password: String,
}

impl Account {
    /// Creates an account from a user name and password.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Returns the built-in RabbitMQ-compatible guest account.
    pub fn guest() -> Self {
        Self::new(GUEST_USER, GUEST_PASSWORD)
    }

    /// Returns the account's user name.
    pub fn username(&self) -> &str {
        &self.username
    }

    fn matches(&self, credentials: &Credentials) -> bool {
        self.username == credentials.username && self.password == credentials.password
    }
}

impl Default for Account {
    fn default() -> Self {
        Self::guest()
    }
}

impl fmt::Debug for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Account")
            .field("username", &self.username)
            .field("password", &"redacted")
            .finish()
    }
}

/// Credentials extracted from a SASL PLAIN response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    /// Authenticated user name.
    pub username: String,
    /// Authenticated password.
    pub password: String,
}

/// Failure while authenticating a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The client selected a mechanism other than PLAIN.
    UnsupportedMechanism(String),
    /// The PLAIN response did not have the expected NUL-separated shape.
    MalformedResponse(&'static str),
    /// The supplied credentials do not match a configured user.
    InvalidCredentials,
    /// The guest account was used from a non-loopback peer.
    GuestFromNonLoopback,
}

impl AuthError {
    /// Converts the authentication failure into a connection-level AMQP error.
    pub fn into_amqp(self) -> AmqpError {
        let text = match &self {
            Self::UnsupportedMechanism(mechanism) => {
                format!("unsupported authentication mechanism '{mechanism}'")
            }
            Self::MalformedResponse(reason) => {
                format!("malformed PLAIN authentication response: {reason}")
            }
            Self::InvalidCredentials => "invalid credentials".to_string(),
            Self::GuestFromNonLoopback => {
                "guest user is only allowed to connect from loopback".to_string()
            }
        };
        AmqpError::connection(ReplyCode::AccessRefused, text).with_offending(10, 11)
    }
}

impl From<AuthError> for AmqpError {
    fn from(error: AuthError) -> Self {
        error.into_amqp()
    }
}

/// Authenticates a client using the selected mechanism and response bytes.
///
/// Only `PLAIN` is supported. Credentials are compared against the configured
/// account, and the `guest` user is additionally restricted to loopback peers.
pub fn authenticate(
    mechanism: &str,
    response: &[u8],
    peer: SocketAddr,
    account: &Account,
) -> Result<Credentials, AuthError> {
    if mechanism != "PLAIN" {
        return Err(AuthError::UnsupportedMechanism(mechanism.to_string()));
    }
    let credentials = parse_plain_response(response)?;
    if !account.matches(&credentials) {
        return Err(AuthError::InvalidCredentials);
    }
    if credentials.username == GUEST_USER && !is_loopback(peer.ip()) {
        return Err(AuthError::GuestFromNonLoopback);
    }
    Ok(credentials)
}

/// Parses a SASL PLAIN response.
///
/// The canonical form is `[authzid] NUL authcid NUL passwd`. The shorter
/// `authcid NUL passwd` form is tolerated as well.
pub fn parse_plain_response(response: &[u8]) -> Result<Credentials, AuthError> {
    let fields: Vec<&[u8]> = response.split(|byte| *byte == 0).collect();
    let (authcid, passwd) = match fields.as_slice() {
        [authcid, passwd] => (*authcid, *passwd),
        [_authzid, authcid, passwd] => (*authcid, *passwd),
        _ => {
            return Err(AuthError::MalformedResponse(
                "expected two or three NUL-separated fields",
            ));
        }
    };
    let username = std::str::from_utf8(authcid)
        .map_err(|_| AuthError::MalformedResponse("user name is not UTF-8"))?
        .to_string();
    let password = std::str::from_utf8(passwd)
        .map_err(|_| AuthError::MalformedResponse("password is not UTF-8"))?
        .to_string();
    Ok(Credentials { username, password })
}

/// Returns whether an address is a loopback address, including IPv4-mapped
/// IPv6 addresses such as `::ffff:127.0.0.1`.
pub fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(address) => address.is_loopback(),
        IpAddr::V6(address) => {
            address.is_loopback()
                || address
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| mapped.is_loopback())
        }
    }
}

/// Parses a PLAIN response as a long string, returning credentials.
pub fn authenticate_long_string(
    mechanism: &str,
    response: &LongString,
    peer: SocketAddr,
    account: &Account,
) -> Result<Credentials, AuthError> {
    authenticate(mechanism, response.as_bytes(), peer, account)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    fn peer(address: &str) -> SocketAddr {
        address.parse().expect("valid socket address")
    }

    fn guest() -> Account {
        Account::guest()
    }

    #[test]
    fn plain_parser_accepts_authzid_and_short_forms() {
        let canonical = parse_plain_response(b"\0guest\0secret").expect("valid response");
        assert_eq!(
            canonical,
            Credentials {
                username: "guest".to_string(),
                password: "secret".to_string(),
            }
        );

        let short = parse_plain_response(b"guest\0secret").expect("valid short response");
        assert_eq!(short, canonical);

        let with_authzid =
            parse_plain_response(b"ignored\0guest\0secret").expect("valid authzid response");
        assert_eq!(with_authzid, canonical);
    }

    #[test]
    fn plain_parser_rejects_malformed_responses() {
        assert!(parse_plain_response(b"guest").is_err());
        assert!(parse_plain_response(b"a\0b\0c\0d").is_err());
        assert!(parse_plain_response(&[0xff, 0, b'x']).is_err());
    }

    #[test]
    fn loopback_detection_covers_ipv4_ipv6_and_mapped_addresses() {
        assert!(is_loopback(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(is_loopback(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(is_loopback(IpAddr::V6(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped()
        )));
        assert!(!is_loopback(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))));
    }

    #[test]
    fn account_defaults_to_the_guest_credentials() {
        assert_eq!(Account::default(), Account::guest());
        assert_eq!(Account::default().username(), "guest");
    }

    #[test]
    fn guest_authenticates_from_loopback() {
        let credentials =
            authenticate("PLAIN", b"\0guest\0guest", peer("127.0.0.1:1234"), &guest())
                .expect("loopback guest is allowed");
        assert_eq!(credentials.username, "guest");
    }

    #[test]
    fn guest_is_rejected_from_non_loopback() {
        let error = authenticate(
            "PLAIN",
            b"\0guest\0guest",
            peer("192.0.2.10:1234"),
            &guest(),
        )
        .expect_err("non-loopback guest must be rejected");
        assert_eq!(error, AuthError::GuestFromNonLoopback);
        let amqp = error.into_amqp();
        assert_eq!(amqp.reply_code, ReplyCode::AccessRefused);
        assert_eq!(amqp.close_level, crate::amqp::error::CloseLevel::Connection);
    }

    #[test]
    fn configured_user_is_not_loopback_restricted() {
        let account = Account::new("alice", "s3cret");
        let credentials = authenticate(
            "PLAIN",
            b"\0alice\0s3cret",
            peer("192.0.2.10:1234"),
            &account,
        )
        .expect("configured users may connect from any peer");
        assert_eq!(credentials.username, "alice");
    }

    #[test]
    fn configured_user_rejects_wrong_password() {
        let account = Account::new("alice", "s3cret");
        assert_eq!(
            authenticate("PLAIN", b"\0alice\0wrong", peer("127.0.0.1:1"), &account),
            Err(AuthError::InvalidCredentials)
        );
        assert_eq!(
            authenticate("PLAIN", b"\0guest\0guest", peer("127.0.0.1:1"), &account),
            Err(AuthError::InvalidCredentials)
        );
    }

    #[test]
    fn unknown_credentials_and_mechanisms_are_rejected() {
        assert_eq!(
            authenticate("PLAIN", b"\0guest\0wrong", peer("127.0.0.1:1"), &guest()),
            Err(AuthError::InvalidCredentials)
        );
        assert_eq!(
            authenticate("AMQPLAIN", b"\0guest\0guest", peer("127.0.0.1:1"), &guest()),
            Err(AuthError::UnsupportedMechanism("AMQPLAIN".to_string()))
        );
    }

    #[test]
    fn debug_redacts_the_password() {
        let rendered = format!("{:?}", Account::new("alice", "s3cret"));
        assert!(rendered.contains("alice"));
        assert!(!rendered.contains("s3cret"));
        assert!(rendered.contains("redacted"));
    }
}
