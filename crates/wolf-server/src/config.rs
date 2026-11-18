//! Server configuration.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::amqp::auth::Account;
use crate::memory::MemoryHighWatermark;

/// Environment variable that overrides the default user name.
pub const DEFAULT_USER_ENV: &str = "WOLF_DEFAULT_USER";
/// Environment variable that overrides the default password.
pub const DEFAULT_PASS_ENV: &str = "WOLF_DEFAULT_PASS";
/// Environment variable that sets the relative memory high watermark.
pub const MEMORY_HIGH_WATERMARK_RELATIVE_ENV: &str = "WOLF_MEMORY_HIGH_WATERMARK_RELATIVE";
/// Environment variable that sets the absolute memory high watermark in bytes.
pub const MEMORY_HIGH_WATERMARK_BYTES_ENV: &str = "WOLF_MEMORY_HIGH_WATERMARK_BYTES";
/// Environment variable that sets the memory check interval in milliseconds.
pub const MEMORY_CHECK_INTERVAL_MS_ENV: &str = "WOLF_MEMORY_CHECK_INTERVAL_MS";
/// Environment variable that sets each connection's outbox capacity.
pub const OUTBOX_CAPACITY_ENV: &str = "WOLF_OUTBOX_CAPACITY";

/// Default relative memory high watermark.
pub const DEFAULT_MEMORY_HIGH_WATERMARK_RELATIVE: f64 = 0.4;
/// Default memory check interval in milliseconds.
pub const DEFAULT_MEMORY_CHECK_INTERVAL_MS: u64 = 1000;
/// Default capacity of each connection's bounded outbox.
pub const DEFAULT_OUTBOX_CAPACITY: usize = crate::delivery::DEFAULT_OUTBOX_CAPACITY;

/// Configuration used to start a Wolf server.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// Address the AMQP listener binds to.
    pub amqp_addr: SocketAddr,
    /// Root directory for persistent broker state.
    pub data_dir: PathBuf,
    /// Maximum time to wait for sessions to finish during shutdown.
    pub shutdown_timeout: Duration,
    /// Account used to authenticate AMQP clients.
    pub account: Account,
    /// Memory high watermark used to trigger the memory alarm.
    pub memory_high_watermark: MemoryHighWatermark,
    /// Interval between process RSS samples.
    pub memory_check_interval: Duration,
    /// Capacity of each connection's bounded delivery outbox.
    pub outbox_capacity: usize,
}

impl ServerConfig {
    /// Builds a configuration from the environment.
    ///
    /// Both [`DEFAULT_USER_ENV`] and [`DEFAULT_PASS_ENV`] must be set together,
    /// or neither may be set. When neither is set the guest account is used.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup_with(environment_value)
    }

    /// Builds a configuration from a lookup function.
    ///
    /// This is the race-free testing counterpart of [`Self::from_env`].
    pub fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        Self::from_lookup_with(|name| Ok(lookup(name)))
    }

    fn from_lookup_with(
        mut lookup: impl FnMut(&'static str) -> Result<Option<String>, ConfigError>,
    ) -> Result<Self, ConfigError> {
        let user = lookup(DEFAULT_USER_ENV)?;
        let password = lookup(DEFAULT_PASS_ENV)?;
        let account = resolve_account(user, password)?;
        let relative = lookup(MEMORY_HIGH_WATERMARK_RELATIVE_ENV)?;
        let absolute = lookup(MEMORY_HIGH_WATERMARK_BYTES_ENV)?;
        let interval = lookup(MEMORY_CHECK_INTERVAL_MS_ENV)?;
        let outbox_capacity = lookup(OUTBOX_CAPACITY_ENV)?;

        let memory_high_watermark = resolve_memory_high_watermark(relative, absolute)?;
        let memory_check_interval = resolve_memory_check_interval(interval)?;
        let outbox_capacity = resolve_outbox_capacity(outbox_capacity)?;

        Ok(Self {
            account,
            memory_high_watermark,
            memory_check_interval,
            outbox_capacity,
            ..Self::default()
        })
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            amqp_addr: SocketAddr::from(([127, 0, 0, 1], 5672)),
            data_dir: PathBuf::from("data"),
            shutdown_timeout: Duration::from_secs(10),
            account: Account::guest(),
            memory_high_watermark: MemoryHighWatermark::Relative(
                DEFAULT_MEMORY_HIGH_WATERMARK_RELATIVE,
            ),
            memory_check_interval: Duration::from_millis(DEFAULT_MEMORY_CHECK_INTERVAL_MS),
            outbox_capacity: DEFAULT_OUTBOX_CAPACITY,
        }
    }
}

fn resolve_account(user: Option<String>, password: Option<String>) -> Result<Account, ConfigError> {
    match (user, password) {
        (None, None) => Ok(Account::guest()),
        (Some(user), Some(password)) => {
            if user.is_empty() {
                return Err(ConfigError::EmptyCredential(DEFAULT_USER_ENV));
            }
            if password.is_empty() {
                return Err(ConfigError::EmptyCredential(DEFAULT_PASS_ENV));
            }
            Ok(Account::new(user, password))
        }
        _ => Err(ConfigError::IncompleteCredentials),
    }
}

fn resolve_memory_high_watermark(
    relative: Option<String>,
    absolute: Option<String>,
) -> Result<MemoryHighWatermark, ConfigError> {
    let relative = match relative.as_deref() {
        Some(raw) => Some(parse_relative_watermark(raw)?),
        None => None,
    };

    if let Some(raw) = absolute.as_deref() {
        return Ok(MemoryHighWatermark::Absolute(parse_absolute_watermark(
            raw,
        )?));
    }

    Ok(MemoryHighWatermark::Relative(
        relative.unwrap_or(DEFAULT_MEMORY_HIGH_WATERMARK_RELATIVE),
    ))
}

fn parse_relative_watermark(raw: &str) -> Result<f64, ConfigError> {
    let value: f64 = raw.trim().parse().map_err(|_| {
        invalid_memory_setting(
            MEMORY_HIGH_WATERMARK_RELATIVE_ENV,
            raw,
            "a finite number between 0.0 and 1.0 inclusive",
        )
    })?;
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(invalid_memory_setting(
            MEMORY_HIGH_WATERMARK_RELATIVE_ENV,
            raw,
            "a finite number between 0.0 and 1.0 inclusive",
        ));
    }
    Ok(value)
}

fn parse_absolute_watermark(raw: &str) -> Result<u64, ConfigError> {
    let value: u64 = raw.trim().parse().map_err(|_| {
        invalid_memory_setting(
            MEMORY_HIGH_WATERMARK_BYTES_ENV,
            raw,
            "a positive byte count",
        )
    })?;
    if value == 0 {
        return Err(invalid_memory_setting(
            MEMORY_HIGH_WATERMARK_BYTES_ENV,
            raw,
            "a positive byte count",
        ));
    }
    Ok(value)
}

fn resolve_memory_check_interval(raw: Option<String>) -> Result<Duration, ConfigError> {
    let Some(raw) = raw else {
        return Ok(Duration::from_millis(DEFAULT_MEMORY_CHECK_INTERVAL_MS));
    };

    let millis: u64 = raw.trim().parse().map_err(|_| {
        invalid_memory_setting(
            MEMORY_CHECK_INTERVAL_MS_ENV,
            &raw,
            "a positive number of milliseconds",
        )
    })?;
    if millis == 0 {
        return Err(invalid_memory_setting(
            MEMORY_CHECK_INTERVAL_MS_ENV,
            &raw,
            "a positive number of milliseconds",
        ));
    }
    Ok(Duration::from_millis(millis))
}

fn resolve_outbox_capacity(raw: Option<String>) -> Result<usize, ConfigError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_OUTBOX_CAPACITY);
    };

    let capacity: usize = raw
        .trim()
        .parse()
        .map_err(|_| ConfigError::InvalidOutboxCapacity { value: raw.clone() })?;
    if capacity == 0 {
        return Err(ConfigError::InvalidOutboxCapacity { value: raw });
    }
    Ok(capacity)
}

fn invalid_memory_setting(
    variable: &'static str,
    value: &str,
    expected: &'static str,
) -> ConfigError {
    ConfigError::InvalidMemorySetting {
        variable,
        value: value.to_string(),
        expected,
    }
}

/// Failure while resolving server configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Exactly one of the credential environment variables was set.
    IncompleteCredentials,
    /// An environment variable was not valid UTF-8.
    NonUnicodeEnv(&'static str),
    /// A credential environment variable was set to an empty value.
    EmptyCredential(&'static str),
    /// A memory setting environment variable had an invalid value.
    InvalidMemorySetting {
        /// Environment variable that was invalid.
        variable: &'static str,
        /// Raw configured value.
        value: String,
        /// Description of the accepted values.
        expected: &'static str,
    },
    /// The outbox capacity environment variable had an invalid value.
    InvalidOutboxCapacity {
        /// Raw configured value.
        value: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteCredentials => write!(
                f,
                "both {DEFAULT_USER_ENV} and {DEFAULT_PASS_ENV} must be set together, or neither"
            ),
            Self::NonUnicodeEnv(name) => {
                write!(f, "environment variable {name} is not valid UTF-8")
            }
            Self::EmptyCredential(name) => {
                write!(f, "environment variable {name} must not be empty")
            }
            Self::InvalidMemorySetting {
                variable,
                value,
                expected,
            } => write!(
                f,
                "environment variable {variable} has invalid value {value:?}: expected {expected}"
            ),
            Self::InvalidOutboxCapacity { value } => write!(
                f,
                "environment variable {OUTBOX_CAPACITY_ENV} has invalid value {value:?}: expected a positive number"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

fn environment_value(name: &'static str) -> Result<Option<String>, ConfigError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::NonUnicodeEnv(name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amqp::auth::Account;

    fn source<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl FnMut(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_string())
        }
    }

    #[test]
    fn defaults_to_guest_when_the_environment_is_absent() {
        let config = ServerConfig::from_lookup(source(&[])).expect("defaults are valid");
        assert_eq!(config.account, Account::guest());
        assert_eq!(config.account.username(), "guest");
    }

    #[test]
    fn environment_credentials_override_the_default() {
        let config = ServerConfig::from_lookup(source(&[
            (DEFAULT_USER_ENV, "alice"),
            (DEFAULT_PASS_ENV, "s3cret"),
        ]))
        .expect("both variables are set");
        assert_eq!(config.account.username(), "alice");
        assert_ne!(config.account, Account::guest());
    }

    #[test]
    fn only_one_environment_variable_refuses_to_start() {
        assert_eq!(
            ServerConfig::from_lookup(source(&[(DEFAULT_USER_ENV, "alice")])),
            Err(ConfigError::IncompleteCredentials)
        );
        assert_eq!(
            ServerConfig::from_lookup(source(&[(DEFAULT_PASS_ENV, "s3cret")])),
            Err(ConfigError::IncompleteCredentials)
        );
    }

    #[test]
    fn empty_credentials_are_rejected() {
        assert_eq!(
            ServerConfig::from_lookup(source(&[(DEFAULT_USER_ENV, ""), (DEFAULT_PASS_ENV, "x")])),
            Err(ConfigError::EmptyCredential(DEFAULT_USER_ENV))
        );
        assert_eq!(
            ServerConfig::from_lookup(source(&[(DEFAULT_USER_ENV, "x"), (DEFAULT_PASS_ENV, "")])),
            Err(ConfigError::EmptyCredential(DEFAULT_PASS_ENV))
        );
    }

    #[test]
    fn incomplete_credentials_error_names_both_variables() {
        let message = ConfigError::IncompleteCredentials.to_string();
        assert!(message.contains(DEFAULT_USER_ENV));
        assert!(message.contains(DEFAULT_PASS_ENV));
    }

    #[test]
    fn non_unicode_environment_error_names_the_variable() {
        let message = ConfigError::NonUnicodeEnv(DEFAULT_USER_ENV).to_string();
        assert!(message.contains(DEFAULT_USER_ENV));
    }

    #[test]
    fn default_configuration_keeps_the_guest_account() {
        assert_eq!(ServerConfig::default().account, Account::guest());
    }

    #[test]
    fn memory_relative_default() {
        let config = ServerConfig::from_lookup(source(&[])).expect("defaults are valid");
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Relative(DEFAULT_MEMORY_HIGH_WATERMARK_RELATIVE)
        );
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Relative(0.4)
        );
        assert_eq!(
            config.memory_check_interval,
            Duration::from_millis(DEFAULT_MEMORY_CHECK_INTERVAL_MS)
        );
        assert_eq!(config.memory_check_interval, Duration::from_millis(1000));

        let config =
            ServerConfig::from_lookup(source(&[(MEMORY_HIGH_WATERMARK_RELATIVE_ENV, "0.4")]))
                .expect("relative watermark is valid");
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Relative(0.4)
        );

        let config =
            ServerConfig::from_lookup(source(&[(MEMORY_HIGH_WATERMARK_RELATIVE_ENV, "0.75")]))
                .expect("relative watermark is valid");
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Relative(0.75)
        );
    }

    #[test]
    fn memory_absolute_wins() {
        let config = ServerConfig::from_lookup(source(&[
            (MEMORY_HIGH_WATERMARK_RELATIVE_ENV, "0.4"),
            (MEMORY_HIGH_WATERMARK_BYTES_ENV, "1048576"),
        ]))
        .expect("both values are valid");
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Absolute(1_048_576)
        );

        let config =
            ServerConfig::from_lookup(source(&[(MEMORY_HIGH_WATERMARK_BYTES_ENV, "2048")]))
                .expect("absolute watermark is valid");
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Absolute(2048)
        );
    }

    fn assert_invalid_memory_setting(pairs: &[(&str, &str)], variable: &'static str) {
        match ServerConfig::from_lookup(source(pairs)) {
            Err(ConfigError::InvalidMemorySetting {
                variable: actual, ..
            }) => assert_eq!(actual, variable),
            other => panic!("expected an invalid memory setting, got {other:?}"),
        }
    }

    #[test]
    fn invalid_memory_settings_are_rejected() {
        for raw in [
            "1.5",
            "-0.1",
            "abc",
            "1.1",
            "NaN",
            "inf",
            "not-a-number",
            "",
        ] {
            assert_invalid_memory_setting(
                &[(MEMORY_HIGH_WATERMARK_RELATIVE_ENV, raw)],
                MEMORY_HIGH_WATERMARK_RELATIVE_ENV,
            );
        }

        for raw in ["0", "abc", "-5", "not-a-number", ""] {
            assert_invalid_memory_setting(
                &[(MEMORY_HIGH_WATERMARK_BYTES_ENV, raw)],
                MEMORY_HIGH_WATERMARK_BYTES_ENV,
            );
        }

        for raw in ["0", "abc", "not-a-number", ""] {
            assert_invalid_memory_setting(
                &[(MEMORY_CHECK_INTERVAL_MS_ENV, raw)],
                MEMORY_CHECK_INTERVAL_MS_ENV,
            );
        }

        assert_invalid_memory_setting(
            &[
                (MEMORY_HIGH_WATERMARK_RELATIVE_ENV, "2.0"),
                (MEMORY_HIGH_WATERMARK_BYTES_ENV, "1024"),
            ],
            MEMORY_HIGH_WATERMARK_RELATIVE_ENV,
        );
        assert_invalid_memory_setting(
            &[
                (MEMORY_HIGH_WATERMARK_RELATIVE_ENV, "0.5"),
                (MEMORY_HIGH_WATERMARK_BYTES_ENV, "0"),
            ],
            MEMORY_HIGH_WATERMARK_BYTES_ENV,
        );
    }

    #[test]
    fn invalid_memory_setting_display_names_variable_value_and_expectation() {
        let error = ConfigError::InvalidMemorySetting {
            variable: MEMORY_CHECK_INTERVAL_MS_ENV,
            value: "0".to_string(),
            expected: "a positive number of milliseconds",
        };
        let message = error.to_string();
        assert!(message.contains(MEMORY_CHECK_INTERVAL_MS_ENV));
        assert!(message.contains('0'));
        assert!(message.contains("a positive number of milliseconds"));
    }

    #[test]
    fn memory_check_interval_units() {
        let config = ServerConfig::from_lookup(source(&[(MEMORY_CHECK_INTERVAL_MS_ENV, "1000")]))
            .expect("interval is valid");
        assert_eq!(config.memory_check_interval, Duration::from_millis(1000));

        let config = ServerConfig::from_lookup(source(&[(MEMORY_CHECK_INTERVAL_MS_ENV, "2500")]))
            .expect("interval is valid");
        assert_eq!(config.memory_check_interval, Duration::from_millis(2500));
        assert_eq!(config.memory_check_interval.as_millis(), 2500);

        let config = ServerConfig::from_lookup(source(&[(MEMORY_CHECK_INTERVAL_MS_ENV, "1")]))
            .expect("interval is valid");
        assert_eq!(config.memory_check_interval, Duration::from_millis(1));
    }

    #[test]
    fn credentials_still_work_with_memory_settings() {
        let config = ServerConfig::from_lookup(source(&[
            (DEFAULT_USER_ENV, "alice"),
            (DEFAULT_PASS_ENV, "s3cret"),
            (MEMORY_HIGH_WATERMARK_BYTES_ENV, "4096"),
            (MEMORY_CHECK_INTERVAL_MS_ENV, "500"),
        ]))
        .expect("all values are valid");
        assert_eq!(config.account.username(), "alice");
        assert_eq!(
            config.memory_high_watermark,
            MemoryHighWatermark::Absolute(4096)
        );
        assert_eq!(config.memory_check_interval, Duration::from_millis(500));

        assert_eq!(
            ServerConfig::from_lookup(source(&[(DEFAULT_USER_ENV, "alice")])),
            Err(ConfigError::IncompleteCredentials)
        );
    }

    #[test]
    fn outbox_capacity_defaults_and_parses_override() {
        let config = ServerConfig::from_lookup(source(&[])).expect("defaults are valid");
        assert_eq!(config.outbox_capacity, DEFAULT_OUTBOX_CAPACITY);
        assert_eq!(
            ServerConfig::default().outbox_capacity,
            DEFAULT_OUTBOX_CAPACITY
        );

        let config = ServerConfig::from_lookup(source(&[(OUTBOX_CAPACITY_ENV, "2")]))
            .expect("capacity is valid");
        assert_eq!(config.outbox_capacity, 2);

        let config = ServerConfig::from_lookup(source(&[(OUTBOX_CAPACITY_ENV, "  64 ")]))
            .expect("capacity is trimmed");
        assert_eq!(config.outbox_capacity, 64);
    }

    #[test]
    fn invalid_outbox_capacities_are_rejected() {
        for raw in ["0", "abc", "-1", "not-a-number", ""] {
            match ServerConfig::from_lookup(source(&[(OUTBOX_CAPACITY_ENV, raw)])) {
                Err(ConfigError::InvalidOutboxCapacity { value }) => {
                    assert_eq!(value, raw);
                }
                other => panic!("expected an invalid outbox capacity, got {other:?}"),
            }
        }
    }

    #[test]
    fn invalid_outbox_capacity_display_names_variable_and_expectation() {
        let error = ConfigError::InvalidOutboxCapacity {
            value: "0".to_string(),
        };
        let message = error.to_string();
        assert!(message.contains(OUTBOX_CAPACITY_ENV));
        assert!(message.contains('0'));
        assert!(message.contains("a positive number"));
    }
}
