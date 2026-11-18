//! Server configuration.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::amqp::auth::Account;

/// Environment variable that overrides the default user name.
pub const DEFAULT_USER_ENV: &str = "WOLF_DEFAULT_USER";
/// Environment variable that overrides the default password.
pub const DEFAULT_PASS_ENV: &str = "WOLF_DEFAULT_PASS";

/// Configuration used to start a Wolf server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Address the AMQP listener binds to.
    pub amqp_addr: SocketAddr,
    /// Root directory for persistent broker state.
    pub data_dir: PathBuf,
    /// Maximum time to wait for sessions to finish during shutdown.
    pub shutdown_timeout: Duration,
    /// Account used to authenticate AMQP clients.
    pub account: Account,
}

impl ServerConfig {
    /// Builds a configuration from the environment.
    ///
    /// Both [`DEFAULT_USER_ENV`] and [`DEFAULT_PASS_ENV`] must be set together,
    /// or neither may be set. When neither is set the guest account is used.
    pub fn from_env() -> Result<Self, ConfigError> {
        let user = environment_value(DEFAULT_USER_ENV)?;
        let password = environment_value(DEFAULT_PASS_ENV)?;
        Self::from_credentials(user, password)
    }

    /// Builds a configuration from a lookup function.
    ///
    /// This is the race-free testing counterpart of [`Self::from_env`].
    pub fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let user = lookup(DEFAULT_USER_ENV);
        let password = lookup(DEFAULT_PASS_ENV);
        Self::from_credentials(user, password)
    }

    fn from_credentials(
        user: Option<String>,
        password: Option<String>,
    ) -> Result<Self, ConfigError> {
        let account = match (user, password) {
            (None, None) => Account::guest(),
            (Some(user), Some(password)) => {
                if user.is_empty() {
                    return Err(ConfigError::EmptyCredential(DEFAULT_USER_ENV));
                }
                if password.is_empty() {
                    return Err(ConfigError::EmptyCredential(DEFAULT_PASS_ENV));
                }
                Account::new(user, password)
            }
            _ => return Err(ConfigError::IncompleteCredentials),
        };
        Ok(Self {
            account,
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
        }
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
}
