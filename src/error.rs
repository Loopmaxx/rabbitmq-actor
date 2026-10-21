use std::{fmt, sync::Arc, time::Duration};

use lapin::protocol::{AMQPErrorKind, AMQPSoftError};

/// Result type of this crate.
pub type Result<T> = std::result::Result<T, AmqpError>;

/// All errors returned by this crate.
///
/// The enum is `#[non_exhaustive]`: new variants can be added in minor releases,
/// so a `match` needs a wildcard arm. All variants are `Clone`, so the error can be
/// shared, e.g. in [`crate::ConnectionState::Failed`].
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AmqpError {
    /// The configuration is invalid. Returned before any connection attempt.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A single connection attempt failed.
    #[error("could not connect to {url}: {source}")]
    Connect {
        /// URL with the password removed.
        url: String,
        /// Error reported by the transport.
        #[source]
        source: TransportError,
    },

    /// `reconnect.max_attempts` was reached. `source` is the error of the last attempt.
    #[error("giving up after {attempts} connection attempts")]
    ReconnectExhausted {
        /// Number of failed attempts.
        attempts: u32,
        /// Error of the last attempt.
        #[source]
        source: Box<AmqpError>,
    },

    /// The client is connecting or reconnecting; nothing was sent.
    #[error("not connected to the broker (connecting or reconnecting)")]
    NotConnected,

    /// The connection was lost while an operation was running.
    #[error("connection to the broker lost: {0}")]
    ConnectionLost(#[source] TransportError),

    /// Opening or using a channel failed.
    #[error("channel error: {0}")]
    Channel(#[source] TransportError),

    /// An exchange or queue already exists on the broker with different arguments
    /// (`PRECONDITION_FAILED`), e.g. a classic queue that is now configured as quorum queue.
    #[error("{object} '{name}' already exists with different arguments: {source}")]
    TopologyMismatch {
        /// Kind of the object that could not be declared.
        object: TopologyObject,
        /// Name of the exchange or queue.
        name: String,
        /// Error reported by the broker.
        #[source]
        source: TransportError,
    },

    /// The broker refused a declaration for another reason (e.g. missing permissions).
    #[error("could not declare {object} '{name}': {source}")]
    Declare {
        /// Kind of the object that could not be declared.
        object: TopologyObject,
        /// Name of the exchange, queue or binding.
        name: String,
        /// Error reported by the broker.
        #[source]
        source: TransportError,
    },

    /// A handler was registered for a queue that is not in the configuration.
    #[error("queue '{0}' is not part of the configuration")]
    UnknownQueue(String),

    /// The message cannot be sent as is, e.g. a routing key or property longer than
    /// the 255 bytes AMQP allows. Nothing was sent.
    #[error("invalid message: {0}")]
    InvalidMessage(String),

    /// The target exchange does not exist on the broker. Nothing was sent.
    ///
    /// Exchanges that are not part of the configuration are checked (passively
    /// declared) before the first publish to them, so that a missing exchange fails
    /// only this publish instead of closing the shared publisher channel.
    #[error("exchange '{0}' does not exist")]
    UnknownExchange(String),

    /// Sending the message failed before it reached the broker. Nothing was sent,
    /// a retry is safe.
    #[error("could not publish message: {0}")]
    Publish(#[source] TransportError),

    /// The message was sent, but the channel or connection closed before the broker
    /// confirmed it. **The outcome is unknown**: the message may or may not have been
    /// delivered. See [`AmqpError::is_delivery_unknown`].
    #[error(
        "message for exchange '{exchange}' with routing key '{routing_key}' was sent but not confirmed, it may or may not have been delivered: {source}"
    )]
    PublishUnconfirmed {
        /// Target exchange.
        exchange: String,
        /// Routing key of the message.
        routing_key: String,
        /// Why the confirm did not arrive.
        #[source]
        source: TransportError,
    },

    /// The broker answered with a negative confirm.
    #[error(
        "broker rejected (NACK) message for exchange '{exchange}' with routing key '{routing_key}'"
    )]
    PublishNacked {
        /// Target exchange.
        exchange: String,
        /// Routing key of the message.
        routing_key: String,
    },

    /// `publisher.mandatory` is set and no queue matched the routing key.
    #[error(
        "message for exchange '{exchange}' with routing key '{routing_key}' could not be routed to any queue"
    )]
    Unroutable {
        /// Target exchange.
        exchange: String,
        /// Routing key of the message.
        routing_key: String,
    },

    /// No publisher confirm arrived within `publisher.confirm_timeout_ms`.
    /// **The outcome is unknown**, see [`AmqpError::is_delivery_unknown`].
    #[error("no publisher confirm within {0:?}, the message may or may not have been delivered")]
    PublishTimeout(Duration),

    /// The payload could not be serialized to JSON.
    #[error("could not serialize message: {0}")]
    Serialization(#[source] Arc<serde_json::Error>),

    /// The payload could not be deserialized from JSON.
    #[error("could not deserialize message from routing key '{routing_key}': {source}")]
    Deserialization {
        /// Routing key of the message.
        routing_key: String,
        /// Error reported by `serde_json`.
        #[source]
        source: Arc<serde_json::Error>,
    },

    /// A broker operation did not finish in time (`connection.connect_timeout_ms`
    /// or `connection.operation_timeout_ms`). The operation is retried.
    #[error("{operation} timed out after {after:?}")]
    Timeout {
        /// What timed out, e.g. `connect` or `declare queue`.
        operation: String,
        /// The configured limit.
        after: Duration,
    },

    /// An internal error of this crate, e.g. a panicked actor. Please report it.
    #[error("internal error: {0}")]
    Internal(String),

    /// The client was shut down.
    #[error("the AMQP service has been shut down")]
    Shutdown,
}

/// Kind of a topology object, used in [`AmqpError::TopologyMismatch`] and [`AmqpError::Declare`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopologyObject {
    /// An exchange.
    Exchange,
    /// A queue.
    Queue,
    /// A binding between an exchange and a queue.
    Binding,
}

impl fmt::Display for TopologyObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Exchange => "exchange",
            Self::Queue => "queue",
            Self::Binding => "binding",
        })
    }
}

/// Error of the underlying AMQP transport.
///
/// Opaque on purpose: the transport library is an implementation detail and not part
/// of this crate's public API. Use [`std::error::Error::source`] and `Display` for details.
#[derive(Clone)]
pub struct TransportError(Arc<lapin::Error>);

impl TransportError {
    /// `true` if retrying the operation (possibly after a reconnect) can succeed.
    pub fn is_recoverable(&self) -> bool {
        self.0.can_be_recovered()
    }

    /// `true` if the broker answered with `PRECONDITION_FAILED (406)`.
    fn is_precondition_failed(&self) -> bool {
        match self.0.kind() {
            lapin::ErrorKind::ProtocolError(e) => matches!(
                e.kind(),
                AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED)
            ),
            _ => false,
        }
    }
}

impl TransportError {
    /// Not a `From` impl, so that no lapin type shows up in the public API (F7).
    pub(crate) fn new(error: lapin::Error) -> Self {
        Self(Arc::new(error))
    }
}

impl fmt::Debug for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for TransportError {}

impl AmqpError {
    /// `true` if the error may disappear after a reconnect or a retry.
    /// Configuration and topology errors are never recoverable.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Connect { source, .. }
            | Self::ConnectionLost(source)
            | Self::Channel(source)
            | Self::Publish(source) => source.is_recoverable(),
            Self::NotConnected
            | Self::PublishUnconfirmed { .. }
            | Self::PublishTimeout(_)
            | Self::PublishNacked { .. }
            | Self::Timeout { .. } => true,
            Self::InvalidConfig(_)
            | Self::ReconnectExhausted { .. }
            | Self::TopologyMismatch { .. }
            | Self::Declare { .. }
            | Self::UnknownQueue(_)
            | Self::InvalidMessage(_)
            | Self::UnknownExchange(_)
            | Self::Internal(_)
            | Self::Unroutable { .. }
            | Self::Serialization(_)
            | Self::Deserialization { .. }
            | Self::Shutdown => false,
        }
    }

    /// `true` if a publish failed **after** the message was sent, so it may have
    /// been delivered anyway ([`AmqpError::PublishTimeout`],
    /// [`AmqpError::PublishUnconfirmed`]).
    ///
    /// For all other publish errors the message was definitely not delivered
    /// (or definitely rejected). Retrying after an unknown outcome can create a
    /// duplicate; it is only safe with a `message_id` and idempotent consumers.
    pub fn is_delivery_unknown(&self) -> bool {
        matches!(
            self,
            Self::PublishTimeout(_) | Self::PublishUnconfirmed { .. }
        )
    }

    /// Maps a failed declaration to [`AmqpError::TopologyMismatch`] when the broker
    /// answered with `PRECONDITION_FAILED (406)`, e.g. a classic queue redeclared as quorum.
    pub(crate) fn declare(
        object: TopologyObject,
        name: impl Into<String>,
        source: lapin::Error,
    ) -> Self {
        let source = TransportError::new(source);
        let name = name.into();
        if source.is_precondition_failed() {
            Self::TopologyMismatch {
                object,
                name,
                source,
            }
        } else {
            Self::Declare {
                object,
                name,
                source,
            }
        }
    }

    pub(crate) fn channel(source: lapin::Error) -> Self {
        Self::Channel(TransportError::new(source))
    }

    pub(crate) fn serialization(source: serde_json::Error) -> Self {
        Self::Serialization(Arc::new(source))
    }
}

/// `true` if the broker answered with `NOT_FOUND (404)`.
pub(crate) fn is_not_found(error: &lapin::Error) -> bool {
    match error.kind() {
        lapin::ErrorKind::ProtocolError(e) => {
            matches!(e.kind(), AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND))
        }
        _ => false,
    }
}

/// Runs `future` with a time limit; on expiry fails with [`AmqpError::Timeout`].
pub(crate) async fn timed<F: std::future::Future>(
    operation: &str,
    after: Duration,
    future: F,
) -> Result<F::Output> {
    tokio::time::timeout(after, future)
        .await
        .map_err(|_| AmqpError::Timeout {
            operation: operation.to_owned(),
            after,
        })
}

/// Removes the password from an AMQP URL so it can be logged.
pub(crate) fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_owned();
    };
    let rest = &url[scheme_end + 3..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => {
            let user_info = &rest[..at];
            let user = user_info.split(':').next().unwrap_or_default();
            format!("{}{}:***{}", &url[..scheme_end + 3], user, &rest[at..])
        }
        None => url.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password() {
        assert_eq!(
            redact_url("amqp://guest:secret@localhost:5672/%2f"),
            "amqp://guest:***@localhost:5672/%2f"
        );
        assert_eq!(redact_url("amqp://localhost:5672"), "amqp://localhost:5672");
    }

    #[test]
    fn display_does_not_recurse() {
        assert_eq!(
            AmqpError::NotConnected.to_string(),
            "not connected to the broker (connecting or reconnecting)"
        );
    }

    #[test]
    fn error_is_clone_send_sync_static() {
        fn assert_traits<T: Clone + Send + Sync + 'static + std::error::Error>() {}
        assert_traits::<AmqpError>();
        assert_traits::<TransportError>();
    }

    #[test]
    fn only_post_send_publish_errors_are_ambiguous() {
        assert!(AmqpError::PublishTimeout(Duration::from_secs(1)).is_delivery_unknown());
        assert!(!AmqpError::NotConnected.is_delivery_unknown());
        assert!(!AmqpError::UnknownExchange("x".into()).is_delivery_unknown());
        assert!(
            !AmqpError::PublishNacked {
                exchange: "x".into(),
                routing_key: "k".into()
            }
            .is_delivery_unknown()
        );
    }

    #[test]
    fn reconnect_exhausted_keeps_cause() {
        let error = AmqpError::ReconnectExhausted {
            attempts: 3,
            source: Box::new(AmqpError::NotConnected),
        };
        let source = std::error::Error::source(&error).map(ToString::to_string);
        assert_eq!(
            source.as_deref(),
            Some("not connected to the broker (connecting or reconnecting)")
        );
    }
}
