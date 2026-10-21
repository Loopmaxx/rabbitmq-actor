use crate::AmqpError;

/// Connection state, observable via [`crate::AmqpClient::state`] and
/// [`crate::AmqpClient::state_changes`].
///
/// `#[non_exhaustive]`: new states may be added in minor releases, so a `match`
/// outside this crate needs a wildcard arm:
///
/// ```compile_fail
/// use amqp_actor_lib::ConnectionState;
/// fn describe(state: &ConnectionState) -> &'static str {
///     match state {
///         ConnectionState::Connecting { .. } => "connecting",
///         ConnectionState::Connected => "connected",
///         ConnectionState::Reconnecting { .. } => "reconnecting",
///         ConnectionState::Failed(_) => "failed",
///         ConnectionState::Closed => "closed",
///     }
/// }
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ConnectionState {
    /// First connection is being established.
    Connecting {
        /// Number of the current attempt, starting at 1 (0 before the first attempt).
        attempt: u32,
    },
    /// Connected, topology declared, publisher ready.
    Connected,
    /// Connection was lost and is being re-established. Publishing fails with
    /// [`AmqpError::NotConnected`] in this state.
    Reconnecting {
        /// Number of the current attempt, starting at 1 (0 right after the loss).
        attempt: u32,
    },
    /// Unrecoverable error (invalid topology, retries exhausted). The service has stopped.
    Failed(AmqpError),
    /// Shut down via [`crate::AmqpClient::shutdown`].
    Closed,
}

impl ConnectionState {
    /// `true` in [`ConnectionState::Connected`].
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// `true` if the state will never change again.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Failed(_) | Self::Closed)
    }
}
