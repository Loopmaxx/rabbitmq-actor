use std::sync::Arc;

use lapin::Connection;
use tokio::sync::oneshot;

use crate::{MessageHandler, Result, actor::PublisherTarget};

/// Mailbox of the [`crate::actor::PublisherActor`]. Publishes do not go through it,
/// see [`crate::actor::publish`].
pub(crate) enum PublisherMsg {
    /// Returns the open publisher channel, reopening it if the broker closed it.
    EnsureChannel(oneshot::Sender<Result<PublisherTarget>>),
    /// Checks (passively declares) an exchange that is not configured.
    CheckExchange {
        name: String,
        reply: oneshot::Sender<Result<()>>,
    },
    ConnectionReady(Arc<Connection>),
    ConnectionLost,
    /// Shutdown: wait for publishes in progress, then reply. Sent after the consumers drained.
    Drain(oneshot::Sender<()>),
}

/// Mailbox of the [`crate::actor::ConsumerActor`].
pub(crate) enum ConsumerMsg {
    ConnectionReady(Arc<Connection>),
    ConnectionLost,
    RegisterHandler {
        queue: String,
        handler: Arc<dyn MessageHandler>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// A consumer stream ended while the connection was still up (e.g. channel closed by the broker).
    ConsumerStopped {
        queue: String,
        generation: u64,
    },
    RestartConsumer {
        queue: String,
        generation: u64,
    },
    /// Shutdown: cancel all consumers, wait for running handlers, then reply.
    Drain(oneshot::Sender<()>),
}
