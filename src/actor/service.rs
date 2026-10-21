use std::sync::Arc;

use tokio::{
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{
    AmqpConfig, ConnectionState,
    actor::{ConsumerActor, PublisherActor, PublisherShared},
    messages::{ConsumerMsg, PublisherMsg},
    reconnect::{ActorMailboxes, LapinTransport, ReconnectSupervisor},
};

pub(crate) struct ServiceHandles {
    pub publisher_tx: mpsc::Sender<PublisherMsg>,
    pub publisher: Arc<PublisherShared>,
    pub consumer_tx: mpsc::Sender<ConsumerMsg>,
    pub state_rx: watch::Receiver<ConnectionState>,
    pub shutdown: CancellationToken,
    pub monitor: JoinHandle<()>,
}

/// Spawns the supervisor and the actors and a monitor task that shuts everything
/// down if one of them panics.
///
/// Two tokens: `shutdown` asks for a graceful shutdown, which the supervisor carries
/// out (drain consumers, drain publisher, close the connection). Only then it cancels
/// `halt`, which ends the actors.
pub(crate) fn spawn(config: AmqpConfig) -> ServiceHandles {
    let config = Arc::new(config);
    let capacity = config.mailbox_capacity;
    let shutdown = CancellationToken::new();
    let halt = CancellationToken::new();
    let (state_tx, state_rx) = watch::channel(ConnectionState::Connecting { attempt: 0 });
    let (publisher_tx, publisher_rx) = mpsc::channel(capacity);
    let (consumer_tx, consumer_rx) = mpsc::channel(capacity);

    let mut tasks = JoinSet::new();
    let publisher = PublisherShared::new(&config);
    tasks.spawn(PublisherActor::new(publisher_rx, publisher.clone(), &config, halt.clone()).run());
    tasks.spawn(
        ConsumerActor::new(
            consumer_rx,
            consumer_tx.clone(),
            config.clone(),
            shutdown.clone(),
            halt.clone(),
        )
        .run(),
    );
    tasks.spawn(
        ReconnectSupervisor::new(
            config.clone(),
            LapinTransport { config },
            ActorMailboxes {
                publisher_tx: publisher_tx.clone(),
                consumer_tx: consumer_tx.clone(),
            },
            state_tx.clone(),
            shutdown.clone(),
            halt.clone(),
        )
        .run(),
    );

    let monitor = tokio::spawn(monitor(tasks, state_tx, shutdown.clone(), halt));

    ServiceHandles {
        publisher_tx,
        publisher,
        consumer_tx,
        state_rx,
        shutdown,
        monitor,
    }
}

async fn monitor(
    mut tasks: JoinSet<()>,
    state_tx: watch::Sender<ConnectionState>,
    shutdown: CancellationToken,
    halt: CancellationToken,
) {
    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result
            && e.is_panic()
        {
            tracing::error!("AMQP actor panicked, shutting down: {e}");
            let error = crate::AmqpError::Internal(format!("an internal actor panicked: {e}"));
            state_tx.send_replace(ConnectionState::Failed(error));
            shutdown.cancel();
            halt.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AmqpError;

    // F16: a panicking actor is reported as `Internal`, not as `Shutdown`.
    #[tokio::test]
    async fn panicking_actor_is_reported_as_internal_error() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async { panic!("boom") });
        let (state_tx, state_rx) = watch::channel(ConnectionState::Connected);
        let shutdown = CancellationToken::new();
        let halt = CancellationToken::new();

        monitor(tasks, state_tx, shutdown.clone(), halt.clone()).await;

        assert!(shutdown.is_cancelled() && halt.is_cancelled());
        match &*state_rx.borrow() {
            ConnectionState::Failed(AmqpError::Internal(msg)) => assert!(msg.contains("panicked")),
            other => panic!("expected Failed(Internal), got {other:?}"),
        }
    }
}
