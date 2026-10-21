use std::sync::Arc;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    AmqpConfig, AmqpError, ConnectionState, Result,
    actor::connection::ConnectionEnd,
    reconnect::{
        backoff::{Backoff, UrlRotation},
        transport::{Actors, Transport},
    },
};

/// Owns the connection: connects with backoff, declares the topology, hands the
/// connection to the publisher and consumer actors and starts over when it is lost.
///
/// Generic over the transport and the actors, so it can be tested without a broker.
pub(crate) struct ReconnectSupervisor<T: Transport, A> {
    config: Arc<AmqpConfig>,
    transport: T,
    actors: A,
    state: watch::Sender<ConnectionState>,
    shutdown: CancellationToken,
    halt: CancellationToken,
    backoff: Backoff,
    urls: UrlRotation,
    has_connected: bool,
}

enum Attempt<C> {
    Connected(C),
    Shutdown,
    Fatal(AmqpError),
}

impl<T: Transport, A: Actors<T::Connection>> ReconnectSupervisor<T, A> {
    pub(crate) fn new(
        config: Arc<AmqpConfig>,
        transport: T,
        actors: A,
        state: watch::Sender<ConnectionState>,
        shutdown: CancellationToken,
        halt: CancellationToken,
    ) -> Self {
        Self {
            backoff: Backoff::new(config.reconnect.clone()),
            urls: UrlRotation::new(config.connection.urls.clone()),
            config,
            transport,
            actors,
            state,
            shutdown,
            halt,
            has_connected: false,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let connection = match self.connect_with_retry().await {
                Attempt::Connected(c) => c,
                Attempt::Shutdown => return self.finish(None, ConnectionState::Closed).await,
                Attempt::Fatal(e) => {
                    tracing::error!("AMQP service stopped: {e}");
                    self.shutdown.cancel();
                    return self.finish(None, ConnectionState::Failed(e)).await;
                }
            };

            self.backoff.reset();
            self.urls.mark_success();
            self.has_connected = true;
            // Order matters: the topology is declared (in `connect_with_retry`), then
            // the actors get the connection, then the state turns `Connected`.
            self.actors.connection_ready(connection.clone()).await;
            self.state.send_replace(ConnectionState::Connected);
            tracing::info!("connected to AMQP broker");

            let end = self
                .transport
                .wait_for_loss(&connection, &self.shutdown)
                .await;

            match end {
                // The connection stays open until the actors have drained.
                ConnectionEnd::Shutdown => {
                    return self.finish(Some(connection), ConnectionState::Closed).await;
                }
                ConnectionEnd::Lost(error) => {
                    self.actors.connection_lost().await;
                    match error {
                        Some(e) => tracing::warn!("connection lost: {e}"),
                        None => tracing::warn!("connection lost"),
                    }
                    self.state
                        .send_replace(ConnectionState::Reconnecting { attempt: 0 });
                }
            }
        }
    }

    /// Tries the URLs in turn until one connects and the topology is declared.
    async fn connect_with_retry(&mut self) -> Attempt<T::Connection> {
        loop {
            let attempt = self.backoff.attempt() + 1;
            self.state.send_replace(if self.has_connected {
                ConnectionState::Reconnecting { attempt }
            } else {
                ConnectionState::Connecting { attempt }
            });

            let url = self.urls.next_url().to_owned();
            let error = tokio::select! {
                _ = self.shutdown.cancelled() => return Attempt::Shutdown,
                result = self.connect_and_declare(&url) => match result {
                    Ok(connection) => return Attempt::Connected(connection),
                    Err(e) if is_fatal(&e, self.has_connected) => return Attempt::Fatal(e),
                    Err(e) => e,
                }
            };

            let Some(delay) = self.backoff.next_delay() else {
                tracing::error!("last connection attempt failed: {error}");
                return Attempt::Fatal(AmqpError::ReconnectExhausted {
                    attempts: self.backoff.attempt(),
                    source: Box::new(error),
                });
            };
            tracing::warn!("connection attempt {attempt} failed, retrying in {delay:?}: {error}");

            tokio::select! {
                _ = self.shutdown.cancelled() => return Attempt::Shutdown,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    async fn connect_and_declare(&self, url: &str) -> Result<T::Connection> {
        let connection = self.transport.connect(url).await?;
        match self.transport.declare(&connection).await {
            Ok(()) => Ok(connection),
            // A declaration that failed because the connection dropped is worth a retry,
            // a declaration the broker refused is not.
            Err(AmqpError::Declare { source, .. } | AmqpError::Channel(source))
                if !self.transport.is_connected(&connection) =>
            {
                Err(AmqpError::ConnectionLost(source))
            }
            Err(e) => {
                self.transport.close(&connection).await;
                Err(e)
            }
        }
    }

    /// Graceful shutdown: consumers first (running handlers finish and ack), then the
    /// publisher (outstanding confirms), then the connection. Limited by
    /// `shutdown_timeout_ms`; handlers still running after that are cancelled.
    async fn finish(self, connection: Option<T::Connection>, state: ConnectionState) {
        let timeout = self.config.shutdown_timeout();
        if tokio::time::timeout(timeout, self.actors.drain())
            .await
            .is_err()
        {
            tracing::warn!(
                "shutdown_timeout_ms ({timeout:?}) reached, cancelling the remaining handlers"
            );
        }
        if let Some(connection) = connection {
            self.transport.close(&connection).await;
        }
        self.halt.cancel();
        tracing::info!(?state, "AMQP supervisor stopped");
        self.state.send_replace(state);
    }
}

/// Errors that a reconnect cannot fix (F8).
///
/// A mismatch between config and broker is a configuration error and always fatal.
/// Any other refused declaration is fatal on the first start, so that a wrong
/// configuration (e.g. missing permissions) is reported right away. Once the client
/// was connected, it is retried with backoff: the cause may be temporary, e.g. a
/// permission change or an exclusive queue still held by the old connection.
fn is_fatal(error: &AmqpError, has_connected: bool) -> bool {
    match error {
        AmqpError::InvalidConfig(_) | AmqpError::TopologyMismatch { .. } => true,
        AmqpError::Declare { .. } => !has_connected,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    //! The supervisor without a broker (F17): a scripted transport and recording actors.

    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use lapin::protocol::{AMQPError, AMQPErrorKind, AMQPSoftError};
    use tokio::sync::{Notify, watch};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{TopologyObject, TransportError};

    type Events = Arc<Mutex<Vec<String>>>;

    fn record(events: &Events, event: impl Into<String>) {
        events.lock().unwrap().push(event.into());
    }

    fn broker_error(kind: AMQPSoftError) -> lapin::Error {
        lapin::ErrorKind::ProtocolError(AMQPError::new(AMQPErrorKind::Soft(kind), "test".into()))
            .into()
    }

    fn connect_error(url: &str) -> AmqpError {
        let io = std::io::Error::other("connection refused");
        AmqpError::Connect {
            url: url.to_owned(),
            source: TransportError::new(lapin::ErrorKind::IOError(Arc::new(io)).into()),
        }
    }

    /// Transport whose `connect` and `declare` results are scripted; once a script is
    /// empty, the call succeeds. A connection is lost when `lose` is notified.
    struct FakeTransport {
        events: Events,
        connect: Mutex<VecDeque<AmqpError>>,
        /// `None` succeeds.
        declare: Mutex<VecDeque<Option<AmqpError>>>,
        lose: Arc<Notify>,
        next_id: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl Transport for FakeTransport {
        type Connection = u32;

        async fn connect(&self, url: &str) -> Result<u32> {
            record(&self.events, format!("connect {url}"));
            if let Some(e) = self.connect.lock().unwrap().pop_front() {
                return Err(e);
            }
            let mut id = self.next_id.lock().unwrap();
            *id += 1;
            Ok(*id)
        }

        async fn declare(&self, connection: &u32) -> Result<()> {
            record(&self.events, format!("declare {connection}"));
            match self.declare.lock().unwrap().pop_front().flatten() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }

        fn is_connected(&self, _connection: &u32) -> bool {
            true
        }

        async fn wait_for_loss(
            &self,
            _connection: &u32,
            shutdown: &CancellationToken,
        ) -> ConnectionEnd {
            tokio::select! {
                _ = shutdown.cancelled() => ConnectionEnd::Shutdown,
                _ = self.lose.notified() => ConnectionEnd::Lost(Some("test".into())),
            }
        }

        async fn close(&self, connection: &u32) {
            record(&self.events, format!("close {connection}"));
        }
    }

    /// Records every call together with the connection state at that moment.
    struct FakeActors {
        events: Events,
        state: watch::Receiver<ConnectionState>,
    }

    #[async_trait::async_trait]
    impl Actors<u32> for FakeActors {
        async fn connection_ready(&self, connection: u32) {
            let connected = self.state.borrow().is_connected();
            record(
                &self.events,
                format!("ready {connection} connected={connected}"),
            );
        }

        async fn connection_lost(&self) {
            record(&self.events, "lost");
        }

        async fn drain(&self) {
            record(&self.events, "drain");
        }
    }

    struct Harness {
        events: Events,
        state: watch::Receiver<ConnectionState>,
        shutdown: CancellationToken,
        halt: CancellationToken,
        lose: Arc<Notify>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn start(
            max_attempts: Option<u32>,
            connect: Vec<AmqpError>,
            declare: Vec<Option<AmqpError>>,
        ) -> Self {
            let mut config = AmqpConfig::new(["amqp://a", "amqp://b"]);
            config.reconnect.initial_delay_ms = 1;
            config.reconnect.max_delay_ms = 2;
            config.reconnect.jitter = 0.0;
            config.reconnect.max_attempts = max_attempts;

            let events = Events::default();
            let lose = Arc::new(Notify::new());
            let (state_tx, state) = watch::channel(ConnectionState::Connecting { attempt: 0 });
            let transport = FakeTransport {
                events: events.clone(),
                connect: Mutex::new(connect.into()),
                declare: Mutex::new(declare.into()),
                lose: lose.clone(),
                next_id: Mutex::new(0),
            };
            let actors = FakeActors {
                events: events.clone(),
                state: state.clone(),
            };
            let shutdown = CancellationToken::new();
            let halt = CancellationToken::new();
            let supervisor = ReconnectSupervisor::new(
                Arc::new(config),
                transport,
                actors,
                state_tx,
                shutdown.clone(),
                halt.clone(),
            );
            Self {
                events,
                state,
                shutdown,
                halt,
                lose,
                task: tokio::spawn(supervisor.run()),
            }
        }

        async fn wait_for(&mut self, f: impl Fn(&ConnectionState) -> bool) -> ConnectionState {
            tokio::time::timeout(Duration::from_secs(5), self.state.wait_for(f))
                .await
                .expect("state not reached")
                .expect("supervisor gone")
                .clone()
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    #[tokio::test]
    async fn declares_topology_before_actors_and_state() {
        let mut h = Harness::start(None, vec![], vec![]);
        h.wait_for(ConnectionState::is_connected).await;
        assert_eq!(
            h.events(),
            ["connect amqp://a", "declare 1", "ready 1 connected=false"]
        );
        h.shutdown.cancel();
        h.task.await.unwrap();
    }

    #[tokio::test]
    async fn retries_with_next_url_and_reconnects_after_loss() {
        let mut h = Harness::start(None, vec![connect_error("amqp://a")], vec![]);
        h.wait_for(ConnectionState::is_connected).await;
        h.lose.notify_one();
        // Reconnecting and Connected again follow too quickly to observe both.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !h.events().iter().any(|e| e.starts_with("ready 2")) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("no reconnect");
        assert_eq!(
            h.events(),
            [
                "connect amqp://a",
                "connect amqp://b",
                "declare 1",
                "ready 1 connected=false",
                "lost",
                // The URL that worked last is tried first.
                "connect amqp://b",
                "declare 2",
                "ready 2 connected=false",
            ]
        );
        h.shutdown.cancel();
        h.task.await.unwrap();
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_and_keeps_the_cause() {
        let errors = (0..3).map(|_| connect_error("amqp://x")).collect();
        let mut h = Harness::start(Some(3), errors, vec![]);
        let state = h.wait_for(ConnectionState::is_terminal).await;
        match state {
            ConnectionState::Failed(AmqpError::ReconnectExhausted { attempts, source }) => {
                assert_eq!(attempts, 3);
                assert!(matches!(*source, AmqpError::Connect { .. }));
            }
            other => panic!("unexpected state {other:?}"),
        }
        assert!(h.halt.is_cancelled());
        h.task.await.unwrap();
    }

    #[tokio::test]
    async fn topology_mismatch_is_fatal() {
        let mismatch = AmqpError::declare(
            TopologyObject::Queue,
            "q",
            broker_error(AMQPSoftError::PRECONDITIONFAILED),
        );
        let mut h = Harness::start(None, vec![], vec![Some(mismatch)]);
        let state = h.wait_for(ConnectionState::is_terminal).await;
        assert!(
            matches!(
                state,
                ConnectionState::Failed(AmqpError::TopologyMismatch { .. })
            ),
            "{state:?}"
        );
        assert!(h.events().contains(&"close 1".to_owned()));
        h.task.await.unwrap();
    }

    #[tokio::test]
    async fn refused_declare_is_fatal_on_first_start() {
        let refused = AmqpError::declare(
            TopologyObject::Queue,
            "q",
            broker_error(AMQPSoftError::ACCESSREFUSED),
        );
        let mut h = Harness::start(None, vec![], vec![Some(refused)]);
        let state = h.wait_for(ConnectionState::is_terminal).await;
        assert!(
            matches!(state, ConnectionState::Failed(AmqpError::Declare { .. })),
            "{state:?}"
        );
        h.task.await.unwrap();
    }

    // F8: after the first connection, a refused declaration is retried.
    #[tokio::test]
    async fn refused_declare_after_reconnect_is_retried() {
        let refused = || {
            Some(AmqpError::declare(
                TopologyObject::Queue,
                "q",
                broker_error(AMQPSoftError::ACCESSREFUSED),
            ))
        };
        let mut h = Harness::start(None, vec![], vec![None, refused(), refused()]);
        h.wait_for(ConnectionState::is_connected).await;
        h.lose.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !h.events().iter().any(|e| e.starts_with("ready 4")) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("declare was not retried");
        assert!(h.state.borrow().is_connected());
        h.shutdown.cancel();
        h.task.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_before_closing() {
        let mut h = Harness::start(None, vec![], vec![]);
        h.wait_for(ConnectionState::is_connected).await;
        h.shutdown.cancel();
        let state = h.wait_for(ConnectionState::is_terminal).await;
        assert!(matches!(state, ConnectionState::Closed));
        let events = h.events();
        assert_eq!(&events[events.len() - 2..], ["drain", "close 1"]);
        assert!(h.halt.is_cancelled());
        h.task.await.unwrap();
    }
}
