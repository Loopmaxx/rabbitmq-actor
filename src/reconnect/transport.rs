use std::sync::Arc;

use lapin::Connection;
use tokio::sync::{mpsc::Sender, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    AmqpConfig, Result,
    actor::connection::{self, ConnectionEnd},
    messages::{ConsumerMsg, PublisherMsg},
    topology,
};

/// What the [`super::ReconnectSupervisor`] needs from the transport. Decouples the
/// supervisor from lapin, so its logic can be tested without a broker (F17).
#[async_trait::async_trait]
pub(crate) trait Transport: Send + Sync + 'static {
    type Connection: Clone + Send + Sync + 'static;

    /// Opens one connection to `url`.
    async fn connect(&self, url: &str) -> Result<Self::Connection>;
    /// Declares exchanges, queues and bindings.
    async fn declare(&self, connection: &Self::Connection) -> Result<()>;
    fn is_connected(&self, connection: &Self::Connection) -> bool;
    /// Returns when the connection is lost or `shutdown` is cancelled.
    async fn wait_for_loss(
        &self,
        connection: &Self::Connection,
        shutdown: &CancellationToken,
    ) -> ConnectionEnd;
    async fn close(&self, connection: &Self::Connection);
}

/// How the supervisor talks to the publisher and consumer actors.
#[async_trait::async_trait]
pub(crate) trait Actors<C>: Send + Sync + 'static {
    async fn connection_ready(&self, connection: C);
    async fn connection_lost(&self);
    /// Shutdown: consumers first (running handlers finish), then the publisher
    /// (outstanding confirms). Returns when both are done.
    async fn drain(&self);
}

/// The real transport: lapin.
pub(crate) struct LapinTransport {
    pub(crate) config: Arc<AmqpConfig>,
}

#[async_trait::async_trait]
impl Transport for LapinTransport {
    type Connection = Arc<Connection>;

    async fn connect(&self, url: &str) -> Result<Self::Connection> {
        connection::connect(url, &self.config.connection)
            .await
            .map(Arc::new)
    }

    async fn declare(&self, connection: &Self::Connection) -> Result<()> {
        topology::declare(connection, &self.config).await
    }

    fn is_connected(&self, connection: &Self::Connection) -> bool {
        connection.status().connected()
    }

    async fn wait_for_loss(
        &self,
        connection: &Self::Connection,
        shutdown: &CancellationToken,
    ) -> ConnectionEnd {
        let interval = self.config.connection.health_check_interval();
        connection::wait_for_loss(connection, interval, shutdown).await
    }

    async fn close(&self, connection: &Self::Connection) {
        connection::close(connection, self.config.connection.operation_timeout()).await;
    }
}

/// The real actors, reached through their mailboxes.
pub(crate) struct ActorMailboxes {
    pub(crate) publisher_tx: Sender<PublisherMsg>,
    pub(crate) consumer_tx: Sender<ConsumerMsg>,
}

#[async_trait::async_trait]
impl Actors<Arc<Connection>> for ActorMailboxes {
    async fn connection_ready(&self, connection: Arc<Connection>) {
        let _ = self
            .publisher_tx
            .send(PublisherMsg::ConnectionReady(connection.clone()))
            .await;
        let _ = self
            .consumer_tx
            .send(ConsumerMsg::ConnectionReady(connection))
            .await;
    }

    async fn connection_lost(&self) {
        let _ = self.publisher_tx.send(PublisherMsg::ConnectionLost).await;
        let _ = self.consumer_tx.send(ConsumerMsg::ConnectionLost).await;
    }

    async fn drain(&self) {
        let (reply, done) = oneshot::channel();
        if self
            .consumer_tx
            .send(ConsumerMsg::Drain(reply))
            .await
            .is_ok()
        {
            let _ = done.await;
        }
        let (reply, done) = oneshot::channel();
        if self
            .publisher_tx
            .send(PublisherMsg::Drain(reply))
            .await
            .is_ok()
        {
            let _ = done.await;
        }
    }
}
