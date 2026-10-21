use std::{sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::{
    AmqpConfig, AmqpError, ConnectionState, MessageHandler, MessagePublisher, OutgoingMessage,
    Result, TypedMessageHandler,
    actor::{PublisherShared, publish, service},
    messages::{ConsumerMsg, PublisherMsg},
    traits::TypedHandlerAdapter,
};

/// Handle to the AMQP service. Cheap to clone; all clones talk to the same actors.
///
/// Dropping the last clone starts the same graceful shutdown as
/// [`AmqpClient::shutdown`] in the background (as long as the tokio runtime is
/// still running). Call `shutdown().await` to wait until it has finished.
#[derive(Clone)]
pub struct AmqpClient {
    inner: Arc<Inner>,
}

struct Inner {
    publisher_tx: mpsc::Sender<PublisherMsg>,
    publisher: Arc<PublisherShared>,
    consumer_tx: mpsc::Sender<ConsumerMsg>,
    state_rx: watch::Receiver<ConnectionState>,
    shutdown: CancellationToken,
    monitor: Mutex<Option<JoinHandle<()>>>,
    /// Cancels `shutdown` when the last clone is dropped.
    _shutdown_on_drop: DropGuard,
}

impl AmqpClient {
    /// Validates the config, starts the service and waits until the first connection
    /// is established and the topology is declared.
    ///
    /// With `reconnect.max_attempts = null` this waits as long as the broker is
    /// unreachable; wrap it in `tokio::time::timeout` if needed, or use
    /// [`AmqpClient::start_in_background`].
    pub async fn start(config: AmqpConfig) -> Result<Self> {
        let client = Self::start_in_background(config)?;
        match client.wait_for_connection(None).await {
            Ok(()) => Ok(client),
            Err(e) => {
                client.shutdown().await;
                Err(e)
            }
        }
    }

    /// Like [`AmqpClient::start`], but returns immediately. Publishing fails with
    /// [`AmqpError::NotConnected`] until the connection is up.
    pub fn start_in_background(config: AmqpConfig) -> Result<Self> {
        config.validate()?;
        let handles = service::spawn(config);
        Ok(Self {
            inner: Arc::new(Inner {
                publisher_tx: handles.publisher_tx,
                publisher: handles.publisher,
                consumer_tx: handles.consumer_tx,
                state_rx: handles.state_rx,
                _shutdown_on_drop: handles.shutdown.clone().drop_guard(),
                shutdown: handles.shutdown,
                monitor: Mutex::new(Some(handles.monitor)),
            }),
        })
    }

    /// The current connection state.
    pub fn state(&self) -> ConnectionState {
        self.inner.state_rx.borrow().clone()
    }

    /// Receiver that is notified on every state change.
    pub fn state_changes(&self) -> watch::Receiver<ConnectionState> {
        self.inner.state_rx.clone()
    }

    /// Waits until the state is [`ConnectionState::Connected`].
    /// Fails if the service stopped or `timeout` elapsed.
    pub async fn wait_for_connection(&self, timeout: Option<Duration>) -> Result<()> {
        let mut rx = self.inner.state_rx.clone();
        let wait = rx.wait_for(|s| s.is_connected() || s.is_terminal());
        let state = match timeout {
            Some(t) => tokio::time::timeout(t, wait)
                .await
                .map_err(|_| AmqpError::NotConnected)?,
            None => wait.await,
        }
        .map_err(|_| AmqpError::Shutdown)?
        .clone();

        match state {
            ConnectionState::Connected => Ok(()),
            ConnectionState::Failed(e) => Err(e),
            _ => Err(AmqpError::Shutdown),
        }
    }

    /// Serializes `data` as JSON and publishes it.
    pub async fn publish_json<T: Serialize + ?Sized>(
        &self,
        exchange: &str,
        routing_key: &str,
        data: &T,
    ) -> Result<()> {
        self.publish(OutgoingMessage::json(exchange, routing_key, data)?)
            .await
    }

    /// Registers a handler for a configured queue. The consumer for the queue starts
    /// with the first registered handler.
    pub async fn register_handler<H: MessageHandler>(&self, queue: &str, handler: H) -> Result<()> {
        self.register(queue, Arc::new(handler)).await
    }

    /// Registers a handler that receives the JSON payload deserialized into `T`.
    pub async fn register_typed_handler<T, H>(&self, queue: &str, handler: H) -> Result<()>
    where
        T: DeserializeOwned + Send + 'static,
        H: TypedMessageHandler<T>,
    {
        self.register(queue, TypedHandlerAdapter::<T, H>::new(handler))
            .await
    }

    async fn register(&self, queue: &str, handler: Arc<dyn MessageHandler>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .consumer_tx
            .send(ConsumerMsg::RegisterHandler {
                queue: queue.to_owned(),
                handler,
                reply,
            })
            .await
            .map_err(|_| self.stopped_error())?;
        rx.await.map_err(|_| self.stopped_error())?
    }

    /// The error for a request the stopped service can no longer answer: the reason
    /// it failed (e.g. [`AmqpError::Internal`] after a panic), or `Shutdown`.
    fn stopped_error(&self) -> AmqpError {
        match &*self.inner.state_rx.borrow() {
            ConnectionState::Failed(e) => e.clone(),
            _ => AmqpError::Shutdown,
        }
    }

    /// Shuts the service down gracefully and waits until it has finished:
    ///
    /// 1. all consumers are cancelled, so no new messages arrive,
    /// 2. running handlers finish and their messages are acknowledged; they can
    ///    still publish during this phase,
    /// 3. outstanding publisher confirms are awaited,
    /// 4. the connection is closed.
    ///
    /// Steps 1–3 are limited by `shutdown_timeout_ms`; handlers still running then
    /// are cancelled and their messages are redelivered later. Calling `shutdown`
    /// from inside a handler waits for that handler and therefore always runs into
    /// the timeout.
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        if let Some(monitor) = self.inner.monitor.lock().await.take() {
            let _ = monitor.await;
        }
    }
}

#[async_trait::async_trait]
impl MessagePublisher for AmqpClient {
    async fn publish(&self, msg: OutgoingMessage) -> Result<()> {
        {
            let state = self.inner.state_rx.borrow();
            if state.is_terminal() {
                drop(state);
                return Err(self.stopped_error());
            }
            if !state.is_connected() {
                return Err(AmqpError::NotConnected);
            }
        }
        publish(&self.inner.publisher, &self.inner.publisher_tx, msg)
            .await
            .map_err(|e| match e {
                AmqpError::Shutdown => self.stopped_error(),
                e => e,
            })
    }
}
