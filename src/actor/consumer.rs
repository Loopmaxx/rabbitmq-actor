use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use lapin::{
    Channel, Connection, Consumer,
    message::Delivery,
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions,
        BasicQosOptions, BasicRejectOptions,
    },
    types::FieldTable,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    AmqpConfig, AmqpError, HandlerResult, IncomingMessage, MessageMeta, MessageProperties, Result,
    actor::subscriber::HandlerRegistry, error::timed, messages::ConsumerMsg, reconnect::Backoff,
    traits::integer_header,
};

/// Starts one consumer per queue that has at least one handler, and restarts
/// them after a reconnect or when the broker closed their channel.
///
/// Shutdown is driven by the supervisor through [`ConsumerMsg::Drain`]; `halt`
/// only ends the actor (and cancels handlers that outlived `shutdown_timeout_ms`).
pub(crate) struct ConsumerActor {
    rx: Receiver<ConsumerMsg>,
    self_tx: Sender<ConsumerMsg>,
    config: Arc<AmqpConfig>,
    shutdown: CancellationToken,
    halt: CancellationToken,
    registry: Arc<HandlerRegistry>,
    connection: Option<Arc<Connection>>,
    /// Incremented on every new connection, so stale stop/restart messages are ignored.
    generation: u64,
    active: HashMap<String, ActiveConsumer>,
    /// Restart backoff per queue (F12), reset on every new connection.
    restart_backoff: HashMap<String, Backoff>,
    /// Tracks the `process` tasks, so a shutdown can wait for running handlers.
    handlers: TaskTracker,
    draining: bool,
}

struct ActiveConsumer {
    task: JoinHandle<()>,
    channel: Channel,
    tag: String,
    started: Instant,
}

impl ConsumerActor {
    pub(crate) fn new(
        rx: Receiver<ConsumerMsg>,
        self_tx: Sender<ConsumerMsg>,
        config: Arc<AmqpConfig>,
        shutdown: CancellationToken,
        halt: CancellationToken,
    ) -> Self {
        Self {
            rx,
            self_tx,
            config,
            shutdown,
            halt,
            registry: Arc::default(),
            connection: None,
            generation: 0,
            active: HashMap::new(),
            restart_backoff: HashMap::new(),
            handlers: TaskTracker::new(),
            draining: false,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            let msg = tokio::select! {
                _ = self.halt.cancelled() => break,
                msg = self.rx.recv() => match msg {
                    Some(msg) => msg,
                    None => break,
                },
            };
            self.handle(msg).await;
        }
        self.stop_all();
    }

    async fn handle(&mut self, msg: ConsumerMsg) {
        match msg {
            ConsumerMsg::ConnectionReady(connection) => {
                self.stop_all();
                self.restart_backoff.clear();
                self.generation += 1;
                self.connection = Some(connection);
                for queue in self.registry.queues() {
                    self.start(&queue).await;
                }
            }
            ConsumerMsg::ConnectionLost => {
                self.stop_all();
                self.connection = None;
            }
            ConsumerMsg::RegisterHandler {
                queue,
                handler,
                reply,
            } => {
                if self.draining {
                    let _ = reply.send(Err(AmqpError::Shutdown));
                    return;
                }
                if self.config.queue(&queue).is_none() {
                    let _ = reply.send(Err(AmqpError::UnknownQueue(queue)));
                    return;
                }
                self.registry.register(&queue, handler);
                let _ = reply.send(Ok(()));
                if !self.active.contains_key(&queue) {
                    self.start(&queue).await;
                }
            }
            ConsumerMsg::ConsumerStopped { queue, generation } => {
                if generation == self.generation {
                    // A consumer that ran for a while starts again with a short delay.
                    if let Some(stopped) = self.active.remove(&queue)
                        && stopped.started.elapsed() >= self.config.reconnect.max_delay()
                        && let Some(backoff) = self.restart_backoff.get_mut(&queue)
                    {
                        backoff.reset();
                    }
                    tracing::warn!(%queue, "consumer stopped, restarting");
                    self.schedule_restart(queue);
                }
            }
            ConsumerMsg::RestartConsumer { queue, generation } => {
                if generation == self.generation && !self.active.contains_key(&queue) {
                    self.start(&queue).await;
                }
            }
            ConsumerMsg::Drain(reply) => {
                self.drain().await;
                let _ = reply.send(());
            }
        }
    }

    /// Stops all consumers and waits until every running handler has finished
    /// and acknowledged its message. Deliveries that were prefetched but not yet
    /// handed to a handler are redelivered by the broker when the channel closes.
    async fn drain(&mut self) {
        self.draining = true;
        let timeout = self.config.connection.operation_timeout();
        for (queue, consumer) in self.active.drain() {
            consumer.task.abort();
            let cancel = consumer
                .channel
                .basic_cancel(consumer.tag.as_str().into(), BasicCancelOptions::default());
            match timed("cancel consumer", timeout, cancel).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::debug!(%queue, "could not cancel consumer: {e}"),
                Err(e) => tracing::debug!(%queue, "could not cancel consumer: {e}"),
            }
        }
        self.handlers.close();
        self.handlers.wait().await;
    }

    async fn start(&mut self, queue: &str) {
        let Some(connection) = self.connection.clone() else {
            return; // started on the next ConnectionReady
        };
        if self.draining || !connection.status().connected() || !self.registry.has_handlers(queue) {
            return;
        }
        match self.create_consumer(&connection, queue).await {
            Ok((channel, tag, consumer)) => {
                let consumer_config = self.config.queue(queue).map(|q| &q.consumer);
                let dispatch = Dispatch {
                    channel: channel.clone(),
                    queue: queue.to_owned(),
                    max_requeues: consumer_config.and_then(|c| c.max_requeues),
                    handler_timeout: consumer_config
                        .and_then(|c| c.handler_timeout_ms)
                        .map(Duration::from_millis),
                    registry: self.registry.clone(),
                    handlers: self.handlers.clone(),
                    halt: self.halt.clone(),
                };
                let task = tokio::spawn(consume(
                    consumer,
                    dispatch,
                    self.generation,
                    self.self_tx.clone(),
                ));
                let active = ActiveConsumer {
                    task,
                    channel,
                    tag,
                    started: Instant::now(),
                };
                self.active.insert(queue.to_owned(), active);
                tracing::info!(%queue, "consumer started");
            }
            Err(e) => {
                tracing::error!(%queue, "could not start consumer: {e}");
                self.schedule_restart(queue.to_owned());
            }
        }
    }

    async fn create_consumer(
        &self,
        connection: &Connection,
        queue: &str,
    ) -> Result<(Channel, String, Consumer)> {
        let queue_config = self
            .config
            .queue(queue)
            .ok_or_else(|| AmqpError::UnknownQueue(queue.to_owned()))?;
        let timeout = self.config.connection.operation_timeout();
        let channel = timed("open channel", timeout, connection.create_channel())
            .await?
            .map_err(AmqpError::channel)?;
        // Quorum queues do not support global QoS, so it is always per consumer.
        let qos = channel.basic_qos(
            queue_config.consumer.prefetch_count,
            BasicQosOptions { global: false },
        );
        timed("set prefetch", timeout, qos)
            .await?
            .map_err(AmqpError::channel)?;
        // Queue name and consumer tag were checked against the 255 byte limit by
        // `AmqpConfig::validate`, so the conversions below cannot panic.
        let tag = queue_config.consumer_tag();
        let consume = channel.basic_consume(
            queue.into(),
            tag.as_str().into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        );
        let consumer = timed("start consumer", timeout, consume)
            .await?
            .map_err(AmqpError::channel)?;
        Ok((channel, tag, consumer))
    }

    /// Restarts the consumer after a delay that grows with every failed start (F12).
    fn schedule_restart(&mut self, queue: String) {
        let tx = self.self_tx.clone();
        let generation = self.generation;
        let reconnect = &self.config.reconnect;
        let delay = self
            .restart_backoff
            .entry(queue.clone())
            .or_insert_with(|| {
                // Consumers are retried for as long as the connection is up.
                let mut config = reconnect.clone();
                config.max_attempts = None;
                Backoff::new(config)
            })
            .next_delay()
            .unwrap_or_else(|| reconnect.max_delay());
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = tokio::time::sleep(delay) => {
                    let _ = tx.send(ConsumerMsg::RestartConsumer { queue, generation }).await;
                }
            }
        });
    }

    fn stop_all(&mut self) {
        for (_, consumer) in self.active.drain() {
            consumer.task.abort();
        }
    }
}

/// Everything needed to process the deliveries of one consumer.
struct Dispatch {
    channel: Channel,
    queue: String,
    max_requeues: Option<u32>,
    handler_timeout: Option<Duration>,
    registry: Arc<HandlerRegistry>,
    /// Tracks the handler tasks, so a shutdown can wait for them.
    handlers: TaskTracker,
    halt: CancellationToken,
}

impl Dispatch {
    fn spawn(&self, delivery: Delivery) {
        let process = process(
            delivery,
            self.channel.clone(),
            self.queue.clone(),
            self.max_requeues,
            self.handler_timeout,
            self.registry.clone(),
        );
        let halt = self.halt.clone();
        // A handler still running when the shutdown timeout expires is
        // cancelled; its message is redelivered.
        self.handlers.spawn(async move {
            halt.run_until_cancelled(process).await;
        });
    }
}

/// Reads deliveries and processes each one in its own task; `prefetch_count`
/// limits how many run at the same time.
async fn consume(
    mut consumer: Consumer,
    dispatch: Dispatch,
    generation: u64,
    actor_tx: Sender<ConsumerMsg>,
) {
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok(delivery) => dispatch.spawn(delivery),
            Err(e) => {
                tracing::warn!(queue = %dispatch.queue, "consumer error: {e}");
                break;
            }
        }
    }
    let queue = dispatch.queue;
    let _ = actor_tx
        .send(ConsumerMsg::ConsumerStopped { queue, generation })
        .await;
}

/// `_channel` keeps the channel open until the message is acknowledged: lapin closes a
/// channel when its last handle is dropped, and the consume task (holding the
/// `Consumer`) may already be gone during a shutdown.
async fn process(
    delivery: Delivery,
    _channel: Channel,
    queue: String,
    max_requeues: Option<u32>,
    handler_timeout: Option<Duration>,
    registry: Arc<HandlerRegistry>,
) {
    let Delivery {
        exchange,
        routing_key,
        redelivered,
        properties,
        data,
        acker,
        ..
    } = delivery;

    let msg = IncomingMessage {
        meta: MessageMeta {
            queue,
            exchange: exchange.as_str().to_owned(),
            routing_key: routing_key.as_str().to_owned(),
            redelivered,
            delivery_count: integer_header(&properties, "x-delivery-count"),
            acquired_count: integer_header(&properties, "x-acquired-count"),
            properties: MessageProperties::from_amqp(&properties),
        },
        payload: data,
    };

    let mut result = match handler_timeout {
        None => registry.dispatch(&msg).await,
        Some(timeout) => match tokio::time::timeout(timeout, registry.dispatch(&msg)).await {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(queue = %msg.meta.queue, ?timeout, "handler timed out, requeueing message");
                HandlerResult::Requeue
            }
        },
    };
    if result == HandlerResult::Requeue
        && let (Some(max), Some(acquired)) = (max_requeues, msg.meta.acquired_count)
        && acquired >= i64::from(max)
    {
        tracing::warn!(queue = %msg.meta.queue, acquired, "max_requeues reached, rejecting message");
        result = HandlerResult::Reject;
    }
    let outcome = match result {
        HandlerResult::Ack => acker.ack(BasicAckOptions::default()).await,
        HandlerResult::Requeue => {
            acker
                .nack(BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await
        }
        HandlerResult::Reject => acker.reject(BasicRejectOptions { requeue: false }).await,
    };
    if let Err(e) = outcome {
        // The broker redelivers the message once the channel is gone.
        tracing::warn!(queue = %msg.meta.queue, ?result, "could not acknowledge message: {e}");
    }
}
