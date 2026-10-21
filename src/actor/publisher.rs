use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::Duration,
};

use lapin::{
    Channel, Confirmation, Connection,
    options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions},
    types::{FieldTable, ShortString},
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    AmqpConfig, AmqpError, OutgoingMessage, PublisherConfig, Result, TransportError, error::timed,
    messages::PublisherMsg, traits::short_string,
};

/// State shared between the publishing callers and the [`PublisherActor`] (F10).
///
/// Callers publish directly on the shared channel, so publishes run in parallel
/// instead of one after another through the actor's mailbox. The actor only opens,
/// reopens and checks things; those are rare and serialized through its mailbox.
pub(crate) struct PublisherShared {
    config: PublisherConfig,
    target: RwLock<Option<Target>>,
    /// Exchanges known to exist: the configured ones (declared by the topology) and
    /// the ones checked since the last connect (F5).
    known_exchanges: RwLock<HashSet<String>>,
    configured_exchanges: HashSet<String>,
    /// One token per publish in progress, so a shutdown can wait for them.
    in_flight: TaskTracker,
}

/// The publisher channel and the connection it belongs to.
#[derive(Clone)]
pub(crate) struct Target {
    channel: Channel,
    connection: Arc<Connection>,
}

impl PublisherShared {
    pub(crate) fn new(config: &AmqpConfig) -> Arc<Self> {
        let configured_exchanges: HashSet<String> = std::iter::once(String::new())
            .chain(config.exchanges.iter().map(|e| e.name.clone()))
            .collect();
        Arc::new(Self {
            config: config.publisher.clone(),
            target: RwLock::new(None),
            known_exchanges: RwLock::new(configured_exchanges.clone()),
            configured_exchanges,
            in_flight: TaskTracker::new(),
        })
    }

    fn target(&self) -> Option<Target> {
        self.target
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_target(&self, target: Option<Target>) {
        *self.target.write().unwrap_or_else(|e| e.into_inner()) = target;
    }

    fn is_known_exchange(&self, name: &str) -> bool {
        self.known_exchanges
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(name)
    }

    fn add_known_exchange(&self, name: &str) {
        self.known_exchanges
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_owned());
    }

    fn reset_known_exchanges(&self) {
        *self
            .known_exchanges
            .write()
            .unwrap_or_else(|e| e.into_inner()) = self.configured_exchanges.clone();
    }
}

/// Publishes `msg` and waits for the broker's confirm. Runs in the caller's task.
pub(crate) async fn publish(
    shared: &PublisherShared,
    actor: &Sender<PublisherMsg>,
    msg: OutgoingMessage,
) -> Result<()> {
    let _in_flight = shared.in_flight.token();
    let properties = msg.properties.to_amqp(shared.config.persistent)?;
    let exchange = short_string("exchange", &msg.exchange)?;
    let routing_key = short_string("routing_key", &msg.routing_key)?;

    if !shared.is_known_exchange(&msg.exchange) {
        let (reply, rx) = oneshot::channel();
        let check = PublisherMsg::CheckExchange {
            name: msg.exchange.clone(),
            reply,
        };
        ask(actor, check, rx).await??;
    }

    let target = match shared.target() {
        Some(target) if target.channel.status().connected() => target,
        // Not open yet, or closed by the broker (e.g. after a 404): let the actor reopen it.
        _ => {
            let (reply, rx) = oneshot::channel();
            ask(actor, PublisherMsg::EnsureChannel(reply), rx).await??
        }
    };

    let options = BasicPublishOptions {
        mandatory: shared.config.mandatory,
        ..Default::default()
    };
    let timeout = shared.config.confirm_timeout();
    let send =
        target
            .channel
            .basic_publish(exchange, routing_key, options, &msg.payload, properties);
    let confirm = match timed("publish", timeout, send).await {
        Ok(Ok(confirm)) => confirm,
        // Nothing was sent. A closed channel (connection still up) is reopened with
        // the next publish.
        Ok(Err(e)) if target.connection.status().connected() => {
            return Err(AmqpError::Publish(TransportError::new(e)));
        }
        Ok(Err(_)) => return Err(AmqpError::NotConnected),
        // Blocked by the broker (flow control); frames may already be queued.
        Err(_) => return Err(AmqpError::PublishTimeout(timeout)),
    };

    match tokio::time::timeout(timeout, confirm).await {
        Err(_) => Err(AmqpError::PublishTimeout(timeout)),
        // The message was sent; the channel closed before the confirm arrived,
        // e.g. because another publish hit an exchange deleted in the meantime (F5).
        Ok(Err(e)) => Err(AmqpError::PublishUnconfirmed {
            exchange: msg.exchange,
            routing_key: msg.routing_key,
            source: TransportError::new(e),
        }),
        Ok(Ok(Confirmation::Ack(None))) => Ok(()),
        Ok(Ok(Confirmation::Ack(Some(_)))) => Err(AmqpError::Unroutable {
            exchange: msg.exchange,
            routing_key: msg.routing_key,
        }),
        Ok(Ok(Confirmation::Nack(_))) => Err(AmqpError::PublishNacked {
            exchange: msg.exchange,
            routing_key: msg.routing_key,
        }),
        // `confirm_select` is called on every publisher channel, so this cannot
        // happen; reported instead of panicking (F16).
        Ok(Ok(Confirmation::NotRequested)) => Err(AmqpError::Internal(
            "publisher channel is not in confirm mode".into(),
        )),
    }
}

/// Sends a request to the actor and waits for the answer.
async fn ask<T>(
    actor: &Sender<PublisherMsg>,
    msg: PublisherMsg,
    reply: oneshot::Receiver<T>,
) -> Result<T> {
    actor.send(msg).await.map_err(|_| AmqpError::Shutdown)?;
    reply.await.map_err(|_| AmqpError::Shutdown)
}

/// Manages the publisher channel: opens it on connect, reopens it when the broker
/// closed it, checks unknown exchanges. The publishes themselves run in the callers'
/// tasks, see [`publish`].
///
/// While there is no connection every publish fails immediately with
/// [`AmqpError::NotConnected`]; nothing is buffered. During a shutdown the channel
/// stays usable until the supervisor sends [`PublisherMsg::Drain`], so handlers that
/// are being drained can still publish.
pub(crate) struct PublisherActor {
    rx: Receiver<PublisherMsg>,
    shared: Arc<PublisherShared>,
    /// `connection.operation_timeout_ms`, for opening channels and checking exchanges.
    operation_timeout: Duration,
    halt: CancellationToken,
    connection: Option<Arc<Connection>>,
}

impl PublisherActor {
    pub(crate) fn new(
        rx: Receiver<PublisherMsg>,
        shared: Arc<PublisherShared>,
        config: &AmqpConfig,
        halt: CancellationToken,
    ) -> Self {
        Self {
            rx,
            shared,
            operation_timeout: config.connection.operation_timeout(),
            halt,
            connection: None,
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
            match msg {
                PublisherMsg::ConnectionReady(connection) => {
                    self.connection = Some(connection);
                    self.shared.set_target(None);
                    self.shared.reset_known_exchanges();
                    if let Err(e) = self.ensure_channel().await {
                        tracing::warn!("could not open publisher channel: {e}");
                    }
                }
                PublisherMsg::ConnectionLost => {
                    self.connection = None;
                    self.shared.set_target(None);
                }
                PublisherMsg::EnsureChannel(reply) => {
                    let _ = reply.send(self.ensure_channel().await);
                }
                PublisherMsg::CheckExchange { name, reply } => {
                    let _ = reply.send(self.check_exchange(&name).await);
                }
                PublisherMsg::Drain(reply) => {
                    // Each publish is bounded by `confirm_timeout_ms`.
                    self.shared.in_flight.close();
                    self.shared.in_flight.wait().await;
                    self.shared.in_flight.reopen();
                    let _ = reply.send(());
                }
            }
        }
        // Requests still queued get `Shutdown` when their reply senders are dropped.
        self.shared.set_target(None);
    }

    /// Returns the publisher channel, reopening it if the broker closed it
    /// (e.g. after publishing to a non-existent exchange).
    async fn ensure_channel(&mut self) -> Result<Target> {
        if let Some(target) = self.shared.target()
            && target.channel.status().connected()
        {
            return Ok(target);
        }
        let connection = match &self.connection {
            Some(c) if c.status().connected() => c.clone(),
            _ => return Err(AmqpError::NotConnected),
        };
        let timeout = self.operation_timeout;
        let channel = timed("open channel", timeout, connection.create_channel())
            .await?
            .map_err(AmqpError::channel)?;
        let confirm_select = channel.confirm_select(ConfirmSelectOptions::default());
        timed("enable publisher confirms", timeout, confirm_select)
            .await?
            .map_err(AmqpError::channel)?;
        let target = Target {
            channel,
            connection,
        };
        self.shared.set_target(Some(target.clone()));
        Ok(target)
    }

    /// Makes sure the exchange exists before publishing to it (F5). Publishing to a
    /// missing exchange makes the broker close the channel, which would fail every
    /// other publish still waiting for its confirm. The check uses its own channel,
    /// so a missing exchange only fails this publish. Results are cached until the
    /// next connect; configured exchanges are never checked.
    async fn check_exchange(&mut self, name: &str) -> Result<()> {
        if self.shared.is_known_exchange(name) {
            return Ok(());
        }
        let connection = match &self.connection {
            Some(c) if c.status().connected() => c.clone(),
            _ => return Err(AmqpError::NotConnected),
        };
        let short_name: ShortString = short_string("exchange", name)?;
        let timeout = self.operation_timeout;
        let channel = timed("open channel", timeout, connection.create_channel())
            .await?
            .map_err(AmqpError::channel)?;
        let options = ExchangeDeclareOptions {
            passive: true,
            ..Default::default()
        };
        let declare = channel.exchange_declare(
            short_name,
            lapin::ExchangeKind::Direct, // ignored for a passive declare
            options,
            FieldTable::default(),
        );
        match timed("check exchange", timeout, declare).await? {
            Ok(()) => {
                let _ = timed("close channel", timeout, channel.close(200, "OK".into())).await;
                self.shared.add_known_exchange(name);
                Ok(())
            }
            // The broker closed the check channel; 404 means the exchange is missing.
            Err(e) if crate::error::is_not_found(&e) => {
                Err(AmqpError::UnknownExchange(name.to_owned()))
            }
            Err(e) => Err(AmqpError::channel(e)),
        }
    }
}
