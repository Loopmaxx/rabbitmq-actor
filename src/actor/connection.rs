use std::time::Duration;

use futures_util::StreamExt;
use lapin::{Connection, ConnectionProperties, Event, uri::AMQPUri};
use tokio_util::sync::CancellationToken;

use crate::{
    AmqpError, ConnectionConfig, Result, TransportError,
    error::{redact_url, timed},
};

/// Why [`wait_for_loss`] returned.
pub(crate) enum ConnectionEnd {
    Shutdown,
    Lost(Option<String>),
}

/// Opens a single connection. Retrying is done by the [`crate::reconnect::ReconnectSupervisor`].
///
/// The whole attempt (TCP, TLS, AMQP handshake) is limited by `connect_timeout_ms`;
/// `heartbeat_secs` is requested unless the URL sets `heartbeat` itself.
pub(crate) async fn connect(url: &str, config: &ConnectionConfig) -> Result<Connection> {
    let mut uri: AMQPUri = url.parse().map_err(|e| {
        AmqpError::InvalidConfig(format!(
            "'{}' is not a valid AMQP URL: {e}",
            redact_url(url)
        ))
    })?;
    if uri.query.heartbeat.is_none() {
        uri.query.heartbeat = config.heartbeat_secs;
    }
    if uri.query.connection_timeout.is_none() {
        uri.query.connection_timeout = Some(config.connect_timeout_ms);
    }

    let mut properties = ConnectionProperties::default();
    // Length checked by `AmqpConfig::validate`, so the conversion cannot panic.
    if let Some(name) = &config.connection_name {
        properties = properties.with_connection_name(name.as_str().into());
    }
    timed(
        "connect",
        config.connect_timeout(),
        Connection::connect_uri(uri, properties),
    )
    .await?
    .map_err(|source| AmqpError::Connect {
        url: redact_url(url),
        source: TransportError::new(source),
    })
}

/// Waits until the connection is lost or the service is shut down.
///
/// Uses lapin's event stream and additionally checks the connection status periodically.
pub(crate) async fn wait_for_loss(
    connection: &Connection,
    health_check_interval: Duration,
    shutdown: &CancellationToken,
) -> ConnectionEnd {
    let mut events = connection.events_listener();
    let mut health_check = tokio::time::interval(health_check_interval);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return ConnectionEnd::Shutdown,
            event = events.next() => match event {
                // Channel errors are reported here as well; they are handled by the actors.
                Some(Event::Error(e)) if connection.status().connected() => {
                    tracing::debug!("channel error: {e}");
                }
                Some(Event::Error(e)) => return ConnectionEnd::Lost(Some(e.to_string())),
                Some(Event::ConnectionBlocked(reason)) => {
                    tracing::warn!("broker blocked the connection: {reason}");
                }
                Some(Event::ConnectionUnblocked) => tracing::info!("broker unblocked the connection"),
                Some(_) => {}
                None => return ConnectionEnd::Lost(None),
            },
            _ = health_check.tick() => {
                if !connection.status().connected() {
                    return ConnectionEnd::Lost(None);
                }
            }
        }
    }
}

pub(crate) async fn close(connection: &Connection, timeout: Duration) {
    if connection.status().connected() {
        match timed(
            "close connection",
            timeout,
            connection.close(200, "shutdown".into()),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::debug!("error while closing connection: {e}"),
            Err(e) => tracing::warn!("{e}"),
        }
    }
}
