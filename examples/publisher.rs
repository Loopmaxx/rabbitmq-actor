//! cargo run --example publisher -- examples/config.json
use std::time::Duration;

use rabbitmq_actor::{AmqpClient, AmqpConfig, AmqpError, MessagePublisher, Publishable};
use serde::Serialize;

#[derive(Serialize)]
struct OrderCreated {
    id: u64,
    amount: f64,
}

impl Publishable for OrderCreated {
    fn exchange(&self) -> String {
        "orders".into()
    }
    fn routing_key(&self) -> String {
        "order.created".into()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/config.json".into());
    let client = AmqpClient::start(AmqpConfig::from_json_file(path)?).await?;

    for id in 0.. {
        let order = OrderCreated { id, amount: 10.0 };
        match client.publish_message(&order).await {
            Ok(()) => tracing::info!(id, "published"),
            // The caller decides what to do while the connection is re-established.
            Err(AmqpError::NotConnected) => tracing::warn!(id, "not connected, message dropped"),
            Err(e) => tracing::error!(id, "publish failed: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
