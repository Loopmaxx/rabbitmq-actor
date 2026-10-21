//! cargo run --example consumer -- examples/config.json
use amqp_actor_lib::{AmqpClient, AmqpConfig, HandlerResult, MessageMeta, TypedMessageHandler};
use serde::Deserialize;

#[derive(Deserialize)]
struct OrderCreated {
    id: u64,
    amount: f64,
}

struct OrderHandler;

#[async_trait::async_trait]
impl TypedMessageHandler<OrderCreated> for OrderHandler {
    async fn handle(&self, order: OrderCreated, meta: &MessageMeta) -> HandlerResult {
        tracing::info!(id = order.id, amount = order.amount, delivery_count = ?meta.delivery_count, "order received");
        if order.amount < 0.0 {
            // Goes to the dead-letter exchange configured on the quorum queue.
            return HandlerResult::Reject;
        }
        HandlerResult::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/config.json".into());
    let config = AmqpConfig::from_json_file(path)?;

    let client = AmqpClient::start(config).await?;
    client
        .register_typed_handler("orders.created", OrderHandler)
        .await?;

    tokio::signal::ctrl_c().await?;
    client.shutdown().await;
    Ok(())
}
