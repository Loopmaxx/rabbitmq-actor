# rabbitmq-actor

An actor-based RabbitMQ client for Rust, built on [`lapin`](https://crates.io/crates/lapin) and tokio.

- **Quorum queues by default**, with config validation for their rules (durable, non-exclusive, per-consumer QoS) and support for delivery limits and dead-lettering.
- **Automatic reconnect** with exponential backoff, jitter and URL rotation. Topology, publisher channel and consumers are restored after every reconnect.
- **Publisher confirms always on**: `publish().await` resolves only after the broker confirmed the message.
- **JSON configuration** with defaults and strict validation (unknown fields are rejected).
- **Two input and two output traits** that keep lapin types out of your code.

## Installation

```toml
[dependencies]
rabbitmq-actor = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Requires Rust 1.93 or newer (edition 2024).

### Cargo features

| Feature | Default | Effect |
|---|---|---|
| `rustls` | yes | TLS (`amqps://`) with rustls, the `ring` crypto provider and the platform's certificate store |
| `rustls-aws-lc` | no | TLS with rustls and `aws-lc-rs` (FIPS capable; builds C code, needs cmake on some targets) |
| `native-tls` | no | TLS with the operating system's library |

Without TLS (`default-features = false`) the dependency tree is about half the size.

## Quick start

```rust,no_run
use amqp_actor_lib::{
    AmqpClient, AmqpConfig, HandlerResult, MessageMeta, MessagePublisher, Publishable,
    TypedMessageHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderCreated {
    id: u64,
}

impl Publishable for OrderCreated {
    fn exchange(&self) -> String { "orders".into() }
    fn routing_key(&self) -> String { "order.created".into() }
}

struct OrderHandler;

#[async_trait::async_trait]
impl TypedMessageHandler<OrderCreated> for OrderHandler {
    async fn handle(&self, order: OrderCreated, _meta: &MessageMeta) -> HandlerResult {
        println!("order {}", order.id);
        HandlerResult::Ack
    }
}

#[tokio::main]
async fn main() -> Result<(), amqp_actor_lib::AmqpError> {
    let config = AmqpConfig::from_json_file("config.json")?;
    let client = AmqpClient::start(config).await?; // waits for the first connection

    client.register_typed_handler("orders.created", OrderHandler).await?;
    client.publish_message(&OrderCreated { id: 1 }).await?;

    client.shutdown().await;
    Ok(())
}
```

Runnable examples: [`examples/consumer.rs`](examples/consumer.rs), [`examples/publisher.rs`](examples/publisher.rs).

## Traits

| Direction | Trait | Purpose |
|---|---|---|
| Input | `MessageHandler` | Receives an `IncomingMessage` (payload and `MessageMeta`) and returns a `HandlerResult`. |
| Input | `TypedMessageHandler<T>` | Receives the payload deserialized from JSON. Messages that fail to deserialize are rejected. |
| Output | `MessagePublisher` | `publish(OutgoingMessage)`. Implemented by `AmqpClient`, easy to mock in tests. |
| Output | `Publishable` | A type that knows its own exchange, routing key and properties. |

`HandlerResult` is one of:

- `Ack`: the message was processed.
- `Requeue`: put the message back into the queue.
- `Reject`: drop the message, or dead-letter it if a dead-letter exchange is configured.

Several handlers on the same queue run concurrently, and the most severe result wins (`Reject` > `Requeue` > `Ack`). A panicking handler counts as `Requeue`.

## Connection handling

The `ReconnectSupervisor` watches the connection through lapin's event stream and a periodic status check. When the connection is lost, it:

1. reconnects with exponential backoff and jitter, trying the last working URL first,
2. redeclares exchanges, queues and bindings,
3. reopens the publisher channel and restarts all consumers.

While the client is disconnected, `publish` fails immediately with `AmqpError::NotConnected`. Nothing is buffered, so the caller decides what to do with the message.

You can read the current state with `client.state()` or watch it with `client.state_changes()`. Unrecoverable errors stop the service with `ConnectionState::Failed`, for example a queue that already exists with different arguments (`TopologyMismatch`) or a reached `reconnect.max_attempts`.

> **Silent connection drops.** Network failures that send no TCP reset are only noticed through AMQP heartbeats, after twice the heartbeat interval. The client requests a 10 s heartbeat (`connection.heartbeat_secs`), so such drops are detected within about 20 s. With `null` the broker default applies (60 s on RabbitMQ, up to 120 s until a drop is noticed).

## Quorum queues

`queue_type` defaults to `"quorum"`. The following options are supported:

| Option | Queue argument |
|---|---|
| `delivery_limit` | `x-delivery-limit` |
| `dead_letter_exchange` | `x-dead-letter-exchange` |
| `dead_letter_routing_key` | `x-dead-letter-routing-key` |
| `dead_letter_strategy` | `x-dead-letter-strategy` (`at-least-once` requires `overflow: "reject-publish"`) |
| `overflow` | `x-overflow` |
| `initial_group_size` | `x-quorum-initial-group-size` |
| `max_length` | `x-max-length` |
| `max_length_bytes` | `x-max-length-bytes` |

Any other `x-*` argument can be passed through `arguments`.

> **RabbitMQ 4.x and requeues.** A requeue (`basic.nack` with `requeue = true`) does **not** count towards `delivery_limit`; only `x-acquired-count` is incremented. A handler that always requeues would loop forever. To prevent that, the client rejects a message once it was requeued `consumer.max_requeues` times (default 20, `null` disables the limit).

Classic queues are still available with `"queue_type": "classic"`.

## Configuration

A complete file with all defaults is in [`examples/default-config.json`](examples/default-config.json). Only `connection.urls` is required.

| Field | Default | Description |
|---|---|---|
| `connection.urls` | required | Broker URLs, tried in order |
| `connection.connection_name` | `null` | Name shown in the management UI |
| `connection.health_check_interval_ms` | `5000` | Status check in addition to lapin's events |
| `connection.connect_timeout_ms` | `10000` | Limit for one connection attempt including the AMQP handshake |
| `connection.operation_timeout_ms` | `10000` | Limit for opening channels, declaring topology, starting and cancelling consumers |
| `connection.heartbeat_secs` | `10` | Requested heartbeat; `null` uses the broker default (60 s) |
| `reconnect.initial_delay_ms` | `500` | First backoff delay |
| `reconnect.max_delay_ms` | `30000` | Upper limit for the backoff delay |
| `reconnect.multiplier` | `2.0` | Backoff growth factor |
| `reconnect.jitter` | `0.2` | ±20 % random spread; `0.0` disables it |
| `reconnect.max_attempts` | `null` | `null` retries forever |
| `publisher.confirm_timeout_ms` | `5000` | Time to wait for a confirm before `PublishTimeout` |
| `publisher.persistent` | `true` | Sets `delivery_mode = 2` |
| `publisher.mandatory` | `false` | Unroutable messages fail with `Unroutable` |
| `exchanges[]` | `[]` | `name`, `kind` (`topic`, `direct`, `fanout`, `headers`), `durable`, `auto_delete` |
| `queues[]` | `[]` | `name`, `queue_type`, `bindings`, `quorum`, `consumer`, `arguments` |
| `queues[].consumer` | | `prefetch_count` (10), `consumer_tag`, `max_requeues` (20), `handler_timeout_ms` (`null`) |
| `mailbox_capacity` | `1024` | Size of the internal actor mailboxes |
| `shutdown_timeout_ms` | `10000` | How long `shutdown()` waits for running handlers and pending confirms |

## Errors

**Publish errors** fall into two groups. Most of them mean the message was definitely not delivered (`NotConnected`, `Publish`, `UnknownExchange`, `PublishNacked`, `Unroutable`, `InvalidMessage`), so a retry is safe. `PublishTimeout` and `PublishUnconfirmed` mean the message was sent but not confirmed: it may or may not have arrived. `AmqpError::is_delivery_unknown()` tells the two groups apart. Retrying after an unknown outcome can create a duplicate, so set a `message_id` and make your consumers idempotent.

Exchanges that are not in the configuration are checked once per connection before the first publish to them. A missing exchange then fails only that publish with `UnknownExchange`, instead of closing the shared channel for all concurrent publishes.

All fallible operations return `AmqpError`. It is `Clone`, keeps the underlying cause in `source()`, and `AmqpError::is_recoverable()` tells you whether a retry can succeed. Topology errors are structured, e.g. `AmqpError::TopologyMismatch { object, name, .. }`. Passwords are removed from URLs in error messages and logs.

All public enums and configuration structs are `#[non_exhaustive]`. Build configurations from JSON, or in code with `AmqpConfig::new(urls).with_exchange(..).with_queue(..)`.

## Acknowledgements

Thanks to UBIX GmbH for supporting its release as open source.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
