//! Publish throughput: this crate compared with plain lapin on the same setup
//! (quorum queue, publisher confirms, persistent 1 KiB messages).
//!
//! ```sh
//! cargo run --release --example bench -- [AMQP_URL] [messages] [concurrency]
//! ```

use std::{sync::Arc, time::Instant};

use rabbitmq_actor::{AmqpClient, AmqpConfig, MessagePublisher, OutgoingMessage};
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .unwrap_or_else(|| "amqp://guest:guest@localhost:5672/tims-host".into());
    let messages: usize = args.next().map_or(Ok(40_000), |s| s.parse())?;
    let concurrency: usize = args.next().map_or(Ok(64), |s| s.parse())?;
    let queue = format!("bench-{}", std::process::id());
    let payload = vec![b'x'; 1024];

    let config = AmqpConfig::from_json_str(&format!(
        r#"{{ "connection": {{ "urls": ["{url}"] }}, "queues": [{{ "name": "{queue}" }}] }}"#
    ))?;
    let client = AmqpClient::start(config).await?;

    // Warm-up, then the measured runs.
    run_lib(&client, &queue, &payload, 2_000, concurrency).await;
    let lib = run_lib(&client, &queue, &payload, messages, concurrency).await;
    let lib_single = run_lib(&client, &queue, &payload, messages / 20, 1).await;
    client.shutdown().await;

    let raw = run_raw(&url, &queue, &payload, messages, concurrency).await?;

    println!("messages={messages} concurrency={concurrency} payload=1KiB quorum confirms");
    println!("library, {concurrency} callers: {lib:>8.0} msg/s");
    println!("library, 1 caller:   {lib_single:>8.0} msg/s");
    println!("lapin,   {concurrency} callers: {raw:>8.0} msg/s");
    Ok(())
}

async fn run_lib(
    client: &AmqpClient,
    queue: &str,
    payload: &[u8],
    messages: usize,
    concurrency: usize,
) -> f64 {
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let queue = queue.to_owned();
        let payload = payload.to_vec();
        tasks.push(tokio::spawn(async move {
            for _ in 0..messages / concurrency {
                let msg = OutgoingMessage::new("", queue.clone(), payload.clone());
                client.publish(msg).await.expect("publish");
            }
        }));
    }
    for task in tasks {
        task.await.expect("task");
    }
    messages as f64 / start.elapsed().as_secs_f64()
}

async fn run_raw(
    url: &str,
    queue: &str,
    payload: &[u8],
    messages: usize,
    concurrency: usize,
) -> Result<f64, Box<dyn std::error::Error>> {
    let connection = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    let payload: Arc<[u8]> = payload.into();

    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let channel = channel.clone();
        let queue = queue.to_owned();
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..messages / concurrency {
                channel
                    .basic_publish(
                        "".into(),
                        queue.as_str().into(),
                        BasicPublishOptions::default(),
                        &payload,
                        BasicProperties::default().with_delivery_mode(2),
                    )
                    .await
                    .expect("publish")
                    .await
                    .expect("confirm");
            }
        }));
    }
    for task in tasks {
        task.await?;
    }
    let rate = messages as f64 / start.elapsed().as_secs_f64();
    let _ = channel.queue_delete(queue.into(), Default::default()).await;
    connection.close(200, "bench done".into()).await?;
    Ok(rate)
}
