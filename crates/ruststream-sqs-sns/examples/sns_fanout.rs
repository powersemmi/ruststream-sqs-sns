//! SNS fan-out: publish once to a topic, deliver to every subscribed queue.
//!
//! Run a local stack first (`just brokers-up`), then:
//! `cargo run --example sns_fanout`

use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_sqs_sns::{SqsBroker, SqsQueue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connected = SqsBroker::new()
        .endpoint("http://localhost:4566")
        .test_credentials()
        .region("us-east-1")
        .connect()
        .await?;

    // Wire two queues to one topic (raw delivery keeps payloads and headers unwrapped).
    for queue in ["billing", "shipping"] {
        connected
            .subscribe_queue(SqsQueue::new(queue).create_if_missing())
            .await?;
        connected
            .subscribe_queue_to_topic("orders-events", queue)
            .await?;
    }

    let sns = connected.sns_publisher();
    sns.publish(OutgoingMessage::new(
        "orders-events",
        b"{\"id\":1}".as_slice(),
    ))
    .await?;

    connected.shutdown().await?;
    Ok(())
}
