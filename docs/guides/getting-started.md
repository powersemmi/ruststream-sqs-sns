---
sidebar_position: 1
description: "Install ruststream-sqs-sns, start a local SQS, run a first subscriber against it and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-sqs-sns`, starts a local SQS and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-sqs-sns` is the SQS broker, built on the official `aws-sdk-sqs` client.
Its `testing` feature adds the [in-process SQS](../components/testing/index.md) that tests run on.

The broker crate has these features, all off by default:

| Feature | What it gives |
|---|---|
| `sns` | publishing to SNS topics with [`SnsPublish`](../components/sns/index.md), over `aws-sdk-sns` ([Fan-out with SNS](fan-out.md)) |
| `asyncapi` | the `sqs` and `sns` [bindings](../components/asyncapi/index.md) in the generated AsyncAPI document |
| `testing` | the in-process SQS behind `TestApp` |

## A local SQS

[LocalStack](https://github.com/localstack/localstack) runs SQS and SNS in one container:

```yaml title="compose.yaml"
services:
  localstack:
    image: localstack/localstack:4.14.0
    ports:
      - "4566:4566"
    environment:
      SERVICES: sqs,sns
      SQS_ENDPOINT_STRATEGY: path
```

```bash
docker compose up -d
```

The stack answers on `http://localhost:4566` and starts with no queues.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the `orders` queue.
It acknowledges an order with an amount and drops one without.
[`SqsQueue`](../components/subscriptions/sqs-queue.md) names the queue, and [`create_if_missing`](../components/subscriptions/create-if-missing.md) creates it when the subscription opens.
A production service leaves that step out and takes its queues from its infrastructure.
[`SqsBroker::new`](../components/broker/index.md) only records the configuration.
[`endpoint`](../components/broker/endpoint.md), [`region`](api:ruststream_sqs_sns::SqsBroker::region) and [`test_credentials`](api:ruststream_sqs_sns::SqsBroker::test_credentials) point it at the local stack.
The SDK clients are built when the service starts.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Send an order from another terminal with the AWS CLI:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws sqs send-message \
  --queue-url "$(aws sqs get-queue-url --queue-name orders --output text)" \
  --message-body '{"id":1,"amount_cents":1250}'
```

The service logs `order taken` with the order's id.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`SqsBroker` connects to an in-process SQS there, so the tests need no stack:

```bash
cargo test
```

[Testing an SQS service](testing.md) walks through these tests and what the in-process SQS models.

## Coming from aws-sdk-sqs

A service on `aws-sdk-sqs` writes its own receive loop.
Here is where each part goes in RustStream:

| aws-sdk-sqs | RustStream |
|---|---|
| `aws_config::load_from_env` and `Client::new` | [`SqsBroker::new`](api:ruststream_sqs_sns::SqsBroker::new), or [`SqsBroker::from_config`](../components/broker/credentials.md) with an `SdkConfig` of your own ([Running in production](production.md)) |
| `get_queue_url` | the queue name in [`SqsQueue::new`](api:ruststream_sqs_sns::SqsQueue::new), resolved once at startup |
| a `receive_message().wait_time_seconds(20)` loop | a `#[subscriber]` handler ([Consuming queues](consuming.md)) |
| deserializing `message.body()` | the handler's parameter type, decoded by the codec |
| `delete_message` after the work | the handler's outcome, [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) ([Acknowledgement and delivery guarantees](acknowledgement.md)) |
| `change_message_visibility` | [`HandlerOutcome::retry_after`](../components/settlement/retry-after.md) ([Retries and dead-letter queues](retries.md)) |
| a `RedrivePolicy` set in the console | [`max_attempts` and `dead_letter`](../components/settlement/dead-letters.md) at the mount site |
| `send_message` | a reply or an `Out` parameter, sent through [`SqsPublish`](../components/publishing/index.md) |
| `message_group_id` and `message_deduplication_id` | the publish steps [`group_id`](../components/publishing/group-id.md) and [`deduplication_id`](../components/publishing/deduplication-id.md) ([FIFO queues](fifo.md)) |
