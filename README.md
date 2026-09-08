<h1 align="center">ruststream-sqs-sns</h1>

<p align="center">
  <i>The Amazon SQS broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework, with SNS fan-out publishing: long polling, visibility-based retries, and redrive dead-lettering.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-sqs-sns"><img src="https://img.shields.io/crates/v/ruststream-sqs-sns.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-sqs-sns"><img src="https://img.shields.io/crates/dr/ruststream-sqs-sns" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-sqs-sns"><img src="https://img.shields.io/docsrs/ruststream-sqs-sns" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.94-blue.svg" alt="MSRV 1.94">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-sqs-sns/">Documentation</a></b>
</p>

---

`ruststream-sqs-sns` implements the RustStream broker contract over the official [`aws-sdk-sqs`](https://crates.io/crates/aws-sdk-sqs) and [`aws-sdk-sns`](https://crates.io/crates/aws-sdk-sns). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Lazy startup contract.** `SqsBroker::new()` is synchronous and does no I/O (region and credentials resolve from the environment on connect; `from_config` takes a prebuilt `SdkConfig`; `endpoint` + `test_credentials` target a local stack); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`.
- **Native settlement.** `ack` deletes the message, `nack(requeue = true)` zeroes its visibility, and `retry_after(delay)` sets the visibility to the delay - the framework's deferred retry is the transport's own verb, not an emulation. `nack(requeue = false)` deletes: poison routing belongs to the queue's redrive policy, and the receive count is surfaced as a header.
- **Crate-owned visibility extension.** A handler outliving the visibility timeout is protected: the crate keeps extending the visibility of every in-flight message for as long as the handler holds it.
- **Explicit polling settings.** `SqsQueue::new("orders").wait(20s).visibility(30s)` - the parameters that decide cost and latency are on the descriptor, with long polling as the default, and are equally reachable at the mount site through the `SqsSubscription` trait. Logical destination names map onto SQS queue names by replacing characters SQS forbids with `-` (a `.fifo` suffix survives).
- **Native batches.** `ReceiveMessage` is already a batching call, so a batch handler's `batch(n)` becomes `MaxNumberOfMessages` and one receive is one batch - nothing buffers on the client. A size above the protocol's ten is clamped to ten, with a log line, rather than refused.
- **FIFO ordering as the partition key.** On `.fifo` destinations the `partition-key` header becomes the message group id (and comes back as the same header), with a unique deduplication id per send. `publisher.with_group_id("user-42")` carries that header as a publisher base, and a message naming the header itself wins over it.
- **SNS as a fan-out publisher.** A distinct `SnsPublish` policy publishes to topics (names resolve through the idempotent `CreateTopic`); a handler's reply takes it with one mount step, `.out(Reply, SnsPublish)`, and `subscribe_queue_to_topic` wires queues with raw message delivery, so payloads and headers arrive unwrapped. SNS is not a subscriber: its delivery targets are queues and HTTP endpoints.
- **Text bodies.** SQS bodies are text, and the service's idea of text is narrower than UTF-8: a payload it accepts passes through untouched, and anything else - binary, or valid UTF-8 carrying control characters - travels base64-encoded with a marker attribute and decodes transparently on receive. The same rule picks `String` or `Binary` for each header attribute.
- **In-process test broker** (feature `testing`). `SqsTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["testing"] }
```

## Write a service

`ruststream_sqs_sns::prelude::*` is the one import a service file needs: it carries the framework's own prelude plus this crate's broker, descriptor and publish types.

```rust
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(20)))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(SqsBroker::new(), |b| b.include(handle))
}
```

## Test it

The `testing` feature runs handlers against an in-process SQS stand-in - no server, same routing, same ladder. Inject a message as an external producer would with `TestableBroker::inject`, then assert on what a handler published with the free `expect_published`:

```rust
use ruststream::{Broker, OutgoingMessage};
use ruststream::testing::{TestableBroker, expect_published};
use ruststream_sqs_sns::testing::SqsTestBroker;

let broker = SqsTestBroker::new().connect().await?;
broker.inject(OutgoingMessage::new("orders", br#"{"id":1}"#));
let confirmations =
    expect_published(&broker, "confirmations", 1, std::time::Duration::from_secs(1)).await;
```

SQS behaviour itself (visibility, redelivery, FIFO, SNS fan-out) is covered by the env-gated live suite instead: `just test-brokers` starts LocalStack and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-sqs-sns/
├── crates/
│   └── ruststream-sqs-sns/     the published crate
│       └── examples/           runnable sqs_* / sns_* examples (service, batches, FIFO, fan-out)
├── docker-compose.test.yml     LocalStack for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against LocalStack
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
