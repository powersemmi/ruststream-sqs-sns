<h1 align="center">ruststream-sqs-sns</h1>

<p align="center">
  <i>The Amazon SQS broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework, with SNS fan-out publishing: long polling, visibility-based retries, and redrive dead-lettering.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.94-blue.svg" alt="MSRV 1.94">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-sqs-sns` implements the RustStream broker contract over the official [`aws-sdk-sqs`](https://crates.io/crates/aws-sdk-sqs) and [`aws-sdk-sns`](https://crates.io/crates/aws-sdk-sns). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Lazy startup contract.** `SqsBroker::new()` is synchronous and does no I/O (region and credentials resolve from the environment on connect; `from_config` takes a prebuilt `SdkConfig`; `endpoint` + `test_credentials` target a local stack); the runtime connects once at startup, so the broker composes with `#[ruststream::app]`.
- **Native settlement.** `ack` deletes the message, `nack(requeue = true)` zeroes its visibility, and `retry_after(delay)` sets the visibility to the delay - the framework's deferred retry is the transport's own verb, not an emulation. `nack(requeue = false)` deletes: poison routing belongs to the queue's redrive policy, and the receive count is surfaced as a header.
- **Crate-owned visibility extension.** A handler outliving the visibility timeout is protected: the crate keeps extending the visibility of every in-flight message for as long as the handler holds it.
- **Explicit polling settings.** `SqsQueue::new("orders").wait(20s).batch(10).visibility(30s)` - the parameters that decide cost and latency are on the descriptor, with long polling as the default. Logical destination names map onto SQS queue names by replacing characters SQS forbids with `-` (a `.fifo` suffix survives).
- **FIFO ordering as the partition key.** On `.fifo` destinations the `partition-key` header becomes the message group id (and comes back as the same header), with a unique deduplication id per send.
- **SNS as a fan-out publisher.** A distinct `SnsPublish` policy publishes to topics (names resolve through the idempotent `CreateTopic`); `subscribe_queue_to_topic` wires queues with raw message delivery, so payloads and headers arrive unwrapped. SNS is not a subscriber: its delivery targets are queues and HTTP endpoints.
- **Text bodies.** SQS bodies are text: UTF-8 payloads pass through untouched, binary payloads travel base64-encoded with a marker attribute and decode transparently on receive.
- **In-process test broker** (feature `testing`). `SqsTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

## Status

Implemented and verified against LocalStack (the framework's conformance lifecycle suite and the integration tests, including SNS fan-out, run in CI against it). Published on crates.io, tracking the `ruststream` 0.6 line. The design issue is [powersemmi/ruststream#189](https://github.com/powersemmi/ruststream/issues/189).

MSRV is 1.94, tracking the AWS SDK (the core stays at 1.85; a dependent may exceed its dependency's floor).

## Install

```toml
[dependencies]
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-sqs-sns = "0.6"
serde = { version = "1", features = ["derive"] }
```

## Write a service

```rust
use std::time::Duration;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_sqs_sns::{SqsBroker, SqsQueue};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(SqsQueue::new("orders").wait(Duration::from_secs(20)).batch(10))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(SqsBroker::new(), |b| b.include(handle))
}
```

## Test it

The `testing` feature runs handlers against an in-process SQS stand-in - no server, same routing. Product behaviour (visibility, redelivery, FIFO, SNS fan-out) is covered by the env-gated live suite instead: `just test-brokers` starts LocalStack and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-sqs-sns/
├── crates/
│   └── ruststream-sqs-sns/     the published crate
│       └── examples/           runnable sqs_* / sns_* examples
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
