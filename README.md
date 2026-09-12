<h1 align="center">ruststream-sqs-sns</h1>

<p align="center">
  <i>The Amazon SQS broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework, with SNS fan-out publishing: long polling, visibility-based retries, and redrive dead-lettering.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-sqs-sns"><img src="https://img.shields.io/crates/v/ruststream-sqs-sns.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-sqs-sns"><img src="https://img.shields.io/crates/dr/ruststream-sqs-sns" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-sqs-sns"><img src="https://img.shields.io/docsrs/ruststream-sqs-sns" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.94.1-blue.svg" alt="MSRV 1.94.1">
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
- **FIFO ordering as a per-message setting.** `SqsPublishOptions` is the message group id and the deduplication id, named per call with the `group_id` / `deduplication_id` steps on the publish builder, or fixed for a whole position with `Publish::default().group_id("orders")`. A delivery carries its group back in the `partition-key` header, and a message may name its own group there too - the spelling that travels to every other broker. Naming either setting for a standard queue is a publish error rather than a value dropped in silence.
- **SNS as a fan-out publisher.** A distinct `SnsPublish` policy publishes to topics (names resolve through the idempotent `CreateTopic`); a handler's reply takes it with one mount step, `.out(Reply, SnsPublish::default())`, and `subscribe_queue_to_topic` wires queues with raw message delivery, so payloads and headers arrive unwrapped. SNS is not a subscriber: its delivery targets are queues and HTTP endpoints.
- **Text bodies.** SQS bodies are text, and the service's idea of text is narrower than UTF-8: a payload it accepts passes through untouched, and anything else - binary, or valid UTF-8 carrying control characters - travels base64-encoded with a marker attribute and decodes transparently on receive. The same rule picks `String` or `Binary` for each header attribute. A handler that parses the body itself takes the framework's byte lane (`#[derive(Deserialized)]` over `&[u8]`, no codec on the path) and sees the bytes the producer sent: the base64 hop is already undone by then.
- **In-process test broker** (feature `testing`). `SqsTestBroker` reproduces this crate's core routing with no server, so a service's handlers run under the framework's `TestApp` harness, and it answers the way the real queues do, which the crate's own tests hold it to. The crate's own types mount on it: `SqsQueue` opens a subscription there, and `SqsPublish` and `SnsPublish` pair there, so the `#[subscriber(SqsQueue::new(..))]` and the `.out(Reply, Publish::default())` a service ships are what the test runs - no stand-in descriptor, no stand-in policy. The harness reads back the per-message settings a slot publish carried, with `tb.out::<Marker>().with_options(..)`.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Write a service

A routes file globs `ruststream_sqs_sns::prelude::*`: the framework's own prelude plus this crate's broker, queue descriptor, its mount-site settings trait and the publish policies, with `SqsPublish` under `Publish`, the uniform name every broker crate gives the policy a mount site hands over (`SnsPublish` keeps its own, because fan-out is the departure rather than the default). A handler file globs the framework's prelude alone and bounds an injected publisher with `Publisher`, so the uniform name stays free for the policy.

```rust
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct PlaceOrder {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
struct OrderPlaced {
    id: u64,
}

// The reply type names no destination of its own, so it takes the clause's; the mount site
// names who takes it there.
#[subscriber(
    SqsQueue::new("orders").wait(Duration::from_secs(20)),
    publish("orders-events")
)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    println!("accepted order {}", order.id);
    OrderPlaced { id: order.id }
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        // Mounted without the step the reply rides `SqsPublish` onto a queue named
        // `orders-events`; one `.out(Reply, SnsPublish::default())` sends it to the topic
        // instead.
        b.include(accept).out(Reply, SnsPublish::default());
    })
}
```

## Test it

App-level tests go through the framework's `TestApp`: it starts the application on the in-process transport the `testing` feature ships - no server, same routing, same ladder - injects as an external producer would, and drives the reaction to a standstill before the assertions run.

The descriptor and the policies mount there as written, so the handler under test is the one the service ships: `#[subscriber(SqsQueue::new(..))]` opens a subscription on the stand-in, and `SqsPublish` and `SnsPublish` pair there.

```rust
use ruststream::testing::TestApp;
use ruststream_sqs_sns::prelude::*;
use ruststream_sqs_sns::testing::SqsTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct PlaceOrder {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
struct OrderPlaced {
    id: u64,
}

#[subscriber(SqsQueue::new("orders"), publish("orders-events"))]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_order_is_announced() -> Result<(), Box<dyn std::error::Error>> {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsTestBroker::new(), |b| {
            b.include(accept);
        });
    let tb = TestApp::start(app).await?;

    tb.message(&PlaceOrder { id: 1 }).publish().await?;

    tb.broker::<SqsTestBroker>()
        .published::<OrderPlaced>("orders-events")
        .assert_called_once()
        .with(&OrderPlaced { id: 1 });
    Ok(())
}
```

SQS behaviour itself (visibility, redelivery, FIFO, SNS fan-out) is covered by the env-gated live suite instead: `just test-brokers` starts LocalStack and runs the integration tests plus the framework conformance lifecycle against it.

## Layout

```
ruststream-sqs-sns/
├── crates/
│   └── ruststream-sqs-sns/     the published crate
│       └── examples/           runnable sqs_* / sns_* examples (service, batches, FIFO, fan-out)
├── docs/                       the documentation site sources
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
