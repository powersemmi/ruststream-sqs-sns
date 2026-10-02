<h1 align="center">ruststream-sqs-sns</h1>

<p align="center">
  <i>The Amazon SQS broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework, with SNS fan-out publishing: long polling, visibility-based retries, and redrive dead-lettering.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sqs-sns/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-sqs-sns"><img src="https://img.shields.io/crates/v/ruststream-sqs-sns.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-sqs-sns"><img src="https://img.shields.io/crates/dr/ruststream-sqs-sns" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-sqs-sns"><img src="https://img.shields.io/docsrs/ruststream-sqs-sns" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-sqs-sns/">Documentation</a></b>
</p>

---

`ruststream-sqs-sns` connects a RustStream service to Amazon SQS and SNS over the official
[`aws-sdk-sqs`](https://crates.io/crates/aws-sdk-sqs) and
[`aws-sdk-sns`](https://crates.io/crates/aws-sdk-sns) (SNS behind the `sns` feature). Handlers,
routing, codecs and middleware come from the framework; this crate is the transport.

## Features

- **Native settlement:** `ack` deletes the message, a retry resets its visibility, and
  `retry_after(delay)` sets the visibility to the delay.
- **Retry caps as the queue's redrive policy,** so SQS moves a spent delivery itself.
- **Visibility extended** for as long as a handler holds a message.
- **Long polling by default,** with the polling settings on the queue descriptor.
- **Native batches:** one `ReceiveMessage` is one batch.
- **FIFO queues:** the message group and deduplication ids are per-message settings.
- **SNS fan-out** as a publish policy behind the `sns` feature, with queues subscribed to topics by
  raw message delivery.
- **Binary-safe bodies:** a payload SQS cannot carry as text travels base64-encoded and decodes on
  receive.
- **AsyncAPI** with the specification's `sqs` binding, behind the `asyncapi` feature.
- **Tests without AWS:** the production app runs with `SqsBroker` connected to an in-process SQS
  and SNS.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["testing"] }
```

Publishing to SNS topics takes the `sns` feature, which the service below uses: `ruststream-sqs-sns = { version = "0.7", features = ["sns"] }`.

## Write a service

```rust
use std::time::Duration;

use ruststream_sqs_sns::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize)]
struct PlaceOrder {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
struct OrderPlaced {
    id: u64,
}

#[subscriber(
    SqsQueue::new("orders").wait(Duration::from_secs(20)),
    reply("orders-events")
)]
async fn accept(order: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: order.id }
}

#[app]
fn service() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqsBroker::new(), |b| {
        b.include(accept).out_reply(SnsPublish::default());
    })
}
```

`#[app]` generates `main`, so the binary understands `run` and `asyncapi gen`. The reply goes to
the `orders-events` SNS topic; without the `out_reply` step it goes to a queue of that name.

## Test it

`TestApp` runs the app `main` runs, with `SqsBroker` connected to an in-process SQS and SNS and no
AWS account.

```rust
use ruststream::testing::TestApp;
use ruststream_sqs_sns::prelude::*;

let tb = TestApp::start(service()).await?;

tb.broker::<SqsBroker>()
    .message(&PlaceOrder { id: 1 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<SqsBroker>()
    .published::<OrderPlaced>("orders-events")
    .assert_called_once()
    .with(&OrderPlaced { id: 1 });
```

`TestApp::start_live(service())` runs the same test against a running stack.

## Documentation

- This crate: <https://docs.rs/ruststream-sqs-sns>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024, the floor of the RustStream core.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
