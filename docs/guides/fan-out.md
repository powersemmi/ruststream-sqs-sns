---
sidebar_position: 6
description: "Publish a reply to an SNS topic once and let SNS deliver it to every SQS queue subscribed to the topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Fan-out with SNS

This guide publishes a message to an SNS topic once and lets SNS deliver a copy to every SQS queue subscribed to the topic.

SNS to SQS is how AWS services fan out: one topic, one queue per consumer.
The crate publishes to topics; each queue behind a topic is read the ordinary way.
Publishing to SNS needs the `sns` feature:

```toml title="Cargo.toml"
[dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["sns"] }
```

The example is a shop.
It accepts an order and announces it on a topic, and billing and shipping each read the announcement from a queue of their own.

## A reply to a topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#accept"
```

</TabItem>
</Tabs>

`accept` answers each order with an `OrderPlaced`, and the type names where it goes: `shop-order-events`.
The mount site decides whether that name is a queue or a topic.

## The queues behind the topic

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#consumers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#consumers"
```

</TabItem>
</Tabs>

`bill` and `ship` are ordinary queue handlers.
Each queue gets its own copy of every announcement, so billing and shipping work at their own pace and retry on their own.

## Subscribing the queues

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#topology"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#topology"
```

</TabItem>
</Tabs>

[`subscribe_queue_to_topic`](../components/sns/subscribe-queue-to-topic.md) subscribes a queue to a topic with raw message delivery.
So a copy arrives as a plain SQS message: the payload is the body, and each header is a message attribute.
Without raw delivery SNS would wrap the message in a JSON envelope of its own.
SNS delivers to a queue only when the queue's access policy lets the topic send to it.
The call adds that statement to the policy before it subscribes, and keeps the statements already there.
The call creates the topic when it does not exist, and subscribing the same queue again changes nothing.

Topology is usually infrastructure, created with Terraform or the console.
Here a startup hook does it on a clone of the app's broker, which uses the connection the app already holds.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(SnsPublish::default())` binds the reply to [`SnsPublish`](../components/sns/index.md), and the reply becomes an SNS `Publish` to the topic.
Without that step the same reply rides the broker's default policy, [`SqsPublish`](api:ruststream_sqs_sns::SqsPublish), and lands on a queue named `shop-order-events`.
`SnsPublish` opens a topic by name with `CreateTopic`, which returns the existing topic when there is one, or takes the topic's ARN as the name.
The same policy works in an `Out` slot and in a startup hook, and [`ConnectedSqsBroker::sns_publisher`](api:ruststream_sqs_sns::ConnectedSqsBroker::sns_publisher) gives a publisher outside the app.

A topic whose name ends in `.fifo` is a FIFO topic.
It carries the message group of a publish to the FIFO queues subscribed to it, with the [group steps](fifo.md) a FIFO queue takes.
A FIFO queue subscribes only to a FIFO topic.

The service needs `sns:CreateTopic` and `sns:Publish` to publish.
To subscribe queues itself it also needs `sns:Subscribe` on the topic and `sqs:GetQueueAttributes` and `sqs:SetQueueAttributes` on each queue.

## Testing the fan-out

The example file runs one test body twice.
In process, the in-process account delivers a topic's messages to the queues subscribed with `subscribe_queue_to_topic`, so the hook wires the test as it wires production.
A queue an operator subscribed outside the service is named on the policy with [`fans_out_to`](../components/sns/fans-out-to.md), a step of the `testing` feature.
With `SQS_TEST_ENDPOINT` set, the same body runs against LocalStack through `TestApp::start_live`:

```bash
cargo test --features sns
SQS_TEST_ENDPOINT=http://localhost:4566 cargo test --features sns -- --test-threads=1
```

[Testing an SQS service](testing.md) covers the harness and the live mode.
