---
title: Testing
description: "TestApp::start connects SqsBroker to an in-process account with queues, visibility, receive counts, redrive, FIFO groups and topics."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`SqsBroker`](../broker/index.md) to an in-process account with queues, visibility, receive counts, redrive, FIFO groups and topics.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<SqsBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.

The account has no settings of its own.
Its queue URLs are built on the broker's endpoint and region, and every setting a queue has comes from the service's descriptors.
Every queue and topic the service names exists in it.

The account keeps SQS's semantics.
A queue hands each message to one of its subscriptions, and one receive returns at most ten messages.
A received message stays invisible until it is deleted or its visibility timeout lapses; a descriptor that names no [visibility](../subscriptions/visibility.md) gets the 30 seconds of a new queue.
The [receive count](../delivery/receive-count.md) rises with every receive, and a [redrive policy](../settlement/dead-letters.md) moves a message whose receives ran out to the dead-letter queue.
A [FIFO](../subscriptions/fifo.md) group waits while one of its messages is in flight, and a [deduplication id](../publishing/deduplication-id.md) is remembered for five minutes.
With `sns`, a topic delivers a copy to every queue [subscribed](../sns/subscribe-queue-to-topic.md) to it.

The account refuses what SQS and SNS refuse: more than ten headers, a header name the service does not take, an empty body or header value, a message over the size limit, a queue name over 80 characters, a dead-letter queue of the other kind, a `maxReceiveCount` over 1000, a visibility that would end more than twelve hours after the receive, and a FIFO queue on a standard topic.
A publish through the harness that SQS would refuse fails the test.
A standard queue keeps the order of its messages here, which SQS does not promise.
A message sent to a queue no subscription reads is dropped.

A publish through the harness returns once every handler it woke has settled.
`tb.advance(..)`, on a clock paused with `#[tokio::test(start_paused = true)]`, moves past a visibility timeout or a [deferred retry](../settlement/retry-after.md).
What belongs to a running service, such as a queue an operator configured, is for a [live test](./live.md).

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_testing.rs#testapp"
```

</TabItem>
</Tabs>

A declined card is retried after a minute, and the queue's redrive policy moves the payment to the dead-letter queue after the third receive.
The test advances the paused clock past each delay.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_testing.rs#redrive"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_testing.rs#redrive"
```

</TabItem>
</Tabs>
