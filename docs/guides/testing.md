---
sidebar_position: 7
description: "Test an SQS service in process with TestApp: the production app, replies, delays on a paused clock, and the same test against LocalStack."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing an SQS service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process SQS](../components/testing/index.md) or against a running stack.

The service under test is a checkout.
An order arrives on `checkout-orders`, and `confirm` answers it with a confirmation on `checkout-confirmations`, which `notify` reads.
An order without an amount is dropped.
A back-ordered order waits an hour with `retry_after` and comes back to be checked again.
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the production one.
The tests need the `testing` feature of `ruststream-sqs-sns` in `[dev-dependencies]`.

## An order and its confirmation

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`SqsBroker`](api:ruststream_sqs_sns::SqsBroker) to an in-process SQS.
No endpoint is called and no credentials are needed.
The test addresses the broker by its production type, `tb.broker::<SqsBroker>()`.
`publish` returns once the handlers it reached have settled, the reply and its reader included.
So the next lines assert on finished state: how `confirm` settled the order and what `notify` received.
The body takes the started app as an argument, so the live test below runs it too.

## An empty order

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no confirmation was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#delay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#delay"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
An hour of `RESTOCK` costs nothing.
`retry_after` makes the message invisible for the delay, as SQS does, and the queue hands it out again when the clock passes it.
`published` shows one order on `checkout-orders`: the second delivery is the same message, not a copy.

## What the in-process SQS models

The in-process SQS is an account inside the test, and every queue and topic the service names exists in it.
It models what a service's logic depends on:

- A queue hands each message to one of its subscriptions, and a received message stays invisible until it is deleted or its visibility timeout ends.
- The receive count grows with every receive, and a redrive policy moves a message whose receives ran out to the dead-letter queue.
- A batch holds at most the ten messages one receive returns.
- A FIFO group waits while one of its messages is in flight, and a deduplication id is remembered for five minutes.
- With `sns`, a topic delivers a copy to every queue subscribed with `subscribe_queue_to_topic`.

What SQS or SNS refuses is refused there too: more than ten headers, a header name SQS does not take, an empty body or header value, a message over the size limit, a queue name over 80 characters, a dead-letter queue of the other kind, a `maxReceiveCount` over 1000, a visibility that would end more than twelve hours after the receive, and a FIFO queue on a standard topic.

A standard queue keeps the order of its messages there, which SQS does not promise.
A descriptor that names no visibility gets 30 seconds, the default of a new queue.
A message sent to a queue no subscription reads is dropped.
The visibility an operator set on a queue, the queues and topics the service expects to find, and credentials are exercised against a running stack.

## The same test on a running stack

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to a running SQS.
The test reads the endpoint from `SQS_TEST_ENDPOINT` with `live::endpoint`, a helper of the example's own, and skips when it is unset.
It deletes the service's queues first, so nothing an earlier run left on them is counted; the service creates them again on start.
The assertions are the in-process ones, unchanged.

```bash
cargo test
SQS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
