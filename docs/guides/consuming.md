---
sidebar_position: 2
description: "Read SQS queues with long polling, keep a slow handler's message invisible, run handlers in parallel and take batches of up to ten."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Consuming queues

This guide reads SQS queues: long polling, the visibility timeout of a slow handler, parallel handlers, batches, and one handler on several queues.

The example is a warehouse service.
It packs orders, books payments and records changes to orders.

## A queue and its long poll

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#pack"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#pack"
```

</TabItem>
</Tabs>

A subscription long-polls its queue with `ReceiveMessage`.
Each call waits up to 20 seconds for a message, the most SQS allows.
[`wait`](api:ruststream_sqs_sns::SqsQueue::wait) sets a shorter wait.
A queue is named by its name or by its URL, and a name is resolved with `GetQueueUrl` once, at startup.

A single-message subscription still asks for up to ten messages per call, because SQS bills per request.
It hands them to the handler one at a time.
A background task receives the next batch while the handler works, and it never runs more than one batch ahead.

## Visibility and a slow handler

A received message stays invisible to other consumers for the queue's visibility timeout.
If it is not deleted by then, SQS hands it out again.
[`visibility`](api:ruststream_sqs_sns::SqsQueue::visibility) asks for a timeout of its own on each receive, between one second and twelve hours.
`pack` asks for a minute.
Without the step, the subscription reads the queue's own timeout once, when it opens.

A handler may run longer than the timeout.
While a delivery is alive, the crate extends its visibility every half period, so a slow handler does not see its message handed to another consumer.

## Parallel handlers

`workers(4)` runs up to four deliveries of `orders` at once.
The four handlers share the messages each receive returns.
A standard queue has no order to keep, so nothing else is needed.
[FIFO queues](fifo.md) keep one message group in order with `workers(n, by_key)`.

## Batches

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#reconcile"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#reconcile"
```

</TabItem>
</Tabs>

A handler that takes a slice is a batch handler.
The size its mount site names becomes `MaxNumberOfMessages`, so one receive is one batch.
A batch holds what the queue had at the time, and a long poll that returns nothing yields no batch.
`ReceiveMessage` returns at most ten messages, so a larger size is lowered to ten and the service logs it once.
Each message of a batch is settled on its own; [Acknowledgement](acknowledgement.md) shows the outcomes.

## One handler, several queues

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#audit"
```

</TabItem>
</Tabs>

`#[subscriber(SqsQueue)]` fixes the kind of source and leaves the queue's name open.
The mount site names it with `.name(..)`, once per queue.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#app"
```

</TabItem>
</Tabs>

The queue steps also sit at the mount site, after the framework's own steps, through [`SqsSubscription`](api:ruststream_sqs_sns::SqsSubscription).
An empty queue name, a wait over 20 seconds or a visibility outside its range stops the service at startup with [`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue), before any call to AWS.
A name may hold characters SQS does not take: each character outside `A-Z`, `a-z`, `0-9`, `_` and `-` becomes `-`, and a `.fifo` suffix is kept.

[`create_if_missing`](api:ruststream_sqs_sns::SqsQueue::create_if_missing) creates a queue that does not exist.
A production service usually takes its queues from its infrastructure and leaves the step out; a missing queue then stops it at startup.

## What a delivery carries

Each message attribute arrives as a header, and each header of a publish leaves as an attribute.
Two headers are the crate's own.
[`RECEIVE_COUNT_HEADER`](api:ruststream_sqs_sns::RECEIVE_COUNT_HEADER) carries the queue's receive count of the message.
[`PARTITION_KEY_HEADER`](api:ruststream_sqs_sns::PARTITION_KEY_HEADER) carries the message group of a FIFO queue.
An SQS body is text, so a binary payload travels base64-encoded with a marker attribute and arrives decoded.
Base64 makes such a payload a third larger, and SQS refuses a message over its size limit.

The tests in the example file publish orders, payments and changes to the app in process.
[Testing an SQS service](testing.md) covers the harness.
