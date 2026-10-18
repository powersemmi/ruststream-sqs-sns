---
sidebar_position: 3
description: "What each handler outcome does to an SQS message, how a service stays correct when a message arrives twice, and how a batch settles."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement and delivery guarantees

This guide shows what each handler outcome does to an SQS message, how a handler stays correct when a message arrives twice, and how a batch settles.

SQS delivers at least once.
A received message stays on the queue, invisible, until the consumer deletes it.
A standard queue may also deliver a message twice, and in another order than it was sent ([AWS: at-least-once delivery](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html)).

The example is a payments service that books payments into a ledger and pays refunds.

## What each outcome does

Every outcome is one SQS call on the message the handler received:

| Outcome | SQS call | What happens to the message |
|---|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | `DeleteMessage` | it leaves the queue |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | `DeleteMessage` | it leaves the queue: SQS has no call that rejects a message and keeps it |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry) | `ChangeMessageVisibility` to zero | it is handed out again at once |
| [`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) | `ChangeMessageVisibility` to the delay | it is handed out again when the delay ends |

A message the handler never settles, because the process stopped in the middle, gets no call.
It comes back when its visibility timeout ends.
So the work of a handler happens before its outcome, and a message is deleted only after the work is done.

## A handler that books once

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#book"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#book"
```

</TabItem>
</Tabs>

`book` writes a ledger entry for a payment and then acknowledges it.
A payment SQS delivers a second time is found among the booked ids and acknowledged without a second entry.
The set in the app state stands in for the store a service keeps its booked ids in; the id is the payment's own, not the SQS message id, because a producer that sends twice sends two messages.
When the entry cannot be published, nothing is booked, and `retry` hands the payment back for another try.

The handler reads [`RECEIVE_COUNT_HEADER`](api:ruststream_sqs_sns::RECEIVE_COUNT_HEADER), the queue's own `ApproximateReceiveCount`.
It counts this receive too, so the first delivery reads 1.
A payment on an account under review waits a minute with `retry_after`, and on its third receive the handler gives up and drops it.
[Retries and dead-letter queues](retries.md) shows how the queue itself caps the receives and keeps what failed.

## A batch settles message by message

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#refunds"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#refunds"
```

</TabItem>
</Tabs>

A batch handler returns one outcome per message, in the order of the slice.
Each outcome is a call of its own on its message.
Here the small refunds are deleted, and a large one stays on the queue and comes back alone after five minutes.
A batch handler that returns a single outcome settles every message of the batch with it.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

`book` publishes its entries through the `Entries` slot, bound to the queue publisher at the mount site.
A FIFO queue drops a second send with the same deduplication id for five minutes, which keeps a producer from sending one message twice; [FIFO queues](fifo.md) shows it.
A handler on a standard queue still keeps its own record of what it has done.

The tests in the example file deliver a payment twice, let a payment under review run out its receives on a paused clock, and send three refunds in one batch.
[Testing an SQS service](testing.md) covers the harness.
