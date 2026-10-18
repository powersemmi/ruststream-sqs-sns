---
sidebar_position: 4
description: "Retry an SQS message now or later, cap its deliveries and let SQS move what keeps failing to a dead-letter queue."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead-letter queues

This guide retries an SQS message now or later, caps its deliveries and lets SQS move what keeps failing to a dead-letter queue.

SQS does both jobs itself.
A retry is the same message made visible again, with its receive count intact.
The cap and the dead-letter queue are the queue's redrive policy, and the mount site declares it.

The example is an invoices service that charges invoices to customers.

## Retry now and retry later

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#settle"
```

</TabItem>
</Tabs>

An invoice over the credit limit waits for a credit check.
[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) sets the message's visibility timeout to 30 seconds, and SQS hands it out again when they end.
SQS counts whole seconds, so a fraction of a second rounds up.
A delay is at most twelve hours, the longest visibility SQS allows; a longer one is cut to twelve hours.
The message stays on its queue the whole time, so a deferred retry survives a restart of the service.

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) sets the visibility to zero, and the message is handed out again at once.
An invoice without a customer gets the same answer on every try, so `charge` drops it.

## The cap and the dead-letter queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) and [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) together are the queue's redrive policy.
The cap becomes its `maxReceiveCount`, and the dead-letter queue becomes its target.
The subscription writes the policy onto the queue when it opens.
From then on SQS counts the receives and moves a message that ran out of them; the service publishes nothing.
Here an invoice is delivered four times, and on the fifth receive SQS moves it to `invoices-dead`.

The two steps go together.
A cap without a dead-letter queue, or a dead-letter queue without a cap, stops the service at startup with [`SqsError::IncompleteRedrive`](api:ruststream_sqs_sns::SqsError::IncompleteRedrive), which names the missing half.
SQS takes a `maxReceiveCount` of at most 1000.

The dead-letter queue must exist when the subscription opens, unless a descriptor of the app creates it with `create_if_missing`.
It must be of the same kind as its queue: a FIFO queue takes a FIFO dead-letter queue.
Writing the policy needs `sqs:SetQueueAttributes` on the queue and `sqs:GetQueueAttributes` on the dead-letter queue.

`drop` deletes a message.
On a capped queue, a drop of the delivery that used up the last receive returns the message instead, so SQS moves it to the dead-letter queue rather than losing it.

## Reading the dead-letter queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

A dead-letter queue is an ordinary queue, and `review` reads it like any other.
The message arrives as it was sent, with its attributes.
A queue with no reader keeps its messages until its retention period ends, so a person can also look at them in the console.

## Testing the delay

The tests in the example file run the app in process with a paused clock, so `tb.advance(CREDIT_CHECK)` lets the 30 seconds pass at once.
One test runs an invoice through its four deliveries into `invoices-dead`, another checks that a cap without a dead-letter queue does not start.
[Testing an SQS service](testing.md) covers the harness.
