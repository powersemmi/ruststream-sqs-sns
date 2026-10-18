---
sidebar_position: 2
title: Dead letters
description: "max_attempts and dead_letter together become the queue's redrive policy, and SQS moves a spent message to the dead-letter queue itself."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) and [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) together become the queue's redrive policy, and SQS moves a spent message to the dead-letter queue itself.

The cap is the policy's `maxReceiveCount`, at most 1000, and the destination is the queue it points at.
The subscription writes the policy onto its queue with `SetQueueAttributes` when it opens.
SQS then counts the receives and carries the message away on the receive past the cap, so nothing leaves the service.
A bare queue name takes the declaration the same way as an [`SqsQueue`](../subscriptions/sqs-queue.md).
Both halves are needed: a cap without a destination, or a destination without a cap, stops the service at startup with [`SqsError::IncompleteRedrive`](api:ruststream_sqs_sns::SqsError::IncompleteRedrive), which names the missing half.
The dead-letter queue has to exist at startup, unless the subscription [creates its queues](../subscriptions/create-if-missing.md).
A FIFO queue takes a FIFO dead-letter queue, and a standard queue a standard one.
A `drop` normally deletes the message, but on the delivery that has spent the policy it returns the message instead, because the next receive is what moves it to the dead-letter queue.
The dead-letter queue is an ordinary queue, read by an ordinary subscription.
The policy needs `sqs:SetQueueAttributes` on the queue and `sqs:GetQueueAttributes` on the dead-letter queue.

## Example

An invoice that is never ready is received three times and then moves to `invoices-dead`, where a person takes it over.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_dead_letter.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_dead_letter.rs#handler"
```

</TabItem>
</Tabs>

## The declaration

The cap and the dead-letter queue, written at the mount site.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_dead_letter.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_dead_letter.rs#mount"
```

</TabItem>
</Tabs>
