---
sidebar_position: 1
title: RECEIVE_COUNT_HEADER
description: "RECEIVE_COUNT_HEADER carries the queue's ApproximateReceiveCount, one on the first delivery, so a handler can tell the last attempt."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RECEIVE_COUNT_HEADER

[`RECEIVE_COUNT_HEADER`](api:ruststream_sqs_sns::RECEIVE_COUNT_HEADER) carries the queue's `ApproximateReceiveCount`, one on the first delivery, so a handler can tell the last attempt.

The header's name is `sqs-receive-count`, and its value is the count as decimal text.
SQS counts every receive of a message, the one in hand included.
A [retry](../settlement/index.md) or a [deferred retry](../settlement/retry-after.md) returns the message to the same queue, so the count goes on rising.

A [redrive policy](../settlement/dead-letters.md) caps the same count.
After `max_attempts` receives, SQS moves the message to the dead-letter queue on the receive that follows.
A handler that compares the header with the cap knows when it holds the last attempt.
SQS calls the count approximate: on a standard queue a message is delivered at least once, and a duplicate receive counts too.

## Example

A declined payment is tried three times.
On the third receive the handler sends it to review and acknowledges it, so it never reaches the dead-letter queue.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_delivery_receive_count.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_delivery_receive_count.rs#handler"
```

</TabItem>
</Tabs>
