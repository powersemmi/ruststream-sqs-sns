---
sidebar_position: 4
title: create_if_missing
description: "create_if_missing creates the queue at startup when it is missing; a .fifo name creates a FIFO queue, and a dead-letter queue is created too."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# create_if_missing

[`create_if_missing()`](api:ruststream_sqs_sns::SqsQueue::create_if_missing) creates the queue at startup when it is missing; a `.fifo` name creates a FIFO queue, and a dead-letter queue is created too.

The subscription calls `CreateQueue` when it opens and the account does not have the queue yet.
Without the step, a missing queue stops the service at startup.
A name that ends in `.fifo` creates a [FIFO queue](./fifo.md) with content-based deduplication.
A [dead-letter queue](../settlement/dead-letters.md) the registration declares is created the same way.
Every other attribute of a new queue keeps the SQS default, a 30 second visibility timeout among them.
The step needs the `sqs:CreateQueue` [permission](../broker/credentials.md).
It suits a local stack and a test; production queues are usually managed as infrastructure.
In process every queue a service names exists already, so the step shows its effect in a [live test](../testing/live.md).
The step is on [`SqsQueue`](./sqs-queue.md), and on the mount chain through [`SqsSubscription::create_if_missing`](api:ruststream_sqs_sns::SqsSubscription::create_if_missing), as in the example.

## Example

A drafts queue the service opens itself on a fresh stack.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_create.rs#create"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_create.rs#create"
```

</TabItem>
</Tabs>
