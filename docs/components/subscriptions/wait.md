---
sidebar_position: 2
title: wait
description: "wait sets how long one receive waits for a message, 20 seconds by default and at most; a longer wait stops the service at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# wait

[`wait(duration)`](api:ruststream_sqs_sns::SqsQueue::wait) sets how long one receive waits for a message, 20 seconds by default and at most; a longer wait stops the service at startup.

The wait is the `WaitTimeSeconds` of each `ReceiveMessage`: the long poll.
A receive returns as soon as a message arrives, and returns empty when the wait runs out.
An empty receive hands the handler nothing.
A longer wait means fewer empty receives, and SQS bills per request.
The default is the SQS maximum of 20 seconds, so a subscription that names no wait already polls as long as SQS allows.
A wait over 20 seconds stops the service at startup with [`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue).
A zero wait is short polling: each receive returns at once, with what a sample of the queue's servers holds.
The step is on [`SqsQueue`](./sqs-queue.md), and on the mount chain through [`SqsSubscription::wait`](api:ruststream_sqs_sns::SqsSubscription::wait).

## Example

Each receive on `alerts` waits up to five seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_wait.rs#wait"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_wait.rs#wait"
```

</TabItem>
</Tabs>
