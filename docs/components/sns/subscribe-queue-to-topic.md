---
sidebar_position: 1
title: subscribe_queue_to_topic
description: "subscribe_queue_to_topic subscribes a queue to a topic with raw message delivery, so payloads and headers arrive as plain SQS messages."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# subscribe_queue_to_topic

[`subscribe_queue_to_topic`](api:ruststream_sqs_sns::ConnectedSqsBroker::subscribe_queue_to_topic) subscribes a queue to a topic with raw message delivery, so payloads and headers arrive as plain SQS messages.

The call takes a topic, by name or ARN, and a queue, by name or URL.
A topic name resolves through `CreateTopic`, so a missing topic comes into being; the queue has to exist.
With raw message delivery the notification's message is the queue message's body, and its attributes are the queue message's attributes.
A handler on the queue therefore decodes the same payload and reads the same headers a publish to the queue would give it.

The method belongs to the connected broker, [`ConnectedSqsBroker`](api:ruststream_sqs_sns::ConnectedSqsBroker).
A service reaches it from an `after_startup` hook: it clones the app's broker before the app takes it, and the hook connects the clone.
A clone connects through the connection the app's broker already holds, so in a test the call reaches the in-process account.
By the time the hook runs, the subscriptions have opened their queues, [`create_if_missing`](../subscriptions/create-if-missing.md) ones included.
SNS refuses a FIFO queue on a standard topic, and the call fails with [`SqsError::Admin`](api:ruststream_sqs_sns::SqsError::Admin).

In production the topic and its subscriptions are often infrastructure that an operator provisions.
A test of such a service names those queues on the policy with [`fans_out_to`](./fans-out-to.md).

## Example

The hook subscribes both queues to the topic the reply fans out from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sns_fanout.rs#wiring"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sns_fanout.rs#wiring"
```

</TabItem>
</Tabs>
