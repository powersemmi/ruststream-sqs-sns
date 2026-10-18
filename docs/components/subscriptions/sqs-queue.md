---
sidebar_position: 1
title: SqsQueue
description: "SqsQueue describes one subscription by queue name or URL, with its long poll, visibility timeout and queue creation."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqsQueue

[`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) describes one subscription by queue name or URL, with its long poll, visibility timeout and queue creation.

[`SqsQueue::new`](api:ruststream_sqs_sns::SqsQueue::new) takes a queue name or a queue URL (`https://sqs...`).
A name resolves through `GetQueueUrl` once, when the subscription opens; a URL is used as it is.
Three steps set the rest, and each has a default: [`wait`](./wait.md) (20 seconds), [`visibility`](./visibility.md) (the queue's own timeout) and [`create_if_missing`](./create-if-missing.md) (off).
The descriptor goes where the name goes, as the first argument of `#[subscriber(..)]`.
The subscription's name is what the descriptor names: a test reads its deliveries under that name.
A descriptor that cannot form a subscription stops the service at startup with [`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue), before any call to AWS: an empty name, a wait over 20 seconds, or a visibility outside one second to twelve hours.
The same three steps are on the mount chain through [`SqsSubscription`](./sqs-subscription.md).
The manual form builds the descriptor and hands it to the chain with [`map_source`](api:ruststream::runtime::SubscriberSettings::map_source).

## Example

Orders read with a ten second long poll, each hidden from other consumers for a minute.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_queue.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_queue.rs#descriptor"
```

</TabItem>
</Tabs>
