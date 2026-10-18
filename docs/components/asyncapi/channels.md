---
sidebar_position: 2
title: Channels
description: "Each channel carries an sqs binding: the queue's name and FIFO flag, and the polling settings a subscription names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

Each channel carries an `sqs` binding: the queue's name and FIFO flag, and the polling settings a subscription names.

The binding holds one `queue` object.
`name` is the queue's name, and `fifoQueue` is `true` for a name ending in `.fifo`, the suffix that makes a queue FIFO.
A subscription's channel adds `receiveMessageWaitTime`, the long-poll [wait](../subscriptions/wait.md) in seconds, 20 unless the descriptor names another.
It adds `visibilityTimeout` when the descriptor names a [visibility](../subscriptions/visibility.md); the queue's own timeout takes a connection to read, so the document leaves it out.

A channel the service publishes to through [`SqsPublish`](../publishing/index.md) reports the queue the mount site resolved.
That is a `reply("dest")` clause, a reply type's `#[outgoing(name)]`, or the name of a slot entry.
Its binding has `name` and `fifoQueue` only: the polling belongs to the subscription that reads the queue.

## Example

The channel `orders` reports its queue and both polling settings, and `shipments.fifo` a FIFO queue polled with the default wait.
The channel `confirmations` reports the queue the reply clause names.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_asyncapi.rs#channels"
```

</TabItem>
</Tabs>

The binding of `orders` in the document:

```json
{
  "sqs": {
    "bindingVersion": "0.3.0",
    "queue": {
      "name": "orders",
      "fifoQueue": false,
      "visibilityTimeout": 30,
      "receiveMessageWaitTime": 10
    }
  }
}
```
