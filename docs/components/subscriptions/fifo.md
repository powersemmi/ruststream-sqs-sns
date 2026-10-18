---
sidebar_position: 7
title: FIFO
description: "A .fifo queue hands out each message group in order and holds a group back while one of its messages is in flight."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FIFO

A `.fifo` queue hands out each message group in order and holds a group back while one of its messages is in flight.

A queue whose name ends in `.fifo` is a FIFO queue, and a subscription reads it like any other queue.
Every message on it belongs to a message group, which the producer names: the [`group_id`](../publishing/group-id.md) step, the [partition key](../delivery/partition-key.md) header, or `default` when it names neither.
The queue hands out the messages of a group in the order they were sent.
While a message of a group is in flight, the queue holds the rest of that group back until the message is deleted or its visibility lapses.
Other groups go on meanwhile.
So a [`retry`](../settlement/index.md) brings the same message back before the next one of its group.
The delivery carries its group in [`PARTITION_KEY_HEADER`](api:ruststream_sqs_sns::PARTITION_KEY_HEADER).
A FIFO queue drops a message whose [deduplication id](../publishing/deduplication-id.md) it has seen in the last five minutes.
A FIFO queue takes a FIFO dead-letter queue, and [`create_if_missing`](./create-if-missing.md) on a `.fifo` name creates a FIFO queue.
In process the account keeps the same order and holds a group back the same way.

## Example

The orders of one customer, fulfilled in the order they were placed.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_fifo.rs#fifo"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_fifo.rs#fifo"
```

</TabItem>
</Tabs>
