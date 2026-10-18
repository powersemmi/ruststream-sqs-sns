---
sidebar_position: 5
description: "Send to an SQS FIFO queue in message groups, drop duplicate sends with a deduplication id, and handle several groups at once in order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# FIFO queues

This guide sends to an SQS FIFO queue in message groups, drops duplicate sends with a deduplication id, and handles several groups at once, each in order.

A FIFO queue keeps the order of the messages within a message group and hands out a group's next message only after the one before it is deleted.
Each send also carries a deduplication id, and the queue drops a second send with the same id for five minutes ([AWS: deduplication](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html)).
A queue whose name ends in `.fifo` is a FIFO queue.

The example is a shipping service.
It turns each order into a shipment, and the shipments of one customer go out in the order the customer placed them.

## Sending in a group

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#dispatch"
```

</TabItem>
</Tabs>

`dispatch` publishes a shipment to `shipments.fifo` through the `Shipments` slot.
The slot is bounded on [`SqsPublishOptions`](api:ruststream_sqs_sns::SqsPublishOptions), so the publish takes the steps of [`SqsPublishSteps`](api:ruststream_sqs_sns::SqsPublishSteps).
A handler that names these steps imports this crate's prelude.

[`group_id`](api:ruststream_sqs_sns::SqsPublishSteps::group_id) puts the shipment in the customer's group.
When a publish names no group, the group comes from the message's [`PARTITION_KEY_HEADER`](api:ruststream_sqs_sns::PARTITION_KEY_HEADER), then from the group the policy fixed with `Publish::default().group_id(..)`, and last it is `"default"`.
The header is the spelling every RustStream broker reads, so a producer that sets it needs no SQS step.

[`deduplication_id`](api:ruststream_sqs_sns::SqsPublishSteps::deduplication_id) is the order's id.
When `orders` delivers an order twice, the second shipment is a send the queue drops.
A publish that names no deduplication id gets an id unique to the process, so two equal payloads sent on purpose both arrive.
A policy carries no deduplication id: one fixed id would drop every send after the first.

A group or a deduplication id on a standard queue is refused with [`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo).

## Several groups at once

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#ship"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#ship"
```

</TabItem>
</Tabs>

A delivery from a FIFO queue carries its group in `PARTITION_KEY_HEADER`.
`workers(4, by_key)` runs four lanes and sends every message of one group to the same lane.
So four customers ship at once, and the shipments of one customer go out one after another.
One receive can return several messages of a group, and plain `workers(4)` would run them side by side.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/fifo.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/fifo.rs#app"
```

</TabItem>
</Tabs>

[`create_if_missing`](api:ruststream_sqs_sns::SqsQueue::create_if_missing) creates `shipments.fifo` as a FIFO queue with content-based deduplication.
A FIFO queue takes a FIFO dead-letter queue; [Retries and dead-letter queues](retries.md) covers the cap.
SQS limits the throughput of one group, so a service spreads its load over many groups.

The tests in the example file check the group a shipment travels in, send one order twice, and send the orders of two customers at once.
[Testing an SQS service](testing.md) covers the harness.
