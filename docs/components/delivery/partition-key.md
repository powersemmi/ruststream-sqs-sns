---
sidebar_position: 2
title: PARTITION_KEY_HEADER
description: "PARTITION_KEY_HEADER is the portable spelling of the FIFO message group: sent as MessageGroupId on FIFO, an ordinary attribute on a standard queue, read back on delivery either way."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# PARTITION_KEY_HEADER

[`PARTITION_KEY_HEADER`](api:ruststream_sqs_sns::PARTITION_KEY_HEADER) is the portable spelling of the FIFO message group: sent as `MessageGroupId` on a FIFO queue, as an ordinary attribute on a standard one, and read back on delivery either way.

The header's name is `partition-key`, the one the core's in-memory broker uses, so a service that sets it runs unchanged on another broker.
A publish to a `.fifo` queue or topic sends it as the message group, not as an attribute, and a delivery from a FIFO queue carries the group back in it.
A standard queue or topic has no message groups, so there the header travels as an ordinary `String` attribute and counts toward the ten a message carries; a delivery carries it back as the same header.

The group of a FIFO publish resolves from the most specific setting: the call's own [`group_id`](../publishing/group-id.md) step, then this header, then the group the publish policy fixed.
A FIFO publish that names none goes under the group `default`.
SQS hands out one group's messages in order, and holds the group back while one of its messages is in flight, as the [FIFO](../subscriptions/fifo.md) page describes.

## Example

The router forwards each order to the packing line in the customer's group, and the packer reads the group back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_delivery_partition_key.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_delivery_partition_key.rs#handler"
```

</TabItem>
</Tabs>
