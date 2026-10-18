---
sidebar_position: 1
title: group_id
description: "group_id on the policy orders everything a position publishes within one group; a call's own step and the partition key win over it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# group_id

[`group_id`](api:ruststream_sqs_sns::SqsPublish::group_id) on the policy orders everything a position publishes within one group; a call's own step and the partition key win over it.

A [FIFO queue](../subscriptions/fifo.md) delivers the messages of one group in the order they were sent, one group member at a time.
Messages of different groups do not wait for each other.
A group belongs to a position: a slot, a reply or a lifecycle hook.
Every send from that position to a `.fifo` destination falls back to the group its policy fixed.

Three answers can be in play at once, and the most specific wins.
The call's own [`group_id`](./options.md) step comes first.
The message's [`PARTITION_KEY_HEADER`](../delivery/partition-key.md) comes next, the spelling that travels unchanged to every other broker.
The policy's group comes last, and a send that names none at all goes under `"default"`.
A FIFO delivery carries its group in the partition-key header, which is where a handler and a test read it.

A group on a position that publishes to a standard queue fails the publish with [`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo).
A partition key alone is a portable hint, so a standard queue ignores it.
[`SnsPublish::group_id`](api:ruststream_sqs_sns::SnsPublish::group_id) does the same for a FIFO topic.

## Example

At startup the service replays the orders a previous run did not hand on, all in the group `replay`, so fulfilment receives them in the order they were taken.
The `place` handler names each customer's group for its own messages, which wins over its slot's policy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_fifo_group.rs#policy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_fifo_group.rs#policy"
```

</TabItem>
</Tabs>
