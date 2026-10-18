---
sidebar_position: 3
title: SqsPublishOptions
description: "A handler sets one message's group or deduplication id through the publish steps, bounding its slot on SqsPublishOptions."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqsPublishOptions

A handler sets one message's group or deduplication id through the publish steps, bounding its slot on [`SqsPublishOptions`](api:ruststream_sqs_sns::SqsPublishOptions).

`SqsPublishOptions` holds the two settings one publish may differ from the next in: a message group and a deduplication id.
A call site writes them through the steps of [`SqsPublishSteps`](api:ruststream_sqs_sns::SqsPublishSteps), `.group_id(..)` and `.deduplication_id(..)` on the publish builder.
A step wins over the [group the policy fixed](./group-id.md), for that one message.
Both are FIFO settings: a `.fifo` destination takes them, and a standard one fails the publish.

A handler bounds its slot as `Out<impl Publisher<Options = SqsPublishOptions>, Marker>`, which puts the steps in reach.
The body imports `ruststream_sqs_sns::prelude::*`, the one handler file that globs this crate's [prelude](../broker/prelude.md) instead of the framework's.
The same steps are on every publish surface of this crate: an injected slot, the publisher a lifecycle hook is handed, a fan-out publisher.
A slot moved from `SqsPublish` to [`SnsPublish`](../sns/index.md) keeps the body that names them.
A reply has no call site, so its policy is its whole answer.
A test reads back what a publish asked for: `tb.out::<Marker>().with_options(&SqsPublishOptions::default().group_id(..))`.

## Example

Each customer's orders reach fulfilment in the order they were placed: the group of each message is the customer.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_fifo_group.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_fifo_group.rs#publish"
```

</TabItem>
</Tabs>
