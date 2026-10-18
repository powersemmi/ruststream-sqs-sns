---
sidebar_position: 5
title: Prelude
description: "One glob, ruststream_sqs_sns::prelude, brings the core prelude, the broker, the queue descriptor, the policies and the publish steps."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_sqs_sns::prelude`](api:ruststream_sqs_sns::prelude), brings the core prelude, the broker, the queue descriptor, the policies and the publish steps.

`use ruststream_sqs_sns::prelude::*;` is the one import a routes file writes.
It carries the core's own prelude, so `#[subscriber]`, `#[app]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`SqsBroker`](./index.md), the descriptor [`SqsQueue`](../subscriptions/sqs-queue.md) with its mount-site steps [`SqsSubscription`](../subscriptions/sqs-subscription.md), the publish steps `SqsPublishSteps` with [`SqsPublishOptions`](../publishing/options.md), and the publishers `SqsPublisher` and, with `sns`, `SnsPublisher`.

The queue policy comes under the name every broker crate gives it: `Publish` is [`SqsPublish`](../publishing/index.md), so a mount site reads the same on every broker.
[`SnsPublish`](../sns/index.md), with the `sns` feature, keeps its own name.
Two broker preludes in one file collide on `Publish`; the prefixed types at the crate root tell them apart.
The constants [`RECEIVE_COUNT_HEADER`](../delivery/receive-count.md) and [`PARTITION_KEY_HEADER`](../delivery/partition-key.md) and the error type [`SqsError`](./errors.md) are imported from the crate root.

A handler file globs the core prelude and names no broker type: an injected publisher is bounded `Out<impl Publisher, Marker>`.
The one exception is a body that sets a per-message setting: [`group_id`](../publishing/group-id.md) or `deduplication_id` needs `SqsPublishSteps` in scope and the slot bounded `Out<impl Publisher<Options = SqsPublishOptions>, Marker>`, both from this prelude.

## Example

The descriptor, the broker and the reply policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
