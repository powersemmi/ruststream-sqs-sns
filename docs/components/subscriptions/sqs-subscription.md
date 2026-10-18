---
sidebar_position: 5
title: SqsSubscription
description: "SqsSubscription puts the queue steps on the mount chain, after the framework's own steps such as batch(n) and name."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqsSubscription

[`SqsSubscription`](api:ruststream_sqs_sns::SqsSubscription) puts the queue steps on the mount chain, after the framework's own steps such as `batch(n)` and `name`.

The steps are those of [`SqsQueue`](./sqs-queue.md): [`wait`](./wait.md), [`visibility`](./visibility.md) and [`create_if_missing`](./create-if-missing.md).
They chain after a framework step: a [batch size](./batches.md), a [name](./index.md) for a definition that fixed only the kind, or `workers(n)`.
A step at the mount site changes the descriptor the attribute built, so it wins over the attribute's value.
The steps exist only on the chain of a subscription whose source is an `SqsQueue`, so they never reach another broker's subscription.
The trait comes with the crate's [prelude](../broker/prelude.md).
In the manual form the steps follow [`map_source`](api:ruststream::runtime::SubscriberSettings::map_source) or `name`.

## Example

Ten orders per receive, a 20 second long poll, and a queue the service creates.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_batches.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_batches.rs#mount"
```

</TabItem>
</Tabs>
