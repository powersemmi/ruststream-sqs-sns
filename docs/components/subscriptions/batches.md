---
sidebar_position: 6
title: Batches
description: "A slice handler gets one ReceiveMessage worth of messages; batch(n) becomes MaxNumberOfMessages, capped at ten."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler gets one `ReceiveMessage` worth of messages; `batch(n)` becomes `MaxNumberOfMessages`, capped at ten.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
`ReceiveMessage` returns several messages per call, so the size becomes its `MaxNumberOfMessages` and one receive is one batch.
Nothing buffers in the service.
A batch holds what the queue had at that moment, so a quiet queue yields batches of one.
A batch is never empty: a long poll that runs out hands the handler nothing.
SQS returns at most ten messages per receive, so a larger size becomes ten, and the subscription logs that once.
One [outcome](../settlement/index.md) settles every message of the batch.
A `Vec<HandlerOutcome>` settles message `i` with outcome `i`.
A single-message handler also receives up to ten messages per call, because SQS bills per request, and gets them one at a time; `workers(n)` decides how many run at once.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acknowledges the batch and `Err(outcomes)` settles each message.

## Example

Three orders waiting on the queue arrive in one call; twelve arrive as ten and two.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_batches.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_batches.rs#handler"
```

</TabItem>
</Tabs>
