---
sidebar_position: 3
title: Topics
description: "A fan-out position reports its topic through an sns binding, with a FIFO ordering object on a .fifo topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Topics

A fan-out position reports its topic through an `sns` binding, with a FIFO ordering object on a `.fifo` topic.

A channel the service publishes to through [`SnsPublish`](../sns/index.md) carries the `sns` binding instead of the `sqs` one.
`name` is the topic the mount site resolved, from a reply clause, a reply type's `#[outgoing(name)]` or a slot entry.
A topic whose name ends in `.fifo` adds `ordering`, with `type` set to `FIFO`.
Its `contentBasedDeduplication` is `false`: every FIFO send this crate makes carries a [deduplication id](../publishing/deduplication-id.md) of its own, which wins over one derived from the body.
A standard topic carries no `ordering` object, which the specification reads as a standard topic.

## Example

The reply of `settle` fans out from the FIFO topic `payment-events.fifo`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_asyncapi.rs#topics"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_asyncapi.rs#topics"
```

</TabItem>
</Tabs>

The binding of `payment-events.fifo` in the document:

```json
{
  "sns": {
    "bindingVersion": "1.0.0",
    "name": "payment-events.fifo",
    "ordering": {
      "type": "FIFO",
      "contentBasedDeduplication": false
    }
  }
}
```
