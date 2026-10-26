---
title: Delivery
description: "A delivery carries its payload and its headers; each header is one SQS message attribute, String or Binary, at most ten."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Delivery

A delivery carries its payload and its headers; each header is one SQS message attribute, `String` or `Binary`, at most ten.

A header the service takes as text travels as a `String` attribute, and any other value as a `Binary` one.
A handler reads the same bytes either way, and never sees which type carried them.
No envelope wraps the message, so any other SQS producer or consumer reads it as it is.

SQS and SNS take at most ten attributes on one message.
A publish that needs more fails before it is sent, with [`SqsError::TooManyAttributes`](api:ruststream_sqs_sns::SqsError::TooManyAttributes): it names the destination, the count, the limit, the attributes, and the one the crate added, if it added one.
An attribute name is 1 to 256 letters, digits, `_`, `-` and `.`, and a value is never empty.
A message SQS refuses fails the publish with [`SqsError::Publish`](api:ruststream_sqs_sns::SqsError::Publish), and the [in-process account](../testing/index.md) refuses it the same way.
A message is at most 1 MiB, body and attributes together.

A handler reads the headers as a typed contract with [`Headers<T>`](api:ruststream::runtime::Headers), or one by one through `cx.headers()`.
Two headers have meanings of their own: [`RECEIVE_COUNT_HEADER`](./receive-count.md) carries the queue's receive count, and [`PARTITION_KEY_HEADER`](./partition-key.md) the FIFO message group.
A payload SQS would not take as text travels base64-encoded with one more attribute, as the [body](./body.md) page describes.
The crate adds no per-delivery context field, so a handler takes no `Ctx<K>` parameter here; what a delivery carries beyond its payload is in its headers.

## Example

An order carries its tenant and a trace id as two message attributes, and the handler reads them as one contract.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_delivery_headers.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_delivery_headers.rs#handler"
```

</TabItem>
</Tabs>
