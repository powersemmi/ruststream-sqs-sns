---
sidebar_position: 3
title: Body
description: "SQS bodies are text, so a payload SQS would refuse travels base64 with a marker attribute and is decoded again on receive."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Body

SQS bodies are text, so a payload SQS would refuse travels base64 with a marker attribute and is decoded again on receive.

SQS takes a narrower set of characters than UTF-8 in a body.
It refuses the control characters other than tab, line feed and carriage return, and the two non-characters at the end of the basic plane.
A payload that is such text, a JSON document for one, travels as it is.
Any other payload travels base64-encoded, with the attribute `ruststream-payload-encoding` set to `base64`: bytes that are not UTF-8, and equally valid UTF-8 with control characters, which a binary codec often emits.

The receiving side decodes the body again before the handler sees it.
A handler receives the bytes the producer published, and the marker attribute never reaches its headers.
The marker counts towards the ten attributes of a message, so a binary payload leaves nine for the service's own headers.
SQS refuses an empty body, and so does the [in-process account](../testing/index.md).

A handler that reads the bytes itself takes a `#[derive(Deserialized)]` newtype over `&[u8]`, and no codec runs.
The type borrows the delivery's buffer, so nothing is copied.

## Example

The handler stores thumbnails, raw image bytes that SQS would not take as text.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_delivery_body.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_delivery_body.rs#handler"
```

</TabItem>
</Tabs>
