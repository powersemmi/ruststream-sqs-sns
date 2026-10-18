---
title: SQS bindings
description: "With asyncapi the generated document speaks SQS: the server, the queue behind each channel, and the topic of a fan-out position."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SQS bindings

With the `asyncapi` feature the generated document speaks SQS: the server, the queue behind each channel, and the topic of a fan-out position.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds the specification's `sqs` binding, version `0.3.0`, and with the `sns` feature its `sns` binding, version `1.0.0`.
A [server](./server.md) reports where the service connects.
A [channel](./channels.md) reports the queue behind it, whether the service reads it or publishes to it.
A [fan-out](../sns/index.md) position's channel reports its [topic](./topics.md).

Everything comes from the service's own declarations: the broker, the descriptors and the destinations the mount sites resolved.
A setting the queue holds but the service did not name stays out, because reading it takes a connection.
The document is published and shared, so no binding carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-sqs-sns = { version = "0.7", features = ["asyncapi", "sns"] }
```

The service registers its broker under the label `sqs`, the key of its server in the document.
`cargo run --example sqs_asyncapi --features asyncapi,sns -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_asyncapi.rs#app"
```

</TabItem>
</Tabs>
