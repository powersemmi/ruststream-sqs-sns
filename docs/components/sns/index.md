---
title: SnsPublish
description: "SnsPublish fans a reply out from an SNS topic to every queue subscribed to it; one out_reply step switches a position to it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SnsPublish

[`SnsPublish`](api:ruststream_sqs_sns::SnsPublish) fans a reply out from an SNS topic to every queue subscribed to it; one `out_reply` step switches a position to it.

The policy needs the crate's `sns` feature.
A mount site names it where a registration publishes: `.out_reply(SnsPublish::default())` for a reply, `.out(Slot, SnsPublish::default())` for a slot.
The destination reads the same as on [`SqsPublish`](../publishing/index.md): the reply type or the mount site names it, and the policy decides whether the name is a queue or a topic.
Without the step the same reply lands on a queue of that name.
The runtime pairs the policy with the connected broker into an [`SnsPublisher`](api:ruststream_sqs_sns::SnsPublisher).

The destination is a topic name or a topic ARN.
A name resolves to its ARN through `CreateTopic`, which returns an existing topic as it is and creates a missing one.
A name ending in `.fifo` is a FIFO topic, and it takes the same [group](../publishing/group-id.md) and [deduplication](../publishing/deduplication-id.md) steps as a FIFO queue.
A topic and a queue may share a name: each publish goes where its policy sends it.
The payload is the notification's message, and the headers travel as message attributes.
SNS takes a message of up to 256 KiB.

SNS is a publish target only: the service reads the queues subscribed to the topic.
A queue joins a topic through [`subscribe_queue_to_topic`](./subscribe-queue-to-topic.md), or through the operator's own provisioning.

## Example

The dependency line:

```toml
ruststream-sqs-sns = { version = "0.7", features = ["sns"] }
```

The notification names its topic, so the handler answers with a bare `reply` clause.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sns_fanout.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sns_fanout.rs#reply"
```

</TabItem>
</Tabs>

The mount site binds the reply position to `SnsPublish`, and `billing` and `shipping` each receive the order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sns_fanout.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sns_fanout.rs#app"
```

</TabItem>
</Tabs>
