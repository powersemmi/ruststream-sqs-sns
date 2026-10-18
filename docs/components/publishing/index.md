---
title: SqsPublish
description: "SqsPublish, Publish in the prelude, sends each message to the queue it names; it is the broker's default, so a reply needs no policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqsPublish

[`SqsPublish`](api:ruststream_sqs_sns::SqsPublish), `Publish` in the prelude, sends each message to the queue it names; it is the broker's default, so a reply needs no policy.

A mount site names it wherever a registration publishes: `.out(Slot, Publish::default())` for a slot, `.out_reply(Publish::default())` for a reply.
A lifecycle hook takes it too, `b.after_startup(Publish::default(), ..)`, and is handed the live publisher.
The policy holds no connection; the runtime pairs it with the connected broker at startup into an [`SqsPublisher`](api:ruststream_sqs_sns::SqsPublisher).
A registration that names no policy for its reply publishes through this one, so `.out_reply(Publish::default())` only restates the default.

The destination is a queue name or a queue URL.
A name resolves to its URL through `GetQueueUrl`, and the broker caches the answer.
A publish never creates a queue: a name the account lacks fails the publish.
A publish returns once SQS has accepted the message, or with the error SQS reported.

The payload is the message body, and the headers travel as [message attributes](../delivery/index.md).
SQS takes a message of up to 1 MiB, body and attributes together.
On a `.fifo` queue every send carries a message [group](./group-id.md) and a [deduplication id](./deduplication-id.md).
A handler sets either for one message through [`SqsPublishOptions`](./options.md).
[`SnsPublish`](../sns/index.md) sends the same messages to an SNS topic instead.

## Example

The confirmation names its queue, and the mount site binds no policy for the reply.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_publishing_publish.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_publishing_publish.rs#publish"
```

</TabItem>
</Tabs>
