---
title: SqsBroker
description: "SqsBroker connects a service to Amazon SQS; new only records the configuration, and the runtime builds the SDK clients at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SqsBroker

[`SqsBroker`](api:ruststream_sqs_sns::SqsBroker) connects a service to Amazon SQS; `new` only records the configuration, and the runtime builds the SDK clients at startup.

[`SqsBroker::new`](api:ruststream_sqs_sns::SqsBroker::new) takes no arguments and does no I/O, so the whole service fits the synchronous `#[app]` builder.
Left as it is, the broker takes the region, the credentials and the endpoint from the AWS SDK's default chain: the environment, the shared profile files, and the instance or container role.
The [credentials](./credentials.md) page shows a configuration the service builds itself, and the [endpoint](./endpoint.md) page a local stack.

At startup the runtime connects the broker and receives a [`ConnectedSqsBroker`](api:ruststream_sqs_sns::ConnectedSqsBroker).
The connect loads the AWS configuration and builds the SQS client, and the SNS client with the `sns` feature.
Every subscription and every publisher of the service opens against that connected form, and [startup](./startup.md) resolves each subscription's queue before the service reads anything.
At [shutdown](./shutdown.md) the runtime marks the connection closed.

A request goes out through the SDK clients of the runtime that sends it.
A handler on a dedicated thread, set with `threads(n)`, gets clients of its own on its first request, built from the same configuration and sharing its credentials.
Its connections stay in its own pool, so the broker's requests never wait on a thread that is busy computing.

`#[subscriber("orders")]` reads the queue `orders` with the default settings.
[`SqsQueue`](../subscriptions/sqs-queue.md) carries the settings of one subscription, such as the long-polling wait and the visibility timeout.

## Example

The handler reads the queue `orders`, each receive waiting up to 20 seconds for a message.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_service.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_service.rs#handler"
```

</TabItem>
</Tabs>

The app mounts it on a broker that reads its configuration from the environment.
With `AWS_ENDPOINT_URL=http://localhost:4566`, `AWS_REGION` and test keys set, `cargo run --example sqs_service --features testing -- run` starts it against a local stack.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_service.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_service.rs#app"
```

</TabItem>
</Tabs>
