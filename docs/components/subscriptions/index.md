---
title: Subscriptions
description: "A queue name in #[subscriber] long-polls that queue; SqsQueue carries every other setting of the subscription."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A queue name in `#[subscriber]` long-polls that queue; [`SqsQueue`](./sqs-queue.md) carries every other setting of the subscription.

`#[subscriber("orders")]` reads the queue `orders`.
The name resolves to the queue's URL through `GetQueueUrl` once, when the subscription opens.
A bare name takes the descriptor's defaults: a [20 second long poll](./wait.md) and the queue's own [visibility timeout](./visibility.md).
`#[subscriber(SqsQueue)]` fixes the kind and leaves the queue to the mount site: `b.include(audit.name("audit-eu"))` reads `audit-eu`, so one definition serves several queues.
SQS takes only `[A-Za-z0-9_-]` in a queue name, so every other character of the name becomes `-` on the way, and a `.fifo` suffix survives: `order.events` reads the queue `order-events`.
Two subscriptions on one queue compete for its messages, and each message goes to one of them.
A subscription receives at most one batch ahead of its handler.
A queue that does not exist stops the service at [startup](../broker/startup.md), unless the subscription [creates it](./create-if-missing.md).
Once the service runs, a queue deleted under a subscription ends that subscription, and any other [receive error](../broker/errors.md) is retried after one second.
The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber), and [`Unnamed<SqsQueue>`](api:ruststream::Unnamed) in place of the name leaves it to [`name`](api:ruststream::runtime::SubscriberSettings::name) at the mount site.

## Example

The queue `orders`, read with the defaults.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>

## One definition, two queues

The audit handler reads `audit-eu` and `audit-us`, each a subscription of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_name.rs#kind"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_name.rs#kind"
```

</TabItem>
</Tabs>
