---
sidebar_position: 3
title: Startup
description: "Startup opens every subscription: it resolves the queue URL, reads the queue's visibility timeout and writes a declared redrive policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

Startup opens every subscription: it resolves the queue URL, reads the queue's visibility timeout and writes a declared redrive policy.

Each subscription first checks its descriptor, before any call to AWS.
An empty queue name, a [wait](../subscriptions/wait.md) above 20 seconds or a [visibility](../subscriptions/visibility.md) outside one second to twelve hours fails startup with [`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue).

The subscription then resolves the queue name to its URL with `GetQueueUrl`, once, and the broker caches the answer.
A queue URL in the descriptor is used as it is.
A queue that does not exist fails startup with [`SqsError::Queue`](api:ruststream_sqs_sns::SqsError::Queue), naming the queue, unless the subscription [creates it](../subscriptions/create-if-missing.md).
A registration with a [redrive policy](../settlement/dead-letters.md) writes the policy onto the queue next.
A subscription that names no visibility reads the queue's own timeout with `GetQueueAttributes`, and holds every delivery under it.

Every request runs under the per-attempt timeout of 25 seconds that `new` sets on connect.
A broker built [from a config](./credentials.md) runs under the timeouts that config carries.
The first request also fetches the credentials, so a missing credential or an unreachable endpoint fails startup too.

The in-process account a [`TestApp`](../testing/index.md) test runs on has every queue the service names, so a missing queue shows only against a running stack.
A descriptor out of range fails `TestApp::start` as it fails a real startup.

## Example

The subscription reads a queue the infrastructure created, and takes its visibility timeout from the queue.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_broker_startup.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_broker_startup.rs#service"
```

</TabItem>
</Tabs>
