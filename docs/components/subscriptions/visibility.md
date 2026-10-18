---
sidebar_position: 3
title: visibility
description: "visibility sets how long a received message stays hidden; the crate re-arms it halfway through each timeout while the handler runs."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# visibility

[`visibility(duration)`](api:ruststream_sqs_sns::SqsQueue::visibility) sets how long a received message stays hidden; the crate re-arms it halfway through each timeout while the handler runs.

The visibility timeout is what each receive asks for: how long a received message stays hidden from every other receive.
SQS counts the timeout in whole seconds, from one second to twelve hours.
A value outside that range stops the service at startup with [`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue).
A fraction above one second rounds up to the next whole second, so a message never comes back sooner than the timeout asked for.
A subscription that names none takes the queue's own timeout, which it reads once with `GetQueueAttributes` when it opens, so it needs the `sqs:GetQueueAttributes` [permission](../broker/credentials.md).
A handler may take longer than the timeout.
While a handler holds a delivery, the crate re-arms the timeout halfway through each period with `ChangeMessageVisibility`, so no other consumer receives the message in the meantime.
The other half of the period is left for the call to reach the queue, so a one-second timeout is re-armed every half second.
SQS keeps a message hidden for at most twelve hours from its receive, and an extension does not restart them.
A handler that holds a delivery that long gets one last extension to the end of the twelve hours and a warning in the log; after that the queue hands the message to another consumer.
The timeout also decides how soon an abandoned delivery comes back: a message dropped unsettled, or one a stopped process held, is received again once the timeout lapses.
In process, a subscription that names no timeout gets the 30 seconds of a new queue; the timeout an operator set on a queue shows only in a [live test](../testing/live.md).
The step is on [`SqsQueue`](./sqs-queue.md), and on the mount chain through [`SqsSubscription::visibility`](api:ruststream_sqs_sns::SqsSubscription::visibility), as in the example.

## Example

A report takes five seconds to render, and a receive hides it for two; the report is still handled once.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_subscriptions_visibility.rs#visibility"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_subscriptions_visibility.rs#visibility"
```

</TabItem>
</Tabs>
