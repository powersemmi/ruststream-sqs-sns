---
sidebar_position: 6
title: Errors
description: "SqsError is the crate's one error type: configuration, queues, receives, publishes, SNS admin, queue access policies, a closed connection and refused settings."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`SqsError`](api:ruststream_sqs_sns::SqsError) is the crate's one error type: configuration, queues, receives, publishes, SNS admin, queue access policies, a closed connection and refused settings.

Each variant names the step that failed and the queue or topic it was about.
The SDK's failure behind it is its `source`, formatted with its whole cause chain, so the SDK's error types stay out of the crate's API.

| Variant | When |
|---|---|
| `Config` | the [in-process mode](../testing/index.md) was asked of a broker whose handles already connected to AWS |
| `Queue` | a queue could not be resolved, [created](../subscriptions/create-if-missing.md), read or given its redrive policy |
| `Receive` | a receive from an open subscription failed |
| `Publish` | SQS refused a message, or SNS refused a notification |
| `Admin` | a topic could not be created or a queue [subscribed](../sns/subscribe-queue-to-topic.md) to it (`sns` feature) |
| `QueuePolicy` | a queue's access policy could not be read or written while [`subscribe_queue_to_topic`](../sns/subscribe-queue-to-topic.md) granted a topic the right to send to it (`sns` feature) |
| `NotConnected` | a publisher was used before the broker connected, or after it [shut down](./shutdown.md) |
| `InvalidQueue` | a descriptor is out of range: an empty name, a [wait](../subscriptions/wait.md) or a [visibility](../subscriptions/visibility.md) SQS does not allow |
| `IncompleteRedrive` | a registration declared `max_attempts` without `dead_letter`, or the other way round ([dead letters](../settlement/dead-letters.md)) |
| `NotFifo` | a publish named a [group id](../publishing/group-id.md) or a [deduplication id](../publishing/deduplication-id.md) for a standard queue or topic |

A service meets the error in two places.
A failed startup carries it in the cause chain of the runtime's error, and the service exits before it reads anything.
A publish from a handler returns it as the source of the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

A shipment to a warehouse queue that is not FIFO is refused with `NotFifo`.
A retry would be refused again, so the handler drops the order and logs the error; any other failure is retried.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
