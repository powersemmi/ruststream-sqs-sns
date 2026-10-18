---
sidebar_position: 1
title: retry_after
description: "retry_after sets the message's visibility to the delay, so it returns on the same queue with its receive count, after at most 12 hours."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) sets the message's visibility to the delay, so it returns on the same queue with its receive count, after at most 12 hours.

SQS holds a message back natively: `ChangeMessageVisibility` hides it for the delay, and the next receive after that hands it out again.
Nothing is republished and no copy is made, so the message keeps its id, its attributes and its [receive count](../delivery/receive-count.md).
SQS counts whole seconds, so a fraction of a second rounds up.
SQS keeps a message hidden for at most twelve hours from its receive, so the longest delay is what remains of those twelve hours.
The delay waits on the queue, not in the service, so a service that stops in the meantime loses nothing.
SQS moves the message itself, so the registration names no retry publisher, and `.out_retry(..)` does not compile on an [`SqsQueue`](../subscriptions/sqs-queue.md).
A [redrive policy](./dead-letters.md) counts a deferred receive like any other.
In a test, [`tb.advance(delay)`](api:ruststream::testing::TestApp::advance) lets the delay pass on a paused clock.

## Example

An invoice whose payment has not cleared comes back five seconds later and is settled then.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_settlement_retry_after.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_settlement_retry_after.rs#retry_after"
```

</TabItem>
</Tabs>
