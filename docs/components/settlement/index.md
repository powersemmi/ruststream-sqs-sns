---
title: Settlement
description: "Each outcome is one SQS call: ack and drop delete the message, retry makes it visible again at once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settlement

Each outcome is one SQS call: `ack` and `drop` delete the message, `retry` makes it visible again at once.

[`ack`](api:ruststream::runtime::HandlerOutcome::ack) deletes the message with `DeleteMessage`.
[`drop`](api:ruststream::runtime::HandlerOutcome::drop) deletes it too, because SQS has no call that rejects a message without deleting it.
[`retry`](api:ruststream::runtime::HandlerOutcome::retry) sets the message's visibility timeout to zero, so the next receive hands it out again.
[`retry_after`](./retry-after.md) sets the timeout to a delay instead.
A redelivered message is the same message: the queue counts its receives, and the handler reads the count in [`RECEIVE_COUNT_HEADER`](../delivery/receive-count.md).
A delivery the service never settles, as when its process stops, makes no call, and the message comes back once its [visibility timeout](../subscriptions/visibility.md) lapses.
A [redrive policy](./dead-letters.md) caps the receives, and the queue moves a spent message to a dead-letter queue itself.
In the manual form `Ok(())` acknowledges the delivery and `Err(outcome)` settles it with that outcome.

## Example

A malformed payment is dropped, a payment the gateway was too busy for comes back at once, and a charged one is deleted.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_settlement_outcomes.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_settlement_outcomes.rs#outcomes"
```

</TabItem>
</Tabs>
