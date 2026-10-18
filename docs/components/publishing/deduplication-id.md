---
sidebar_position: 2
title: deduplication_id
description: "deduplication_id names one message's idempotency key on a FIFO destination; left unnamed, every send gets a process-unique id."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# deduplication_id

[`deduplication_id`](api:ruststream_sqs_sns::SqsPublishSteps::deduplication_id) names one message's idempotency key on a FIFO destination; left unnamed, every send gets a process-unique id.

A FIFO queue accepts a message whose deduplication id it has seen in the last five minutes and does not deliver it again.
The repeated publish succeeds, so the sender cannot tell the two apart and does not need to.
An id derived from the input makes a handler's publish idempotent: SQS delivers a message at least once, and a second run of the handler sends the same id.

The id is a step of one publish, never a setting of the policy.
A constant id would collapse everything a position sends into one message.
A send that names none carries an id this crate makes unique within the process, `rs-<pid>-<sequence>`.
Two identical payloads sent on purpose therefore stay two messages, even on a queue with content-based deduplication.
The step on a standard queue fails the publish with [`SqsError::NotFifo`](api:ruststream_sqs_sns::SqsError::NotFifo).
A FIFO topic takes the same step through [`SnsPublish`](../sns/index.md).

## Example

A payment SQS delivers twice leads to one charge: both runs send the charge under the payment's id.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_publishing_deduplication.rs#deduplication"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_publishing_deduplication.rs#deduplication"
```

</TabItem>
</Tabs>
