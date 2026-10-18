---
title: Settlement
sidebar_position: 6
description: "What ack, retry, retry_after and drop become on SQS: each outcome is one call against the queue, and a retry waits in the queue itself."
---

import {Diagram} from '@site/src/components/Diagrams';

export const settlement = {
  "title": "Each outcome of a handler, the SQS call it makes, and what happens to the message",
  "text": "The diagram as text",
  "outcome": "Outcome",
  "call": "SQS call",
  "effect": "Effect",
  "ackOutcome": "ack",
  "ackCall": "DeleteMessage",
  "ackEffect": "Gone",
  "retryOutcome": "retry()",
  "retryCall": ["ChangeMessage", "Visibility(0)"],
  "retryEffect": ["Visible now"],
  "retryAfterOutcome": "retry_after(d)",
  "retryAfterCall": ["ChangeMessage", "Visibility(d)"],
  "retryAfterEffect": ["Back after d,", "within 12 h", "of the receive"],
  "dropOutcome": "drop",
  "dropCall": ["DeleteMessage;", "at the cap:", "Visibility(0)"],
  "dropEffect": ["Gone, or to the", "dead-letter", "queue"],
  "unsettledOutcome": ["dropped", "unsettled"],
  "unsettledCall": ["No call,", "extension stops"],
  "unsettledEffect": ["Back when the", "timeout lapses"]
};

export const retries = {
  "title": "A retry on SQS: the message waits in the queue, and the redrive policy decides on the next receive",
  "text": "The diagram as text",
  "delivery": ["A delivery", "its receive count says which attempt it is"],
  "outcome": ["The handler returns", "retry_after(d), or retry()"],
  "wait": ["ChangeMessageVisibility(d)", "the message waits in the queue"],
  "cap": ["The next receive: has the message been", "received maxReceiveCount times?"],
  "again": ["Not yet:", "delivered again,", "the same message"],
  "deadLetter": ["Yes: SQS moves it", "to the dead-letter", "queue"],
  "drop": ["A drop on the last allowed receive returns the message", "with visibility 0, so SQS moves it rather than lose it"],
  "crash": ["The queue holds the waiting message:", "a crash of the service loses no retry"],
  "handled": "handled",
  "retries": "a retry",
  "after": "after d",
  "below": "not yet",
  "past": "yes"
};

# Settlement

What `ack`, `retry`, `retry_after` and `drop` become on SQS: each [outcome](../components/settlement/index.md) is one call against the
queue, and a retry waits in the queue itself.

<Diagram name="settlement" words={settlement} />

`retry_after(d)` counts the delay in whole seconds, rounded up, so the message never comes back
before `d`. A delay that reaches past the 12 hours from the receive is shortened to end there, and
the crate logs a warning that names the queue. A delivery dropped
without a settlement makes no call: its extension stops, and the message returns when its current
timeout lapses.

SQS has no verb that rejects a message without deleting it, so `drop` deletes. A queue whose
registration declares a redrive policy is the exception. There, a delivery received
`maxReceiveCount` times is returned instead of deleted, because the next receive is what moves it
to the dead-letter queue, and a delete would lose it.

The delivery's [`redelivery_count`](api:ruststream::IncomingMessage::redelivery_count) is the
queue's `ApproximateReceiveCount`, and the first delivery counts as one. It counts every receive,
a lapsed timeout included, not only the retries the service asked for.

## Retries

<Diagram name="retries" words={retries} />

A retry never leaves the queue. [`retry_after(d)`](../components/settlement/retry-after.md) makes the message invisible for `d`, and it comes
back as the same message, with the same id and its receive count one higher. Nothing is
republished, so the runtime's republish fallback never runs here, and `.out_retry(..)` does not
compile on an [`SqsQueue`](../components/subscriptions/sqs-queue.md). SQS refuses a visibility that ends
more than 12 hours after the receive.

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) and
[`dead_letter(q)`](api:ruststream::runtime::Mounting::dead_letter) together are the queue's
[redrive policy](../components/settlement/dead-letters.md): `maxReceiveCount` is `n` and the dead-letter queue is `q`. The subscription writes the
policy onto the queue when it opens, and SQS counts the receives and moves a spent message itself.
One half without the other is
[`SqsError::IncompleteRedrive`](api:ruststream_sqs_sns::SqsError::IncompleteRedrive) at startup.

The queue holds a waiting message, so a crash of the service loses no retry: the message comes back
when its visibility ends, to whichever consumer polls next.
