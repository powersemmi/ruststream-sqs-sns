---
title: Visibility
sidebar_position: 1
description: "A received SQS message stays in the queue, invisible for the visibility timeout, until a delete ends it or it comes back for another receive."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One message: a receive hides it, a delete ends it, a lapsed timeout or a zero visibility brings it back",
  "text": "The diagram as text",
  "stored": ["Stored", "visible to the next receive"],
  "inFlight": ["In flight", "invisible for the visibility timeout,", "a receipt handle, receive count + 1"],
  "deleted": ["DeleteMessage:", "gone"],
  "lapsed": ["The timeout", "lapses"],
  "zero": ["ChangeMessage", "Visibility(0)"],
  "again": ["Visible again", "the next receive counts it once more"],
  "deadLetter": ["Received maxReceiveCount times:", "moved to the dead-letter queue"],
  "extend": ["ChangeMessageVisibility(n) hides it n more seconds,", "up to 12 hours after the receive"],
  "receive": "ReceiveMessage",
  "pastCap": "on the next receive"
};

# Visibility

A received SQS message stays in the queue, invisible for the visibility timeout, until a delete
ends it or it comes back for another receive.

<Diagram name="visibility" words={words} />

A receive hands the message out with a receipt handle and hides it from every other receive for
the visibility timeout. The timeout is the queue's own, 30 seconds by default and up to 12 hours,
unless the receive names one. SQS counts it in whole seconds, from one second up. The consumer deletes the message with the receipt handle of that
receive.

A message that is not deleted in time becomes visible again and is delivered once more.
`ChangeMessageVisibility` sets a new timeout, counted from the call. A timeout of zero makes the
message visible at once. A timeout that would end more than 12 hours after the receive is refused.

Every receive adds one to the message's `ApproximateReceiveCount`. A queue with a redrive policy
names a dead-letter queue and a `maxReceiveCount` from 1 to 1000. A message received that many
times without a delete is moved to the dead-letter queue on its next receive, instead of being
delivered. The
dead-letter queue is of the same kind as the source: a FIFO queue takes a FIFO dead-letter queue.

SQS has no lease to renew. While a handler holds a delivery, this crate calls
`ChangeMessageVisibility` halfway through each timeout, so a slow handler is not raced by a second
delivery of the same message. The other half is left for the call to reach the queue. The
extension stops when the delivery settles or is dropped. A queue whose timeout is zero gets no
extension.

The 12 hours from the receive bound the extension too. A handler that holds a delivery that long
gets one last extension to the end of them, and the crate logs a warning. After that the queue
hands the message to another consumer.

The behaviour is Amazon SQS's own
([visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html),
[`ChangeMessageVisibility`](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html),
[dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)).
