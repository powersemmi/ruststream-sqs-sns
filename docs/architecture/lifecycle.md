---
title: Lifecycle
sidebar_position: 3
description: "SqsBroker::new records configuration, connect loads the AWS configuration, startup opens a pump per queue, and shutdown closes the connection."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's states as types, and what each transition does on AWS",
  "text": "The diagram as text",
  "states": "Broker",
  "aws": "On AWS",
  "unconnected": ["SqsBroker::new()"],
  "connected": ["ConnectedSqsBroker"],
  "closed": ["Closed = ()", "nothing to report"],
  "config": ["Configuration only,", "no I/O"],
  "load": ["AWS configuration,", "25 s per attempt,", "SDK clients built"],
  "subscriptions": ["Each subscription:", "GetQueueUrl or", "CreateQueue, the redrive", "policy, the visibility,", "the pump started"],
  "publish": ["A publish resolves", "its queue or topic", "once, then caches it"],
  "flag": ["The connection", "is marked closed"],
  "afterShutdown": ["A publisher that outlives the connection answers", "SqsError::NotConnected, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`SqsBroker::new`](api:ruststream_sqs_sns::SqsBroker::new) records configuration, connect loads the
AWS configuration, startup opens a pump per queue, and shutdown closes the connection.

<Diagram name="lifecycle" words={words} />

`new`, `endpoint`, `region`, `test_credentials` and `from_config` do no I/O, so the service's
builder stays synchronous. A clone of the broker shares its connection, which is how a startup hook
reaches the connection the runtime opened. The runtime connects the broker when the app starts.

Connect loads the AWS configuration through the SDK's default chain, with the endpoint and region
the broker names, or takes the `SdkConfig` given to `from_config`. It sets a 25-second timeout per
attempt, above the 20-second long poll, and builds the SDK clients. The SDK fetches credentials
lazily, on the first request. In a service that request is the first subscription's
`GetQueueUrl`, so a bad credential fails the start on subscribe.

Each subscription's [`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) is checked first: an empty name,
a wait over 20 seconds or a visibility outside one second to 12 hours is
[`SqsError::InvalidQueue`](api:ruststream_sqs_sns::SqsError::InvalidQueue) before any call to AWS.
Then the subscription resolves the queue URL, or creates the queue under `create_if_missing`. It
writes the redrive policy when the registration declares `max_attempts` and `dead_letter`. It reads
the queue's own visibility timeout when the descriptor names none. Then it starts the pump.

A receive error reaches the subscription's stream as an item. A queue that no longer exists ends
the pump, and any other error makes it wait one second before the next receive.

Shutdown marks the connection closed and returns `()`: the SDK clients have nothing to close or
flush. A publisher created before shutdown answers
[`SqsError::NotConnected`](api:ruststream_sqs_sns::SqsError::NotConnected) afterwards.
