---
sidebar_position: 4
title: Shutdown
description: "Shutdown marks the connection closed; a publisher handed out earlier reports SqsError::NotConnected instead of sending."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown marks the connection closed; a publisher handed out earlier reports [`SqsError::NotConnected`](api:ruststream_sqs_sns::SqsError::NotConnected) instead of sending.

SQS calls complete one by one, and the SDK clients hold nothing to flush.
The runtime stops the subscriptions and marks the connection closed, which is the whole teardown.
A delivery a handler had not settled returns to its queue when its visibility timeout lapses.

A publisher taken with [`SqsBroker::publisher`](api:ruststream_sqs_sns::SqsBroker::publisher) shares the broker's connection.
It can be taken before the app starts and handed to code outside the handlers, such as an HTTP endpoint.
It sends once the runtime has connected the broker.
After shutdown it returns `SqsError::NotConnected` inside the core's `PublishError`, and never sends over a connection the service has given up.
A publisher of a lifecycle hook or an `Out` slot behaves the same way.

## Example

The service hands a publisher to code outside its handlers.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_broker_shutdown.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_broker_shutdown.rs#service"
```

</TabItem>
</Tabs>
