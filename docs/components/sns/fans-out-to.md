---
sidebar_position: 2
title: fans_out_to
description: "fans_out_to names the queues an operator subscribed to a topic, so a test sees every queue the topic delivers to."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# fans_out_to

[`fans_out_to`](api:ruststream_sqs_sns::SnsPublish::fans_out_to) names the queues an operator subscribed to a topic, so a test sees every queue the topic delivers to.

A queue the service subscribes itself, through [`subscribe_queue_to_topic`](./subscribe-queue-to-topic.md), is known to the broker.
A queue an operator subscribed in AWS is known only to AWS.
The step names those queues on the policy, and they apply to every topic the position publishes to.
In process the account subscribes them to the topic before the publish, as the operator did.
A live test waits for the subscription of each named queue to handle its copy.
A queue SNS refuses, such as a FIFO queue on a standard topic, fails the publish.

The step exists with the crate's `testing` feature, which a service enables in its `[dev-dependencies]`.
The service builds the policy with the step under `#[cfg(test)]`, which holds in the build of the tests beside its code.
A production build neither has the step nor calls it.
A test under `tests/` builds the service without `cfg(test)`.
There the service gates the step on a feature of its own that enables `ruststream-sqs-sns/testing`, and the tests run with that feature.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["sns", "testing"] }
```

The operator subscribed `billing` to `orders-events`, and the test build of the policy says so.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_sns_fans_out_to.rs#policy"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_sns_fans_out_to.rs#policy"
```

</TabItem>
</Tabs>
