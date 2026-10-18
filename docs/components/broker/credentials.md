---
sidebar_position: 1
title: Credentials
description: "The AWS SDK's default chain supplies the region and the credentials, fetched on the first request; from_config takes a prebuilt SdkConfig instead."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Credentials

The AWS SDK's default chain supplies the region and the credentials, fetched on the first request; [`from_config`](api:ruststream_sqs_sns::SqsBroker::from_config) takes a prebuilt `SdkConfig` instead.

A broker built with [`new`](api:ruststream_sqs_sns::SqsBroker::new) loads its configuration with `aws_config` when it connects.
The region comes from `AWS_REGION` or the shared profile, and the credentials from the environment, the shared profile files, SSO, or the instance or container role.
The SDK fetches the credentials on the first request, a subscription's `GetQueueUrl` at [startup](./startup.md), so a service with a subscription meets a missing credential there.
The configuration follows the SDK's latest behaviour version.
The connect also sets a per-attempt timeout of 25 seconds, above the 20 seconds a long poll waits.

`from_config` takes an `aws_config::SdkConfig` the service builds itself: a named profile, a pinned behaviour version, its own HTTP client or credential provider.
The broker uses that config as it is.
The region, the credentials, the endpoint and the timeouts all come from it, and the broker's own `region` and `test_credentials` steps do not apply.
The SDK sets no attempt timeout of its own, so a config without one lets every long poll run its full wait.
A config with an attempt timeout under 20 seconds ends every long poll early, and each receive fails; the example sets 25 seconds, as `new` does.
Transport security is the SDK's HTTPS client in both cases.

The service's role needs these permissions:

| Action | When |
|---|---|
| `sqs:GetQueueUrl`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` | every subscription |
| `sqs:GetQueueAttributes` | a subscription that names no [visibility](../subscriptions/visibility.md), and the dead-letter queue of a [redrive policy](../settlement/dead-letters.md) |
| `sqs:SetQueueAttributes` | a subscription with a redrive policy, and the queue of [`subscribe_queue_to_topic`](../sns/subscribe-queue-to-topic.md) |
| `sqs:CreateQueue` | a subscription with [`create_if_missing`](../subscriptions/create-if-missing.md) |
| `sqs:SendMessage` | every publish to a queue |
| `sns:Publish` | every publish to a [topic](../sns/index.md), and `sns:CreateTopic` for a topic named by name rather than ARN |
| `sns:Subscribe` | [`subscribe_queue_to_topic`](../sns/subscribe-queue-to-topic.md), with `sns:CreateTopic` on the topic and `sqs:GetQueueUrl`, `sqs:GetQueueAttributes` and `sqs:SetQueueAttributes` on the queue |

## Example

The service signs its requests with the `orders` profile of the shared AWS files, in `eu-west-1`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_broker_credentials.rs#config"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_broker_credentials.rs#config"
```

</TabItem>
</Tabs>
