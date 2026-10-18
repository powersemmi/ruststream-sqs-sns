---
sidebar_position: 8
description: "Run an SQS service on AWS: credentials and region from the environment or a config of your own, timeouts, and the IAM permissions it needs."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

This guide runs an SQS service on AWS: where its credentials and region come from, the timeouts it runs with, and the IAM permissions it needs.

## Credentials and region from the environment

`SqsBroker::new()` takes nothing from the code.
The AWS SDK's default chain finds the region and the credentials when the service starts: environment variables, the shared `~/.aws` files, SSO, and the role of the container or the instance.
A service that runs under an IAM role on ECS, EKS, Lambda or EC2 needs no configuration at all.

The SDK reads credentials when it signs its first request.
That request is the `GetQueueUrl` of the first subscription, at startup, so a service with wrong credentials stops before it handles a message.

## A configuration of your own

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#config"
```

</TabItem>
</Tabs>

[`SqsBroker::from_config`](api:ruststream_sqs_sns::SqsBroker::from_config) takes an `SdkConfig` the service builds.
`aws_config` builds one from the environment with an `async` loader, while the app function is synchronous.
So the example builds it with `SdkConfig::builder()`, which does no I/O.
The profile provider reads the `orders-service` profile on the first request; a profile with a `role_arn` assumes that role.
The pinned behaviour version keeps the SDK's defaults from changing under the service when the SDK is updated.
The broker uses the configuration as it is, so the configuration also carries the request timeout described [below](#timeouts-and-transport).
The example needs two more dependencies:

```toml title="Cargo.toml"
[dependencies]
aws-config = "1"
aws-sdk-sqs = "1"
```

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

The `orders` queue belongs to the infrastructure: Terraform, CloudFormation or the console create it, with its redrive policy and encryption.
The descriptor leaves out `create_if_missing`, so a missing queue stops the service at startup instead of creating an empty one.
It names its visibility, so the subscription does not read the queue's own timeout.

On a broker built with `SqsBroker::new()`, [`endpoint`](api:ruststream_sqs_sns::SqsBroker::endpoint) and [`region`](api:ruststream_sqs_sns::SqsBroker::region) point it at another endpoint, a VPC endpoint or a local stack.
A configuration of your own names them with `endpoint_url` and `region` on the `SdkConfig` builder.
The queue URLs SQS returns are rebased onto the endpoint.

## Timeouts and transport

`SqsBroker::new()` gives each SQS request 25 seconds per attempt, above the 20 seconds a long poll may wait.
A configuration of your own sets this with `timeout_config`, as the example does; an attempt timeout under 20 seconds would end every long poll.
The SDK retries a failed request with its own retry policy.
Requests go over the SDK's default HTTPS client.
A handler on a dedicated thread (`threads(n)`) sends through SDK clients of its own, built from the same configuration and sharing its credentials.

## IAM permissions

The service above needs these permissions on its queue:

```json title="policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": "arn:aws:sqs:eu-west-1:123456789012:orders"
    }
  ]
}
```

Each feature adds its own:

| Feature | Permissions |
|---|---|
| a subscription that names no visibility | `sqs:GetQueueAttributes` on the queue |
| `max_attempts` and `dead_letter` | `sqs:SetQueueAttributes` on the queue, `sqs:GetQueueAttributes` on the dead-letter queue |
| `create_if_missing` | `sqs:CreateQueue` |
| a publish to a queue | `sqs:GetQueueUrl` and `sqs:SendMessage` on the queue |
| a publish to a topic | `sns:CreateTopic` and `sns:Publish` on the topic |

## What the AsyncAPI document shows

With the `asyncapi` feature, the service's AsyncAPI document names one server: the host of the broker's `endpoint`, or `sqs.amazonaws.com`.
The document never holds a credential or a queue's ARN, so it can be published.

The tests in the example file start the same app in process; the in-process SQS takes the region from the configuration and signs nothing.
[Testing an SQS service](testing.md) covers the harness.
