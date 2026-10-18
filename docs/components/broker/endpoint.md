---
sidebar_position: 2
title: Endpoint
description: "endpoint, region and test_credentials point the broker at a local stack; queue URLs the service returns are rebased onto the endpoint."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Endpoint

[`endpoint`](api:ruststream_sqs_sns::SqsBroker::endpoint), [`region`](api:ruststream_sqs_sns::SqsBroker::region) and [`test_credentials`](api:ruststream_sqs_sns::SqsBroker::test_credentials) point the broker at a local stack; queue URLs the service returns are rebased onto the endpoint.

`endpoint` replaces the AWS endpoint with an address such as `http://localhost:4566`, where LocalStack listens.
`region` sets the region every request is signed for.
`test_credentials` signs requests with a fixed dummy key: a local stack checks that a request is signed and ignores the key.
Each step overrides only its own setting, and the SDK's default chain resolves the rest.

A local stack returns queue URLs on a host of its own choosing, which may not resolve from the service.
The broker keeps the path of each URL and puts the configured endpoint in front of it, so a stack's host strategy changes nothing.
An endpoint from `AWS_ENDPOINT_URL` or from the [config](./credentials.md) given to `from_config` is rebased the same way.

The generated [AsyncAPI document](../asyncapi/server.md) reports the endpoint's host and port as the server.
A broker with no endpoint step is reported as `sqs.amazonaws.com`, whatever its region.

## Example

The service runs against LocalStack on port 4566.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_broker_endpoint.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_broker_endpoint.rs#broker"
```

</TabItem>
</Tabs>
