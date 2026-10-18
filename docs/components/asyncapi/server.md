---
sidebar_position: 1
title: Server
description: "The server reports the endpoint's host and port with the protocol sqs, never a credential and never a protocol version."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the endpoint's host and port with the protocol `sqs`, never a credential and never a protocol version.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the host and port of the broker's [endpoint](../broker/endpoint.md); the scheme, any user info and the path stay out.
A broker with no endpoint reports `sqs.amazonaws.com`.
The `protocol` is `sqs`.
`protocolVersion` stays out: SQS has one wire protocol, and a client matches no version of it.
SNS publishes go through the same broker, so they share its server.

Credentials come from the SDK's chain or a [prebuilt configuration](../broker/credentials.md) once the service runs, and the document is built before that.
No credential reaches it.

## Example

The broker reaches its region through the regional endpoint.
Its server in the document is `sqs.eu-west-1.amazonaws.com` with the protocol `sqs`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_asyncapi.rs#server"
```

</TabItem>
</Tabs>
