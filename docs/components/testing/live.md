---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against LocalStack or AWS, for what belongs to the service: queues, timeouts, credentials."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against LocalStack or AWS, for what belongs to the service: queues, timeouts, credentials.

It connects the broker through its ordinary connect, so every subscription resolves its queue on the stack and every request is signed.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process account leaves to the service: a message kept on a queue no subscription reads, the visibility timeout an operator set on a queue, the queues and topics the service expects to find without creating them, and credentials.
A stack keeps what earlier runs left in it, so each run names its queues uniquely, and creates them with [`create_if_missing`](../subscriptions/create-if-missing.md) or before the app starts.

In the example, `live::endpoint` returns the stack's address from `SQS_TEST_ENDPOINT`, and the test returns early without one, so `cargo test` passes on a machine with no stack.
A job that stood a stack up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is LocalStack in `docker-compose.test.yml`, which `just brokers-up` starts.

## Example

The two tests share one body: one in process, one against the stack at `SQS_TEST_ENDPOINT`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqs_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqs_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
