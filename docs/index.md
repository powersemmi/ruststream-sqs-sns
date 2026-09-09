# ruststream-sqs-sns

**`ruststream-sqs-sns`** is the Amazon SQS broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, with SNS fan-out
publishing. A subscriber long-polls its queue and can take native batches over `ReceiveMessage`.
Retries run through the message's visibility timeout, and dead-lettering through the queue's
redrive policy. You can set a message group when you publish to a FIFO queue, and read it back
from the message you receive.

The transport is built on the official [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) and
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) clients. The `testing` feature ships an in-process
test broker.

The crate tracks the released `ruststream` 0.7 line:

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-aws: **[SQS guide](sqs.md)** - queue descriptors, settlement, batches, FIFO groups, SNS fan-out, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-sqs-sns)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site covers SQS and SNS. Framework concepts that apply to every broker live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).
