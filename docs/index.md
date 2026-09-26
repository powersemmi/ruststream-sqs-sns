# ruststream-sqs-sns

**`ruststream-sqs-sns`** is the Amazon SQS broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, with SNS fan-out
publishing. A subscriber long-polls its queue and can take native batches over `ReceiveMessage`.
Retries run through the message's visibility timeout, and dead-lettering through the queue's
redrive policy. You set a message group per publish to a FIFO queue, or fix one for a whole
publish position, and read it back from the message you receive.

The transport is built on the official [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) and
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) clients. The `testing` feature gives the broker an
in-process mode, so a test runs the production app with no server, and the `asyncapi` feature adds
the `sqs` bindings to the document the framework generates.

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

The crate's own documentation is its rustdoc on docs.rs, and it opens with the guide this site
used to carry: what the queue answers natively, how a delivery is settled, where a spent one
goes, and what a publish may carry.

<div class="grid cards" markdown>

- :material-download: **[Subscribing](https://docs.rs/ruststream-sqs-sns/latest/ruststream_sqs_sns/index.html#subscribing)** - the queue descriptor, settlement, the attempt cap, batches, and what a delivery carries.
- :material-upload: **[Publishing](https://docs.rs/ruststream-sqs-sns/latest/ruststream_sqs_sns/index.html#publishing)** - the two publish policies, replies, SNS fan-out, and FIFO message groups.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-sqs-sns)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site is the entry page. What SQS and SNS do is in the crate's
[rustdoc](https://docs.rs/ruststream-sqs-sns), including testing the production app in process
and the operational surface. Framework concepts that apply to every broker live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).
