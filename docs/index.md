# ruststream-sqs-sns

**`ruststream-sqs-sns`** is the Amazon SQS broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework, with SNS fan-out
publishing. It covers long polling, visibility-based retries and dead-lettering through the
queue's redrive policy, FIFO message groups, and ships an in-process test broker under its
`testing` feature.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport over the official [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) and
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) clients, and nothing broker-specific leaks back into
the framework.

The crate is not published to crates.io yet; it builds against the released `ruststream` 0.6 line
and is depended on from git:

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-sqs-sns = { git = "https://github.com/powersemmi/ruststream-sqs-sns" }
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-aws: **[SQS guide](sqs.md)** - queue descriptors, settlement, FIFO groups, SNS fan-out, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-sqs-sns)** - the crate's rustdoc, published with the first crates.io release.

</div>

## How this site relates to the RustStream docs

This site documents the SQS broker only. Framework concepts that apply to every broker (writing
subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to SQS and SNS and link back to the framework docs where the two meet.
