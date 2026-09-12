# ruststream-sqs-sns

**`ruststream-sqs-sns`** 是 [RustStream](https://powersemmi.github.io/ruststream/) 消息框架的
Amazon SQS Broker，并支持 SNS 扇出发布。订阅者用长轮询读自己的队列，也可以通过 `ReceiveMessage`
取得原生的批次。重试走消息的可见性超时，进死信队列则由队列的 redrive 策略决定。向 FIFO 队列发布
时，你可以逐条指定消息组，也可以为整个发布位置固定一个，并从收到的消息里读回它。

传输建立在官方的 [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) 和
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) 客户端之上。`testing` feature 提供一个进程内的测试
Broker。

这个 crate 跟随已发布的 `ruststream` 0.7 线：

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sqs-sns/examples/sqs_service.rs:app"
```

## 接下来看什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-aws: **[SQS 指南](sqs.md)** - 队列描述符、结算、批次、FIFO 组、SNS 扇出和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-sqs-sns)** - crate 在 docs.rs 上的 rustdoc。

</div>

## 本站与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站讲 SQS 和 SNS。适用于每个 Broker 的框架概念，写在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
