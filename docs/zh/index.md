# ruststream-sqs-sns

**`ruststream-sqs-sns`** 是 [RustStream](https://powersemmi.github.io/ruststream/) 消息框架的
Amazon SQS Broker，并支持 SNS 扇出发布。订阅者用长轮询读自己的队列，也可以通过 `ReceiveMessage`
取得原生的批次。重试走消息的可见性超时，进死信队列则由队列的重新驱动策略决定。向 FIFO 队列发布
时，你可以逐条指定消息组，也可以为整个发布位置固定一个，并从收到的消息里读回它。

传输建立在官方的 [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) 和
[`aws-sdk-sns`](https://docs.rs/aws-sdk-sns) 客户端之上。`testing` feature 给 Broker 一个进程内
模式，测试无需服务器就能运行生产应用；`asyncapi` feature 则把 `sqs` 绑定写进框架生成的文档。

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

crate 自己的文档就是它在 docs.rs 上的 rustdoc，开头正是本站从前那份指南：队列自己能做什么、一次
投递如何结算、用尽次数的消息去哪里，以及一次发布可以带上什么。

<div class="grid cards" markdown>

- :material-download: **[订阅](https://docs.rs/ruststream-sqs-sns/latest/ruststream_sqs_sns/index.html#subscribing)** - 队列描述符、结算、尝试次数上限、批次，以及一条投递带着什么。
- :material-upload: **[发布](https://docs.rs/ruststream-sqs-sns/latest/ruststream_sqs_sns/index.html#publishing)** - 两种发布策略、回复、SNS 扇出和 FIFO 消息组。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-sqs-sns)** - crate 在 docs.rs 上的 rustdoc。

</div>

## 本站与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站是入口页。SQS 和 SNS 做什么，写在 crate 的
[rustdoc](https://docs.rs/ruststream-sqs-sns) 里，在进程内测试生产应用的方法和运维一面（发布者、
权限、服务的各项上限）也在那里。适用于每个 Broker 的框架概念，写在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
