---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-sqs-sns，启动本地 SQS，让第一个订阅者连上它运行，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-sqs-sns`，启动一个本地 SQS，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sqs-sns = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-sqs-sns` 是 SQS Broker，基于官方客户端 `aws-sdk-sqs` 构建。
它的 `testing` feature 加入测试所用的[进程内 SQS](../components/testing/index.md)。

这个 Broker crate 有以下 feature，默认全部关闭：

| Feature | 提供的内容 |
|---|---|
| `sns` | 通过 `aws-sdk-sns`，用 [`SnsPublish`](../components/sns/index.md) 发布到 SNS 主题（[用 SNS 扇出](fan-out.md)） |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [`sqs` 和 `sns` 绑定](../components/asyncapi/index.md) |
| `testing` | `TestApp` 背后的进程内 SQS |

## 本地 SQS {/* #a-local-sqs */}

[LocalStack](https://github.com/localstack/localstack) 在一个容器里运行 SQS 和 SNS：

```yaml title="compose.yaml"
services:
  localstack:
    image: localstack/localstack:4.14.0
    ports:
      - "4566:4566"
    environment:
      SERVICES: sqs,sns
      SQS_ENDPOINT_STRATEGY: path
```

```bash
docker compose up -d
```

它在 `http://localhost:4566` 上应答，启动时没有任何队列。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 `orders` 队列读取 JSON 订单。
带金额的订单被 ack，没有金额的订单被丢弃。
[`SqsQueue`](../components/subscriptions/sqs-queue.md) 指定队列，[`create_if_missing`](../components/subscriptions/create-if-missing.md) 在订阅打开时创建它。
生产环境的服务不用这一步，它的队列来自基础设施。
[`SqsBroker::new`](../components/broker/index.md) 只记录配置。
[`endpoint`](../components/broker/endpoint.md)、[`region`](api:ruststream_sqs_sns::SqsBroker::region) 和 [`test_credentials`](api:ruststream_sqs_sns::SqsBroker::test_credentials) 让它指向本地环境。
SDK 客户端在服务启动时才构建。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，用 AWS CLI 发送一个订单：

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws sqs send-message \
  --queue-url "$(aws sqs get-queue-url --queue-name orders --output text)" \
  --message-body '{"id":1,"amount_cents":1250}'
```

服务记录一条带订单 id 的 `order taken` 日志。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`SqsBroker` 连接的是进程内 SQS，所以测试不需要本地环境：

```bash
cargo test
```

[测试 SQS 服务](testing.md)逐一讲解这些测试，以及进程内 SQS 模拟了什么。

## 从 aws-sdk-sqs 迁移 {/* #coming-from-aws-sdk-sqs */}

基于 `aws-sdk-sqs` 的服务要自己编写接收循环。
下表列出每一部分在 RustStream 中的位置：

| aws-sdk-sqs | RustStream |
|---|---|
| `aws_config::load_from_env` 和 `Client::new` | [`SqsBroker::new`](api:ruststream_sqs_sns::SqsBroker::new)，或者 [`SqsBroker::from_config`](../components/broker/credentials.md) 加上你自己的 `SdkConfig`（[在生产环境中运行](production.md)） |
| `get_queue_url` | [`SqsQueue::new`](api:ruststream_sqs_sns::SqsQueue::new) 中的队列名称，启动时解析一次 |
| 一个 `receive_message().wait_time_seconds(20)` 循环 | 一个 `#[subscriber]` 处理器（[消费队列](consuming.md)） |
| 反序列化 `message.body()` | 处理器的参数类型，由编解码器解码 |
| 处理完成后调用 `delete_message` | 处理器的结果 [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack)（[确认与投递保证](acknowledgement.md)） |
| `change_message_visibility` | [`HandlerOutcome::retry_after`](../components/settlement/retry-after.md)（[重试与死信队列](retries.md)） |
| 在控制台中设置的 `RedrivePolicy` | 挂载处的 `max_attempts` 和 [`dead_letter`](../components/settlement/dead-letters.md) |
| `send_message` | 回复或 `Out` 参数，经由 [`SqsPublish`](../components/publishing/index.md) 发送 |
| `message_group_id` 和 `message_deduplication_id` | [`SqsPublishSteps`](api:ruststream_sqs_sns::SqsPublishSteps) 的发布步骤 [`group_id`](../components/publishing/group-id.md) 和 [`deduplication_id`](../components/publishing/deduplication-id.md)（[FIFO 队列](fifo.md)） |
