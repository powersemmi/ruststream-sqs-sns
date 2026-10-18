---
title: 用 SNS 扇出
sidebar_position: 6
description: "把回复向一个 SNS 主题发布一次，让 SNS 把它投递给订阅该主题的每个 SQS 队列。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 用 SNS 扇出

本指南把一条消息向一个 SNS 主题发布一次，让 SNS 把一份副本投递给订阅该主题的每个 SQS 队列。

SNS 到 SQS 是 AWS 服务做扇出的方式：一个主题，每个消费者一个队列。
本 crate 向主题发布；主题背后的每个队列按普通方式读取。
向 SNS 发布需要 `sns` feature：

```toml title="Cargo.toml"
[dependencies]
ruststream-sqs-sns = { version = "0.7", features = ["sns"] }
```

示例是一个商店。
它接受订单并在一个主题上发布通知，计费和发货各自从自己的队列读取这条通知。

## 发往主题的回复 {/* #a-reply-to-a-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#accept"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#accept"
```

</TabItem>
</Tabs>

`accept` 用一个 `OrderPlaced` 回复每个订单，这个类型指定了去向：`shop-order-events`。
这个名称是队列还是主题，由挂载处决定。

## 主题背后的队列 {/* #the-queues-behind-the-topic */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#consumers"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#consumers"
```

</TabItem>
</Tabs>

`bill` 和 `ship` 是普通的队列处理器。
每个队列都得到每条通知的一份副本，所以计费和发货按各自的节奏工作，各自重试。

## 为队列建立订阅 {/* #subscribing-the-queues */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#topology"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#topology"
```

</TabItem>
</Tabs>

[`subscribe_queue_to_topic`](../components/sns/subscribe-queue-to-topic.md) 让一个队列以原始消息传输方式订阅一个主题。
因此副本作为普通的 SQS 消息到达：载荷就是正文，每个消息头就是一个消息属性。
不启用原始消息传输时，SNS 会把消息包装在它自己的 JSON 信封里。
主题不存在时，这个调用会创建它；再次为同一个队列建立订阅不会改变任何东西。

拓扑通常属于基础设施，用 Terraform 或控制台创建。
这里由一个启动钩子在应用 Broker 的克隆上完成，克隆使用应用已经持有的连接。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/fan_out.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/fan_out.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(SnsPublish::default())` 把回复绑定到 [`SnsPublish`](../components/sns/index.md)，回复就变成向主题发出的一次 SNS `Publish`。
没有这一步时，同样的回复走 Broker 的默认策略 [`SqsPublish`](api:ruststream_sqs_sns::SqsPublish)，落到名为 `shop-order-events` 的队列上。
`SnsPublish` 用 `CreateTopic` 按名称打开主题，主题已存在时返回已有的主题；它也接受主题的 ARN 作为名称。
同一个策略可以用在 `Out` 槽和启动钩子里，[`ConnectedSqsBroker::sns_publisher`](api:ruststream_sqs_sns::ConnectedSqsBroker::sns_publisher) 则在应用之外提供一个发布者。

名称以 `.fifo` 结尾的主题是 FIFO 主题。
它把一次发布的消息组带给订阅它的 FIFO 队列，使用的是 FIFO 队列所用的那些[组步骤](fifo.md)。
FIFO 队列只能订阅 FIFO 主题。

服务发布需要 `sns:CreateTopic` 和 `sns:Publish` 权限，自己为队列建立订阅时还需要 `sns:Subscribe`。

## 测试扇出 {/* #testing-the-fan-out */}

示例文件把同一个测试主体运行两次。
在进程内，进程内账户把主题的消息投递给用 `subscribe_queue_to_topic` 订阅的队列，所以钩子为测试连接拓扑的方式与生产环境相同。
运维人员在服务之外订阅的队列，用 [`fans_out_to`](../components/sns/fans-out-to.md) 在策略上指定，这是 `testing` feature 提供的步骤。
设置了 `SQS_TEST_ENDPOINT` 时，同一个主体通过 `TestApp::start_live` 在 LocalStack 上运行：

```bash
cargo test --features sns
SQS_TEST_ENDPOINT=http://localhost:4566 cargo test --features sns -- --test-threads=1
```

[测试 SQS 服务](testing.md)介绍测试工具和线上模式。
