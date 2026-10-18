---
title: 在生产环境中运行
sidebar_position: 8
description: "在 AWS 上运行 SQS 服务：凭证和区域来自环境或你自己的配置、超时，以及它所需的 IAM 权限。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

本指南在 AWS 上运行一个 SQS 服务：它的凭证和区域从哪里来，它使用的超时，以及它所需的 IAM 权限。

## 来自环境的凭证和区域 {/* #credentials-and-region-from-the-environment */}

`SqsBroker::new()` 不从代码中获取任何东西。
服务启动时，AWS SDK 的默认链查找区域和[凭证](../components/broker/credentials.md)：环境变量、共享的 `~/.aws` 文件、SSO，以及容器或实例的角色。
在 ECS、EKS、Lambda 或 EC2 上以 IAM 角色运行的服务完全不需要配置。

SDK 在签名第一个请求时读取凭证。
这个请求就是第一个订阅在启动时发出的 `GetQueueUrl`，所以凭证错误的服务会在处理任何消息之前停止。

## 你自己的配置 {/* #a-configuration-of-your-own */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#config"
```

</TabItem>
</Tabs>

[`SqsBroker::from_config`](api:ruststream_sqs_sns::SqsBroker::from_config) 接收服务构建的 `SdkConfig`。
`aws_config` 用一个 `async` 加载器从环境构建配置，而应用函数是同步的。
所以示例用不做 I/O 的 `SdkConfig::builder()` 构建它。
profile 提供者在第一次请求时读取 `orders-service` profile；带有 `role_arn` 的 profile 会承担该角色。
固定的行为版本让 SDK 更新时，服务所用的 SDK 默认值不随之改变。
Broker 原样使用这份配置，所以配置还要携带[下文](#timeouts-and-transport)所说的请求超时。
示例还需要两个依赖：

```toml title="Cargo.toml"
[dependencies]
aws-config = "1"
aws-sdk-sqs = "1"
```

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`orders` 队列属于基础设施：由 Terraform、CloudFormation 或控制台创建，连同它的重新驱动策略和加密设置。
描述符不用 `create_if_missing`，所以缺失的队列会让服务在启动时停止，而不是创建一个空队列。
描述符指定了可见性，所以订阅不读取队列自己的超时。

对于用 `SqsBroker::new()` 构建的 Broker，[`endpoint`](../components/broker/endpoint.md) 和 [`region`](api:ruststream_sqs_sns::SqsBroker::region) 让它指向另一个端点、VPC 端点或本地环境。
你自己的配置在 `SdkConfig` 构建器上用 `endpoint_url` 和 `region` 指定它们。
SQS 返回的队列 URL 会被改写到该端点上。

## 超时与传输 {/* #timeouts-and-transport */}

`SqsBroker::new()` 给每个 SQS 请求每次尝试 25 秒，高于长轮询最多等待的 20 秒。
你自己的配置用 `timeout_config` 设置它，示例就是这样做的；低于 20 秒的尝试超时会让每次长轮询都中途结束。
SDK 按自己的重试策略重试失败的请求。
请求通过 SDK 默认的 HTTPS 客户端发送。
专用线程上的处理器（`threads(n)`）通过自己的 SDK 客户端发送，这些客户端由同一份配置构建，共用它的凭证。

## IAM 权限 {/* #iam-permissions */}

上面的服务需要在它的队列上拥有这些权限：

```json title="policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": "arn:aws:sqs:eu-west-1:123456789012:orders"
    }
  ]
}
```

每项功能都会增加自己的权限：

| 功能 | 权限 |
|---|---|
| 没有指定可见性的订阅 | 队列上的 `sqs:GetQueueAttributes` |
| `max_attempts` 和 `dead_letter` | 队列上的 `sqs:SetQueueAttributes`，死信队列上的 `sqs:GetQueueAttributes` |
| `create_if_missing` | `sqs:CreateQueue` |
| 向队列发布 | 队列上的 `sqs:GetQueueUrl` 和 `sqs:SendMessage` |
| 向主题发布 | 主题上的 `sns:CreateTopic` 和 `sns:Publish` |

## AsyncAPI 文档展示什么 {/* #what-the-asyncapi-document-shows */}

启用 `asyncapi` feature 时，服务的 AsyncAPI 文档指定一个[服务器](../components/asyncapi/server.md)：Broker 的 `endpoint` 所在主机，或者 `sqs.amazonaws.com`。
文档从不包含凭证或队列的 ARN，因此可以公开发布。

示例文件中的测试在进程内启动同一个应用；进程内 SQS 从配置中获取区域，不签名任何东西。
[测试 SQS 服务](testing.md)介绍测试工具。
