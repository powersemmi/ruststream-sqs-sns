---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 SQS Broker ruststream-sqs-sns 构建 Amazon SQS 服务的指南，并可发布到 SNS 主题。"
---

# 指南

`ruststream-sqs-sns` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 Amazon SQS Broker，
基于官方客户端 [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs) 构建。服务读写 SQS 队列，启用 `sns` feature 后还能发布到 SNS 主题。
这些指南带你把一个 SQS 服务从安装一路推进到生产环境。
