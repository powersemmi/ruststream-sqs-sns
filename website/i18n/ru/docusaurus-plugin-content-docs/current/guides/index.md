---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на Amazon SQS с ruststream-sqs-sns, брокером SQS для RustStream, с публикацией в темы SNS."
---

# Руководства

Крейт `ruststream-sqs-sns` реализует брокер Amazon SQS для [RustStream](https://powersemmi.github.io/ruststream/latest/ru/).
Он построен на официальном клиенте [`aws-sdk-sqs`](https://docs.rs/aws-sdk-sqs). Сервис читает
очереди SQS и пишет в них, а с фичей `sns` ещё и публикует в темы SNS. Руководства проводят
сервис на SQS от установки до промышленной эксплуатации.
Начните с руководства [Начало работы](getting-started.md).
