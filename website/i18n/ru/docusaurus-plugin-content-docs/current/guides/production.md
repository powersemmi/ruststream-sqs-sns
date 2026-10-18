---
sidebar_position: 8
description: "Сервис на SQS в AWS: учётные данные и регион из окружения или из собственной конфигурации, тайм-ауты и нужные ему права IAM."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Промышленная эксплуатация

Это руководство запускает сервис на SQS в AWS: откуда берутся его учётные данные и регион, с какими тайм-аутами он работает и какие права IAM ему нужны.

## Учётные данные и регион из окружения {/* #credentials-and-region-from-the-environment */}

`SqsBroker::new()` ничего не берёт из кода.
При старте сервиса регион и [учётные данные](../components/broker/credentials.md) находит цепочка AWS SDK по умолчанию: переменные окружения, общие файлы `~/.aws`, SSO и роль контейнера или инстанса.
Сервису, который работает под ролью IAM в ECS, EKS, Lambda или EC2, конфигурация вообще не нужна.

SDK читает учётные данные, когда подписывает первый запрос.
Этот запрос - `GetQueueUrl` первой подписки при старте, поэтому сервис с неверными учётными данными останавливается раньше, чем обработает сообщение.

## Собственная конфигурация {/* #a-configuration-of-your-own */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#config"
```

</TabItem>
</Tabs>

[`SqsBroker::from_config`](api:ruststream_sqs_sns::SqsBroker::from_config) принимает `SdkConfig`, который собирает сервис.
`aws_config` собирает его из окружения асинхронным загрузчиком, а функция приложения синхронна.
Поэтому пример собирает конфигурацию через `SdkConfig::builder()`, который не выполняет ввода-вывода.
Поставщик профилей читает профиль `orders-service` при первом запросе; профиль с `role_arn` принимает эту роль.
Закреплённая версия поведения не даёт умолчаниям SDK измениться при обновлении SDK.
Брокер использует конфигурацию как есть, поэтому конфигурация задаёт и тайм-аут запроса, описанный [ниже](#timeouts-and-transport).

Примеру нужны ещё две зависимости:

```toml title="Cargo.toml"
[dependencies]
aws-config = "1"
aws-sdk-sqs = "1"
```

## Сервис {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

Очередь `orders` относится к инфраструктуре: её создают Terraform, CloudFormation или консоль, вместе с политикой перенаправления и шифрованием.
Дескриптор опускает `create_if_missing`, поэтому отсутствующая очередь останавливает сервис при старте, а не создаётся пустой.
Он называет свою видимость, поэтому подписка не читает собственный тайм-аут очереди.

У брокера, собранного через `SqsBroker::new()`, [`endpoint`](../components/broker/endpoint.md) и [`region`](api:ruststream_sqs_sns::SqsBroker::region) направляют его на другую конечную точку, конечную точку VPC или локальный стенд.
Собственная конфигурация задаёт их через `endpoint_url` и `region` в билдере `SdkConfig`.
Адреса очередей, которые возвращает SQS, переносятся на эту конечную точку.

## Тайм-ауты и транспорт {/* #timeouts-and-transport */}

`SqsBroker::new()` даёт каждому запросу к SQS 25 секунд на попытку, больше 20 секунд, которые может ждать длинный опрос.
Собственная конфигурация задаёт это через `timeout_config`, как в примере; тайм-аут попытки меньше 20 секунд обрывал бы каждый длинный опрос.
Неудавшийся запрос SDK повторяет по собственной политике повторов.

Запросы идут через HTTPS-клиент SDK по умолчанию.
Обработчик на выделенном потоке (`threads(n)`) отправляет запросы через собственные клиенты SDK, собранные из той же конфигурации и разделяющие её учётные данные.

## Права IAM {/* #iam-permissions */}

Сервису выше нужны такие права на его очередь:

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

Каждая возможность добавляет свои:

| Возможность | Права |
|---|---|
| подписка, которая не называет видимость | `sqs:GetQueueAttributes` на очереди |
| `max_attempts` и `dead_letter` | `sqs:SetQueueAttributes` на очереди, `sqs:GetQueueAttributes` на DLQ |
| `create_if_missing` | `sqs:CreateQueue` |
| публикация в очередь | `sqs:GetQueueUrl` и `sqs:SendMessage` на очереди |
| публикация в тему | `sns:CreateTopic` и `sns:Publish` на теме |
| `subscribe_queue_to_topic` | `sns:CreateTopic` и `sns:Subscribe` на теме, `sqs:GetQueueUrl`, `sqs:GetQueueAttributes` и `sqs:SetQueueAttributes` на очереди |

## Что показывает документ AsyncAPI {/* #what-the-asyncapi-document-shows */}

С фичей `asyncapi` документ AsyncAPI сервиса называет [один сервер](../components/asyncapi/server.md): хост из `endpoint` брокера или `sqs.amazonaws.com`.
Документ никогда не содержит учётных данных или ARN очереди, поэтому его можно публиковать.

Тесты в файле примера запускают то же приложение внутри процесса; SQS внутри процесса берёт регион из конфигурации и ничего не подписывает.
Обвязку разбирает руководство [Тестирование сервиса на SQS](testing.md).
