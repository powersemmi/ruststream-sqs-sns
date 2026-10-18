---
sidebar_position: 1
description: "Установка ruststream-sqs-sns, локальный SQS, первый подписчик на нём и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-sqs-sns`, запускает локальный SQS и первый сервис на нём.

## Установка {/* #installation */}

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

`ruststream` - это фреймворк: `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-sqs-sns` - брокер SQS, построенный на официальном клиенте `aws-sdk-sqs`.
Его фича `testing` добавляет [SQS внутри процесса](../components/testing/index.md), на котором работают тесты.

У крейта брокера есть такие фичи, все выключены по умолчанию:

| Фича | Что даёт |
|---|---|
| `sns` | публикацию в темы SNS через [`SnsPublish`](../components/sns/index.md) поверх `aws-sdk-sns` ([Доставка всем подписчикам разом через SNS](fan-out.md)) |
| `asyncapi` | привязки `sqs` и `sns` в генерируемом [документе AsyncAPI](../components/asyncapi/index.md) |
| `testing` | SQS внутри процесса для `TestApp` |

## Локальный SQS {/* #a-local-sqs */}

[LocalStack](https://github.com/localstack/localstack) запускает SQS и SNS в одном контейнере:

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

Стенд отвечает на `http://localhost:4566` и стартует без очередей.

## Первый сервис {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из очереди `orders`.
Заказ с суммой обработчик подтверждает, а заказ без суммы отбрасывает.
Дескриптор [`SqsQueue`](../components/subscriptions/sqs-queue.md) называет очередь, а [`create_if_missing`](../components/subscriptions/create-if-missing.md) создаёт её при открытии подписки.
Сервис в промышленной эксплуатации этот шаг опускает и получает очереди от своей инфраструктуры.
[`SqsBroker::new`](../components/broker/index.md) только записывает конфигурацию.
[`endpoint`](../components/broker/endpoint.md), [`region`](api:ruststream_sqs_sns::SqsBroker::region) и [`test_credentials`](api:ruststream_sqs_sns::SqsBroker::test_credentials) направляют его на локальный стенд.
Клиенты SDK собираются при старте сервиса.
`#[ruststream::app]` превращает `app` в `main`, а в ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Отправьте заказ из другого терминала через AWS CLI:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws sqs send-message \
  --queue-url "$(aws sqs get-queue-url --queue-name orders --output text)" \
  --message-body '{"id":1,"amount_cents":1250}'
```

Сервис пишет в журнал `order taken` с идентификатором заказа.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест {/* #the-first-test */}

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` его завершил.
`SqsBroker` подключается там к SQS внутри процесса, поэтому стенд тестам не нужен:

```bash
cargo test
```

Эти тесты и то, что моделирует SQS внутри процесса, разбирает руководство [Тестирование сервиса на SQS](testing.md).

## Переход с aws-sdk-sqs {/* #coming-from-aws-sdk-sqs */}

Сервис на `aws-sdk-sqs` пишет собственный цикл получения.
Вот куда в RustStream переходит каждая его часть:

| aws-sdk-sqs | RustStream |
|---|---|
| `aws_config::load_from_env` и `Client::new` | [`SqsBroker::new`](api:ruststream_sqs_sns::SqsBroker::new) или [`SqsBroker::from_config`](../components/broker/credentials.md) с собственным `SdkConfig` ([Промышленная эксплуатация](production.md)) |
| `get_queue_url` | имя очереди в [`SqsQueue::new`](api:ruststream_sqs_sns::SqsQueue::new), которое разрешается один раз при старте |
| цикл `receive_message().wait_time_seconds(20)` | обработчик `#[subscriber]` ([Чтение очередей](consuming.md)) |
| десериализация `message.body()` | тип параметра обработчика, который декодирует кодек |
| `delete_message` после работы | исход обработчика, [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) ([Подтверждение и гарантии доставки](acknowledgement.md)) |
| `change_message_visibility` | [`HandlerOutcome::retry_after`](../components/settlement/retry-after.md) ([Переобработка и DLQ](retries.md)) |
| `RedrivePolicy`, заданная в консоли | [`max_attempts` и `dead_letter`](../components/settlement/dead-letters.md) в точке монтирования |
| `send_message` | ответ или параметр `Out`, отправленный через [`SqsPublish`](../components/publishing/index.md) |
| `message_group_id` и `message_deduplication_id` | шаги публикации [`SqsPublishSteps`](api:ruststream_sqs_sns::SqsPublishSteps): [группа](../components/publishing/group-id.md) и [идентификатор дедупликации](../components/publishing/deduplication-id.md) ([Очереди FIFO](fifo.md)) |
