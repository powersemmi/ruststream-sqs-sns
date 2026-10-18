---
sidebar_position: 7
description: "Тест сервиса на SQS внутри процесса через TestApp: приложение из промышленного кода, ответы, задержки на остановленных часах и тот же тест на LocalStack."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на SQS

Тест запускает собственное приложение сервиса через [`TestApp`](../components/testing/index.md) на SQS внутри процесса или на запущенном стенде.

Тестируемый сервис - оформление заказа.
Заказ приходит в `checkout-orders`, `confirm` отвечает на него подтверждением в `checkout-confirmations`, а `notify` его читает.
Заказ без суммы отбрасывается.
Заказ, которого нет на складе, ждёт час через `retry_after` и возвращается на повторную проверку.

Приложение собирает `service(broker)`, поэтому тест выбирает брокер сам, а `main` использует промышленный.
Тестам нужна фича `testing` крейта `ruststream-sqs-sns` в `[dev-dependencies]`.

## Заказ и его подтверждение {/* #an-order-and-its-confirmation */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`SqsBroker`](api:ruststream_sqs_sns::SqsBroker) к SQS внутри процесса.
Никакая конечная точка не вызывается, и учётные данные не нужны.
Тест обращается к брокеру по его промышленному типу, `tb.broker::<SqsBroker>()`.

`publish` возвращается, когда обработчики, до которых дошла публикация, завершили работу, включая ответ и его читателя.
Поэтому следующие строки проверяют уже готовое состояние: как `confirm` завершил заказ и что получил `notify`.
Тело принимает запущенное приложение аргументом, поэтому его выполняет и тест на стенде ниже.

## Пустой заказ {/* #an-empty-order */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что подтверждение не публиковалось.

## Задержка без ожидания {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#delay"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#delay"
```

</TabItem>
</Tabs>

Тест стартует на остановленных часах, и [`advance`](api:ruststream::testing::TestApp::advance) переводит их вперёд.
Час `RESTOCK` ничего не стоит.
`retry_after` делает сообщение невидимым на время задержки, как это делает SQS, и очередь выдаёт его повторно, когда часы её пройдут.
`published` показывает один заказ в `checkout-orders`: повторная доставка - это то же сообщение, а не копия.

## Что моделирует SQS внутри процесса {/* #what-the-in-process-sqs-models */}

SQS внутри процесса - это аккаунт внутри теста, и в нём существует каждая очередь и каждая тема, которую называет сервис.
Он моделирует то, от чего зависит логика сервиса:

- Очередь отдаёт каждое сообщение одной из своих подписок, а полученное сообщение остаётся невидимым, пока его не удалят или не истечёт его тайм-аут видимости.
- Счётчик получений растёт с каждым получением, а политика перенаправления переносит в DLQ сообщение, у которого кончились получения.
- Пакет содержит не больше десяти сообщений, которые возвращает одно получение.
- Группа FIFO ждёт, пока одно из её сообщений в обработке, а идентификатор дедупликации запоминается на пять минут.
- С фичей `sns` тема доставляет копию в каждую очередь, подписанную через `subscribe_queue_to_topic`.

То, что отвергают SQS или SNS, отвергается и там: больше десяти заголовков, имя заголовка, которого SQS не принимает, пустое тело или значение заголовка, сообщение больше предельного размера, имя очереди длиннее 80 символов, DLQ другого вида, `maxReceiveCount` больше 1000, видимость, которая закончилась бы позже двенадцати часов после получения, и очередь FIFO на стандартной теме.

Стандартная очередь там сохраняет порядок сообщений, чего SQS не обещает.
Дескриптор, который не называет видимость, получает 30 секунд - значение по умолчанию для новой очереди.
Сообщение, отправленное в очередь, которую не читает ни одна подписка, отбрасывается.
Видимость, которую оператор задал очереди, очереди и темы, которые сервис ожидает найти, и учётные данные проверяются на запущенном стенде.

## Тот же тест на запущенном стенде {/* #the-same-test-on-a-running-stack */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к запущенному SQS.
Тест читает конечную точку из `SQS_TEST_ENDPOINT` через `live::endpoint`, вспомогательную функцию самого примера, и пропускается, если переменная не задана.
Сначала он удаляет очереди сервиса, чтобы не учитывать то, что в них оставил прошлый прогон; при старте сервис создаёт их заново.
Проверки те же, что внутри процесса, без изменений.

```bash
cargo test
SQS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
