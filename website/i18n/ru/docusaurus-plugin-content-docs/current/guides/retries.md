---
sidebar_position: 4
description: "Переобработка сообщения SQS сразу или позже, предел доставок и перенос в DLQ того, что раз за разом не обрабатывается, силами самого SQS."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Переобработка и DLQ

Это руководство обрабатывает сообщение SQS заново сразу или позже, ограничивает число его доставок и поручает SQS переносить в DLQ то, что раз за разом не обрабатывается.

Обе задачи SQS решает сам.
Переобработка - это то же сообщение, снова сделанное видимым, с сохранённым счётчиком получений.
Предел и DLQ - это политика перенаправления очереди, и объявляет её точка монтирования.

Пример - сервис счетов, который списывает оплату счетов с клиентов.

## Переобработка сейчас и позже {/* #retry-now-and-retry-later */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#settle"
```

</TabItem>
</Tabs>

Счёт выше кредитного лимита ждёт проверки кредита.
[`retry_after`](../components/settlement/retry-after.md) задаёт тайм-аут видимости сообщения 30 секунд, и по их истечении SQS выдаёт его повторно.
SQS считает целые секунды, поэтому доля секунды округляется вверх.
SQS скрывает сообщение не дольше двенадцати часов с момента получения, поэтому задержка заканчивается в их пределах.
Более долгая задержка сокращается до конца этих двенадцати часов, и крейт пишет в журнал предупреждение с именем очереди.
Всё это время сообщение остаётся в своей очереди, поэтому отложенная переобработка переживает перезапуск сервиса.

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) задаёт нулевую видимость, и сообщение сразу выдаётся повторно.
Счёт без клиента при каждой попытке получает один и тот же ответ, поэтому `charge` его отбрасывает.

## Предел и DLQ {/* #the-cap-and-the-dead-letter-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) и [`dead_letter`](../components/settlement/dead-letters.md) вместе образуют политику перенаправления очереди.
Предел становится её `maxReceiveCount`, а DLQ - её целью.
Подписка записывает политику в очередь при открытии.
С этого момента SQS сам считает получения и переносит сообщение, у которого они кончились; сервис ничего не публикует.
Здесь счёт доставляется четыре раза, а на пятом получении SQS переносит его в `invoices-dead`.

Два шага задаются вместе.
Предел без DLQ или DLQ без предела останавливают сервис при старте с ошибкой [`SqsError::IncompleteRedrive`](../components/broker/errors.md), которая называет недостающую половину.
SQS принимает `maxReceiveCount` не больше 1000.

DLQ должна существовать, когда открывается подписка, если только её не создаёт дескриптор приложения с `create_if_missing`.
Она должна быть того же вида, что и её очередь: очереди FIFO нужна DLQ типа FIFO.
Для записи политики нужны `sqs:SetQueueAttributes` на очереди и `sqs:GetQueueAttributes` на DLQ.

`drop` удаляет сообщение.
На очереди с пределом `drop` доставки, которая израсходовала последнее получение, вместо удаления возвращает сообщение, и SQS переносит его в DLQ, а не теряет.

## Чтение DLQ {/* #reading-the-dead-letter-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#review"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#review"
```

</TabItem>
</Tabs>

DLQ - обычная очередь, и `review` читает её, как любую другую.
Сообщение приходит таким, каким его отправили, со своими атрибутами.
Очередь без читателя хранит сообщения до конца срока хранения, поэтому человек может посмотреть их и в консоли.

## Тестирование задержки {/* #testing-the-delay */}

Тесты в файле примера запускают приложение внутри процесса на остановленных часах, поэтому `tb.advance(CREDIT_CHECK)` пропускает 30 секунд мгновенно.
Один тест проводит счёт через четыре доставки в `invoices-dead`, другой проверяет, что предел без DLQ не запускается.
Обвязку разбирает руководство [Тестирование сервиса на SQS](testing.md).
