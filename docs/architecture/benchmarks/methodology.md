---
title: Methodology
description: "How ruststream-sqs-sns measures itself: the three loops, the run on an SQS endpoint, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-sqs-sns` measures itself: the three loops, the run on an SQS endpoint, the
broker-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on SQS.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `aws-sdk-sqs`, driven directly: receive ten messages per call, decode each one,
  delete each one. One receive stays in flight while the loop settles the batch it holds, as this
  crate's pump does.
- **Broker crate.** This crate and nothing above it: the broker connects, the
  [`SqsQueue`](api:ruststream_sqs_sns::SqsQueue) descriptor opens a subscription, and a loop pulls
  deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the AWS configuration, the 25-second attempt timeout,
the receive of ten messages with a 20-second wait, a `DeleteMessage` per message, the decode into
the same struct, the body bytes, the tokio runtime with four worker threads, and the binary.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh queue per run.** The queue is created before the run and deleted after it, so a run
  never sees what an earlier one left.
- **The consumer attaches first.** Eight publishers then fill the queue, one `SendMessage` per
  message, and stay at most 4096 messages ahead of the consumer.
- **The window opens at the first delivery and closes when the last one is handled**, in all three
  loops. Creating the queue, building the clients and opening the subscription are outside it.
- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when round trips to the endpoint account for at least half of the time a
delivery took. That is arithmetic. A probe times the smallest request the endpoint answers, on a
client of its own, over 2000 calls. A delivery costs one round trip for its `DeleteMessage` and a
tenth of the `ReceiveMessage` that carried its batch. The row is flagged when their product reaches
half the measured time per message. The round trip is published with the machine, so the
arithmetic can be checked.

## The code count

Each scenario of the [cost chart](/architecture/benchmarks/) is the service a user writes, on
[`SqsBroker`](api:ruststream_sqs_sns::SqsBroker) against the same endpoint, built without the
`testing` feature and started on a single-threaded runtime. Another thread fills the queue before
the measured region opens, and that thread is not counted.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery, then
1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

The count covers everything the service's thread runs: the runtime, this crate and the AWS SDK with
its HTTP stack. Threads the SDK runs on its own are not counted, and neither is the endpoint's side
of a round trip. Four runs of one binary agree within half a percent on instructions, because how
the endpoint answers and how the socket's reads split move a little between runs. A run against a
saved baseline fails on two percent more instructions in a scenario. Allocations are held to a
floor each scenario declares, the highest count observed plus a tenth of a percent, on every run.

## Running it yourself

```bash
just bench
```

The recipe starts the LocalStack stand from `docker-compose.test.yml`, runs both comparison
scenarios, stops the stand and rewrites `docs/benchmarks/results.json`. It takes a few minutes and
wants the machine to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was
built on. Both recipes build the benchmarks the way a release is built, with fat link-time
optimization across crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes a few minutes and needs valgrind. `just bench-code 5000` measures every
scenario over five thousand deliveries instead of a thousand. `just bench-code --save-baseline=main`
on `main` records a baseline, and `just bench-code --baseline=main` on a branch compares with it.
