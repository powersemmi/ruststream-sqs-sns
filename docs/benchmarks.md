# Benchmarks

This crate's consumer and publisher sit between the SQS client and your code, and the runtime sits
above them. Both cost time on every message: the receive pump, the delivery it hands over, the
delete behind its `ack`, the dispatch into your handler. This page says how much of each, measured
against the same work written by hand on `aws-sdk-sqs`.

One process runs the same scenario three times over, as three loops that differ in one thing each:
what carries the messages.

- **Raw client** - `aws-sdk-sqs`, driven directly by a loop in the benchmark.
- **This crate** - the same loop, on this crate's broker, subscription, delivery, `ack` and
  publisher; no handler, no router, no dispatch.
- **Full service** - the service a user writes: `#[subscriber]`, the app, the runtime.

So the two overhead columns answer two questions. **Crate over raw** is what this crate's consumer
and publisher cost over the client they wrap, which is the question this repository is responsible
for. **Service over raw** is what the whole stack costs; the gap between the two columns is the
runtime's share over this broker, and it is published here because a share that differs from broker
to broker is a fact about how the two meet rather than about the core.

Everything else is held equal across the three - the AWS configuration, the receive parameters, the
wait the receive asks for, the delete per message and where it happens, the decode into the same
type, the body bytes, the tokio runtime and the build. The procedure is the framework's own and is
described [under Methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology).

The stand is the LocalStack emulator the live suites use, not the hosted service. Read every figure
below as a statement about what a delivery costs inside this crate, and about nothing else.

## The numbers

Medians over interleaved pairs, with the observed spread in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "Full service", "adapterOverhead": "Crate over raw", "overhead": "Service over raw", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

Both rows are marked broker-bound, and that is the important thing on this page. A delivery costs a
`DeleteMessage` of its own, because that is what this crate's `ack` does, plus a tenth of the
`ReceiveMessage` that carried its batch over. The run measures the round trip to the
stand separately and publishes it below, and those round trips account for most of what a delivery
took. The framework's work therefore happened inside a wait the raw client was already paying, which
makes the difference between the two halves a lower bound on what dispatch costs rather than a
measurement of it.

A row reported as `indistinguishable` is one whose two halves differ by less than the spread between
runs of either. A figure below the run-to-run noise would read as precision that was never measured,
so none is published.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-sqs-sns/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

The stand is an emulator. It speaks the SQS and SNS APIs over plain HTTP on the loopback, which
leaves out the TLS handshake, the credential refresh and the latency of a region. The pair stays
valid, because both halves talk to the same emulator and the difference between them is what is
published. The saturation profile does not: a queue in a region answers on another timescale, and
none of these figures says what a service costs there.

Both halves ask for a 20-second wait, the crate's default and the protocol maximum. On a queue that
always has a backlog the wait never elapses, because a receive answers as soon as it has anything to
answer with - but it decides what an empty queue costs, and a pair whose halves disagreed about it
would be publishing a polling policy rather than a dispatch cost.

This is one consumer, one queue, a small body and an emulator on the loopback. It measures what a
delivery costs in this crate, and a row here is not comparable with a row published for another
broker: the transports do different work per message.

The window a run measures opens at the first delivery and closes when the last one has been read,
on both halves alike, so the delete that settles it sits outside the number on both sides. That is
one delete out of the thousands a run carries.

The FIFO figure is taken on a queue whose bodies are spread over 64 message groups. A FIFO queue
holds a group back while one of its deliveries is in flight, so a run over a handful of groups would
be measuring that hold; with this many, a receive always has untouched groups to answer from.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about ten minutes and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.
