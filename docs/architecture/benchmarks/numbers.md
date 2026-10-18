---
title: Numbers
description: "What the benchmark numbers of ruststream-sqs-sns measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-sqs-sns` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, against one endpoint. A row is not comparable with a row another broker crate
publishes: the transports do different work per message.

Every message costs an HTTP round trip of its own, the `DeleteMessage` behind its `ack`. Those round
trips set the pace of both rows. In a `broker-bound` row they took at least half of the time a
delivery took. The work of the crate and the runtime then happens inside a wait the raw client pays
as well, and the difference between the loops is a lower bound on that work, not a measurement of
it.

## The cost of the code

An instruction count holds the runtime, this crate and the AWS SDK's work on the service's thread:
building and signing each request, the HTTP exchange and parsing each answer. A delivery pays its
share of a `ReceiveMessage` and a `DeleteMessage` of its own, and a reply adds a `SendMessage`. So
most of every figure is the SDK's work rather than this crate's, and the figures are not comparable
with those of a broker whose client works on threads of its own. The same count costs more time
where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Loading the AWS configuration, resolving the queue, reading its visibility timeout and the first
allocations behind them are the cold start, paid once.

## What they do not mean

The stand is LocalStack, an emulator of SQS and SNS, not the hosted service. It answers over plain
HTTP on the loopback, which leaves out the TLS handshake, the credential refresh and the latency of
a region. The comparison stays valid, because all three loops talk to the same emulator. The
throughput does not carry over: a queue in a region answers on another timescale.

All three loops ask for a 20-second wait. On a queue with a backlog the wait never elapses, because
a receive answers as soon as it has messages. The wait decides only what an empty queue costs.

A row is one consumer reading one queue with a small body. It measures what a delivery costs inside
this crate, not what SQS can carry. The FIFO row spreads its messages over 64 message groups, so a
receive always finds a group that is not held.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
