// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Consuming in batches of ten, the most one `ReceiveMessage` returns: one receive is one batch,
//! the handler gets a slice, and the runtime acks every delivery in it with a `DeleteMessage` of
//! its own.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqs_sns::prelude::*;

#[subscriber(SqsQueue::new(common::input()))]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume.batch(nonzero!(10)));
    })
}

// The longest run allocated 921,930 blocks in each of four runs. The floor is that plus a tenth
// of a percent, 922,852, stated over a thousand deliveries; one more allocation per message is
// 2,000 more blocks and exceeds it.
#[library_benchmark(config = common::config_every(461_300, 1_000, 252))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
