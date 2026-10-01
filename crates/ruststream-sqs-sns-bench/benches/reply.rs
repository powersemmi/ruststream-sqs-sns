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
//! Replying: the handler returns a value, the runtime encodes it and hands it to the publisher
//! this crate's default `SqsPublish` policy pairs, which sends it to the queue the reply type
//! declares with a `SendMessage` of its own, and the delivery is acked as in the consume
//! scenario.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqs_sns::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it. The name is
/// `common::REPLIES`, the queue each run creates on the stand.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(SqsQueue::new(common::input()), publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(confirm);
    })
}

// The longest run allocated between 1,745,550 and 1,745,553 blocks over four runs. The floor is
// the highest plus a tenth of a percent, 1,747,299, stated over a thousand deliveries; one more
// allocation per message is 2,000 more blocks and exceeds it.
#[library_benchmark(config = common::config_every(871_300, 1_000, 4_699))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
