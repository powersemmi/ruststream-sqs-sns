//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod asyncapi;
mod both_modes_sns;
mod both_modes_sqs;
mod conformance_sqs;
mod connect_runtime;
mod dead_letter_sqs;
mod fifo_sqs;
mod handlers;
mod in_process_sqs;
mod integration_sqs;
mod prelude;
mod sns_fanout_sqs;
mod sns_queue_policy;
mod testing_core;
