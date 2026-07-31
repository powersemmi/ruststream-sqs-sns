//! Amazon SQS broker implementation for `RustStream`, with SNS fan-out publishing.
//!
//! This crate is not implemented yet. The design and scope are tracked in
//! [powersemmi/ruststream#189](https://github.com/powersemmi/ruststream/issues/189).
//! The broker contract it will implement (lazy startup, the typed connect/shutdown
//! lifecycle, and the optional capability traits) is defined by
//! [`ruststream`](https://docs.rs/ruststream) and verified by `ruststream::conformance`.

#![forbid(unsafe_code)]
