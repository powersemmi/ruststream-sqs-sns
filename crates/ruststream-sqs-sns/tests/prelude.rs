//! The shape of the crate prelude, pinned.
//!
//! The glob carries the framework's own prelude, and the bare capability names in it stay the
//! framework's. A publish policy re-exported under one of those names would win over the glob
//! silently - an explicit re-export beats a glob import - and only a use site like the bound
//! below would report it, as `E0404: expected trait, found struct`.

use ruststream_sqs_sns::prelude::*;

/// `Publish` is the framework's slot capability, the bound a handler body writes on an injected
/// publisher. This crate's policies keep their prefixed names so it stays reachable.
fn _publish_is_the_frameworks_slot_capability<T: Publish>() {}

#[test]
fn the_publish_policies_keep_their_prefixed_names() {
    // Both policies are unit structs, so naming them is the whole construction; what is under
    // test is that the prefixed names are the ones the glob resolves.
    let _ = (SqsPublish, SnsPublish);
}
