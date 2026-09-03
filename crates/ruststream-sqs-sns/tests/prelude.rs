//! The shape of the crate prelude, pinned.
//!
//! A service writes two kinds of file, and each globs one prelude. A handler file globs the
//! framework's, where the capability names are the framework's traits. A routes file globs this
//! one, where the uniform mount-site names are this broker's policy values. Both spellings below
//! have to resolve through this glob: the framework's capability, because the glob carries the
//! framework's prelude, and the policy under the uniform name, because this crate aliases it
//! there.

use ruststream_sqs_sns::prelude::*;

/// The framework's publish capability still reaches a routes file through this glob: the bound an
/// injected publisher carries, which a handler file names on the framework's prelude instead.
fn _p<T: Publisher>() {}

#[test]
fn the_uniform_mount_site_name_is_this_brokers_policy() {
    // The value a mount site or a lifecycle hook hands over. The policy is a unit struct, so
    // naming it is the whole construction, and the annotation is what pins the alias down: a
    // trait under this name would not be accepted in type position.
    let _: Publish = Publish;
    // Fan-out is the departure from the default, so it keeps its own name beside the uniform one.
    let _ = SnsPublish;
}
