//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber, its publishers and its delivery each carry this
//! transport as a variant of their own, so a service's routes, descriptors and publish policies
//! run against it unchanged. It has no configuration of its own: the queue URLs are built on the
//! broker's endpoint and region, and every setting a queue has comes from the service's
//! descriptors, as it does on AWS. It answers the service calls the crate makes, and a message
//! is framed for it by the same conversion a live publish goes through, so the crate's own logic
//! (the FIFO settings ladder, the base64 lane, the discard that returns a spent delivery) runs
//! over it as it runs over AWS. It never succeeds where the service fails: a message the service
//! refuses (an empty body, more than ten attributes, an attribute name it does not take, an empty
//! attribute value, over the size limit), a queue name over 80 characters, a redrive policy SQS
//! refuses (a dead-letter queue of the other kind, a `maxReceiveCount` over 1000), a FIFO queue
//! subscribed to a standard topic, and a publish after `shutdown` are refused here with the error
//! a live call reports.
//!
//! What it models: a queue hands each message to one receiver among its subscriptions; a receive
//! returns up to ten messages; a received message stays invisible until it is deleted, its
//! visibility is changed, or its handle is dropped and the visibility timeout lapses; the receive
//! count, and the redrive policy that moves a message whose receives ran out to the dead-letter
//! queue on the receive after; FIFO queues, where a group with a message in flight hands out
//! nothing and a deduplication id is remembered for five minutes; with the `sns` feature, SNS
//! topics, which deliver a copy to every queue subscribed to them. What belongs to the service and
//! is left to the live mode: a queue's storage while no subscription reads it (a message sent to
//! such a queue is dropped here), the visibility timeout an operator set on a queue (a descriptor
//! that names none gets the 30 seconds SQS gives a new queue), a topology the service expects to
//! find but did not create (every queue and topic it names exists here), the ordering of a standard
//! queue, which SQS does not keep and this transport does, and credentials.

mod bus;
mod deliveries;
#[cfg(feature = "sns")]
mod routing;

use std::sync::Arc;

use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{BytesMut, HeaderMap, OutgoingFor, OutgoingMessage, RawMessage, Take};

pub(crate) use bus::Bus;
#[cfg(feature = "sns")]
use bus::topic_key;
use bus::{DEFAULT_VISIBILITY, Outbound, queue_key};
pub(crate) use deliveries::{BusDeliveries, BusReceipt};
#[cfg(feature = "sns")]
pub(crate) use routing::{Routing, Surface};

use crate::SqsPublishOptions;
use crate::broker::{ConnectedSqsBroker, Transport};
use crate::error::SqsError;
use crate::message::{encode_attributes, encode_body};
use crate::publisher::{fifo_settings, is_fifo};
use crate::queue::SqsQueue;
use crate::subscriber::SqsSubscriber;

/// Opens the subscription `queue` describes on the account, writing its redrive policy first.
///
/// # Errors
///
/// Returns [`SqsError::Queue`] for a queue name SQS refuses and a redrive policy SQS refuses, and
/// [`SqsError::IncompleteRedrive`] for half a declaration.
pub(crate) fn subscribe(bus: &Arc<Bus>, queue: &SqsQueue) -> Result<SqsSubscriber, SqsError> {
    let refused = |reason: String| SqsError::Queue {
        name: queue.queue().to_owned(),
        source: reason.into(),
    };
    // `create_if_missing` has nothing to add: every queue the service names exists here.
    let key = queue_key(queue.queue()).map_err(refused)?;
    let redrive = queue.redrive()?;
    if let Some(redrive) = &redrive {
        bus.set_redrive(&key, redrive.max_receive_count, &redrive.dead_letter)
            .map_err(refused)?;
    }
    let visibility = queue.visibility_value().unwrap_or(DEFAULT_VISIBILITY);
    Ok(SqsSubscriber::in_process(BusDeliveries::open(
        bus,
        key,
        visibility,
        redrive.map(|redrive| redrive.max_receive_count),
    )))
}

/// Subscribes the queue `queue` to the topic `topic` on the account.
///
/// # Errors
///
/// Returns [`SqsError::Admin`] for a topic name SNS refuses and a FIFO queue on a standard
/// topic, and [`SqsError::Queue`] for a queue name SQS refuses.
#[cfg(feature = "sns")]
pub(crate) fn subscribe_topic(bus: &Bus, topic: &str, queue: &str) -> Result<(), SqsError> {
    let admin = |reason: String| SqsError::Admin {
        name: topic.to_owned(),
        source: reason.into(),
    };
    let topic_key = topic_key(topic).map_err(admin)?;
    let queue_key = queue_key(queue).map_err(|reason| SqsError::Queue {
        name: queue.to_owned(),
        source: reason.into(),
    })?;
    bus.subscribe_topic(&topic_key, &queue_key, queue)
        .map_err(admin)
}

/// Frames one message the way a live publish frames it: the body, the attributes the headers
/// become, and the FIFO settings the crate's ladder resolves.
fn frame(
    destination: &str,
    fifo: bool,
    payload: BytesMut,
    headers: &HeaderMap,
    options: Option<&SqsPublishOptions>,
    default_group: Option<&str>,
) -> Result<Outbound, SqsError> {
    let (body, base64_marker) = encode_body(payload);
    let (attributes, partition_key) = encode_attributes(headers, base64_marker);
    let fifo = fifo_settings(destination, fifo, options, partition_key, default_group)?;
    Ok(Outbound {
        body,
        attributes,
        fifo,
    })
}

/// `SendMessage` of one message to the queue it names.
///
/// # Errors
///
/// Returns [`SqsError::Queue`] for a queue name SQS refuses, [`SqsError::NotFifo`] for a FIFO
/// setting on a standard queue, and [`SqsError::Publish`] for a message SQS refuses.
pub(crate) fn send(
    bus: &Bus,
    msg: OutgoingFor<'_, Take>,
    options: Option<&SqsPublishOptions>,
    default_group: Option<&str>,
) -> Result<(), SqsError> {
    let key = queue_key(msg.name()).map_err(|reason| SqsError::Queue {
        name: msg.name().to_owned(),
        source: reason.into(),
    })?;
    let (destination, payload, headers) = msg.into_parts();
    let outbound = frame(
        destination,
        is_fifo(&key),
        payload,
        &headers,
        options,
        default_group,
    )?;
    bus.send(destination, &key, outbound)
        .map_err(|reason| SqsError::Publish {
            destination: destination.to_owned(),
            source: reason.into(),
        })
}

/// `Publish` of one message to the topic it names.
///
/// # Errors
///
/// Returns [`SqsError::Admin`] for a topic name SNS refuses, [`SqsError::NotFifo`] for a FIFO
/// setting on a standard topic, and [`SqsError::Publish`] for a message SNS refuses.
#[cfg(feature = "sns")]
pub(crate) fn publish_topic(
    bus: &Bus,
    msg: OutgoingFor<'_, Take>,
    options: Option<&SqsPublishOptions>,
    default_group: Option<&str>,
) -> Result<(), SqsError> {
    let key = topic_key(msg.name()).map_err(|reason| SqsError::Admin {
        name: msg.name().to_owned(),
        source: reason.into(),
    })?;
    let (destination, payload, headers) = msg.into_parts();
    let outbound = frame(
        destination,
        is_fifo(&key),
        payload,
        &headers,
        options,
        default_group,
    )?;
    bus.publish_topic(destination, &key, &outbound)
        .map_err(|reason| SqsError::Publish {
            destination: destination.to_owned(),
            source: reason.into(),
        })
}

impl ConnectedSqsBroker {
    /// Notes that `queue` is subscribed to `topic`, for [`TestableBroker::routes`].
    #[cfg(feature = "sns")]
    pub(crate) fn record_topic_queue(&self, topic: &str, queue: &str) {
        if let (Ok(topic), Ok(queue)) = (topic_key(topic), queue_key(queue)) {
            self.core.routing.subscribe(topic, queue);
        }
    }

    /// The queues a publish to `destination` reaches: the topic's subscribed queues when the
    /// publish went to a topic of that name, otherwise the queue the name addresses.
    // Without `sns` a name only ever addresses a queue, so the broker's records have nothing to
    // add; the method keeps its receiver for the build that reads them.
    #[cfg_attr(not(feature = "sns"), allow(clippy::unused_self))]
    fn reached_queues(&self, destination: &str) -> Vec<String> {
        #[cfg(feature = "sns")]
        if let Some(queues) = topic_key(destination)
            .ok()
            .and_then(|topic| self.core.routing.fanned_out(&topic))
            .filter(|_| self.core.routing.next_surface(destination) != Some(Surface::Queue))
        {
            return queues;
        }
        queue_key(destination).into_iter().collect()
    }

    /// The in-process account, which is all the harness drives.
    fn bus(&self, what: &str) -> &Bus {
        match &self.core.transport {
            Transport::InProcess(bus) => bus,
            Transport::Aws(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the connection `connect_in_process` produces"
            ),
        }
    }
}

/// The harness's view of the in-process account: what it injects, what it reads back, the
/// coordinator it counts the in-flight deliveries with, and where a publish goes.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the connection `connect_in_process` produced, and AWS has no log to read. `inject` also panics
/// on a message SQS refuses, which is a test's mistake, not the service's.
impl TestableBroker for ConnectedSqsBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Transport::InProcess(bus) = &self.core.transport {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let bus = self.bus("inject");
        let (name, payload, headers) = message.into_parts();
        // An external producer sends to the queue the name addresses, with the group SQS would
        // need on a FIFO queue coming from the message's partition key or the default one.
        let sent = queue_key(name)
            .map_err(|reason| SqsError::Queue {
                name: name.to_owned(),
                source: reason.into(),
            })
            .and_then(|key| {
                let outbound = frame(
                    name,
                    is_fifo(&key),
                    BytesMut::from(payload),
                    &headers,
                    None,
                    None,
                )?;
                bus.send(name, &key, outbound)
                    .map_err(|reason| SqsError::Publish {
                        destination: name.to_owned(),
                        source: reason.into(),
                    })
            });
        if let Err(err) = sent {
            panic!("the injected message to {name:?} is not one SQS takes: {err}");
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published").published(name)
    }

    /// SQS and SNS routing. A destination that names a topic this broker subscribed queues to
    /// reaches those queues (with the `sns` feature); any other destination names a queue. A
    /// queue hands each message to one receiver, so of the subscriptions that read one queue the
    /// first is the one owed it. Two names reach the same queue when they map onto the same queue
    /// name: a queue URL by its last path segment, a name through the alphabet SQS takes
    /// (`order.events` is `order-events`).
    ///
    /// A queue and a topic may carry one name. The publisher tells them apart: each publish a
    /// paired [`SqsPublisher`](crate::SqsPublisher) sent reaches the queue, each one a paired
    /// `SnsPublisher` sent reaches the topic's queues, and the harness, which asks once per
    /// publish on every pass, is answered with each as many times as it happened.
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        let queues = self.reached_queues(destination);
        let mut positions: Vec<usize> = queues
            .iter()
            .filter_map(|queue| {
                subscriptions
                    .iter()
                    .position(|name| queue_key(name).as_ref() == Ok(queue))
            })
            .collect();
        positions.sort_unstable();
        positions.dedup();
        positions
    }
}
