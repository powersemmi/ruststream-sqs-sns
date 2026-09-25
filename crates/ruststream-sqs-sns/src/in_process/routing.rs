//! What the test harness asks a connected broker after a publish: which of the app's
//! subscriptions the publish reaches.
//!
//! The answer follows the publisher. A publish through [`SqsPublisher`](crate::SqsPublisher)
//! reaches the queue its name addresses; one through [`SnsPublisher`](crate::SnsPublisher)
//! reaches every queue subscribed to the topic, those the service subscribed with
//! `subscribe_queue_to_topic` and those its policy names with `fans_out_to`, and nothing else.
//! Each publisher a policy paired notes which of the two it went through, per destination.
//!
//! The harness hands over only the destination's name, once per publish it recorded, in publish
//! order, and again from the start on every pass of a settle. Where every publish to a name went
//! through one publisher the answer is fixed. Where a queue and a topic share the name and the
//! test published to both, a call cannot say which of those publishes it is about, so the notes
//! keep a count per surface and a turn that walks them round: every pass over the publishes of
//! the name hands out each surface as many times as it was published to, whichever publish the
//! pass starts at. A publish the harness never records (one from a publisher the service built
//! itself rather than a policy paired) is never noted. One the harness does not record although a
//! policy paired its publisher (made outside a task the harness drives) is noted all the same,
//! and for a name both surfaces share it tips the count.

use std::collections::HashMap;
use std::sync::Mutex;

/// Which of the crate's publishers a publish went through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Surface {
    /// `SendMessage` to a queue.
    Queue,
    /// `Publish` to a topic.
    Topic,
}

/// The publishes to one destination, by surface, and how many times the harness asked.
#[derive(Debug, Default)]
struct Tally {
    queue: usize,
    topic: usize,
    asked: usize,
}

impl Tally {
    fn count(&mut self, surface: Surface) {
        match surface {
            Surface::Queue => self.queue += 1,
            Surface::Topic => self.topic += 1,
        }
    }
}

/// What a connected broker knows about where its publishes went, on either transport: a live
/// test waits on the same answer an in-process one does.
#[derive(Debug, Default)]
pub(crate) struct Routing {
    /// The queues this broker subscribed to each topic, by topic key.
    topics: Mutex<HashMap<String, Vec<String>>>,
    /// The paired publishes, by destination as the publish named it.
    publishes: Mutex<HashMap<String, Tally>>,
}

impl Routing {
    /// Notes that the queue keyed `queue` is subscribed to the topic keyed `topic`.
    pub(crate) fn subscribe(&self, topic: String, queue: String) {
        let mut topics = self
            .topics
            .lock()
            .expect("the topic record mutex is poisoned");
        let queues = topics.entry(topic).or_default();
        if !queues.contains(&queue) {
            queues.push(queue);
        }
        drop(topics);
    }

    /// The queues subscribed to the topic keyed `topic`, when it has any.
    pub(crate) fn fanned_out(&self, topic: &str) -> Option<Vec<String>> {
        self.topics
            .lock()
            .expect("the topic record mutex is poisoned")
            .get(topic)
            .filter(|queues| !queues.is_empty())
            .cloned()
    }

    /// Notes one publish to `destination` through `surface`.
    pub(crate) fn published(&self, destination: &str, surface: Surface) {
        let mut publishes = self
            .publishes
            .lock()
            .expect("the publish record mutex is poisoned");
        // A destination already noted is counted without allocating its name again.
        if let Some(tally) = publishes.get_mut(destination) {
            tally.count(surface);
        } else {
            publishes
                .entry(destination.to_owned())
                .or_default()
                .count(surface);
        }
        drop(publishes);
    }

    /// The surface the harness's next question about `destination` is about, or `None` when no
    /// paired publisher sent anything there.
    pub(crate) fn next_surface(&self, destination: &str) -> Option<Surface> {
        let mut publishes = self
            .publishes
            .lock()
            .expect("the publish record mutex is poisoned");
        let tally = publishes.get_mut(destination)?;
        let total = tally.queue + tally.topic;
        if total == 0 {
            return None;
        }
        let turn = tally.asked % total;
        tally.asked = tally.asked.wrapping_add(1);
        let surface = if turn < tally.queue {
            Surface::Queue
        } else {
            Surface::Topic
        };
        drop(publishes);
        Some(surface)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_pass_hands_out_each_surface_as_often_as_it_was_published_to() {
        let routing = Routing::default();
        routing.published("events", Surface::Queue);
        routing.published("events", Surface::Topic);
        routing.published("events", Surface::Topic);
        // The first question lands mid-pass, as it does when a settle starts before the last
        // record: the passes after it still see one queue publish and two topic publishes each.
        let _ = routing.next_surface("events");
        for _ in 0..4 {
            let pass: Vec<Surface> = (0..3)
                .filter_map(|_| routing.next_surface("events"))
                .collect();
            let queue = pass.iter().filter(|s| **s == Surface::Queue).count();
            assert_eq!((queue, pass.len() - queue), (1, 2));
        }
        assert_eq!(routing.next_surface("orders"), None);
    }
}
