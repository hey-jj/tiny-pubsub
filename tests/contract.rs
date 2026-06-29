//! Contract points the other suites leave unpinned: token shape and counter
//! lifetime, emptied-topic observability, the immediate-exceptions flag read
//! and capture, deferred ordering and depth, and the deferred-drain semantics
//! for re-entry and panics.

mod common;

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use tiny_pubsub::PubSub;

// Token format is exactly uid_0, uid_1, ... and the counter is global.
#[test]
fn token_format_is_uid_zero_based() {
    let bus: PubSub<String> = PubSub::new();
    assert_eq!(bus.subscribe("a", |_, _| {}).as_str(), "uid_0");
    assert_eq!(bus.subscribe("a", |_, _| {}).as_str(), "uid_1");
}

// clear_all_subscriptions empties the registry but does not reset the counter.
#[test]
fn token_counter_survives_clear_all() {
    let bus: PubSub<String> = PubSub::new();
    assert_eq!(bus.subscribe("a", |_, _| {}).as_str(), "uid_0");
    assert_eq!(bus.subscribe("a", |_, _| {}).as_str(), "uid_1");
    bus.clear_all_subscriptions();
    // The next token continues the sequence rather than restarting at uid_0.
    assert_eq!(bus.subscribe("a", |_, _| {}).as_str(), "uid_2");
}

// Removing the last subscriber leaves the topic key present but empty.
#[test]
fn emptied_topic_key_persists_and_is_observable() {
    let bus: PubSub<String> = PubSub::new();
    let tok = bus.subscribe("a", |_, _| {});
    bus.unsubscribe(&tok);

    // No live subscriber, so a publish reports false.
    assert!(!bus.publish_sync("a", String::new()));
    // The key still lists and still counts as a topic.
    assert_eq!(bus.get_subscriptions("a"), vec!["a".to_string()]);
    assert!(bus.unsubscribe_topic("a"));
    // The emptied topic counts zero subscribers.
    let bus2: PubSub<String> = PubSub::new();
    let tok2 = bus2.subscribe("a", |_, _| {});
    bus2.unsubscribe(&tok2);
    assert_eq!(bus2.count_subscriptions("a"), 0);
}

// count_subscriptions stops at the first prefix match, even when that first
// topic is emptied.
#[test]
fn count_breaks_on_first_prefix_match_even_when_empty() {
    let bus: PubSub<String> = PubSub::new();
    let tok = bus.subscribe("t", |_, _| {});
    bus.subscribe("t.a", |_, _| {});
    bus.subscribe("t.a", |_, _| {});

    // Empty "t" but keep its key first in insertion order.
    bus.unsubscribe(&tok);

    // The scan stops at "t" with zero, never reaching the two on "t.a".
    assert_eq!(bus.count_subscriptions("t"), 0);
}

// get_subscriptions("") and count_subscriptions("") use the empty prefix, which
// matches every topic.
#[test]
fn empty_prefix_matches_every_topic() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("a", |_, _| {});
    bus.subscribe("b", |_, _| {});
    bus.subscribe("b", |_, _| {});

    let mut all = bus.get_subscriptions("");
    all.sort();
    assert_eq!(all, vec!["a".to_string(), "b".to_string()]);
    // Counts only the first matching topic, then stops.
    assert_eq!(bus.count_subscriptions(""), 1);
}

// immediate_exceptions is read fresh on each publish. Toggling it changes only
// later publishes.
#[test]
fn immediate_exceptions_read_per_publish() {
    let bus: PubSub<String> = PubSub::new();
    let ran = Rc::new(Cell::new(0u32));
    bus.subscribe("t", |_, _| panic!("boom"));
    let r = ran.clone();
    bus.subscribe("t", move |_, _| r.set(r.get() + 1));

    // Default delayed: peer after the panicking subscriber still runs.
    let r1 = catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.publish_sync("t", String::new());
    }));
    assert!(r1.is_err());
    assert_eq!(ran.get(), 1);

    // Immediate: peer does not run.
    bus.set_immediate_exceptions(true);
    let r2 = catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.publish_sync("t", String::new());
    }));
    assert!(r2.is_err());
    assert_eq!(ran.get(), 1);

    // Back to delayed: peer runs again.
    bus.set_immediate_exceptions(false);
    let r3 = catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.publish_sync("t", String::new());
    }));
    assert!(r3.is_err());
    assert_eq!(ran.get(), 2);
}

// A deferred publish captures the flag at publish time, not at drain time.
#[test]
fn deferred_job_captures_immediate_flag_at_publish() {
    let bus: PubSub<String> = PubSub::new();
    let ran = Rc::new(Cell::new(0u32));
    bus.subscribe("t", |_, _| panic!("boom"));
    let r = ran.clone();
    bus.subscribe("t", move |_, _| r.set(r.get() + 1));

    // Publish under delayed mode, then switch to immediate before draining.
    let _ = bus.publish("t", String::new());
    bus.set_immediate_exceptions(true);

    let drained = catch_unwind(AssertUnwindSafe(|| bus.process_deferred()));
    assert!(drained.is_err());
    // Delayed semantics captured at publish: the peer after the panic ran.
    assert_eq!(ran.get(), 1);
}

// Multiple deferred publishes queue and drain in call order. pending reports the
// queue depth.
#[test]
fn deferred_delivery_is_fifo_with_depth() {
    let bus: PubSub<u32> = PubSub::new();
    let seen = Rc::new(std::cell::RefCell::new(Vec::<u32>::new()));
    let s = seen.clone();
    bus.subscribe("t", move |_, n| s.borrow_mut().push(*n));

    let _ = bus.publish("t", 1);
    let _ = bus.publish("t", 2);
    let _ = bus.publish("t", 3);
    assert_eq!(bus.pending(), 3);

    bus.process_deferred();
    assert_eq!(*seen.borrow(), vec![1, 2, 3]);
    assert_eq!(bus.pending(), 0);
}

// publish and publish_sync return true when an ancestor matches.
#[test]
fn ancestor_match_returns_true() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("a", |_, _| {});
    assert!(bus.publish_sync("a.b.c", String::new()));
    assert!(bus.publish("a.b.c", String::new()));
}

// publish returns false when no level matches, even with unrelated topics
// present.
#[test]
fn no_match_returns_false_among_unrelated_topics() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("x", |_, _| {});
    bus.subscribe("y.z", |_, _| {});
    assert!(!bus.publish_sync("q.r", String::new()));
    assert!(!bus.publish("q.r", String::new()));
}

// Under delayed exceptions one panicking deferred job does not abort the rest.
// Every job runs and the queue drains to empty.
#[test]
fn deferred_panic_does_not_abort_other_jobs() {
    let bus: PubSub<()> = PubSub::new();
    let ran = Rc::new(Cell::new(false));
    bus.subscribe("x", |_, _| panic!("boom"));
    let r = ran.clone();
    bus.subscribe("y", move |_, _| r.set(true));

    let _ = bus.publish("x", ());
    let _ = bus.publish("y", ());

    let drained = catch_unwind(AssertUnwindSafe(|| bus.process_deferred()));
    // The first job's panic still surfaces.
    assert!(drained.is_err());
    // The second job ran and the queue is empty.
    assert!(ran.get());
    assert_eq!(bus.pending(), 0);
}

// A subscriber that republishes during the drain queues its job for the next
// call, not the current one.
#[test]
fn deferred_republish_runs_on_next_tick() {
    let bus: Rc<PubSub<u32>> = Rc::new(PubSub::new());
    let depth = Rc::new(Cell::new(0u32));

    let bus_inner = bus.clone();
    let depth_inner = depth.clone();
    bus.subscribe("t", move |_, n| {
        depth_inner.set(*n);
        if *n < 3 {
            // Republish a deeper level from inside the deferred delivery.
            let _ = bus_inner.publish("t", n + 1);
        }
    });

    let _ = bus.publish("t", 1);

    // First drain runs only the job queued before the call.
    bus.process_deferred();
    assert_eq!(depth.get(), 1);
    assert_eq!(bus.pending(), 1);

    // Each further drain advances exactly one level.
    bus.process_deferred();
    assert_eq!(depth.get(), 2);
    assert_eq!(bus.pending(), 1);

    bus.process_deferred();
    assert_eq!(depth.get(), 3);
    assert_eq!(bus.pending(), 0);
}
