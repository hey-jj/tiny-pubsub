//! Behavior the source suite leaves untested but the contract requires:
//! original-message argument, wildcard ordering, once re-entrancy, delivery
//! order, edge topics, and a reference-checked hierarchy property.

mod common;

use common::Spy;
use std::cell::RefCell;
use std::rc::Rc;
use tiny_pubsub::PubSub;

// An ancestor subscriber receives the original leaf topic, not the ancestor it
// matched.
#[test]
fn ancestor_subscriber_receives_original_message() {
    let bus: PubSub<String> = PubSub::new();
    let spy_a = Spy::new();
    let spy_abc = Spy::new();
    bus.subscribe("a", spy_a.subscriber());
    bus.subscribe("a.b.c", spy_abc.subscriber());

    let _ = bus.publish_sync("a.b.c", "payload".into());

    assert!(spy_a.called_with_message("a.b.c"));
    assert!(spy_abc.called_with_message("a.b.c"));
}

// The wildcard fires after the hierarchy walk, and makes publish report
// subscribers even when only the wildcard is registered.
#[test]
fn wildcard_fires_last_and_counts_for_publish_return() {
    let bus: PubSub<String> = PubSub::new();

    // Wildcard alone makes any publish report a subscriber.
    bus.subscribe_all(|_, _| {});
    assert!(bus.publish_sync("anything", String::new()));

    // Ordering: a record-shared spy across "x" and "*" shows "x" first.
    let bus2: PubSub<&'static str> = PubSub::new();
    let order = Rc::new(RefCell::new(Vec::<&'static str>::new()));
    let o1 = order.clone();
    bus2.subscribe("x", move |_, _| o1.borrow_mut().push("x"));
    let o2 = order.clone();
    bus2.subscribe_all(move |_, _| o2.borrow_mut().push("star"));

    let _ = bus2.publish_sync("x", "");
    assert_eq!(*order.borrow(), vec!["x", "star"]);
}

// A once-handler that republishes its own topic must not retrigger itself.
#[test]
fn subscribe_once_is_not_reentrant() {
    let bus: Rc<PubSub<String>> = Rc::new(PubSub::new());
    let count = Rc::new(RefCell::new(0u32));

    let bus_inner = bus.clone();
    let count_inner = count.clone();
    bus.subscribe_once("topic", move |_, _| {
        *count_inner.borrow_mut() += 1;
        // Re-publish synchronously from inside the once-handler.
        let _ = bus_inner.publish_sync("topic", String::new());
    });

    let _ = bus.publish_sync("topic", String::new());
    assert_eq!(*count.borrow(), 1);
}

// Two subscribers on one topic fire in subscription order.
#[test]
fn delivery_preserves_subscription_order() {
    let bus: PubSub<&'static str> = PubSub::new();
    let order = Rc::new(RefCell::new(Vec::<&'static str>::new()));
    let o1 = order.clone();
    bus.subscribe("topic", move |_, _| o1.borrow_mut().push("first"));
    let o2 = order.clone();
    bus.subscribe("topic", move |_, _| o2.borrow_mut().push("second"));

    let _ = bus.publish_sync("topic", "");
    assert_eq!(*order.borrow(), vec!["first", "second"]);
}

// A topic with no dot delivers to itself then the wildcard.
#[test]
fn no_dot_topic_delivers_self_then_wildcard() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    let star = Spy::new();
    bus.subscribe("plain", spy.subscriber());
    bus.subscribe_all(star.subscriber());

    let _ = bus.publish_sync("plain", String::new());
    assert!(spy.called_once());
    assert!(star.called_once());
}

// The empty topic is a valid key. Publishing it reaches its own subscriber.
#[test]
fn empty_topic_is_valid() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("", spy.subscriber());
    let _ = bus.publish_sync("", String::new());
    assert!(spy.called_once());
}

// A trailing-dot topic trims to the same string minus the dot, then on up.
// "a." delivers to "a.", "a", then the wildcard.
#[test]
fn trailing_dot_topic_walk() {
    let bus: PubSub<String> = PubSub::new();
    let spy_dot = Spy::new();
    let spy_a = Spy::new();
    bus.subscribe("a.", spy_dot.subscriber());
    bus.subscribe("a", spy_a.subscriber());

    let _ = bus.publish_sync("a.", String::new());
    assert!(spy_dot.called_once());
    assert!(spy_a.called_once());
}

// Reference-checked hierarchy property over a fixed topic set. The fired set
// must equal the published topic's subscribers plus every strict-ancestor's
// plus the wildcard, computed independently.
#[test]
fn hierarchy_matches_independent_reference() {
    let all_topics = ["a", "a.b", "a.b.c", "a.b.c.d", "a.x", "a.x.y", "z", "*"];
    let published = ["a.b.c.d", "a.b", "a", "a.x.y", "z", "q.r"];

    for pub_topic in published {
        let bus: PubSub<String> = PubSub::new();
        // One spy per subscribed topic, keyed by topic name.
        let spies: Vec<(&str, Spy)> = all_topics.iter().map(|t| (*t, Spy::new())).collect();
        for (topic, spy) in &spies {
            bus.subscribe(*topic, spy.subscriber());
        }

        let _ = bus.publish_sync(pub_topic, String::new());

        // Reference set: the published topic, all strict ancestors, plus "*".
        let expected = expected_matches(pub_topic);
        for (topic, spy) in &spies {
            let want = expected.contains(&topic.to_string());
            assert_eq!(
                spy.called(),
                want,
                "topic {topic} should fire={want} when publishing {pub_topic}"
            );
        }
    }
}

/// Independent reference: the set of topic strings that should fire for a
/// published topic. Splits on dots, takes every prefix, adds the wildcard.
fn expected_matches(published: &str) -> Vec<String> {
    let mut out = vec![published.to_string()];
    let mut topic = published.to_string();
    while let Some(pos) = topic.rfind('.') {
        topic.truncate(pos);
        out.push(topic.clone());
    }
    out.push("*".to_string());
    out
}
