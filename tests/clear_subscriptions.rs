//! clear_all_subscriptions and clear_subscriptions behavior.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::PubSub;

#[test]
fn clear_all_removes_every_subscription() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe(&topic, spy1.subscriber());
    bus.subscribe(&topic, spy2.subscriber());

    bus.clear_all_subscriptions();

    let _ = bus.publish_sync(&topic, unique_string());

    assert!(!spy1.called());
    assert!(!spy2.called());
}

// clear_subscriptions prefix-deletes by raw string prefix. Pin that behavior.
#[test]
fn clear_subscriptions_prefix_deletes() {
    let bus: PubSub<String> = PubSub::new();
    let spy_t = Spy::new();
    let spy_ta = Spy::new();
    let spy_tab = Spy::new();
    let spy_other = Spy::new();
    bus.subscribe("t", spy_t.subscriber());
    bus.subscribe("t.a", spy_ta.subscriber());
    bus.subscribe("t.a.b", spy_tab.subscriber());
    bus.subscribe("other", spy_other.subscriber());

    bus.clear_subscriptions("t");

    let _ = bus.publish_sync("t", unique_string());
    let _ = bus.publish_sync("t.a", unique_string());
    let _ = bus.publish_sync("t.a.b", unique_string());
    let _ = bus.publish_sync("other", unique_string());

    assert!(!spy_t.called());
    assert!(!spy_ta.called());
    assert!(!spy_tab.called());
    assert!(spy_other.called());
}

// The match is a raw string prefix, not dot-boundary aware. clearing "a"
// removes "ab" as well as "a.b".
#[test]
fn clear_subscriptions_is_raw_prefix_not_dot_aware() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("a", |_, _| {});
    bus.subscribe("a.b", |_, _| {});
    bus.subscribe("ab", |_, _| {});
    bus.subscribe("b", |_, _| {});

    bus.clear_subscriptions("a");

    assert_eq!(bus.get_subscriptions("").len(), 1);
    assert_eq!(bus.get_subscriptions(""), vec!["b".to_string()]);
}
