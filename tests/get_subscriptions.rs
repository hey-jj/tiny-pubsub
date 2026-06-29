//! get_subscriptions behavior: list topic names by prefix, in insertion order.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::PubSub;

#[test]
fn lists_one_matching_topic() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    bus.subscribe(&topic, Spy::new().subscriber());
    assert_eq!(bus.get_subscriptions(&topic).len(), 1);
}

// Returns every prefix-matching topic name in insertion order. The source suite
// never exercises the multi-topic case, so pin it here.
#[test]
fn lists_all_matching_topics_in_insertion_order() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("t", Spy::new().subscriber());
    bus.subscribe("t.a", Spy::new().subscriber());
    bus.subscribe("t.a.b", Spy::new().subscriber());
    bus.subscribe("other", Spy::new().subscriber());

    assert_eq!(
        bus.get_subscriptions("t"),
        vec!["t".to_string(), "t.a".to_string(), "t.a.b".to_string()]
    );
}

#[test]
fn lists_nothing_when_no_prefix_match() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("library", Spy::new().subscriber());
    assert!(bus.get_subscriptions("music").is_empty());
}
