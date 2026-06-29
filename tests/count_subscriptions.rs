//! count_subscriptions behavior, including the first-match-then-stop quirk.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::PubSub;

#[test]
fn counts_one_subscription() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    bus.subscribe(&topic, Spy::new().subscriber());
    assert_eq!(bus.count_subscriptions(&topic), 1);
}

#[test]
fn counts_two_subscriptions_on_same_topic() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    bus.subscribe(&topic, Spy::new().subscriber());
    bus.subscribe(&topic, Spy::new().subscriber());
    assert_eq!(bus.count_subscriptions(&topic), 2);
}

// count_subscriptions counts only the first topic whose name matches the
// prefix, then stops. It does not sum across the hierarchy. Pin that behavior.
#[test]
fn counts_only_first_prefix_matching_topic_then_stops() {
    let bus: PubSub<String> = PubSub::new();
    // Two subscribers on "t", one on "t.a". Prefix "t" matches "t" first.
    bus.subscribe("t", Spy::new().subscriber());
    bus.subscribe("t", Spy::new().subscriber());
    bus.subscribe("t.a", Spy::new().subscriber());

    // Counts the two on "t", not the three across both buckets.
    assert_eq!(bus.count_subscriptions("t"), 2);
}

#[test]
fn counts_zero_when_no_prefix_match() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("library", Spy::new().subscriber());
    assert_eq!(bus.count_subscriptions("music"), 0);
}
