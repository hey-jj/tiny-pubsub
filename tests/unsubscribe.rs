//! Unsubscribe behavior in both modes: by token and by topic.
//!
//! Rust closures cannot be compared by identity, so removal by function value
//! has no analogue. The token covers single-subscription removal. The topic
//! mode removes a topic and all its descendants by string prefix.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::PubSub;

#[test]
fn token_mode_returns_token_when_successful() {
    let bus: PubSub<String> = PubSub::new();
    let token = bus.subscribe(unique_string(), |_, _| {});
    assert_eq!(bus.unsubscribe(&token), Some(token.clone()));
}

#[test]
fn token_mode_returns_none_when_unsuccessful() {
    let bus: PubSub<String> = PubSub::new();

    let token = bus.subscribe(unique_string(), |_, _| {});

    // Remove once, then a second removal of the same token finds nothing.
    bus.unsubscribe(&token);
    assert_eq!(bus.unsubscribe(&token), None);
}

#[test]
fn token_mode_removes_only_its_own_subscription() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let spy_keep = Spy::new();
    let spy_drop = Spy::new();
    bus.subscribe(&topic, spy_keep.subscriber());
    let drop_token = bus.subscribe(&topic, spy_drop.subscriber());

    bus.unsubscribe(&drop_token);

    let _ = bus.publish_sync(&topic, unique_string());
    assert!(spy_keep.called());
    assert!(!spy_drop.called());
}

#[test]
fn token_removal_leaves_other_topics_intact() {
    let bus: PubSub<String> = PubSub::new();
    let topic_a = unique_string();
    let topic_b = unique_string();
    let spy_a = Spy::new();
    let spy_b = Spy::new();
    let token_a = bus.subscribe(&topic_a, spy_a.subscriber());
    bus.subscribe(&topic_b, spy_b.subscriber());

    bus.unsubscribe(&token_a);

    // The other topic key survives and its subscriber still fires.
    assert_eq!(bus.get_subscriptions(&topic_b), vec![topic_b.clone()]);
    let _ = bus.publish_sync(&topic_b, unique_string());
    assert!(spy_b.called());
}

#[test]
fn topic_mode_clears_exact_matches() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe(&topic, spy1.subscriber());
    bus.subscribe(&topic, spy2.subscriber());

    assert!(bus.unsubscribe_topic(&topic));

    let _ = bus.publish_sync(&topic, unique_string());

    assert!(!spy1.called());
    assert!(!spy2.called());
}

#[test]
fn topic_mode_returns_false_on_empty_bus() {
    let bus: PubSub<String> = PubSub::new();
    assert!(!bus.unsubscribe_topic(&unique_string()));
}

#[test]
fn topic_mode_clears_only_matched() {
    let bus: PubSub<String> = PubSub::new();
    let topic1 = unique_string();
    let topic2 = unique_string();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe(&topic1, spy1.subscriber());
    bus.subscribe(&topic2, spy2.subscriber());

    bus.unsubscribe_topic(&topic1);

    let _ = bus.publish_sync(&topic1, unique_string());
    let _ = bus.publish_sync(&topic2, unique_string());

    assert!(!spy1.called());
    assert!(spy2.called());
}

#[test]
fn topic_mode_clears_hierarchical_descendants() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let topic_a = format!("{topic}.a");
    let topic_b = format!("{topic}.a.b");
    let topic_c = format!("{topic}.a.b.c");
    let spy_a = Spy::new();
    let spy_b = Spy::new();
    let spy_c = Spy::new();
    bus.subscribe(&topic_a, spy_a.subscriber());
    bus.subscribe(&topic_b, spy_b.subscriber());
    bus.subscribe(&topic_c, spy_c.subscriber());

    bus.unsubscribe_topic(&topic_b);

    let _ = bus.publish_sync(&topic_c, unique_string());

    assert!(spy_a.called());
    assert!(!spy_b.called());
    assert!(!spy_c.called());
}

#[test]
fn parent_topic_clears_child_subscriptions() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let topic_a = format!("{topic}.a");
    let topic_b = format!("{topic}.a.b");
    let topic_c = format!("{topic}.a.b.c");
    let spy_b = Spy::new();
    let spy_c = Spy::new();

    // Subscribe only to children.
    bus.subscribe(&topic_b, spy_b.subscriber());
    bus.subscribe(&topic_c, spy_c.subscriber());

    // Unsubscribe from a parent that has no direct subscriber. The prefix match
    // still clears both children.
    bus.unsubscribe_topic(&topic_a);

    let _ = bus.publish_sync(&topic_b, unique_string());
    let _ = bus.publish_sync(&topic_c, unique_string());

    assert!(!spy_b.called());
    assert!(!spy_c.called());
}

#[test]
fn empty_prefix_clears_every_topic() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("a", |_, _| {});
    bus.subscribe("b", |_, _| {});

    // "" is a prefix of every topic, so it clears all of them.
    assert!(bus.unsubscribe_topic(""));
    assert!(bus.get_subscriptions("").is_empty());
}

#[test]
fn self_unsubscribe_during_publish_does_not_panic() {
    use std::rc::Rc;
    let bus: Rc<PubSub<String>> = Rc::new(PubSub::new());
    let topic = unique_string();

    let token_slot: Rc<std::cell::RefCell<Option<tiny_pubsub::Token>>> =
        Rc::new(std::cell::RefCell::new(None));
    let bus2 = bus.clone();
    let slot2 = token_slot.clone();
    let tok = bus.subscribe(&topic, move |_, _| {
        if let Some(t) = slot2.borrow().clone() {
            bus2.unsubscribe(&t);
        }
    });
    *token_slot.borrow_mut() = Some(tok);
    bus.subscribe(&topic, |_, _| {});

    // Must not panic.
    let _ = bus.publish_sync(&topic, "hello world!".into());
}
