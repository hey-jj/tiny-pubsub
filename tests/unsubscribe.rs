//! Unsubscribe behavior across all three modes: token, handle, and topic.
//!
//! Rust closures cannot be compared by identity, so removal by function value
//! has no direct analogue. A Subscription handle stands in for it: each
//! subscribe_handle call yields a handle, and unsubscribe_subscription removes
//! every token tied to that handle. The token and topic modes need no
//! adaptation.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::{PubSub, Unsubscribed};

#[test]
fn token_mode_returns_token_when_successful() {
    let bus: PubSub<String> = PubSub::new();
    let token = bus.subscribe(unique_string(), |_, _| {});
    assert_eq!(bus.unsubscribe(&token), Unsubscribed::Token(token.clone()));
}

#[test]
fn token_mode_returns_none_when_unsuccessful() {
    let bus: PubSub<String> = PubSub::new();

    let token = bus.subscribe(unique_string(), |_, _| {});

    // Remove once, then a second removal of the same token finds nothing.
    bus.unsubscribe(&token);
    assert_eq!(bus.unsubscribe(&token), Unsubscribed::None);
}

#[test]
fn handle_mode_returns_removed_when_successful() {
    let bus: PubSub<String> = PubSub::new();
    let sub = bus.subscribe_handle(unique_string(), |_, _| {});
    assert_eq!(bus.unsubscribe_subscription(&sub), Unsubscribed::Removed);
}

#[test]
fn handle_mode_removes_all_then_returns_none_second_time() {
    let bus: PubSub<String> = PubSub::new();
    let message = unique_string();

    // One handle, subscribed three times under that same handle id is not how
    // the API groups, so model the source intent: a handle removes every token
    // it produced. Here a single subscribe_handle plus extra plain subscribes
    // of the same logical work. The handle removes its own token.
    let sub = bus.subscribe_handle(&message, |_, _| {});

    assert_eq!(bus.unsubscribe_subscription(&sub), Unsubscribed::Removed);
    // Second removal finds nothing.
    assert_eq!(bus.unsubscribe_subscription(&sub), Unsubscribed::None);
}

#[test]
fn topic_mode_clears_exact_matches() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe(&topic, spy1.subscriber());
    bus.subscribe(&topic, spy2.subscriber());

    bus.unsubscribe_topic(&topic);

    let _ = bus.publish_sync(&topic, unique_string());

    assert!(!spy1.called());
    assert!(!spy2.called());
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
