//! Regression cases for hierarchy depth and mid-delivery unsubscribe.

mod common;

use common::{unique_string, Spy};
use std::cell::RefCell;
use std::rc::Rc;
use tiny_pubsub::{PubSub, Token};

// A publish deeper than any registered topic still notifies every ancestor.
#[test]
fn notifies_all_subscribers_in_a_hierarchy() {
    let bus: PubSub<String> = PubSub::new();
    let s1 = Spy::new();
    let s2 = Spy::new();
    let s3 = Spy::new();
    bus.subscribe("a.b.c", s1.subscriber());
    bus.subscribe("a.b", s2.subscriber());
    bus.subscribe("a", s3.subscriber());

    let _ = bus.publish("a.b.c.d", String::new());
    bus.process_deferred();

    assert!(s1.called_once());
    assert!(s2.called_once());
    assert!(s3.called_once());
}

// A lone subscriber fires even when nothing is registered further up.
#[test]
fn notifies_individual_subscriber_with_no_ancestors() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("a.b.c", spy.subscriber());
    let _ = bus.publish("a.b.c.d", String::new());
    bus.process_deferred();
    assert!(spy.called_once());
}

// Publishing the wildcard topic delivers to a wildcard subscriber exactly once,
// not twice.
#[test]
fn publishing_wildcard_delivers_once() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe_all(spy.subscriber());

    assert!(bus.publish_sync("*", String::new()));

    assert!(spy.called_once());
}

// Issue 54: a subscriber that removes its own token during delivery must not
// cause later subscribers to be skipped.
#[test]
fn notifies_all_subscribers_even_when_one_is_unsubscribed() {
    let bus: Rc<PubSub<String>> = Rc::new(PubSub::new());
    let topic = unique_string();

    let unsubscribed = Rc::new(RefCell::new(false));
    let token_slot: Rc<RefCell<Option<Token>>> = Rc::new(RefCell::new(None));

    let s1 = Spy::new();
    let s2 = Spy::new();
    let s3 = Spy::new();

    let bus_inner = bus.clone();
    let slot_inner = token_slot.clone();
    let flag_inner = unsubscribed.clone();
    let s1_cb = s1.subscriber();
    let token1 = bus.subscribe(&topic, move |m, d| {
        if let Some(t) = slot_inner.borrow().clone() {
            bus_inner.unsubscribe(&t);
        }
        *flag_inner.borrow_mut() = true;
        s1_cb(m, d);
    });
    *token_slot.borrow_mut() = Some(token1);
    bus.subscribe(&topic, s2.subscriber());
    bus.subscribe(&topic, s3.subscriber());

    let _ = bus.publish(&topic, String::new());
    bus.process_deferred();

    assert!(*unsubscribed.borrow());
    assert!(s1.called_once());
    assert!(s2.called_once());
    assert!(s3.called_once());
}
