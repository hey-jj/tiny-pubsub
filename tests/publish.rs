//! Publish and publish-sync behavior.

mod common;

use common::Spy;
use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use tiny_pubsub::{PubSub, Token};

#[test]
fn returns_false_when_no_subscribers() {
    let bus: PubSub<String> = PubSub::new();
    assert!(!bus.publish("topic", String::new()));
}

#[test]
fn returns_true_when_subscribers_present() {
    let bus: PubSub<String> = PubSub::new();
    bus.subscribe("topic", |_, _| {});
    assert!(bus.publish("topic", String::new()));
}

#[test]
fn returns_false_when_no_longer_any_subscribers() {
    let bus: PubSub<String> = PubSub::new();
    let token = bus.subscribe("topic", |_, _| {});
    bus.unsubscribe(&token);
    assert!(!bus.publish("topic", String::new()));
}

#[test]
fn calls_all_subscribers_exactly_once() {
    let bus: PubSub<String> = PubSub::new();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe("topic", spy1.subscriber());
    bus.subscribe("topic", spy2.subscriber());

    let _ = bus.publish_sync("topic", "my payload".into());

    assert!(spy1.called_once());
    assert!(spy2.called_once());
}

#[test]
fn calls_only_subscribers_of_the_published_message() {
    let bus: PubSub<String> = PubSub::new();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe("message1", spy1.subscriber());
    bus.subscribe("message2", spy2.subscriber());

    let _ = bus.publish_sync("message1", "some payload".into());

    assert!(spy1.called());
    assert_eq!(spy2.call_count(), 0);
}

#[test]
fn calls_subscribers_with_message_as_first_argument() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("topic", spy.subscriber());
    let _ = bus.publish_sync("topic", "some payload".into());

    assert!(spy.called_with_message("topic"));
}

#[test]
fn calls_subscribers_with_data_as_second_argument() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("topic", spy.subscriber());
    let _ = bus.publish_sync("topic", "the data".into());

    assert!(spy.called_with("topic", "the data"));
}

// Deferred publish is the analogue of the source's async delivery. Nothing
// fires until process_deferred, which stands in for the fake clock tick.
#[test]
fn publish_is_deferred() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("topic", spy.subscriber());
    let _ = bus.publish("topic", "data".into());

    assert_eq!(spy.call_count(), 0);
    bus.process_deferred();
    assert_eq!(spy.call_count(), 1);
}

#[test]
fn publish_sync_delivers_immediately() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("topic", spy.subscriber());
    let _ = bus.publish_sync("topic", "data".into());

    assert_eq!(spy.call_count(), 1);
}

// Default mode: a panicking subscriber does not block the others. The panic is
// re-raised after the full dispatch.
#[test]
fn calls_all_subscribers_even_if_one_panics() {
    let bus: PubSub<String> = PubSub::new();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe("topic", |_, _| panic!("some error"));
    bus.subscribe("topic", spy1.subscriber());
    bus.subscribe("topic", spy2.subscriber());

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.publish_sync("topic", "some data".into());
    }));
    assert!(result.is_err());

    assert!(spy1.called());
    assert!(spy2.called());
}

// Immediate mode: the first panic aborts delivery. Later subscribers do not
// run.
#[test]
fn fails_immediately_on_panic_when_immediate_exceptions_is_true() {
    let bus: PubSub<String> = PubSub::new();
    let spy1 = Spy::new();
    let spy2 = Spy::new();
    bus.subscribe("topic", |_, _| panic!("some error"));
    bus.subscribe("topic", spy1.subscriber());

    bus.set_immediate_exceptions(true);

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.publish_sync("topic", "some data".into());
    }));
    assert!(result.is_err());

    assert!(!spy1.called());
    assert!(!spy2.called());
}

#[test]
fn fails_immediately_on_panic_in_namespaces_when_immediate_exceptions_is_true() {
    let bus: PubSub<String> = PubSub::new();
    let spy1 = Spy::new();
    bus.subscribe("buy", |_, _| panic!("some error"));
    bus.subscribe("buy", spy1.subscriber());

    bus.set_immediate_exceptions(true);

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = bus.publish_sync("buy.tomatoes", "some data".into());
    }));
    assert!(result.is_err());

    assert!(!spy1.called());
}

// A subscriber that removes itself mid-delivery must not skip its peers. The
// snapshot taken before invocation guarantees every matched subscriber runs.
#[test]
fn calls_all_subscribers_even_with_unsubscriptions_within() {
    let bus: Rc<PubSub<String>> = Rc::new(PubSub::new());
    let spy1 = Spy::new();
    let spy2 = Spy::new();

    let token1: Rc<RefCellOpt> = Rc::new(RefCellOpt::new());
    let token2: Rc<RefCellOpt> = Rc::new(RefCellOpt::new());

    {
        let bus2 = bus.clone();
        let t1 = token1.clone();
        let s1 = spy1.subscriber();
        let tok = bus.subscribe("topic", move |m, d| {
            if let Some(tk) = t1.take() {
                bus2.unsubscribe(&tk);
            }
            s1(m, d);
        });
        token1.set(tok);
    }
    {
        let bus2 = bus.clone();
        let t2 = token2.clone();
        let s2 = spy2.subscriber();
        let tok = bus.subscribe("topic", move |m, d| {
            if let Some(tk) = t2.take() {
                bus2.unsubscribe(&tk);
            }
            s2(m, d);
        });
        token2.set(tok);
    }

    let _ = bus.publish("topic", "some data".into());
    bus.process_deferred();

    assert!(spy1.called(), "expected spy1 to be called");
    assert!(spy2.called(), "expected spy2 to be called");
}

// Small interior-mutable slot so a self-unsubscribing closure can learn its own
// token after subscribe returns.
struct RefCellOpt(RefCell<Option<Token>>);
impl RefCellOpt {
    fn new() -> Self {
        RefCellOpt(RefCell::new(None))
    }
    fn set(&self, t: Token) {
        *self.0.borrow_mut() = Some(t);
    }
    fn take(&self) -> Option<Token> {
        self.0.borrow_mut().take()
    }
}
