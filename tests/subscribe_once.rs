//! subscribe_once behavior.
//!
//! subscribe_once returns the subscription token. The real invariant is that
//! the handler fires at most once.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::PubSub;

#[test]
fn fires_at_most_once_across_three_publishes() {
    let bus: PubSub<String> = PubSub::new();
    let topic = unique_string();
    let spy = Spy::new();

    bus.subscribe_once(&topic, spy.subscriber());
    for _ in 0..3 {
        let _ = bus.publish_sync(&topic, unique_string());
    }

    assert!(spy.called_once());
}

#[test]
fn returns_a_token() {
    let bus: PubSub<String> = PubSub::new();
    let token = bus.subscribe_once(unique_string(), |_, _| {});
    assert!(token.as_str().starts_with("uid_"));
}
