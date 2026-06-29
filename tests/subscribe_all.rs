//! subscribe_all behavior: the wildcard topic receives every message.

mod common;

use common::{unique_string, Spy};
use tiny_pubsub::PubSub;

#[test]
fn returns_a_token() {
    let bus: PubSub<String> = PubSub::new();
    let token = bus.subscribe_all(|_, _| {});
    assert!(token.as_str().starts_with("uid_"));
}

#[test]
fn subscribes_for_all_messages() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe_all(spy.subscriber());
    let _ = bus.publish_sync(unique_string(), "some payload".into());

    assert!(spy.called_once());
}
