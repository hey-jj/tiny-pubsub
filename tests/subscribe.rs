//! Subscribe behavior: token shape and uniqueness.
//!
//! Rust's type system rejects a non-callable subscriber at compile time, so
//! there is no runtime case for passing one. The guarantee that a topic with no
//! valid subscriber does not panic on publish is covered in publish.rs by
//! returns_false_when_no_subscribers.

mod common;

use common::{assert_all_tokens_different, unique_string};
use tiny_pubsub::PubSub;

#[test]
fn returns_a_token() {
    let bus: PubSub<String> = PubSub::new();
    let token = bus.subscribe(unique_string(), |_, _| {});
    assert!(token.as_str().starts_with("uid_"));
}

#[test]
fn returns_new_token_for_repeated_subscriptions_of_same_callback() {
    let bus: PubSub<String> = PubSub::new();
    let message = unique_string();
    let func = |_: &str, _: &String| {};
    let tokens: Vec<_> = (0..10).map(|_| bus.subscribe(&message, func)).collect();
    assert_all_tokens_different(&tokens);
}

#[test]
fn returns_unique_tokens_for_namespaced_subscriptions() {
    let bus: PubSub<String> = PubSub::new();
    let topics = ["library", "library.music", "library.music.jazz"];
    let tokens: Vec<_> = topics
        .iter()
        .map(|t| bus.subscribe(*t, |_, _| {}))
        .collect();
    assert_all_tokens_different(&tokens);
}

#[test]
fn returns_unique_tokens_for_distinct_callbacks() {
    let bus: PubSub<String> = PubSub::new();
    let message = unique_string();
    // Each closure captures a distinct value, so they are distinct callbacks.
    let tokens: Vec<_> = (0..10)
        .map(|i| {
            bus.subscribe(&message, move |_, _| {
                let _ = i;
            })
        })
        .collect();
    assert_all_tokens_different(&tokens);
}
