//! Shared test helpers: call-recording spies, a unique-string generator, and
//! a token-uniqueness assertion.

#![allow(dead_code)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tiny_pubsub::Token;

/// Monotonic unique topic/data string, matching the source test helper.
///
/// Returns `"my unique String number N"`. Useful when several tests share one
/// bus, though most tests here use a fresh bus and do not need it.
pub fn unique_string() -> String {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    format!("my unique String number {n}")
}

/// A spy that records every `(message, data)` it receives.
///
/// Clone it to share one record across the bus and the test body.
#[derive(Clone, Default)]
pub struct Spy {
    calls: Rc<RefCell<Vec<(String, String)>>>,
}

impl Spy {
    /// New empty spy.
    pub fn new() -> Self {
        Spy::default()
    }

    /// A subscriber closure that records into this spy.
    pub fn subscriber(&self) -> impl Fn(&str, &String) + 'static {
        let calls = self.calls.clone();
        move |message: &str, data: &String| {
            calls.borrow_mut().push((message.to_string(), data.clone()));
        }
    }

    /// Number of times the spy was called.
    pub fn call_count(&self) -> usize {
        self.calls.borrow().len()
    }

    /// True if called at least once.
    pub fn called(&self) -> bool {
        self.call_count() > 0
    }

    /// True if called exactly once.
    pub fn called_once(&self) -> bool {
        self.call_count() == 1
    }

    /// True if any call had `message` as its first argument.
    pub fn called_with_message(&self, message: &str) -> bool {
        self.calls.borrow().iter().any(|(m, _)| m == message)
    }

    /// True if any call had this exact `(message, data)` pair.
    pub fn called_with(&self, message: &str, data: &str) -> bool {
        self.calls
            .borrow()
            .iter()
            .any(|(m, d)| m == message && d == data)
    }

    /// All recorded calls, in order.
    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls.borrow().clone()
    }
}

/// Assert that a non-empty token list holds only distinct tokens.
///
/// Mirrors the source test helper: the list must be non-empty and every pair
/// must differ.
pub fn assert_all_tokens_different(tokens: &[Token]) {
    assert!(!tokens.is_empty(), "token list must be non-empty");
    for (j, a) in tokens.iter().enumerate() {
        for b in &tokens[j + 1..] {
            assert!(a != b, "tokens must all differ: {a} == {b}");
        }
    }
}
