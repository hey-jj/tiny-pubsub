//! In-process, topic-based publish/subscribe message bus.
//!
//! A subscriber is a function called with `(topic, data)`. You register it
//! against a topic string and get back a [`Token`]. A publisher sends a value
//! to a topic. Every subscriber whose topic matches runs.
//!
//! Topics are hierarchical and dot-delimited. Publishing `a.b.c` notifies
//! subscribers of `a.b.c`, then `a.b`, then `a`, then the wildcard `*`. A
//! subscriber on an ancestor topic receives the original leaf topic as its
//! first argument, not the ancestor it matched.
//!
//! # Delivery modes
//!
//! [`PubSub::publish`] is deferred. It queues delivery and returns right away.
//! Subscribers run later, when you call [`PubSub::process_deferred`]. This
//! mirrors a non-blocking publisher that does not wait on subscriber work.
//!
//! [`PubSub::publish_sync`] runs every matching subscriber before it returns.
//!
//! Both return `true` if the topic had at least one matching subscriber
//! (direct, ancestor, or `*`), else `false`. The return value is computed
//! before any subscriber runs.
//!
//! # Panics during delivery
//!
//! Default mode catches a panicking subscriber, finishes delivery to the rest,
//! then re-raises the first panic after the dispatch. Set
//! [`PubSub::immediate_exceptions`] to `true` to stop at the first panic and
//! let it propagate, skipping the remaining subscribers.
//!
//! # Example
//!
//! ```
//! use tiny_pubsub::PubSub;
//! use std::cell::Cell;
//! use std::rc::Rc;
//!
//! let bus: PubSub<&str> = PubSub::new();
//! let hits = Rc::new(Cell::new(0));
//! let h = hits.clone();
//! bus.subscribe("car.engine", move |_topic, _data| h.set(h.get() + 1));
//!
//! assert!(bus.publish_sync("car.engine", "start"));
//! assert_eq!(hits.get(), 1);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

/// The wildcard topic. A subscriber here receives every published message.
pub const ALL_SUBSCRIBING: &str = "*";

/// A handle to one subscription, returned by [`PubSub::subscribe`].
///
/// Tokens are unique for the life of a bus and never reused, even after a
/// subscriber is removed. Pass a token to [`PubSub::unsubscribe`] to remove
/// that single subscription.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Token(String);

impl Token {
    /// The token's string form, shaped `uid_<n>`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The outcome of [`PubSub::unsubscribe`].
///
/// The four variants map one to one to the four outcomes of the source library:
/// topic clear returns nothing, a found token returns itself, a removed handle
/// returns true, and any miss returns false.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unsubscribed {
    /// A topic and its descendants were cleared. No token is recoverable.
    Topic,
    /// A token matched and was removed. Holds that token.
    Token(Token),
    /// A handle matched and at least one subscription was removed.
    Removed,
    /// Nothing matched.
    None,
}

impl Unsubscribed {
    /// True for any outcome that removed at least one subscription.
    #[must_use]
    pub fn is_some(&self) -> bool {
        !matches!(self, Unsubscribed::None)
    }
}

/// A boxed subscriber callback.
type Callback<D> = Rc<dyn Fn(&str, &D)>;

/// One stored subscription: its token paired with the callback.
struct Entry<D> {
    token: Token,
    callback: Callback<D>,
    /// Identifies the logical handler so [`PubSub::unsubscribe`] can remove
    /// every token a single [`PubSub::subscribe`] call group shares.
    handle: u64,
    /// When true, the entry is removed before its first invocation.
    once: bool,
}

/// One topic's subscribers, kept in insertion order so delivery order is
/// stable.
struct Topic<D> {
    entries: Vec<Entry<D>>,
}

impl<D> Topic<D> {
    fn new() -> Self {
        Topic {
            entries: Vec::new(),
        }
    }
}

/// Mutable state shared behind a [`RefCell`].
struct Inner<D> {
    /// Topic name to its subscriber list. Insertion-ordered to match the
    /// source library's key-iteration order.
    topics: Vec<(String, Topic<D>)>,
    /// Monotonic token counter. Starts at -1 so the first token is `uid_0`.
    last_uid: i64,
    /// Monotonic handle counter for [`Subscription`] grouping.
    last_handle: u64,
    /// Pending deferred deliveries, drained by [`PubSub::process_deferred`].
    deferred: Vec<DeferredJob<D>>,
}

/// A captured deferred publish: the leaf topic and the data to deliver.
struct DeferredJob<D> {
    message: String,
    data: D,
    immediate_exceptions: bool,
}

/// A grouping handle returned by [`PubSub::subscribe`] alongside its token.
///
/// Closures cannot be compared in Rust, so removal by function identity has no
/// direct analogue. A [`Subscription`] fills that gap. Each
/// [`PubSub::subscribe`] call produces one subscription. Subscribing the same
/// logical handler to several topics under one subscription lets you remove
/// them all at once with [`PubSub::unsubscribe_subscription`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subscription {
    token: Token,
    handle: u64,
}

impl Subscription {
    /// The token for this subscription.
    #[must_use]
    pub fn token(&self) -> &Token {
        &self.token
    }
}

/// An in-process hierarchical-topic pub/sub message bus.
///
/// `D` is the payload type passed to subscribers. The bus is single-threaded
/// and uses interior mutability, so subscribers may call back into the bus
/// during synchronous delivery.
pub struct PubSub<D> {
    inner: RefCell<Inner<D>>,
    /// Read at publish time. When true, the first panicking subscriber aborts
    /// delivery and propagates immediately.
    immediate_exceptions: std::cell::Cell<bool>,
}

impl<D> Default for PubSub<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> PubSub<D> {
    /// Create an empty bus.
    #[must_use]
    pub fn new() -> Self {
        PubSub {
            inner: RefCell::new(Inner {
                topics: Vec::new(),
                last_uid: -1,
                last_handle: 0,
                deferred: Vec::new(),
            }),
            immediate_exceptions: std::cell::Cell::new(false),
        }
    }

    /// Set whether a panicking subscriber aborts delivery immediately.
    ///
    /// Read fresh on each publish. Toggling it changes only later publishes.
    pub fn set_immediate_exceptions(&self, value: bool) {
        self.immediate_exceptions.set(value);
    }

    /// Current value of the immediate-exceptions flag.
    #[must_use]
    pub fn immediate_exceptions(&self) -> bool {
        self.immediate_exceptions.get()
    }

    /// Subscribe `func` to `topic`. Returns a [`Token`] for later removal.
    ///
    /// Each call gets a fresh, unique token, even for the same callback on the
    /// same topic. The topic does not need to exist first.
    pub fn subscribe<F>(&self, topic: impl Into<String>, func: F) -> Token
    where
        F: Fn(&str, &D) + 'static,
    {
        self.subscribe_with_handle(topic.into(), Rc::new(func)).0
    }

    /// Subscribe and get back a [`Subscription`] handle as well as the token.
    ///
    /// Use the returned subscription with [`PubSub::unsubscribe_subscription`]
    /// to remove every token tied to this call.
    pub fn subscribe_handle<F>(&self, topic: impl Into<String>, func: F) -> Subscription
    where
        F: Fn(&str, &D) + 'static,
    {
        let (token, handle) = self.subscribe_with_handle(topic.into(), Rc::new(func));
        Subscription { token, handle }
    }

    /// Subscribe an existing callback under a fresh handle, returning the
    /// token and handle id.
    fn subscribe_with_handle(&self, topic: String, callback: Callback<D>) -> (Token, u64) {
        self.subscribe_entry(topic, callback, false)
    }

    /// Insert a subscriber entry, optionally marked one-shot.
    fn subscribe_entry(&self, topic: String, callback: Callback<D>, once: bool) -> (Token, u64) {
        let mut inner = self.inner.borrow_mut();
        inner.last_handle += 1;
        let handle = inner.last_handle;
        inner.last_uid += 1;
        let token = Token(format!("uid_{}", inner.last_uid));
        let entry = Entry {
            token: token.clone(),
            callback,
            handle,
            once,
        };
        match inner.topics.iter_mut().find(|(name, _)| name == &topic) {
            Some((_, t)) => t.entries.push(entry),
            None => {
                let mut t = Topic::new();
                t.entries.push(entry);
                inner.topics.push((topic, t));
            }
        }
        (token, handle)
    }

    /// Subscribe `func` to the wildcard topic `*`. It then runs for every
    /// published message. Returns the token, like [`PubSub::subscribe`].
    pub fn subscribe_all<F>(&self, func: F) -> Token
    where
        F: Fn(&str, &D) + 'static,
    {
        self.subscribe(ALL_SUBSCRIBING, func)
    }

    /// Subscribe `func` to fire at most once.
    ///
    /// Delivery removes the entry before it calls `func`. A republish of the
    /// same topic from inside `func` does not retrigger it.
    pub fn subscribe_once<F>(&self, topic: impl Into<String>, func: F) -> Token
    where
        F: Fn(&str, &D) + 'static,
    {
        self.subscribe_entry(topic.into(), Rc::new(func), true).0
    }

    /// Publish `data` to `topic` for deferred delivery.
    ///
    /// Queues delivery and returns at once. Subscribers run on the next
    /// [`PubSub::process_deferred`] call. Returns `true` if the topic had at
    /// least one matching subscriber when this was called.
    #[must_use]
    pub fn publish(&self, topic: impl Into<String>, data: D) -> bool {
        let message = topic.into();
        let immediate = self.immediate_exceptions.get();
        let has = self.message_has_subscribers(&message);
        if !has {
            return false;
        }
        self.inner.borrow_mut().deferred.push(DeferredJob {
            message,
            data,
            immediate_exceptions: immediate,
        });
        true
    }

    /// Publish `data` to `topic` and deliver to all matching subscribers now.
    ///
    /// Returns `true` if there was at least one matching subscriber, else
    /// `false`. The return value is computed before any subscriber runs.
    #[must_use]
    pub fn publish_sync(&self, topic: impl Into<String>, data: D) -> bool {
        let message = topic.into();
        let immediate = self.immediate_exceptions.get();
        let has = self.message_has_subscribers(&message);
        if !has {
            return false;
        }
        self.deliver(&message, &data, immediate);
        true
    }

    /// Run every delivery queued by [`PubSub::publish`], in order.
    ///
    /// This stands in for an event loop tick. Until you call it, deferred
    /// subscribers do not run.
    pub fn process_deferred(&self) {
        loop {
            let job = {
                let mut inner = self.inner.borrow_mut();
                if inner.deferred.is_empty() {
                    break;
                }
                inner.deferred.remove(0)
            };
            self.deliver(&job.message, &job.data, job.immediate_exceptions);
        }
    }

    /// Number of queued deferred deliveries.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.borrow().deferred.len()
    }

    /// Deliver `data` to every subscriber of `message` and its ancestors,
    /// then the wildcard topic.
    fn deliver(&self, message: &str, data: &D, immediate_exceptions: bool) {
        // Walk the hierarchy right to left: exact topic, then each ancestor.
        let mut levels: Vec<String> = vec![message.to_string()];
        let mut topic = message.to_string();
        while let Some(pos) = topic.rfind('.') {
            topic.truncate(pos);
            levels.push(topic.clone());
        }
        levels.push(ALL_SUBSCRIBING.to_string());

        // Hold the first caught panic and re-raise it after the whole
        // dispatch, so the remaining subscribers still run.
        let mut held_panic: Option<Box<dyn std::any::Any + Send>> = None;

        for level in &levels {
            // Snapshot the matched callbacks before invoking any, so a
            // subscriber that mutates the registry mid-delivery cannot skip a
            // peer that was matched for this publish. One-shot entries are
            // removed here, before invocation, so a re-entrant publish from a
            // subscriber cannot retrigger them.
            let snapshot: Vec<Callback<D>> = {
                let mut inner = self.inner.borrow_mut();
                match inner.topics.iter_mut().find(|(name, _)| name == level) {
                    Some((_, t)) => {
                        let snapshot = t.entries.iter().map(|e| e.callback.clone()).collect();
                        t.entries.retain(|e| !e.once);
                        snapshot
                    }
                    None => Vec::new(),
                }
            };
            for callback in snapshot {
                if immediate_exceptions {
                    callback(message, data);
                } else if let Err(panic) =
                    catch_unwind(AssertUnwindSafe(|| callback(message, data)))
                {
                    if held_panic.is_none() {
                        held_panic = Some(panic);
                    }
                }
            }
        }

        if let Some(panic) = held_panic {
            std::panic::resume_unwind(panic);
        }
    }

    /// True if `message`, any ancestor, or the wildcard topic has a subscriber.
    fn message_has_subscribers(&self, message: &str) -> bool {
        let inner = self.inner.borrow();
        let has_direct = |topic: &str| {
            inner
                .topics
                .iter()
                .any(|(name, t)| name == topic && !t.entries.is_empty())
        };
        if has_direct(message) || has_direct(ALL_SUBSCRIBING) {
            return true;
        }
        let mut topic = message.to_string();
        while let Some(pos) = topic.rfind('.') {
            topic.truncate(pos);
            if has_direct(&topic) {
                return true;
            }
        }
        false
    }

    /// Remove the single subscription identified by `token`.
    ///
    /// Returns [`Unsubscribed::Token`] with the token if it matched, else
    /// [`Unsubscribed::None`].
    pub fn unsubscribe(&self, token: &Token) -> Unsubscribed {
        let mut inner = self.inner.borrow_mut();
        for (_, t) in inner.topics.iter_mut() {
            if let Some(idx) = t.entries.iter().position(|e| &e.token == token) {
                t.entries.remove(idx);
                return Unsubscribed::Token(token.clone());
            }
        }
        Unsubscribed::None
    }

    /// Remove every subscription created under `subscription`.
    ///
    /// This is the handle-keyed stand-in for removal by function identity.
    /// Returns [`Unsubscribed::Removed`] if it removed at least one, else
    /// [`Unsubscribed::None`].
    pub fn unsubscribe_subscription(&self, subscription: &Subscription) -> Unsubscribed {
        let mut inner = self.inner.borrow_mut();
        let mut removed = false;
        for (_, t) in inner.topics.iter_mut() {
            let before = t.entries.len();
            t.entries.retain(|e| e.handle != subscription.handle);
            if t.entries.len() != before {
                removed = true;
            }
        }
        if removed {
            Unsubscribed::Removed
        } else {
            Unsubscribed::None
        }
    }

    /// Remove a topic and every descendant topic, by string prefix.
    ///
    /// Returns [`Unsubscribed::Topic`] if `topic` names an existing topic or a
    /// prefix of one, else [`Unsubscribed::None`]. Matching is a raw string
    /// prefix, not dot-boundary aware. `clear_subscriptions("a")` therefore
    /// removes `a`, `a.b`, and `ab` alike.
    pub fn unsubscribe_topic(&self, topic: &str) -> Unsubscribed {
        let is_topic = {
            let inner = self.inner.borrow();
            inner.topics.iter().any(|(name, _)| name.starts_with(topic))
        };
        if is_topic {
            self.clear_subscriptions(topic);
            Unsubscribed::Topic
        } else {
            Unsubscribed::None
        }
    }

    /// Empty the whole registry. Does not reset the token counter.
    pub fn clear_all_subscriptions(&self) {
        self.inner.borrow_mut().topics.clear();
    }

    /// Remove every topic whose name starts with `topic` (raw string prefix).
    pub fn clear_subscriptions(&self, topic: &str) {
        self.inner
            .borrow_mut()
            .topics
            .retain(|(name, _)| !name.starts_with(topic));
    }

    /// Count subscribers in the first registered topic whose name starts with
    /// `topic`, then stop.
    ///
    /// This counts one topic, not the sum across the hierarchy. It mirrors the
    /// source library, which breaks after the first prefix match.
    #[must_use]
    pub fn count_subscriptions(&self, topic: &str) -> usize {
        let inner = self.inner.borrow();
        for (name, t) in &inner.topics {
            if name.starts_with(topic) {
                return t.entries.len();
            }
        }
        0
    }

    /// List every registered topic name that starts with `topic`, in
    /// insertion order.
    #[must_use]
    pub fn get_subscriptions(&self, topic: &str) -> Vec<String> {
        let inner = self.inner.borrow();
        inner
            .topics
            .iter()
            .filter(|(name, _)| name.starts_with(topic))
            .map(|(name, _)| name.clone())
            .collect()
    }
}
