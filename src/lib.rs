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
pub const WILDCARD: &str = "*";

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

/// A boxed subscriber callback.
type Callback<D> = Rc<dyn Fn(&str, &D)>;

/// One stored subscription: its token paired with the callback.
struct Entry<D> {
    token: Token,
    callback: Callback<D>,
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
    /// Topic name to its subscriber list. Insertion-ordered so delivery and
    /// iteration order stay stable.
    topics: Vec<(String, Topic<D>)>,
    /// Monotonic token counter. Starts at -1 so the first token is `uid_0`.
    last_uid: i64,
    /// Pending deferred deliveries, drained by [`PubSub::process_deferred`].
    deferred: Vec<DeferredJob<D>>,
}

/// A captured deferred publish: the leaf topic and the data to deliver.
struct DeferredJob<D> {
    message: String,
    data: D,
    immediate_exceptions: bool,
}

/// Yield `message`, then each ancestor prefix, then the wildcard topic.
///
/// The ancestor prefixes are slices of `message`, so the walk allocates
/// nothing. For `a.b.c` this yields `a.b.c`, `a.b`, `a`, `*`.
fn delivery_levels(message: &str) -> impl Iterator<Item = &str> {
    std::iter::once(message)
        .chain(message.rmatch_indices('.').map(move |(i, _)| &message[..i]))
        .chain(std::iter::once(WILDCARD))
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

impl<D> std::fmt::Debug for PubSub<D> {
    /// Print topic names with their subscriber counts. Payloads and callbacks
    /// are never formatted, so `D` need not be `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow();
        f.debug_struct("PubSub")
            .field(
                "topics",
                &inner
                    .topics
                    .iter()
                    .map(|(name, t)| (name.as_str(), t.entries.len()))
                    .collect::<Vec<_>>(),
            )
            .field("last_uid", &inner.last_uid)
            .field("pending", &inner.deferred.len())
            .field("immediate_exceptions", &self.immediate_exceptions.get())
            .finish()
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
        self.subscribe_entry(topic.into(), Rc::new(func), false)
    }

    /// Insert a subscriber entry, optionally marked one-shot. Returns its token.
    fn subscribe_entry(&self, topic: String, callback: Callback<D>, once: bool) -> Token {
        let mut inner = self.inner.borrow_mut();
        inner.last_uid += 1;
        let token = Token(format!("uid_{}", inner.last_uid));
        let entry = Entry {
            token: token.clone(),
            callback,
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
        token
    }

    /// Subscribe `func` to the wildcard topic `*`. It then runs for every
    /// published message. Returns the token, like [`PubSub::subscribe`].
    pub fn subscribe_all<F>(&self, func: F) -> Token
    where
        F: Fn(&str, &D) + 'static,
    {
        self.subscribe(WILDCARD, func)
    }

    /// Subscribe `func` to fire at most once.
    ///
    /// Delivery removes the entry before it calls `func`. A republish of the
    /// same topic from inside `func` does not retrigger it.
    pub fn subscribe_once<F>(&self, topic: impl Into<String>, func: F) -> Token
    where
        F: Fn(&str, &D) + 'static,
    {
        self.subscribe_entry(topic.into(), Rc::new(func), true)
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

    /// Run the deliveries queued by [`PubSub::publish`] so far, in call order.
    ///
    /// This stands in for one event loop tick. It drains the jobs queued before
    /// the call. A subscriber that publishes during the drain queues its job for
    /// the next call, not this one. Call again to drain those.
    ///
    /// Under delayed exceptions a panicking subscriber does not stop the other
    /// jobs in the batch. Every job runs, then the first panic is re-raised
    /// after the batch drains. Under immediate exceptions the first panic
    /// propagates at once and the rest of the batch does not run.
    pub fn process_deferred(&self) {
        let batch = std::mem::take(&mut self.inner.borrow_mut().deferred);
        let mut held_panic: Option<Box<dyn std::any::Any + Send>> = None;
        for job in batch {
            if job.immediate_exceptions {
                self.deliver(&job.message, &job.data, true);
            } else if let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
                self.deliver(&job.message, &job.data, false);
            })) {
                if held_panic.is_none() {
                    held_panic = Some(panic);
                }
            }
        }
        if let Some(panic) = held_panic {
            std::panic::resume_unwind(panic);
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
        // Hold the first caught panic and re-raise it after the whole
        // dispatch, so the remaining subscribers still run.
        let mut held_panic: Option<Box<dyn std::any::Any + Send>> = None;

        for level in delivery_levels(message) {
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
        delivery_levels(message).any(|level| {
            inner
                .topics
                .iter()
                .any(|(name, t)| name == level && !t.entries.is_empty())
        })
    }

    /// Remove the single subscription identified by `token`.
    ///
    /// Returns the token if it matched a live subscription, else `None`. A
    /// second removal of the same token returns `None`.
    pub fn unsubscribe(&self, token: &Token) -> Option<Token> {
        let mut inner = self.inner.borrow_mut();
        for (_, t) in inner.topics.iter_mut() {
            if let Some(idx) = t.entries.iter().position(|e| &e.token == token) {
                t.entries.remove(idx);
                return Some(token.clone());
            }
        }
        None
    }

    /// Remove a topic and every descendant topic, by string prefix.
    ///
    /// Returns `true` if `topic` names an existing topic or a prefix of one,
    /// else `false`. Matching is a raw string prefix, not dot-boundary aware.
    /// `unsubscribe_topic("a")` therefore removes `a`, `a.b`, and `ab` alike.
    pub fn unsubscribe_topic(&self, topic: &str) -> bool {
        let is_topic = {
            let inner = self.inner.borrow();
            inner.topics.iter().any(|(name, _)| name.starts_with(topic))
        };
        if is_topic {
            self.clear_subscriptions(topic);
        }
        is_topic
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
    /// This counts one topic, not the sum across the hierarchy. It stops at the
    /// first prefix match and returns that topic's subscriber count.
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
