# tiny-pubsub

In-process, topic-based publish/subscribe message bus for Rust. Hierarchical
topics, deferred or synchronous delivery, no dependencies.

## Install

```toml
[dependencies]
tiny-pubsub = "0.1"
```

## Use

```rust
use tiny_pubsub::PubSub;
use std::cell::Cell;
use std::rc::Rc;

let bus: PubSub<&str> = PubSub::new();
let hits = Rc::new(Cell::new(0));
let h = hits.clone();

// Subscribe a callback. You get back a token for later removal.
let token = bus.subscribe("car.engine", move |topic, data| {
    h.set(h.get() + 1);
    println!("{topic}: {data}");
});

// Synchronous publish runs every matching subscriber before it returns.
assert!(bus.publish_sync("car.engine", "start"));
assert_eq!(hits.get(), 1);

bus.unsubscribe(&token);
```

## Topics are hierarchical

Topics are dot-delimited. Publishing a leaf notifies the leaf and every ancestor
prefix, then the wildcard `*`. Children and siblings are never notified.

Publishing `a.b.c` reaches subscribers of `a.b.c`, then `a.b`, then `a`, then
`*`. An ancestor subscriber receives the original leaf topic as its first
argument, not the ancestor it matched.

```rust
use tiny_pubsub::PubSub;

let bus: PubSub<&str> = PubSub::new();
bus.subscribe("a", |topic, _| assert_eq!(topic, "a.b.c"));
bus.subscribe("a.b.c", |topic, _| assert_eq!(topic, "a.b.c"));
assert!(bus.publish_sync("a.b.c", "data"));
```

## Deferred delivery

`publish` queues delivery and returns at once. Subscribers run when you call
`process_deferred`. This models a non-blocking publisher.

```rust
use tiny_pubsub::PubSub;
use std::cell::Cell;
use std::rc::Rc;

let bus: PubSub<()> = PubSub::new();
let fired = Rc::new(Cell::new(false));
let f = fired.clone();
bus.subscribe("ping", move |_, _| f.set(true));

bus.publish("ping", ());
assert!(!fired.get());      // nothing has run yet
bus.process_deferred();     // drain the queue
assert!(fired.get());
```

Both `publish` and `publish_sync` return `true` when the topic had at least one
matching subscriber (direct, ancestor, or `*`), computed before any subscriber
runs.

## Removing subscriptions

- `unsubscribe(&token)` removes one subscription. It returns the token on a
  match, `None` otherwise.
- `unsubscribe_topic(topic)` removes a topic and every descendant, by string
  prefix. It returns `true` when the prefix matched a topic.
- `clear_subscriptions(prefix)` and `clear_all_subscriptions()` clear in bulk.

Rust closures cannot be compared by identity, so removal by function value has
no analogue. The token covers single-subscription removal.

## Panics during delivery

By default a panicking subscriber does not block the others. Delivery finishes,
then the first panic is re-raised. Call `set_immediate_exceptions(true)` to stop
at the first panic and propagate it, skipping the rest.

## License

[MIT](LICENSE).
