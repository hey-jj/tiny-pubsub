//! Hierarchical topic addressing.
//!
//! Publishing a leaf notifies the leaf and every ancestor prefix, never
//! children or siblings, then the wildcard. The table below is the canonical
//! addressing matrix. Each row runs twice: once through deferred publish plus
//! drain, once through synchronous publish.

mod common;

use common::Spy;
use tiny_pubsub::PubSub;

/// One addressing case: topics to subscribe, the topic to publish, and the
/// expected total subscriber call count.
struct HCase {
    subs: &'static [&'static str],
    publish: &'static str,
    expect: usize,
}

const HIERARCHY: &[HCase] = &[
    // Publishing a parent never reaches its children.
    HCase {
        subs: &["library", "library.music"],
        publish: "library",
        expect: 1,
    },
    // Publishing a child reaches itself and its parent.
    HCase {
        subs: &["library", "library.music"],
        publish: "library.music",
        expect: 2,
    },
    // A deeper sibling subscriber is not reached.
    HCase {
        subs: &["library", "library.music", "library.music.jazz"],
        publish: "library.music",
        expect: 2,
    },
    // Publishing a leaf reaches all of its ancestors.
    HCase {
        subs: &["library", "library.music", "library.music.jazz"],
        publish: "library.music.jazz",
        expect: 3,
    },
    // Only ancestors of the published topic fire, not off-branch siblings.
    HCase {
        subs: &[
            "library",
            "library.music",
            "library.music.jazz",
            "library.playlist",
            "library.playlist.mine",
        ],
        publish: "library.music.jazz",
        expect: 3,
    },
    // Deep chain: every ancestor fires, the off-branch playlist.jazz does not.
    HCase {
        subs: &[
            "library",
            "library.music",
            "library.music.jazz",
            "library.music.jazz.soft",
            "library.music.jazz.soft.swing",
            "library.music.playlist.jazz",
        ],
        publish: "library.music.jazz.soft.swing",
        expect: 5,
    },
    // Bug 9: a publish deeper than any subscriber still notifies all ancestors.
    HCase {
        subs: &["a.b.c", "a.b", "a"],
        publish: "a.b.c.d",
        expect: 3,
    },
    // Bug 9: a single subscriber fires even with no ancestors registered.
    HCase {
        subs: &["a.b.c"],
        publish: "a.b.c.d",
        expect: 1,
    },
];

#[test]
fn hierarchy_via_deferred_publish() {
    for case in HIERARCHY {
        let bus: PubSub<String> = PubSub::new();
        let spy = Spy::new();
        for topic in case.subs {
            bus.subscribe(*topic, spy.subscriber());
        }
        let _ = bus.publish(case.publish, "data".into());
        assert_eq!(
            spy.call_count(),
            0,
            "deferred publish must not fire before drain for {}",
            case.publish
        );
        bus.process_deferred();
        assert_eq!(
            spy.call_count(),
            case.expect,
            "publishing {} should reach {} subscribers",
            case.publish,
            case.expect
        );
    }
}

#[test]
fn hierarchy_via_publish_sync() {
    for case in HIERARCHY {
        let bus: PubSub<String> = PubSub::new();
        let spy = Spy::new();
        for topic in case.subs {
            bus.subscribe(*topic, spy.subscriber());
        }
        let _ = bus.publish_sync(case.publish, "data".into());
        assert_eq!(
            spy.call_count(),
            case.expect,
            "sync publishing {} should reach {} subscribers",
            case.publish,
            case.expect
        );
    }
}

#[test]
fn still_calls_all_parents_when_middle_child_unsubscribed() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    bus.subscribe("library", spy.subscriber());
    bus.subscribe("library.music.jazz", spy.subscriber());
    let token = bus.subscribe("library.music", spy.subscriber());

    bus.unsubscribe(&token);

    let _ = bus.publish("library.music.jazz", "data".into());
    assert_eq!(spy.call_count(), 0);
    bus.process_deferred();
    assert_eq!(spy.call_count(), 2);
}

#[test]
fn unsubscribe_returns_tokens_for_namespaced_messages() {
    use tiny_pubsub::Unsubscribed;
    let bus: PubSub<String> = PubSub::new();
    let token1 = bus.subscribe("playlist.music", |_, _| {});
    let token2 = bus.subscribe("playlist.music.jazz", |_, _| {});

    assert_eq!(
        bus.unsubscribe(&token1),
        Unsubscribed::Token(token1.clone())
    );
    assert_eq!(
        bus.unsubscribe(&token2),
        Unsubscribed::Token(token2.clone())
    );
}

#[test]
fn unsubscribe_parent_without_affecting_orphans() {
    let bus: PubSub<String> = PubSub::new();
    let spy = Spy::new();
    let token = bus.subscribe("playlist", spy.subscriber());
    bus.subscribe("playlist.music", spy.subscriber());
    bus.subscribe("playlist.music.jazz", spy.subscriber());

    bus.unsubscribe(&token);

    let _ = bus.publish("playlist.music.jazz", "data".into());
    assert_eq!(spy.call_count(), 0);
    bus.process_deferred();
    assert_eq!(spy.call_count(), 2);
}
