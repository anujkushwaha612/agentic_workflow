//! Bus parity: replicate the Python golden scenario
//! (`tests/parity/bus_capture.py` → `tests/parity/fixtures/bus_golden.json`)
//! in Rust and compare every semantic observable: deliveries, drops,
//! ordering, topic strings, depth/budget accounting, wait lifecycle,
//! timeouts, mailbox contents, stats, snapshot, journal evidence.
//!
//! Runtime-specific identifiers (mids, correlation ids, hashes) are excluded
//! on both sides. Regenerate the golden with
//! `PYTHONPATH=. python3 tests/parity/bus_capture.py`.

use arena::bus::Bus;
use arena::journal::{EventFilter, Journal};
use arena::lifecycle::{AgentState, Lifecycle};
use arena::msg::{Message, MAX_CAUSAL_DEPTH};
use arena::registry::AgentRegistry;
use arena::sys::json::{parse, JMap, JValue};
use std::path::PathBuf;

fn golden() -> JValue {
    let p: PathBuf = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/parity/fixtures/bus_golden.json"
    )
    .into();
    parse(&std::fs::read_to_string(p).expect("golden file present")).expect("golden parses")
}

fn g<'a>(v: &'a JValue, key: &str) -> &'a JValue {
    v.get(key).unwrap_or_else(|| panic!("missing key {key}"))
}

struct Fixture {
    bus: Bus,
    journal: Journal,
    registry: AgentRegistry,
}

fn fixture() -> Fixture {
    let mut f = Fixture {
        bus: Bus::new(),
        journal: Journal::open_memory().unwrap(),
        registry: AgentRegistry::default(),
    };
    for aid in [
        "a",
        "b",
        "c",
        "flooder",
        "victim",
        "quiet",
        "loud",
        "x",
        "y",
        "be",
        "unrelated",
    ] {
        f.registry
            .register(
                aid,
                "talker",
                &["t"],
                0,
                "parent",
                "",
                Some(Lifecycle::with_state(AgentState::Working, 0.0)),
                &[],
            )
            .unwrap();
        f.bus.register_actor(aid);
    }
    f
}

fn msg(t: arena::msg::EventType, from: &str, to: &str, body: &str) -> Message {
    let mut m = Message::new(t, from, to);
    m.body = body.to_string();
    m
}

/// The reference's `_msgdict`: the semantic view of a mailbox message.
fn msgdict(m: &Message) -> JMap {
    let mut d = JMap::new();
    d.insert("type".into(), JValue::Str(m.msg_type.as_str().to_string()));
    d.insert("plane".into(), JValue::Str(m.plane.as_str().to_string()));
    d.insert("topic".into(), JValue::Str(m.topic.clone()));
    d.insert(
        "from".into(),
        JValue::Str(m.from_actor.as_str().to_string()),
    );
    d.insert("to".into(), JValue::Str(m.to_actor.as_str().to_string()));
    d.insert("body".into(), JValue::Str(m.body.clone()));
    d.insert(
        "task_id".into(),
        m.task_id
            .as_ref()
            .map(|t| JValue::Str(t.as_str().to_string()))
            .unwrap_or(JValue::Null),
    );
    d
}

fn check_delivery(case: &str, got: &arena::bus::Delivery, want: &JValue, where_: &str) {
    assert_eq!(
        JValue::Arr(got.recipients.iter().cloned().map(JValue::Str).collect()),
        *want
            .get("recipients")
            .unwrap_or_else(|| panic!("{where_}: {case} missing recipients")),
        "{where_}/{case}: recipients"
    );
    assert_eq!(
        JValue::Arr(
            got.not_subscribed
                .iter()
                .cloned()
                .map(JValue::Str)
                .collect()
        ),
        *want.get("not_subscribed").unwrap(),
        "{where_}/{case}: not_subscribed"
    );
    assert_eq!(
        JValue::Arr(
            got.dropped_budget
                .iter()
                .cloned()
                .map(JValue::Str)
                .collect()
        ),
        *want.get("dropped_budget").unwrap(),
        "{where_}/{case}: dropped_budget"
    );
    assert_eq!(
        JValue::Arr(got.dropped_depth.iter().cloned().map(JValue::Str).collect()),
        *want.get("dropped_depth").unwrap(),
        "{where_}/{case}: dropped_depth"
    );
}

#[test]
fn bus_matches_python_golden() {
    let want = golden();
    let mut f = fixture();

    // -------------------------------------------------------- subscriptions
    f.bus.subscribe(&mut f.registry, "loud", &["resource.*"]);
    f.bus.subscribe(&mut f.registry, "ghost", &[]);
    let want_patterns = g(&want, "patterns");
    for actor in ["parent", "kernel", "loud", "quiet"] {
        assert_eq!(
            JValue::Arr(
                f.bus
                    .patterns_for(actor)
                    .into_iter()
                    .map(JValue::Str)
                    .collect()
            ),
            *want_patterns.get(actor).unwrap(),
            "patterns_for({actor})"
        );
    }

    // -------------------------------------------------------------- routing
    let mut seq: Vec<(String, arena::bus::Delivery)> = Vec::new();
    let mut m = msg(arena::msg::EventType::DependencyRequest, "a", "b", "hi");
    let out_topic_direct = m.topic.clone();
    seq.push((
        "direct_dependency".into(),
        f.bus.publish(&mut m, &mut f.journal).unwrap(),
    ));

    let mut m = msg(
        arena::msg::EventType::DependencyReady,
        "a",
        "broadcast",
        "schema is ready",
    );
    seq.push((
        "broadcast_dependency".into(),
        f.bus.publish(&mut m, &mut f.journal).unwrap(),
    ));

    let mut m = msg(arena::msg::EventType::TaskProgress, "a", "parent", "30%");
    seq.push((
        "progress_to_parent".into(),
        f.bus.publish(&mut m, &mut f.journal).unwrap(),
    ));

    let mut res = msg(
        arena::msg::EventType::ResourceUpdated,
        "a",
        "broadcast",
        "changed",
    );
    res.resource = Some("shared_types.ts".to_string());
    let out_topic_resource = res.topic.clone();
    seq.push((
        "resource_broadcast".into(),
        f.bus.publish(&mut res, &mut f.journal).unwrap(),
    ));

    let mut m = msg(arena::msg::EventType::StatusUpdate, "parent", "be", "hello");
    seq.push((
        "status_direct".into(),
        f.bus.publish(&mut m, &mut f.journal).unwrap(),
    ));

    let mut m = msg(arena::msg::EventType::PlanAmended, "parent", "be", "amend");
    seq.push((
        "plan_gated".into(),
        f.bus.publish(&mut m, &mut f.journal).unwrap(),
    ));

    let mut m = msg(
        arena::msg::EventType::PlanCreated,
        "parent",
        "broadcast",
        "plan",
    );
    seq.push((
        "parent_broadcast".into(),
        f.bus.publish(&mut m, &mut f.journal).unwrap(),
    ));

    let want_deliveries = match g(&want, "deliveries") {
        JValue::Arr(a) => a.clone(),
        _ => panic!("deliveries not a list"),
    };
    for (got, want_case) in seq.iter().zip(want_deliveries.iter()) {
        let case = want_case.get("case").unwrap().as_str().unwrap();
        assert_eq!(&got.0, &case.to_string(), "delivery case order");
        check_delivery(case, &got.1, want_case, "routing");
    }
    assert_eq!(out_topic_direct, "dependency.dependency.request");
    assert_eq!(
        JValue::Str(out_topic_resource.clone()),
        *g(&want, "resource_topic"),
        "resource topic string"
    );
    assert_eq!(
        *g(&want, "direct_topic"),
        JValue::Str(out_topic_direct.clone())
    );

    // ------------------------------------------------------- causal depth cap
    let mut root = msg(arena::msg::EventType::DependencyRequest, "x", "y", "");
    f.bus.publish(&mut root, &mut f.journal).unwrap();
    let mut cur = root.clone();
    let mut hops = 0i64;
    loop {
        let mut nxt = cur.child(
            arena::msg::EventType::DependencyRequest,
            cur.to_actor.as_str(),
            cur.from_actor.as_str(),
        );
        nxt.body = format!("reply {hops}");
        let out = f.bus.publish(&mut nxt, &mut f.journal).unwrap();
        if !out.delivered_any() {
            break;
        }
        cur = nxt;
        hops += 1;
        assert!(hops <= MAX_CAUSAL_DEPTH + 3, "loop guard failed to engage");
    }
    let want_depth = g(&want, "depth");
    assert_eq!(
        JValue::Int(hops),
        *want_depth.get("hops_delivered").unwrap()
    );
    assert_eq!(
        JValue::Int(f.bus.stats.dropped_depth),
        *want_depth.get("dropped_depth_stat").unwrap()
    );
    assert_eq!(
        JValue::Int(
            f.journal
                .events(&EventFilter::new().etype("BUDGET_EXCEEDED"))
                .unwrap()
                .len() as i64
        ),
        *want_depth.get("budget_exceeded_events").unwrap()
    );

    // --------------------------------------------------------- sender budget
    f.bus.reset_tick();
    let mut delivered = 0i64;
    let mut dropped = 0i64;
    let mut last = None;
    for i in 0..40 {
        let mut m = msg(
            arena::msg::EventType::DependencyRequest,
            "flooder",
            "victim",
            &format!("spam {i}"),
        );
        let out = f.bus.publish(&mut m, &mut f.journal).unwrap();
        delivered += out.recipients.len() as i64;
        dropped += out.dropped_budget.len() as i64;
        last = Some(out);
    }
    let want_budget = g(&want, "budget");
    assert_eq!(
        JValue::Int(delivered),
        *want_budget.get("delivered").unwrap()
    );
    assert_eq!(JValue::Int(dropped), *want_budget.get("dropped").unwrap());
    assert_eq!(
        JValue::Int(f.bus.mailbox_len("victim") as i64),
        *want_budget.get("victim_queue_len").unwrap(),
        "8 flood deliveries + the earlier broadcast copy"
    );
    check_delivery(
        "flood_last",
        &last.unwrap(),
        want_budget.get("last_delivery").unwrap(),
        "budget",
    );

    // ---------------------------------------------------------- durable waits
    // now == 1.0
    let e1 = f
        .bus
        .wait_for(
            &mut f.journal,
            &mut f.registry,
            1.0,
            "be",
            "artifact:schema.sql",
            Some("t1"),
            "",
            Some(5.0),
        )
        .unwrap();
    let e2 = f
        .bus
        .wait_for(
            &mut f.journal,
            &mut f.registry,
            1.0,
            "unrelated",
            "artifact:schema.sql",
            None,
            "",
            None,
        )
        .unwrap();
    let e3 = f
        .bus
        .wait_for(
            &mut f.journal,
            &mut f.registry,
            1.0,
            "quiet",
            "artifact:never",
            None,
            "",
            Some(0.5),
        )
        .unwrap();
    let got_wait_ids: Vec<String> = [
        e1.wait_id.as_str(),
        e2.wait_id.as_str(),
        e3.wait_id.as_str(),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(
        JValue::Arr(got_wait_ids.iter().cloned().map(JValue::Str).collect()),
        *g(&want, "wait_ids")
    );
    let got_timeouts: Vec<JValue> = [e1.timeout_at, e2.timeout_at, e3.timeout_at]
        .iter()
        .map(|t| t.map(JValue::Float).unwrap_or(JValue::Null))
        .collect();
    assert_eq!(
        JValue::Arr(got_timeouts),
        *g(&want, "timeout_at"),
        "armed at 1.0: 5s deadline -> 6.0"
    );
    // not yet expired at t=1.0
    assert_eq!(
        JValue::Arr(
            f.bus
                .expire_timeouts(&mut f.journal, &mut f.registry, 1.0)
                .unwrap()
                .iter()
                .map(|r| JValue::Str(r.condition.clone()))
                .collect()
        ),
        *g(&want, "expire_at_1.0")
    );
    // targeted wake of exactly the interested pair, in wait order
    let woken = f
        .bus
        .resolve(
            &mut f.journal,
            &mut f.registry,
            "artifact:schema.sql",
            "published",
            None,
        )
        .unwrap();
    assert_eq!(
        JValue::Arr(woken.iter().cloned().map(JValue::Str).collect()),
        *g(&want, "woken")
    );
    // timeout at t=2.0
    let expired = f
        .bus
        .expire_timeouts(&mut f.journal, &mut f.registry, 2.0)
        .unwrap();
    let mut expired_view = JMap::new();
    expired_view.insert(
        "wait_id".into(),
        JValue::Str(expired[0].wait_id.as_str().to_string()),
    );
    expired_view.insert(
        "condition".into(),
        JValue::Str(expired[0].condition.clone()),
    );
    expired_view.insert(
        "agent".into(),
        JValue::Str(expired[0].agent_id.as_str().to_string()),
    );
    let want_expired = match g(&want, "expired") {
        JValue::Arr(a) => a[0].clone(),
        _ => panic!("expired not a list"),
    };
    assert_eq!(JValue::Obj(expired_view), want_expired);
    assert_eq!(expired.len(), 1);

    // no waits remain active; every row records its end state
    assert!(
        f.journal.active_waits(None).unwrap().is_empty(),
        "active_waits_after"
    );
    let got_states: Vec<JValue> = f
        .journal
        .all_waits()
        .unwrap()
        .iter()
        .map(|r| {
            let mut m = JMap::new();
            m.insert(
                "wait_id".into(),
                JValue::Str(r.wait_id.as_str().to_string()),
            );
            m.insert("state".into(), JValue::Str(r.state.clone()));
            m.insert(
                "wake_reason".into(),
                r.wake_reason
                    .clone()
                    .map(JValue::Str)
                    .unwrap_or(JValue::Null),
            );
            JValue::Obj(m)
        })
        .collect();
    assert_eq!(
        JValue::Arr(got_states),
        *g(&want, "all_waits_states"),
        "every wait ends RESOLVED or TIMED_OUT with its reason"
    );
    // the timeout body is byte-identical (Python f-string .2f)
    let timeout_body = f
        .bus
        .mailbox("quiet")
        .iter()
        .find(|m| m.msg_type == arena::msg::EventType::WaitTimeout)
        .map(|m| m.body.clone())
        .unwrap();
    assert_eq!(JValue::Str(timeout_body), *g(&want, "timeout_body"));
    // registry mirrors cleared
    for aid in ["be", "unrelated", "quiet"] {
        assert!(
            f.registry.get(aid).unwrap().pending_waits.is_empty(),
            "{aid} still holds a pending wait"
        );
    }

    // ------------------------------------------------------- journal evidence
    let rows = f.journal.iterate().unwrap();
    let got_events: Vec<JValue> = rows
        .iter()
        .map(|r| {
            JValue::Arr(vec![
                JValue::Int(r.seq),
                JValue::Str(r.etype.clone()),
                JValue::Str(r.actor.clone()),
                JValue::Str(r.target.clone().unwrap_or_default()),
                JValue::Str(r.task_id.clone().unwrap_or_default()),
            ])
        })
        .collect();
    assert_eq!(
        JValue::Arr(got_events),
        *g(&want, "journal_events"),
        "journal event sequence (seq/etype/actor/target/task)"
    );
    // payloads with volatile keys stripped
    let strip = |p: &JValue| -> JValue {
        let mut m = match p {
            JValue::Obj(m) => m.clone(),
            other => panic!("payload not an object: {}", other.to_canon_string()),
        };
        for k in ["correlation_id", "caused_by", "dropped"] {
            m.remove(k);
        }
        JValue::Obj(m)
    };
    let be_row = &f
        .journal
        .events(&EventFilter::new().etype("BUDGET_EXCEEDED"))
        .unwrap()[0];
    assert_eq!(
        strip(&be_row.payload()),
        *g(&want, "budget_exceeded_payload")
    );
    let wr: Vec<JValue> = rows
        .iter()
        .filter(|r| r.etype == "WAIT_REGISTERED")
        .map(|r| strip(&r.payload()))
        .collect();
    assert_eq!(JValue::Arr(wr), *g(&want, "wait_registered_payloads"));
    let wres: Vec<JValue> = rows
        .iter()
        .filter(|r| r.etype == "WAIT_RESOLVED")
        .map(|r| strip(&r.payload()))
        .collect();
    assert_eq!(JValue::Arr(wres), *g(&want, "wait_resolved_payloads"));

    // ------------------------------------------------------------- mailboxes
    let want_mailboxes = match g(&want, "mailboxes") {
        JValue::Obj(m) => m.clone(),
        _ => panic!("mailboxes not an object"),
    };
    for (actor, want_msgs) in &want_mailboxes {
        let got: Vec<JValue> = f
            .bus
            .mailbox(actor)
            .iter()
            .map(|m| JValue::Obj(msgdict(m)))
            .collect();
        assert_eq!(
            JValue::Arr(got),
            want_msgs.clone(),
            "mailbox[{actor}] contents"
        );
    }
    assert_eq!(
        JValue::Int(f.bus.mailbox_len("kernel") as i64),
        *g(&want, "kernel_broadcast_count"),
        "kernel receives broadcasts only"
    );

    // ------------------------------------------------------------ stats/snap
    let mut got_stats = JMap::new();
    got_stats.insert("published".into(), JValue::Int(f.bus.stats.published));
    got_stats.insert("delivered".into(), JValue::Int(f.bus.stats.delivered));
    got_stats.insert(
        "dropped_depth".into(),
        JValue::Int(f.bus.stats.dropped_depth),
    );
    got_stats.insert(
        "dropped_budget".into(),
        JValue::Int(f.bus.stats.dropped_budget),
    );
    got_stats.insert(
        "dropped_unsubscribed".into(),
        JValue::Int(f.bus.stats.dropped_unsubscribed),
    );
    got_stats.insert("wakes".into(), JValue::Int(f.bus.stats.wakes));
    assert_eq!(JValue::Obj(got_stats), *g(&want, "stats"));
    assert_eq!(JValue::Obj(f.bus.snapshot()), *g(&want, "snapshot"));
    assert_eq!(
        JValue::Int(f.bus.polls.polls),
        *g(&want, "polls_default_zero"),
        "the default path never polls"
    );
}

/// §15 replay purity: a journal containing bus/wait events must project
/// without side effects — no new messages, no new waits, no live-runtime
/// mutation, no file writes. `from_journal` = pure projection.
#[test]
fn replaying_bus_wait_journals_is_side_effect_free() {
    let src: PathBuf = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/parity/fixtures/midrun/journal.db"
    )
    .into();
    let tmp = std::env::temp_dir().join(format!("arena-bus-purity-{}.db", std::process::id()));
    std::fs::copy(&src, &tmp).expect("copy fixture");
    let before_bytes = std::fs::read(&tmp).unwrap();
    let before_waits = {
        let j = Journal::open_file(&tmp, None).unwrap();
        let rows = j.iterate().unwrap().len() as i64;
        let waits = j.all_waits().unwrap();
        (rows, waits.len(), j.count().unwrap())
    };

    let j = Journal::open_file(&tmp, None).unwrap();
    let fold1 = JValue::Obj(j.fold().unwrap());
    let fold2 = JValue::Obj(j.fold().unwrap());
    assert_eq!(fold1, fold2, "fold is deterministic");
    let (rows, waits, count) = {
        let rows = j.iterate().unwrap().len() as i64;
        let waits = j.all_waits().unwrap();
        (rows, waits.len(), j.count().unwrap())
    };
    assert_eq!((rows, waits, count), before_waits, "replay added nothing");

    // a fresh bus over the replayed state publishes nothing and arms nothing
    let registry = AgentRegistry::default();
    let bus = Bus::new();
    assert_eq!(bus.stats.published, 0);
    assert_eq!(bus.stats.wakes, 0);
    assert!(bus.mailbox("parent").is_empty());
    assert!(registry.agents.is_empty());

    // and the journal file on disk is byte-identical (no writes on replay)
    drop(j);
    let after_bytes = std::fs::read(&tmp).unwrap();
    assert_eq!(before_bytes, after_bytes, "replay must not write files");
    std::fs::remove_file(&tmp).ok();
}
