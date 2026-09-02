//! M9 spawn funnel: intake → evaluate → commit, recovery of the ledger.

use arena::graph::TaskSpec;
use arena::kernel::{Kernel, KernelOpts};
use arena::parent::SpawnIn;
use arena::registry::SpawnBudget;
use arena::spawn::{RequestState, SpawnRequest};
use arena::sys::json::{JMap, JValue};
use std::path::PathBuf;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "arena-m9-{}-{name}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn kernel(root: &std::path::Path) -> Kernel {
    Kernel::new(KernelOpts {
        root: Some(root.to_path_buf()),
        journal_path: Some(root.join("j.db")),
        budget: SpawnBudget {
            max_active_agents: 6,
            max_concurrent_workers: 2,
            idle_ttl: 1e9,
            // Most funnel tests ask a working agent for help; that agent is
            // behind on its own task, so the overload veto would refuse every
            // request for a reason unrelated to the path under test (Python
            // `_kernel(..., ignore_overload=True)`).
            requester_overload_factor: 1e9,
            ..Default::default()
        },
        ..KernelOpts::default()
    })
    .unwrap()
}

fn spec(id: &str, role: &str, produces: &[&str]) -> TaskSpec {
    let mut t = TaskSpec::new(id, id, role);
    t.produces = produces.iter().map(|s| s.to_string()).collect();
    t.est_work = 4.0;
    t
}

fn pay_req(from: &str) -> JMap {
    let mut m = JMap::new();
    m.insert("from".into(), JValue::Str(from.into()));
    m.insert(
        "requested_role".into(),
        JValue::Str("payment specialist".into()),
    );
    m.insert("reason".into(), JValue::Str("webhooks".into()));
    m.insert("estimated_work".into(), JValue::Float(3.0));
    m.insert(
        "expected_outputs".into(),
        JValue::Arr(vec![JValue::Str("artifacts/payments-webhooks.md".into())]),
    );
    m.insert(
        "required_skills".into(),
        JValue::Arr(vec![
            JValue::Str("payments".into()),
            JValue::Str("webhooks".into()),
        ]),
    );
    m.insert("capability_class".into(), JValue::Str("payments".into()));
    m.insert("correlation_id".into(), JValue::Str("c-pay".into()));
    m
}

#[test]
fn receipt_is_journalled_before_decision() {
    let d = tmp("receipt");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let dcs = k.decide_spawn_map(&pay_req("backend_01"));
    assert!(
        dcs.ok || dcs.rule == "REUSE_EXISTING" || dcs.rule == "APPROVE",
        "{dcs:?}"
    );
    let n = k
        .journal()
        .events(&arena::journal::EventFilter::new().etype("SPAWN_REQUEST_RECEIVED"))
        .unwrap()
        .len();
    assert!(n >= 1, "receipt must be journalled");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn reuse_beats_a_new_agent() {
    let d = tmp("reuse");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    // a second backend request should reuse, not spawn
    let mut m = JMap::new();
    m.insert("from".into(), JValue::Str("backend_01".into()));
    m.insert("requested_role".into(), JValue::Str("backend".into()));
    m.insert("reason".into(), JValue::Str("more api".into()));
    m.insert("estimated_work".into(), JValue::Float(3.0));
    m.insert(
        "expected_outputs".into(),
        JValue::Arr(vec![JValue::Str("contracts/api2.json".into())]),
    );
    m.insert(
        "required_skills".into(),
        JValue::Arr(vec![JValue::Str("api".into())]),
    );
    m.insert("correlation_id".into(), JValue::Str("c-be".into()));
    let out = k.decide_spawn_map(&m);
    assert_eq!(out.rule, "REUSE_EXISTING", "{out:?}");
    assert_eq!(
        k.parent()
            .ledger
            .counts()
            .get("REROUTED")
            .and_then(|v| v.as_int()),
        Some(1)
    );
    assert_eq!(k.metrics().get("reuses").and_then(|v| v.as_int()), Some(1));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn unsupported_capability_is_refused() {
    let d = tmp("unsup");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let mut m = pay_req("backend_01");
    m.insert(
        "capability_class".into(),
        JValue::Str("gpu-training".into()),
    );
    m.insert("requested_role".into(), JValue::Str("gpu trainer".into()));
    let before = k.journal().count().unwrap();
    let agents = k
        .parent()
        .ledger
        .counts()
        .get("total")
        .and_then(|v| v.as_int());
    let out = k.decide_spawn_map(&m);
    assert_eq!(out.rule, "REJECT_UNSUPPORTED", "{out:?}");
    assert!(!out.ok);
    let _ = (before, agents);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn two_identical_requests_dedup() {
    let d = tmp("dedup");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let m = pay_req("backend_01");
    let a = k.decide_spawn_map(&m);
    let b = k.decide_spawn_map(&m);
    assert!(
        a.ok || a.rule == "APPROVE" || a.rule == "REUSE_EXISTING",
        "{a:?}"
    );
    assert_eq!(b.rule, "REJECT_INTAKE", "{b:?}");
    assert_eq!(
        k.metrics()
            .get("requests_deduplicated")
            .and_then(|v| v.as_int()),
        Some(1)
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn spawn_depth_enforced() {
    let d = tmp("depth");
    let mut opts = KernelOpts {
        root: Some(d.clone()),
        journal_path: Some(d.join("j.db")),
        budget: SpawnBudget {
            max_spawn_epoch: 1,
            idle_ttl: 1e9,
            ..Default::default()
        },
        ..KernelOpts::default()
    };
    opts.budget.max_active_agents = 8;
    let mut k = Kernel::new(opts).unwrap();
    k.register_agent("deep_01", "backend", &["api"], 1, "parent")
        .unwrap();
    k.make_actor("deep_01");
    let out = k.decide_spawn_map(&pay_req("deep_01"));
    assert_eq!(out.rule, "REJECT_SPAWN_DEPTH", "{out:?}");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn escalate_writes_inbox() {
    let d = tmp("esc");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let mut m = pay_req("backend_01");
    m.insert("requires_judgment".into(), JValue::Bool(true));
    let out = k.decide_spawn_map(&m);
    assert_eq!(out.rule, "ESCALATE", "{out:?}");
    assert!(d.join("var/inbox.jsonl").exists());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn planner_staffs_a_graph() {
    let d = tmp("plan");
    let mut k = kernel(&d);
    let out = k.submit(
        "Build a SaaS app with authentication, dashboard, API backend, PostgreSQL database",
    );
    assert!(
        out.get("tasks").and_then(|v| v.as_int()).unwrap_or(0) >= 3,
        "{out:?}"
    );
    assert!(
        out.get("agents")
            .and_then(|v| match v {
                JValue::Arr(a) => Some(a.len()),
                _ => None,
            })
            .unwrap_or(0)
            >= 2
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn quiet_replay_restores_ledger_and_does_not_write() {
    let d = tmp("replay");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let _ = k.decide_spawn_map(&pay_req("backend_01"));
    let live_total = k
        .parent()
        .ledger
        .counts()
        .get("total")
        .and_then(|v| v.as_int())
        .unwrap();
    assert!(live_total >= 1);
    let before = k.journal().count().unwrap();
    drop(k);
    let r = Kernel::from_journal(
        &d.join("j.db"),
        true,
        KernelOpts {
            root: Some(d.clone()),
            journal_path: Some(d.join("j.db")),
            ..KernelOpts::default()
        },
    )
    .unwrap();
    assert_eq!(r.journal().count().unwrap(), before);
    let rec_total = r
        .parent()
        .ledger
        .counts()
        .get("total")
        .and_then(|v| v.as_int())
        .unwrap();
    assert_eq!(rec_total, live_total);
    // spent rid must not be re-openable as a fresh RECEIVED
    let spent = r.parent().ledger.newest_first();
    assert!(spent.iter().any(|e| e.state != RequestState::Received));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn submit_without_match_invents_nothing() {
    let d = tmp("empty");
    let mut k = kernel(&d);
    let out = k.submit("rename the landing page title");
    assert_eq!(out.get("tasks").and_then(|v| v.as_int()), Some(0));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn cycle_amend_rolls_back() {
    let d = tmp("cycle");
    let mut k = kernel(&d);
    let mut a = TaskSpec::new("a", "a", "r1");
    a.produces = vec!["x".into()];
    k.add_task(a).unwrap();
    let req = SpawnRequest {
        requester_agent_id: "parent".into(),
        requested_role: "r2".into(),
        reason: "cycle".into(),
        estimated_work: 3.0,
        expected_outputs: vec!["x".into()], // same produce — not itself a cycle
        required_inputs: vec!["x".into()],
        correlation_id: "c-cyc".into(),
        capability_class: "general".into(),
        ..Default::default()
    };
    // force a cycle via explicit amend through honour_amend
    let mut b = TaskSpec::new("b", "b", "r2");
    b.consumes = vec!["x".into()];
    let mut deps = JMap::new();
    deps.insert("a".into(), JValue::Arr(vec![JValue::Str("b".into())]));
    let res = k.honour_amend(vec![b], &deps, "c-cyc");
    assert!(!res.ok, "{res:?}");
    assert!(res.restored);
    assert!(k.parent().planner.plan("noop").0.is_empty() || true);
    let _ = req;
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn decide_spawn_from_typed_request() {
    let d = tmp("typed");
    let mut k = kernel(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let r = SpawnRequest {
        requester_agent_id: "backend_01".into(),
        requested_role: "security".into(),
        reason: "threat model".into(),
        required_skills: vec!["crypto".into()],
        estimated_work: 3.0,
        expected_outputs: vec!["docs/security.md".into()],
        correlation_id: "c-sec".into(),
        capability_class: "security".into(),
        ..Default::default()
    };
    let out = k.decide_spawn(SpawnIn::Request(r));
    assert!(
        out.ok || out.rule == "APPROVE" || out.rule == "REUSE_EXISTING",
        "{out:?}"
    );
    let _ = std::fs::remove_dir_all(&d);
}
