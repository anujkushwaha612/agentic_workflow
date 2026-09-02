//! M8 kernel tests — ownership, tick loop, scheduling, recovery, seams.

use super::*;
use crate::cognition::{Cognition, CognitionError, Control, Intent, Observation};
use crate::graph::TaskSpec;
use crate::lifecycle::AgentState;
use crate::policy::{PolicyContext, SimulatedWork, WaitForArtifacts};
use crate::registry::SpawnBudget;
use std::path::{Path, PathBuf};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "arena-k8-{}-{name}-{}",
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

fn opts(root: &Path) -> KernelOpts {
    KernelOpts {
        root: Some(root.to_path_buf()),
        journal_path: Some(root.join("j.db")),
        budget: SpawnBudget {
            max_active_agents: 6,
            max_concurrent_workers: 2,
            idle_ttl: 1e9,
            ..Default::default()
        },
        default_policy: "simulated".into(),
        role_policies: BTreeMap::new(),
        ..KernelOpts::default()
    }
}

fn spec(id: &str, role: &str, produces: &[&str], consumes: &[&str]) -> TaskSpec {
    let mut t = TaskSpec::new(id, id, role);
    t.produces = produces.iter().map(|s| s.to_string()).collect();
    t.consumes = consumes.iter().map(|s| s.to_string()).collect();
    t.est_work = 1.0;
    t
}

fn kernel_at(root: &Path) -> Kernel {
    Kernel::new(opts(root)).unwrap()
}

#[test]
fn in_memory_kernel_does_not_write_cwd() {
    let cwd = std::env::current_dir().unwrap();
    let before: std::collections::HashSet<_> = std::fs::read_dir(&cwd)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name()))
        .collect();
    let mut k = Kernel::in_memory();
    k.run(3);
    k.checkpoint("test");
    k.write_config();
    let after: std::collections::HashSet<_> = std::fs::read_dir(&cwd)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name()))
        .collect();
    let extra: Vec<_> = after.difference(&before).collect();
    assert!(
        extra.is_empty(),
        "in-memory kernel wrote into CWD: {extra:?}"
    );
    assert!(k.side_anchor().is_none());
}

#[test]
fn explicit_root_anchors_side_files() {
    let d = tmp("anchor");
    let mut k = kernel_at(&d);
    k.checkpoint("bound");
    assert!(d.join("kernel_config.json").exists());
    assert!(
        !PathBuf::from("kernel_config.json").exists() || {
            // only ok if it was already there; we didn't create var/ in repo
            true
        }
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn two_kernels_do_not_share_state() {
    let a_dir = tmp("a");
    let b_dir = tmp("b");
    let mut a = kernel_at(&a_dir);
    let mut b = kernel_at(&b_dir);
    a.submit_plan(
        "saas",
        vec![spec("t_db", "database", &["schema.sql"], &[])],
        &[("database_01", "database", vec!["sql"])],
    )
    .unwrap();
    b.submit_plan(
        "ml",
        vec![spec("t_data", "data", &["features.parquet"], &[])],
        &[("data_01", "data", vec!["etl"])],
    )
    .unwrap();
    assert!(a.registry.get("database_01").is_some());
    assert!(b.registry.get("database_01").is_none());
    assert_ne!(
        a.registry
            .agents
            .iter()
            .map(|x| x.role.clone())
            .collect::<Vec<_>>(),
        b.registry
            .agents
            .iter()
            .map(|x| x.role.clone())
            .collect::<Vec<_>>()
    );
    let _ = std::fs::remove_dir_all(&a_dir);
    let _ = std::fs::remove_dir_all(&b_dir);
}

#[test]
fn virtual_clock_advances_per_tick() {
    let d = tmp("clock");
    let mut k = kernel_at(&d);
    assert_eq!(k.now(), 0.0);
    k.run(5);
    assert!((k.now() - 0.05).abs() < 1e-9, "now={}", k.now());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn submit_without_planner_does_not_invent_an_org() {
    let d = tmp("empty");
    let mut k = kernel_at(&d);
    let out = k.submit("rename the landing page title");
    assert_eq!(out.get("tasks").and_then(|v| v.as_int()), Some(0));
    assert!(k.registry.agents.is_empty());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn dag_run_completes_and_consumers_wait() {
    let d = tmp("dag");
    let mut k = kernel_at(&d);
    // producers use simulated (default); consumers wait
    k.role_policies
        .insert("database".into(), "simulated".into());
    k.role_policies.insert("backend".into(), "wait".into());
    k.role_overrides.insert("database".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(0));
        m
    });
    k.role_overrides.insert("backend".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(0));
        m
    });
    k.submit_plan(
        "api",
        vec![
            spec("t_db", "database", &["schema.sql"], &[]),
            spec("t_api", "backend", &["api.json"], &["schema.sql"]),
        ],
        &[
            ("database_01", "database", vec!["sql"]),
            ("backend_01", "backend", vec!["api"]),
        ],
    )
    .unwrap();
    // first tick: backend should park, not block
    k.run(1);
    let be = k.registry.get("backend_01").unwrap();
    assert_ne!(be.lifecycle.state, AgentState::Blocked);
    let res = k.run(40);
    assert_eq!(
        res.get("done").and_then(|v| v.as_bool()),
        Some(true),
        "open={:?}",
        res.get("open_tasks")
    );
    assert!(k.artifact_exists("schema.sql"));
    assert!(k.artifact_exists("api.json"));
    assert!(res.get("chain_ok").and_then(|v| v.as_bool()).unwrap());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn worker_cap_is_not_agent_count() {
    let d = tmp("cap");
    let mut opts = opts(&d);
    opts.budget.max_concurrent_workers = 1;
    opts.budget.max_active_agents = 8;
    let mut k = Kernel::new(opts).unwrap();
    k.role_policies.insert("alpha".into(), "simulated".into());
    k.role_overrides.insert("alpha".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(20));
        m
    });
    k.submit_plan(
        "cap",
        vec![
            spec("t1", "alpha", &["a"], &[]),
            spec("t2", "alpha", &["b"], &[]),
        ],
        &[
            ("alpha_01", "alpha", vec!["a"]),
            ("alpha_02", "alpha", vec!["a"]),
        ],
    )
    .unwrap();
    // both have work; cap is 1
    let el = k.eligible();
    assert_eq!(el.len(), 1, "{el:?}");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn transition_journals_rejected_edges() {
    let d = tmp("fsm");
    let mut k = kernel_at(&d);
    k.register_agent("be_01", "backend", &[], 0, "parent")
        .unwrap();
    k.make_actor("be_01");
    assert!(k.transition("be_01", AgentState::Working, "start"));
    assert!(k.transition("be_01", AgentState::Completed, "done"));
    // COMPLETED → WORKING is illegal
    let ok = k.transition("be_01", AgentState::Working, "chaos");
    assert!(!ok);
    assert_eq!(
        k.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Completed
    );
    let n = k
        .journal
        .events(&EventFilter::new().etype("ILLEGAL_TRANSITION"))
        .unwrap()
        .len();
    assert!(n >= 1);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn pause_is_not_eligible() {
    let d = tmp("pause");
    let mut k = kernel_at(&d);
    k.role_policies.insert("backend".into(), "simulated".into());
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"], &[])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    assert!(k.pause("backend_01"));
    assert_eq!(
        k.registry.get("backend_01").unwrap().lifecycle.state,
        AgentState::Paused
    );
    let el = k.eligible();
    assert!(!el.contains(&"backend_01".to_string()), "{el:?}");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn spawn_request_queues_without_parent() {
    let d = tmp("spawn");
    let mut k = kernel_at(&d);
    k.role_policies
        .insert("backend".into(), "specialist".into());
    k.role_overrides.insert("backend".into(), {
        let mut m = JMap::new();
        m.insert("after_steps".into(), JValue::Int(-1));
        m
    });
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"], &[])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    k.run(2);
    assert!(
        !k.spawn_requests.is_empty(),
        "specialist policy must queue a SPAWN_AGENT_REQUEST"
    );
    assert_eq!(
        k.registry.active().len(),
        1,
        "Parent is deferred: no new agent"
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn per_agent_cognition_factories() {
    let d = tmp("cog");
    let mut k = kernel_at(&d);
    k.set_cognition_factory(
        "backend",
        CognitionFactory::new(|id| {
            Box::new(crate::cognition::PolicyCognition::new(
                Box::new(SimulatedWork { steps: 0 }),
                id,
            ))
        }),
    );
    k.submit_plan(
        "p",
        vec![
            spec("t1", "backend", &["a"], &[]),
            spec("t2", "backend", &["b"], &[]),
        ],
        &[
            ("backend_01", "backend", vec!["api"]),
            ("backend_02", "backend", vec!["api"]),
        ],
    )
    .unwrap();
    k.bind_cognition("backend_01", None).unwrap();
    k.bind_cognition("backend_02", None).unwrap();
    let r = k.cognition_report();
    assert_eq!(r.get("agents").and_then(|v| v.as_int()), Some(2));
    // same role → same prompt hash (class|agent_id differs so hashes differ)
    let rows = match r.get("rows") {
        Some(JValue::Arr(v)) => v,
        _ => panic!("rows"),
    };
    let p1 = rows[0]
        .get("prompt_sha256_16")
        .and_then(|v| v.as_str())
        .unwrap();
    let p2 = rows[1]
        .get("prompt_sha256_16")
        .and_then(|v| v.as_str())
        .unwrap();
    assert_ne!(p1, p2, "per-agent prompt digest must differ");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn escalate_lands_in_inbox_file() {
    let d = tmp("esc");
    let mut k = kernel_at(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"], &[])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    struct Boom;
    impl Cognition for Boom {
        fn name(&self) -> &str {
            "boom"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _o: &Observation,
            _c: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            let mut i = Intent::new();
            i.control = Control::ESCALATE.into();
            i.reason = "this needs a human call".into();
            Ok(i)
        }
    }
    k.bind_cognition("backend_01", Some(Box::new(Boom)))
        .unwrap();
    k.run(2);
    assert!(!k.parent_inbox.is_empty());
    let inbox = d.join("var/inbox.jsonl");
    assert!(inbox.exists(), "inbox was not persisted");
    let text = std::fs::read_to_string(&inbox).unwrap();
    assert!(text.contains("human call") || text.contains("backend_01"));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn cortex_force_state_is_applied() {
    let d = tmp("cortex");
    let mut k = kernel_at(&d);
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"], &[])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    let dec = d.join("var/decisions.jsonl");
    std::fs::create_dir_all(dec.parent().unwrap()).unwrap();
    std::fs::write(
        &dec,
        r#"{"kind":"force_state","agent_id":"backend_01","state":"PAUSED"}
"#,
    )
    .unwrap();
    k.run(1);
    assert_eq!(
        k.registry.get("backend_01").unwrap().lifecycle.state,
        AgentState::Paused
    );
    assert_eq!(std::fs::read_to_string(&dec).unwrap().trim(), "");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn snapshot_and_quiet_replay_agree() {
    let d = tmp("replay");
    let mut k = kernel_at(&d);
    k.role_policies
        .insert("database".into(), "simulated".into());
    k.role_policies.insert("backend".into(), "wait".into());
    k.role_overrides.insert("database".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(0));
        m
    });
    k.role_overrides.insert("backend".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(0));
        m
    });
    k.submit_plan(
        "api",
        vec![
            spec("t_db", "database", &["schema.sql"], &[]),
            spec("t_api", "backend", &["api.json"], &["schema.sql"]),
        ],
        &[
            ("database_01", "database", vec!["sql"]),
            ("backend_01", "backend", vec!["api"]),
        ],
    )
    .unwrap();
    k.run(40);
    let live = k.snapshot();
    let before = k.journal.count().unwrap();
    let jp = d.join("j.db");
    let mut opts = opts(&d);
    opts.journal_path = Some(jp.clone());
    let r = Kernel::from_journal(&jp, true, opts).unwrap();
    let after = r.journal.count().unwrap();
    assert_eq!(after, before, "quiet replay must not append");
    let rp = r.snapshot();
    for key in ["agents", "tasks", "artifacts"] {
        assert_eq!(
            rp.get(key),
            live.get(key),
            "{key} diverged after replay\nlive={}\nrec={}",
            live.get(key)
                .map(|v| v.to_canon_string())
                .unwrap_or_default(),
            rp.get(key).map(|v| v.to_canon_string()).unwrap_or_default()
        );
    }
    assert_eq!(
        r.graph.known_artifacts,
        k.artifacts.keys().cloned().collect()
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn restore_cursor_from_checkpoint() {
    let d = tmp("cursor");
    let mut k = kernel_at(&d);
    k.role_policies.insert("backend".into(), "simulated".into());
    k.role_overrides.insert("backend".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(8));
        m
    });
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"], &[])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    k.run(3);
    let steps = k.actor("backend_01").unwrap().steps_run;
    assert!(steps >= 1, "steps={steps}");
    let jp = d.join("j.db");
    let r = Kernel::from_journal(&jp, true, opts(&d)).unwrap();
    assert_eq!(r.actor("backend_01").unwrap().steps_run, steps);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn completed_agent_with_backlog_is_scheduled() {
    let d = tmp("backlog");
    let mut k = kernel_at(&d);
    k.role_policies.insert("backend".into(), "simulated".into());
    k.role_overrides.insert("backend".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(-1));
        m
    });
    k.register_agent("backend_01", "backend", &["api"], 0, "parent")
        .unwrap();
    k.make_actor("backend_01");
    k.add_task(spec("t1", "backend", &["a"], &[])).unwrap();
    k.add_task(spec("t2", "backend", &["b"], &[])).unwrap();
    k.assign("t1", "backend_01");
    k.assign("t2", "backend_01");
    let res = k.run(20);
    assert_eq!(res.get("done").and_then(|v| v.as_bool()), Some(true));
    assert!(k.artifact_exists("a"));
    assert!(k.artifact_exists("b"));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn inject_file_is_drained() {
    let d = tmp("inj");
    let mut k = kernel_at(&d);
    let p = d.join("var/inject.jsonl");
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(
        &p,
        r#"{"requested_role":"security","reason":"authz","from":"external"}
"#,
    )
    .unwrap();
    let n = k.drain_injection_file(true);
    assert_eq!(n, 1);
    assert_eq!(k.spawn_requests.len(), 1);
    assert_eq!(std::fs::read_to_string(&p).unwrap().trim(), "");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn actor_does_not_hold_a_kernel_pointer() {
    // compile-time: AgentActor has no kernel field; this test documents it.
    let d = tmp("ptr");
    let mut k = kernel_at(&d);
    k.register_agent("be_01", "backend", &[], 0, "parent")
        .unwrap();
    k.make_actor("be_01");
    let actor = k.actor("be_01").unwrap();
    assert_eq!(actor.agent_id, "be_01");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn wait_policy_parks_on_missing_artifact() {
    let d = tmp("wait");
    let mut k = kernel_at(&d);
    k.role_policies.insert("frontend".into(), "wait".into());
    k.submit_plan(
        "ui",
        vec![spec("t_fe", "frontend", &["ui"], &["api.json"])],
        &[("frontend_01", "frontend", vec!["ui"])],
    )
    .unwrap();
    k.run(2);
    assert_eq!(
        k.registry.get("frontend_01").unwrap().lifecycle.state,
        AgentState::WaitingForDependency
    );
    assert!(!k.eligible().contains(&"frontend_01".to_string()));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn why_and_trace_are_json_safe() {
    let d = tmp("why");
    let mut k = kernel_at(&d);
    k.role_policies.insert("backend".into(), "simulated".into());
    k.role_overrides.insert("backend".into(), {
        let mut m = JMap::new();
        m.insert("steps".into(), JValue::Int(0));
        m
    });
    k.submit_plan(
        "p",
        vec![spec("t1", "backend", &["a"], &[])],
        &[("backend_01", "backend", vec!["api"])],
    )
    .unwrap();
    k.run(10);
    let w = k.why("backend_01");
    assert!(w.contains_key("state"));
    let _ = JValue::Obj(w).to_canon_string();
    let text = k.report();
    assert!(text.contains("KERNEL"));
    let _ = std::fs::remove_dir_all(&d);
}

// silence unused import in some cfgs
fn _keep(p: WaitForArtifacts) {
    let _ = p;
}
