//! Registry parity: replicate the Python golden scenario
//! (`tests/parity/registry_capture.py` → `tests/parity/fixtures/registry_golden.json`)
//! in Rust and compare every observable section structurally.
//!
//! Regenerate the golden with `PYTHONPATH=. python3 tests/parity/registry_capture.py`.

use arena::graph::{DependencyGraph, TaskSpec, TaskStatus};
use arena::ids::TaskId;
use arena::journal::Journal;
use arena::lifecycle::{AgentState, Lifecycle};
use arena::registry::{
    emit_registered, emit_terminated, AgentRegistry, RegistryError, SpawnBudget,
};
use arena::sys::json::{parse, JMap, JValue};
use std::path::PathBuf;

fn golden() -> JValue {
    let p: PathBuf = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/parity/fixtures/registry_golden.json"
    )
    .into();
    parse(&std::fs::read_to_string(p).expect("golden file present")).expect("golden parses")
}

fn g<'a>(v: &'a JValue, key: &str) -> &'a JValue {
    v.get(key).unwrap_or_else(|| panic!("missing key {key}"))
}

fn ids(v: &[&arena::registry::AgentRecord]) -> Vec<String> {
    v.iter().map(|a| a.agent_id.as_str().to_string()).collect()
}

fn jstr(v: &JValue) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn jarr_str(v: &JValue) -> Vec<String> {
    match v {
        JValue::Arr(items) => items.iter().map(jstr).collect(),
        _ => panic!("not an array"),
    }
}

#[test]
fn registry_matches_python_golden() {
    let want = golden();

    // ------------------------------------------------ graph + registry setup
    let mut graph = DependencyGraph::default();
    graph
        .add(
            TaskSpec {
                est_work: 10.0,
                ..TaskSpec::new("big", "lots of work", "backend")
            },
            true,
        )
        .unwrap();
    graph.add(TaskSpec::new("t2", "other", "b"), true).unwrap();
    let mut closed = TaskSpec::new("closed", "done thing", "backend");
    closed.est_work = 8.0;
    graph.add(closed, true).unwrap();
    graph.tasks.get_mut(&TaskId::new("closed")).unwrap().status = TaskStatus::Done;

    let mut r = AgentRegistry::new(SpawnBudget {
        max_active_agents: 3,
        max_spawn_epoch: 2,
        min_share_of_remaining: 0.25,
        idle_ttl: 1.0,
        ..Default::default()
    });

    // ---------------------------------------------------------- id slugs
    let next_ids: Vec<String> = [
        "Frontend Engineer",
        "Frontend Engineer",
        "ml engineer",
        "dev ops/infra",
        "ml engineer",
    ]
    .iter()
    .map(|role| r.next_id(role).as_str().to_string())
    .collect();
    assert_eq!(
        JValue::Arr(next_ids.iter().cloned().map(JValue::Str).collect()),
        *g(&want, "next_ids"),
        "next_id sequence"
    );
    assert_eq!(
        next_ids,
        vec![
            "frontend_engineer_01",
            "frontend_engineer_02",
            "ml_engineer_01",
            "dev_ops/infra_01",
            "ml_engineer_02"
        ]
    );

    // ------------------------------------------------------------ register
    let add = |r: &mut AgentRegistry,
               aid: &str,
               role: &str,
               skills: &[&str],
               epoch: i64,
               state: AgentState,
               since: f64,
               spawned_by: &str,
               reason: &str| {
        r.register(
            aid,
            role,
            skills,
            epoch,
            spawned_by,
            reason,
            Some(Lifecycle::with_state(state, since)),
            &[],
        )
        .unwrap()
    };
    add(
        &mut r,
        "be",
        "backend",
        &["api", "http"],
        0,
        AgentState::Idle,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "generalist",
        "backend",
        &["api"],
        0,
        AgentState::Idle,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "specialist",
        "auth engineer",
        &["auth", "oauth", "jwt"],
        0,
        AgentState::Idle,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "idle",
        "x",
        &[],
        0,
        AgentState::Idle,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "busy",
        "y",
        &[],
        0,
        AgentState::Working,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "done_guy",
        "z",
        &[],
        0,
        AgentState::Completed,
        0.25,
        "parent",
        "",
    );
    add(
        &mut r,
        "ghost",
        "gone",
        &[],
        0,
        AgentState::Terminated,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "paused",
        "p",
        &[],
        0,
        AgentState::Paused,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "newbie",
        "n",
        &[],
        0,
        AgentState::Created,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "boot",
        "init",
        &[],
        0,
        AgentState::Initializing,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "stuck",
        "s",
        &[],
        0,
        AgentState::Blocked,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "up",
        "e",
        &[],
        0,
        AgentState::Escalated,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "waiter",
        "w",
        &[],
        0,
        AgentState::WaitingForDependency,
        0.0,
        "parent",
        "",
    );
    add(
        &mut r,
        "db_02",
        "database engineer",
        &["sql", "schema"],
        1,
        AgentState::Idle,
        0.0,
        "backend_01",
        "plan task t_db",
    );

    let dup = r
        .register("be", "backend", &[], 0, "parent", "", None, &[])
        .unwrap_err();
    assert!(
        matches!(dup, RegistryError::AlreadyRegistered(_)),
        "wrong error {dup:?}"
    );
    assert_eq!(
        JValue::Str(dup.to_string()),
        *g(&want, "duplicate_error"),
        "duplicate registration message"
    );

    // --------------------------------------------------------------- cover
    let cover_cases: Vec<(&str, Vec<&str>)> = vec![
        ("backend", vec![]),
        ("api design", vec!["api"]),
        ("oauth auth flow", vec!["auth", "oauth"]),
        ("database", vec![]),
        ("crew_0", vec![]),
        ("", vec![]),
        ("payments", vec!["stripe"]),
        ("sql tuning", vec!["sql"]),
    ];
    let want_cover = g(&want, "cover");
    for (role, skills) in cover_cases {
        let key = if role.is_empty() {
            "empty".to_string()
        } else if role == "payments" {
            "payments stripe".to_string()
        } else {
            role.to_string()
        };
        let got: Vec<String> = ids(&r.cover(role, &skills));
        let want_ids = jarr_str(
            want_cover
                .get(&key)
                .unwrap_or_else(|| panic!("cover case {key}")),
        );
        assert_eq!(got, want_ids, "cover({role:?}, {skills:?})");
    }

    // ------------------------------------------------------ membership
    assert_eq!(
        JValue::Arr(ids(&r.active()).into_iter().map(JValue::Str).collect()),
        *g(&want, "active"),
        "active() insertion order"
    );
    assert_eq!(
        JValue::Arr(
            ids(&r.with_state(&[AgentState::Idle]))
                .into_iter()
                .map(JValue::Str)
                .collect()
        ),
        *g(&want, "with_state_IDLE")
    );
    assert_eq!(
        JValue::Arr(
            ids(&r.with_state(&[AgentState::Working]))
                .into_iter()
                .map(JValue::Str)
                .collect()
        ),
        *g(&want, "with_state_WORKING")
    );
    assert_eq!(
        JValue::Int(r.working_count() as i64),
        *g(&want, "working_count")
    );

    // ------------------------------------------------------ queues / load
    {
        let rec = r.get_mut("be").unwrap();
        rec.task_id = Some(TaskId::new("big"));
        rec.task_queue = ["t2", "big", "extra"]
            .iter()
            .map(|t| TaskId::new(*t))
            .collect();
    }
    let rec = r.get("be").unwrap();
    let pw: Vec<String> = rec
        .pending_work()
        .iter()
        .map(|t| t.as_str().to_string())
        .collect();
    assert_eq!(
        JValue::Arr(pw.clone().into_iter().map(JValue::Str).collect()),
        *g(&want, "pending_work"),
        "pending_work dedups the hat task"
    );
    assert_eq!(JValue::Int(rec.load() as i64), *g(&want, "load"));

    // ---------------------------------------------------------- overload
    graph.tasks.get_mut(&TaskId::new("big")).unwrap().owner = Some(arena::ids::AgentId::new("be"));
    graph.tasks.get_mut(&TaskId::new("closed")).unwrap().owner =
        Some(arena::ids::AgentId::new("be"));
    graph.tasks.get_mut(&TaskId::new("t2")).unwrap().owner = Some(arena::ids::AgentId::new("idle"));
    let want_over = g(&want, "overloaded");
    let cases: Vec<(&str, bool)> = vec![
        ("be@0.0", r.overloaded(&graph, "be")),
        ("idle", r.overloaded(&graph, "idle")),
        ("unknown", r.overloaded(&graph, "nobody")),
    ];
    for (k, v) in cases {
        assert_eq!(
            JValue::Bool(v),
            *want_over.get(k).unwrap_or_else(|| panic!("overload {k}")),
            "overloaded({k})"
        );
    }
    r.get_mut("be").unwrap().work_done = 9.0;
    assert_eq!(
        JValue::Bool(r.overloaded(&graph, "be")),
        *want_over.get("be@9.0").unwrap(),
        "overloaded(be@9.0)"
    );
    r.get_mut("be").unwrap().work_done = 0.0;

    // ---------------------------------------------------------- idle TTL
    let want_ttl = g(&want, "idle_overdue");
    for (t, k) in [(0.5, "0.5"), (1.5, "1.5"), (0.3, "0.3")] {
        let got: Vec<String> = ids(&r.idle_overdue(t));
        let want_ids = jarr_str(want_ttl.get(k).unwrap());
        assert_eq!(got, want_ids, "idle_overdue({t})");
    }

    // ------------------------------------------------------ waits / edges
    {
        let waiter = r.get_mut("waiter").unwrap();
        let mut w1 = JMap::new();
        w1.insert("task_id".into(), JValue::Str("t2".into()));
        w1.insert("condition".into(), JValue::Str("artifact:t2.done".into()));
        waiter.pending_waits.push(w1);
        let mut w2 = JMap::new();
        w2.insert("condition".into(), JValue::Str("tick:9".into()));
        waiter.pending_waits.push(w2);

        let stuck = r.get_mut("stuck").unwrap();
        let mut w3 = JMap::new();
        w3.insert("task_id".into(), JValue::Str("big".into()));
        w3.insert("condition".into(), JValue::Str("artifact:big.out".into()));
        stuck.pending_waits.push(w3);

        let idle = r.get_mut("idle").unwrap();
        let mut w4 = JMap::new();
        w4.insert("task_id".into(), JValue::Str("big".into()));
        idle.pending_waits.push(w4);
    }
    graph.tasks.get_mut(&TaskId::new("big")).unwrap().owner =
        Some(arena::ids::AgentId::new("waiter"));
    let edges = r.wait_for_edges(&graph);
    let mut got_edges: Vec<(String, Vec<String>)> = edges
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_string(),
                v.iter().map(|x| x.as_str().to_string()).collect(),
            )
        })
        .collect();
    // both sides sorted for comparison (golden stores sorted member lists)
    got_edges.sort();
    let mut want_edges: Vec<(String, Vec<String>)> = match g(&want, "wait_for_edges") {
        JValue::Obj(m) => m.iter().map(|(k, v)| (k.clone(), jarr_str(v))).collect(),
        _ => panic!("wait_for_edges not an object"),
    };
    want_edges.sort();
    assert_eq!(
        got_edges, want_edges,
        "wait-for edges (IDLE agent's wait ignored)"
    );

    // ---------------------------------------------------------- snapshot
    {
        let rec = r.get_mut("db_02").unwrap();
        rec.lifecycle.since = 0.123456789;
        rec.work_done = 1.23456789;
        rec.msgs_sent = 7;
        rec.progress_steps = 3;
        let mut cog = JMap::new();
        cog.insert("kind".into(), JValue::Str("policy".into()));
        cog.insert("name".into(), JValue::Str("simulated".into()));
        rec.cognition = cog;
    }
    let snap = JValue::Obj(r.snapshot());
    let want_snap = g(&want, "snapshot");
    let snap_map = match &snap {
        JValue::Obj(m) => m,
        _ => panic!(),
    };
    let want_map = match want_snap {
        JValue::Obj(m) => m,
        _ => panic!(),
    };
    assert_eq!(
        snap_map.len(),
        want_map.len(),
        "snapshot agent count (terminated-but-present agents included)"
    );
    for (aid, want_agent) in want_map {
        let got_agent = snap_map
            .get(aid)
            .unwrap_or_else(|| panic!("snapshot missing {aid}"));
        assert_eq!(
            got_agent,
            want_agent,
            "snapshot[{aid}] — got {} want {}",
            got_agent.to_canon_string(),
            want_agent.to_canon_string()
        );
    }

    // ------------------------------------------------------ status lines
    let lines = r.status_lines();
    let want_lines = jarr_str(g(&want, "status_lines"));
    assert_eq!(lines, want_lines);

    // ---------------------------------------------------- journal emits
    let mut j = Journal::open_memory().unwrap();
    emit_registered(&mut j, r.get("db_02").unwrap());
    emit_terminated(&mut j, r.get("db_02").unwrap(), "idle reap");
    let rows = j.iterate().unwrap();
    let got_types: Vec<String> = rows.iter().map(|x| x.etype.clone()).collect();
    assert_eq!(
        JValue::Arr(got_types.into_iter().map(JValue::Str).collect()),
        *g(&want, "emit_types")
    );
    let got_targets: Vec<String> = rows.iter().map(|x| x.target.clone().unwrap()).collect();
    assert_eq!(
        JValue::Arr(got_targets.into_iter().map(JValue::Str).collect()),
        *g(&want, "emit_targets")
    );
    for (i, key) in [
        (0usize, "emit_registered_payload"),
        (1, "emit_terminated_payload"),
    ] {
        let got_payload = rows[i].payload();
        let want_payload = g(&want, key);
        assert_eq!(
            got_payload,
            *want_payload,
            "{key}: got {} want {}",
            got_payload.to_canon_string(),
            want_payload.to_canon_string()
        );
    }

    // -------------------------------------------------------- terminate
    let v0 = r.version;
    let rec = r.terminate("idle", "ttl");
    let want_term = g(&want, "terminate");
    assert_eq!(
        JValue::Str(
            rec.as_ref()
                .map(|x| x.agent_id.as_str().to_string())
                .unwrap_or_default()
        ),
        *want_term.get("returned").unwrap()
    );
    assert_eq!(
        JValue::Bool(r.version > v0),
        *want_term.get("version_bumped").unwrap()
    );
    assert_eq!(
        JValue::Bool(r.get("idle").is_none()),
        *want_term.get("gone").unwrap()
    );
}
