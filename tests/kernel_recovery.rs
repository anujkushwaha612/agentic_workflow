//! Integration: live kernel == recovered kernel, and quiet replay is write-free.

use arena::graph::TaskSpec;
use arena::kernel::{Kernel, KernelOpts};
use arena::registry::SpawnBudget;
use arena::sys::json::JValue;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn tmp() -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "arena-k8-int-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn spec(id: &str, role: &str, produces: &[&str], consumes: &[&str]) -> TaskSpec {
    let mut t = TaskSpec::new(id, id, role);
    t.produces = produces.iter().map(|s| s.to_string()).collect();
    t.consumes = consumes.iter().map(|s| s.to_string()).collect();
    t.est_work = 1.0;
    t
}

#[test]
fn live_equals_recovered_and_quiet_replay_is_pure() {
    let d = tmp();
    let mut opts = KernelOpts {
        root: Some(d.clone()),
        journal_path: Some(d.join("j.db")),
        budget: SpawnBudget {
            idle_ttl: 1e9,
            max_concurrent_workers: 2,
            ..Default::default()
        },
        default_policy: "simulated".into(),
        role_policies: BTreeMap::new(),
        ..KernelOpts::default()
    };
    opts.role_policies
        .insert("database".into(), "simulated".into());
    opts.role_policies.insert("backend".into(), "wait".into());
    let mut steps = arena::sys::json::JMap::new();
    steps.insert("steps".into(), JValue::Int(0));
    opts.role_overrides.insert("database".into(), steps.clone());
    opts.role_overrides.insert("backend".into(), steps);

    let mut k = Kernel::new(opts).unwrap();
    k.submit_plan(
        "build api",
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
    let summary = k.run(60);
    assert_eq!(
        summary.get("done").and_then(|v| v.as_bool()),
        Some(true),
        "{summary:?}"
    );
    assert!(k.artifact_exists("schema.sql"));
    assert!(k.artifact_exists("api.json"));

    let live = k.snapshot();
    let events_before = k.journal().count().unwrap();
    let chain_before = k.journal().verify_chain().unwrap();
    assert!(chain_before.0, "{}", chain_before.1);

    // drop the live connection so recovery opens the same file cleanly
    drop(k);

    let rec_opts = KernelOpts {
        root: Some(d.clone()),
        journal_path: Some(d.join("j.db")),
        budget: SpawnBudget {
            idle_ttl: 1e9,
            ..Default::default()
        },
        ..KernelOpts::default()
    };
    let r = Kernel::from_journal(&d.join("j.db"), true, rec_opts).unwrap();
    let events_after = r.journal().count().unwrap();
    assert_eq!(
        events_after, events_before,
        "quiet from_journal must not write"
    );
    let recovered = r.snapshot();
    for key in ["agents", "tasks", "artifacts", "tick"] {
        assert_eq!(
            recovered.get(key).map(|v| v.to_canon_string()),
            live.get(key).map(|v| v.to_canon_string()),
            "{key} live vs recovered"
        );
    }
    let chain_after = r.journal().verify_chain().unwrap();
    assert!(chain_after.0);

    // a loud resume must append exactly one REPLAY_COMPLETE
    drop(r);
    let rec_opts2 = KernelOpts {
        root: Some(d.clone()),
        journal_path: Some(d.join("j.db")),
        ..KernelOpts::default()
    };
    let loud = Kernel::from_journal(&d.join("j.db"), false, rec_opts2).unwrap();
    assert_eq!(loud.journal().count().unwrap(), events_before + 1);
    let _ = std::fs::remove_dir_all(&d);
}
