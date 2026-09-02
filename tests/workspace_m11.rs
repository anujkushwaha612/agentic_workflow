//! M11: project workspace, jail bind to source/, engine/product split.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arena::journal::EventFilter;
use arena::kernel::{Kernel, KernelOpts};
use arena::product::{
    organise, project_id, OrganiseOpts, Plan, PlanAgent, PlanTask, Project, ProjectError,
};
use arena::sys::json::{parse, JMap, JValue};
use arena::tools::GitBind;

static N: AtomicU64 = AtomicU64::new(0);

fn tmp(name: &str) -> PathBuf {
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("arena-m11-{name}-{n}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn project_id_is_sortable_and_slugged() {
    let id = project_id("Task Tracker!!!", Some(1_704_067_200.0));
    let s = id.as_str();
    assert!(s.contains("task-tracker"), "{s}");
    assert!(
        s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
        "{s}"
    );
}

#[test]
fn create_writes_layout_and_journals_project_created() {
    let ws = tmp("create");
    let p = Project::create(
        &ws,
        "hello world",
        "hello-world",
        false,
        false,
        None,
        Some(1_704_067_200.0),
    )
    .unwrap();
    assert!(p.arena_dir().is_dir());
    assert!(p.source_dir().is_dir());
    assert!(p.artifacts_dir().is_dir());
    assert!(p.logs_dir().is_dir());
    assert!(p.arena_dir().join("project.json").is_file());
    assert!(p.root.join("project.json").is_file());
    assert_eq!(p.project_id(), "hello-world");
    assert!(p.journal_path().is_file());

    let text = std::fs::read_to_string(p.arena_dir().join("project.json")).unwrap();
    let man = parse(&text).unwrap();
    assert_eq!(man.get("state").and_then(|v| v.as_str()), Some("created"));
    assert_eq!(
        man.get("runtime_version").and_then(|v| v.as_str()),
        Some(arena::RUNTIME_VERSION)
    );

    let mut j = arena::journal::Journal::open_file(p.journal_path(), Some(0.0)).unwrap();
    let rows = j
        .events(&EventFilter::new().etype("PROJECT_CREATED"))
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "{:?}",
        rows.iter().map(|r| r.etype.clone()).collect::<Vec<_>>()
    );
    j.close();
    let _ = std::fs::remove_dir_all(&ws);
}

#[test]
fn refuse_foreign_directory_and_existing_project() {
    let ws = tmp("refuse");
    let root = ws.join("projects").join("taken");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("secret.txt"), "nope\n").unwrap();
    let err = Project::create(&ws, "x", "taken", false, false, None, None).unwrap_err();
    assert!(matches!(err, ProjectError::Exists(_)), "{err}");

    let p = Project::create(&ws, "ok", "fresh", false, false, None, None).unwrap();
    let again = Project::create(&ws, "ok", "fresh", false, false, None, None).unwrap_err();
    assert!(matches!(again, ProjectError::Exists(_)), "{again}");
    let loaded = Project::load(&ws, "fresh").unwrap();
    assert_eq!(loaded.project_id(), p.project_id());
    let found = Project::discover(&ws);
    assert!(found
        .iter()
        .any(|m| m.get("project_id").and_then(|v| v.as_str()) == Some("fresh")));
    let _ = std::fs::remove_dir_all(&ws);
}

#[test]
fn kernel_on_project_does_not_write_cwd() {
    let cwd = std::env::current_dir().unwrap();
    let before: Vec<_> = std::fs::read_dir(&cwd)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name()))
        .collect();
    let ws = tmp("anchor");
    let p = Project::create(&ws, "anchored", "anchored", false, false, None, None).unwrap();
    let opts = KernelOpts {
        journal_path: Some(p.journal_path()),
        root: Some(p.arena_dir()),
        clock_mode: "virtual".into(),
        ..KernelOpts::default()
    };
    let mut k = Kernel::new(opts).unwrap();
    k.bind_tools("coder", &p.source_dir(), &[""], &[""], None, GitBind::Jail);
    k.set_tool_logs_dir(p.logs_dir());
    let after: Vec<_> = std::fs::read_dir(&cwd)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name()))
        .collect();
    assert_eq!(before, after, "kernel must not create CWD side files");
    assert!(
        k.side_anchor()
            .map(|a| a.ends_with(".arena"))
            .unwrap_or(false),
        "side files live in .arena, got {:?}",
        k.side_anchor()
    );
    let _ = std::fs::remove_dir_all(&ws);
}

#[test]
fn organise_binds_jail_to_source_and_refuses_arena() {
    let ws = tmp("org");
    let mut seed = JMap::new();
    seed.insert("src/hello.txt".into(), JValue::Str("hello\n".into()));
    let plan = Plan {
        goal: "hello".into(),
        agents: vec![PlanAgent {
            agent_id: "coder_01".into(),
            role: "backend".into(),
            skills: vec!["code".into()],
            writes: vec!["".into()],
            reads: vec!["".into()],
            allowed_tools: None,
            task: PlanTask {
                task_id: "t1".into(),
                title: "hello".into(),
                role: "backend".into(),
                produces: vec!["src/hello.txt".into()],
                verify: vec![vec!["true".into()]],
                est_work: 1.0,
                ..Default::default()
            },
            seed,
        }],
        commit_after_verify: false,
    };
    let (res, mut k) = organise(OrganiseOpts {
        workspace: ws.clone(),
        goal: "hello".into(),
        plan,
        ticks: 2,
        project_id_hint: "hello-run".into(),
        resume: false,
        clock_mode: "virtual".into(),
        when: Some(1_704_067_200.0),
    })
    .unwrap();
    let src = res.project.source_dir().join("src/hello.txt");
    assert!(src.is_file(), "seed must land in source/ via the tool path");
    assert_eq!(std::fs::read_to_string(&src).unwrap(), "hello\n");
    assert!(!res.project.arena_dir().join("src/hello.txt").exists());

    let mut args = JMap::new();
    args.insert("path".into(), JValue::Str("../.arena/hack.txt".into()));
    args.insert("content".into(), JValue::Str("no".into()));
    let attack = k.execute_tool("coder_01", "write_file", &args, "r-x", None, "");
    assert!(!attack.ok, "{}", attack.block);
    assert!(
        attack.refused == "REFUSE_OUTSIDE_ROOT" || attack.refused == "REFUSE_PROTECTED_PATH",
        "got {}",
        attack.refused
    );
    assert!(!res.project.arena_dir().join("hack.txt").exists());

    let types: Vec<String> = k
        .journal()
        .events(&EventFilter::new())
        .unwrap()
        .into_iter()
        .map(|e| e.etype)
        .collect();
    assert!(types.contains(&"PROJECT_CREATED".into()), "{types:?}");
    assert!(types.contains(&"WORKSPACE_BOUND".into()), "{types:?}");
    assert!(types.contains(&"TOOL_CALL".into()), "{types:?}");
    let call_at = types.iter().position(|t| t == "TOOL_CALL").unwrap();
    let result_at = types.iter().position(|t| t == "TOOL_RESULT").unwrap();
    assert!(call_at < result_at);

    let v_ok = res.verification.get("ok").and_then(|v| v.as_bool());
    assert_eq!(v_ok, Some(true), "{:?}", res.verification);
    let _ = std::fs::remove_dir_all(&ws);
}
