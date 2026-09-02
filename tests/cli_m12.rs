//! Engine `arena` CLI: Kernel across processes, quiet replay, std argv.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arena::cli;
use arena::sys::json::{parse, JValue};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch() -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("arena-cli-m12-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn invoke(args: &[&str]) -> (i32, String, String) {
    let v: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = cli::run(&v, &mut out, &mut err);
    (
        code,
        String::from_utf8_lossy(&out).into_owned(),
        String::from_utf8_lossy(&err).into_owned(),
    )
}

#[test]
fn help_and_version() {
    let (c, o, _) = invoke(&["--help"]);
    assert_eq!(c, 0);
    assert!(o.contains("submit TEXT"));
    let (c, o, _) = invoke(&["--version"]);
    assert_eq!(c, 0);
    assert!(o.contains("arena "));
}

#[test]
fn plan_is_pure_and_empty_match_is_valid() {
    let (c, o, e) = invoke(&["plan", "rename the landing page title"]);
    assert_eq!(c, 0, "{e}");
    let v = parse(o.trim()).expect("json");
    let n = match v.get("tasks") {
        Some(JValue::Arr(a)) => a.len(),
        _ => 999,
    };
    assert_eq!(n, 0);
    let (c, o, e) = invoke(&["plan", "postgres API dashboard"]);
    assert_eq!(c, 0, "{e}");
    let v = parse(o.trim()).expect("json");
    let n = match v.get("tasks") {
        Some(JValue::Arr(a)) => a.len(),
        _ => 0,
    };
    assert!(n >= 2, "{o}");
}

#[test]
fn submit_then_status_across_processes() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let (c, o, e) = invoke(&["--root", rs, "submit", "postgres schema and REST API"]);
    assert_eq!(c, 0, "{e}\n{o}");
    assert!(o.contains("planned"));
    let (c, o, e) = invoke(&["--root", rs, "status"]);
    assert_eq!(c, 0, "{e}");
    let v = parse(o.trim()).expect("status json");
    assert!(v.get("agents").is_some() || v.get("tasks").is_some(), "{o}");
}

#[test]
fn run_completes_and_verify_is_quiet() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let (c, _, e) = invoke(&["--root", rs, "submit", "postgres schema"]);
    assert_eq!(c, 0, "{e}");
    let (c, o, e) = invoke(&["--root", rs, "run", "--ticks", "80"]);
    assert!(c == 0 || o.contains("\"done\""), "code={c} err={e} out={o}");
    let (c1, o1, e1) = invoke(&["--root", rs, "status"]);
    assert_eq!(c1, 0, "{e1}");
    let (c2, o2, e2) = invoke(&["--root", rs, "status"]);
    assert_eq!(c2, 0, "{e2}");
    assert_eq!(o1, o2, "quiet from_journal must not append on status");
    let (c, o, e) = invoke(&["--root", rs, "verify"]);
    assert_eq!(c, 0, "{e}\n{o}");
    assert!(o.contains("chain: VALID"), "{o}");
    assert!(o.contains("replay parity : OK"), "{o}");
}

#[test]
fn run_without_submit_fails() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let (c, _, e) = invoke(&["--root", rs, "run", "--ticks", "4"]);
    assert_eq!(c, 1);
    assert!(e.contains("nothing submitted"));
}

#[test]
fn watch_once_and_why_unknown() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let _ = invoke(&["--root", rs, "submit", "postgres schema"]);
    let (c, o, e) = invoke(&["--root", rs, "watch", "--once"]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("tick="));
    let (c, o, e) = invoke(&["--root", rs, "why", "no_such_agent"]);
    assert_eq!(c, 1, "{e}");
    assert!(o.contains("unknown agent"));
}

#[test]
fn request_pause_resume_terminate() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let (c, o, e) = invoke(&["--root", rs, "submit", "postgres schema"]);
    assert_eq!(c, 0, "{e}\n{o}");
    let (c, o, e) = invoke(&[
        "--root",
        rs,
        "request",
        "--agent",
        "parent",
        "--role",
        "testing",
        "--reason",
        "need qa",
        "--work",
        "2",
        "--no-run",
    ]);
    assert_eq!(c, 0, "{e}\n{o}");
    assert!(o.contains("injected"), "{o}");
    let (c, o, e) = invoke(&["--root", rs, "spawns"]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("SPAWNS"));
    let (c, o, e) = invoke(&["--root", rs, "agents"]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("AGENTS"));
    // pick first agent id from agents listing if present
    let agent = o
        .lines()
        .find(|l| l.trim_start().starts_with("database_") || l.contains("database_"))
        .and_then(|l| l.split_whitespace().next())
        .unwrap_or("database_01");
    let (c, o, e) = invoke(&["--root", rs, "pause", agent]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("pause") && o.contains("ok"), "{o}");
    let (c, o, e) = invoke(&["--root", rs, "resume", agent]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("resume") && o.contains("ok"), "{o}");
    let (c, o, e) = invoke(&["--root", rs, "terminate", agent, "cli-test"]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("terminate") && o.contains("ok"), "{o}");
}

#[test]
fn board_inbox_trace_json_agents() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let _ = invoke(&["--root", rs, "submit", "postgres schema"]);
    let (c, o, e) = invoke(&["--root", rs, "board"]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("tick=") || o.contains("KERNEL") || o.contains("t_"));
    let (c, o, e) = invoke(&["--root", rs, "inbox"]);
    assert_eq!(c, 0, "{e}");
    assert!(o.contains("empty") || !o.is_empty());
    let (c, o, e) = invoke(&["--root", rs, "agents", "--json"]);
    assert_eq!(c, 0, "{e}");
    let v = parse(o.trim()).expect("agents json");
    assert!(
        v.get("roster").is_some() || v.get("monitoring").is_some(),
        "{o}"
    );
    let (c, _, e) = invoke(&["--root", rs, "trace", "no-such-cid"]);
    assert_eq!(c, 1, "{e}");
    assert!(e.contains("no events"));
}

#[test]
fn chaos_verbs_refused() {
    for cmd in ["accept", "chaos-report", "chaos-run"] {
        let (c, _, e) = invoke(&[cmd]);
        assert_eq!(c, 1, "{cmd}");
        assert!(e.contains("not in this milestone"), "{cmd}: {e}");
    }
}

#[test]
fn verify_missing_journal() {
    let root = scratch();
    let rs = root.to_str().unwrap();
    let (c, _, e) = invoke(&["--root", rs, "verify"]);
    assert_eq!(c, 1);
    assert!(e.contains("no journal"));
}
