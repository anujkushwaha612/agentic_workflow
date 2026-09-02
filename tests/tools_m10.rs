//! M10: jail, real tools, git, verification through the executor.
//! Refusals are results. TOOL_CALL is journalled before the effect.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arena::graph::TaskSpec;
use arena::journal::EventFilter;
use arena::kernel::Kernel;
use arena::sys::json::{JMap, JValue};
use arena::tools::{redact, Executor, GitBind, Jail, ToolExecutor, REFUSE_NO_EXECUTOR, TOOLS};

static N: AtomicU64 = AtomicU64::new(0);

fn tmp(name: &str) -> PathBuf {
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("arena-m10-{name}-{n}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn jstr(k: &str, v: &str) -> JMap {
    let mut m = JMap::new();
    m.insert(k.into(), JValue::Str(v.into()));
    m
}

fn bound_exec(root: &Path) -> Executor {
    let mut ex = Executor::new();
    std::fs::create_dir_all(root.join("src")).unwrap();
    let jail = Jail::new(root, &["src", ""], &["", "src"], "coder");
    ex.bind_agent("coder", jail, None, Some(root.to_path_buf()));
    ex
}

#[test]
fn thirteen_tools_in_the_table() {
    assert_eq!(TOOLS.len(), 13);
}

#[test]
fn refusal_is_not_a_failure_exception() {
    let root = tmp("refuse");
    let mut ex = bound_exec(&root);
    let res = ex.execute(
        "coder",
        "write_file",
        &{
            let mut m = JMap::new();
            m.insert("path".into(), JValue::Str("/etc/passwd".into()));
            m.insert("content".into(), JValue::Str("no".into()));
            m
        },
        "r-1",
        None,
        "",
    );
    assert!(!res.ok);
    assert_eq!(res.refused, "REFUSE_ABS_PATH");
    assert!(root.join("src").exists());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn agent_may_list_its_root_but_not_escape() {
    let root = tmp("list");
    std::fs::write(root.join("src").join("a.rs"), "fn a(){}\n").ok();
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(root.join("src").join("a.rs"), "fn a(){}\n").unwrap();
    let mut ex = bound_exec(&root);
    let ok = ex.execute("coder", "list_files", &jstr("path", ""), "r-1", None, "");
    assert!(ok.ok, "{}", ok.block);
    assert!(
        ok.block.contains("src/a.rs")
            || ok.data.get("n").and_then(|v| v.as_int()).unwrap_or(0) >= 1
    );

    let escape = ex.execute("coder", "list_files", &jstr("path", "../"), "r-2", None, "");
    assert_eq!(escape.refused, "REFUSE_OUTSIDE_ROOT");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn write_read_edit_are_real_bytes() {
    let root = tmp("rw");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut ex = bound_exec(&root);
    let w = ex.execute(
        "coder",
        "write_file",
        &{
            let mut m = JMap::new();
            m.insert("path".into(), JValue::Str("src/hello.rs".into()));
            m.insert("content".into(), JValue::Str("fn main() {}".into()));
            m
        },
        "r-1",
        None,
        "",
    );
    assert!(w.ok, "{}", w.block);
    let on_disk = std::fs::read_to_string(root.join("src/hello.rs")).unwrap();
    assert_eq!(on_disk, "fn main() {}\n");

    let r = ex.execute(
        "coder",
        "read_file",
        &jstr("path", "src/hello.rs"),
        "r-2",
        None,
        "",
    );
    assert!(r.ok);
    assert!(r.block.contains("fn main()"));

    let miss = ex.execute(
        "coder",
        "edit_file",
        &{
            let mut m = JMap::new();
            m.insert("path".into(), JValue::Str("src/hello.rs".into()));
            m.insert("old".into(), JValue::Str("does-not-exist".into()));
            m.insert("new".into(), JValue::Str("x".into()));
            m
        },
        "r-3",
        None,
        "",
    );
    assert_eq!(miss.refused, "REFUSE_EDIT_TARGET_MISSING");

    let e = ex.execute(
        "coder",
        "edit_file",
        &{
            let mut m = JMap::new();
            m.insert("path".into(), JValue::Str("src/hello.rs".into()));
            m.insert("old".into(), JValue::Str("main".into()));
            m.insert("new".into(), JValue::Str("entry".into()));
            m
        },
        "r-4",
        None,
        "",
    );
    assert!(e.ok, "{}", e.block);
    let on_disk = std::fs::read_to_string(root.join("src/hello.rs")).unwrap();
    assert!(on_disk.contains("fn entry()"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn protected_paths_and_write_root_refused() {
    let root = tmp("prot");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut ex = bound_exec(&root);
    for (path, want) in [
        (".git/config", "REFUSE_PROTECTED_PATH"),
        (".arena/x", "REFUSE_PROTECTED_PATH"),
        ("", "REFUSE_WRITE_ROOT"),
    ] {
        let mut args = JMap::new();
        args.insert("path".into(), JValue::Str(path.into()));
        args.insert("content".into(), JValue::Str("x".into()));
        let res = ex.execute("coder", "write_file", &args, "r", None, "");
        assert_eq!(res.refused, want, "path={path:?} got {}", res.refused);
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn never_run_egress_path_miss_and_shell_meta() {
    let root = tmp("cmd");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut ex = bound_exec(&root);
    let kill = ex.execute(
        "coder",
        "run_command",
        &{
            let mut m = JMap::new();
            m.insert("argv".into(), JValue::Arr(vec![JValue::Str("kill".into())]));
            m
        },
        "r-1",
        None,
        "",
    );
    assert_eq!(kill.refused, "REFUSE_NEVER_RUN");

    let eg = ex.execute(
        "coder",
        "run_command",
        &{
            let mut m = JMap::new();
            m.insert(
                "argv".into(),
                JValue::Arr(vec![
                    JValue::Str("echo".into()),
                    JValue::Str("https://github.com/x".into()),
                ]),
            );
            m
        },
        "r-2",
        None,
        "",
    );
    assert_eq!(eg.refused, "REFUSE_EGRESS");

    let miss = ex.execute(
        "coder",
        "run_command",
        &{
            let mut m = JMap::new();
            m.insert(
                "argv".into(),
                JValue::Arr(vec![JValue::Str("definitely-not-a-binary-zzzz".into())]),
            );
            m
        },
        "r-3",
        None,
        "",
    );
    assert!(!miss.ok);
    assert_eq!(miss.exit_code, Some(127));
    assert!(
        miss.refused.is_empty(),
        "PATH miss is an exit, not a refusal"
    );

    let meta = ex.execute(
        "coder",
        "run_command",
        &{
            let mut m = JMap::new();
            m.insert("cmd".into(), JValue::Str("echo hi | cat".into()));
            m
        },
        "r-4",
        None,
        "",
    );
    assert_eq!(meta.refused, "REFUSE_SHELL_META");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn run_command_is_a_real_process() {
    let root = tmp("echo");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut ex = bound_exec(&root);
    let res = ex.execute(
        "coder",
        "run_command",
        &{
            let mut m = JMap::new();
            m.insert(
                "argv".into(),
                JValue::Arr(vec![
                    JValue::Str("echo".into()),
                    JValue::Str("hello-arena".into()),
                ]),
            );
            m
        },
        "r-1",
        None,
        "",
    );
    assert!(res.ok, "{}", res.block);
    assert_eq!(res.exit_code, Some(0));
    assert!(res.block.contains("hello-arena"), "{}", res.block);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn redact_at_the_boundary() {
    let s = redact("token=abcdefghij sk-1234567890abcd", &[]);
    assert!(!s.contains("abcdefghij"));
    assert!(!s.contains("sk-1234567890abcd"));
}

#[test]
fn unbound_kernel_refuses_honestly() {
    let mut k = Kernel::in_memory();
    let res = k.execute_tool("a", "write_file", &JMap::new(), "r-1", None, "");
    assert_eq!(res.refused, REFUSE_NO_EXECUTOR);
    assert!(!res.ok);
}

#[test]
fn tool_call_is_journalled_before_the_effect() {
    let root = tmp("journal");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut k = Kernel::in_memory();
    k.bind_tools("coder", &root, &["src"], &["", "src"], None, GitBind::Jail);
    let mut call = JMap::new();
    call.insert("tool".into(), JValue::Str("write_file".into()));
    let mut args = JMap::new();
    args.insert("path".into(), JValue::Str("src/x.txt".into()));
    args.insert("content".into(), JValue::Str("hi".into()));
    call.insert("args".into(), JValue::Obj(args.clone()));
    let rids = k.plan_tools("coder", &[call], None, "");
    assert_eq!(rids.len(), 1);
    let types: Vec<String> = k
        .journal()
        .events(&EventFilter::new())
        .unwrap()
        .into_iter()
        .map(|e| e.etype)
        .collect();
    assert!(
        types.iter().any(|t| t == "TOOL_CALL"),
        "plan must journal TOOL_CALL before execute; got {types:?}"
    );
    assert!(
        !types.iter().any(|t| t == "TOOL_RESULT"),
        "effect must not have run yet"
    );
    assert!(!root.join("src/x.txt").exists());

    let res = k.execute_tool("coder", "write_file", &args, &rids[0], None, "");
    assert!(res.ok, "{}", res.block);
    assert!(root.join("src/x.txt").exists());
    let types: Vec<String> = k
        .journal()
        .events(&EventFilter::new())
        .unwrap()
        .into_iter()
        .map(|e| e.etype)
        .collect();
    let call_at = types.iter().position(|t| t == "TOOL_CALL").unwrap();
    let result_at = types.iter().position(|t| t == "TOOL_RESULT").unwrap();
    assert!(call_at < result_at);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn ensure_git_and_clean_commit_noop() {
    if arena::tools::which("git").is_none() {
        return;
    }
    let root = tmp("git");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut k = Kernel::in_memory();
    k.bind_tools("coder", &root, &["src"], &["", "src"], None, GitBind::Jail);
    let g = k.ensure_git("coder");
    assert_eq!(g.get("ok").and_then(|v| v.as_bool()), Some(true), "{g:?}");
    assert!(root.join(".git").is_dir());

    let clean = k.execute_tool("coder", "commit", &jstr("message", "noop"), "r-c", None, "");
    assert!(!clean.ok);
    assert_eq!(clean.data.get("noop").and_then(|v| v.as_bool()), Some(true));

    let mut args = JMap::new();
    args.insert("path".into(), JValue::Str("src/a.txt".into()));
    args.insert("content".into(), JValue::Str("one".into()));
    assert!(
        k.execute_tool("coder", "write_file", &args, "r-w", None, "")
            .ok
    );
    let dirty = k.execute_tool(
        "coder",
        "commit",
        &jstr("message", "add a"),
        "r-c2",
        None,
        "",
    );
    assert!(dirty.ok, "{}", dirty.block);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn verify_runs_through_the_tool_path() {
    if arena::tools::which("true").is_none() && arena::tools::which("/bin/true").is_none() {
        return;
    }
    let root = tmp("verify");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut k = Kernel::in_memory();
    let _ = k.register_agent("coder", "backend", &[], 0, "parent");
    let mut t = TaskSpec::new("t1", "do", "backend");
    let bin = if Path::new("/bin/true").exists() {
        "/bin/true"
    } else {
        "true"
    };
    t.verify = vec![vec![bin.to_string()]];
    k.add_task(t).unwrap();
    k.assign("t1", "coder");
    k.bind_tools("coder", &root, &["src"], &["", "src"], None, GitBind::Jail);
    let v = k.run_verify("coder", Some("t1"));
    assert!(v.ok, "{:?}", v.results);
    let types: Vec<String> = k
        .journal()
        .events(&EventFilter::new())
        .unwrap()
        .into_iter()
        .map(|e| e.etype)
        .collect();
    assert!(types.iter().any(|t| t == "TASK_VERIFIED"));
    assert!(types.iter().any(|t| t == "TOOL_RESULT"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn empty_verify_is_not_a_pass() {
    let mut k = Kernel::in_memory();
    let _ = k.register_agent("coder", "backend", &[], 0, "parent");
    k.add_task(TaskSpec::new("t1", "do", "backend")).unwrap();
    k.assign("t1", "coder");
    let root = tmp("emptyv");
    std::fs::create_dir_all(root.join("src")).unwrap();
    k.bind_tools("coder", &root, &["src"], &["", "src"], None, GitBind::Jail);
    let v = k.run_verify("coder", Some("t1"));
    assert!(!v.ok, "empty verify must not pass");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn repeated_refusal_stays_a_refusal() {
    let root = tmp("rep");
    std::fs::create_dir_all(root.join("src")).unwrap();
    let mut ex = bound_exec(&root);
    for i in 0..3 {
        let res = ex.execute(
            "coder",
            "write_file",
            &{
                let mut m = JMap::new();
                m.insert("path".into(), JValue::Str("/etc/shadow".into()));
                m.insert("content".into(), JValue::Str("x".into()));
                m
            },
            &format!("r-{i}"),
            None,
            "",
        );
        assert_eq!(res.refused, "REFUSE_ABS_PATH");
        assert!(!res.ok);
    }
    let _ = std::fs::remove_dir_all(&root);
}
