//! Engine operator CLI (`arena`). Std argv only — no clap, no crates.io.
//!
//! Drives [`crate::kernel::Kernel`] across processes. Persistence is
//! `--root/journal.db` (explicit root, never CWD as a side-file anchor).
//! Read commands reopen with quiet [`Kernel::from_journal`] so they do not
//! re-execute tools or append `REPLAY_COMPLETE`.
//!
//! Product launcher (`arena-code`) is a different binary and is not this
//! module. Chaos/accept live in a later milestone; those verbs are parsed
//! and refused honestly.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::kernel::{Kernel, KernelOpts};
use crate::lifecycle::AgentState;
use crate::parent::RuleBasedPlanner;
use crate::registry::SpawnBudget;
use crate::sys::json::{JMap, JValue};
use crate::RUNTIME_VERSION;

const USAGE: &str = "\
arena — engine operator CLI

Usage:
  arena [--root DIR] [--budget N] [--workers N] [--idle-ttl SECS] [--wall] <command> ...

Commands:
  plan TEXT
  submit TEXT
  run [--ticks N] [--resident SECS]
  status
  board
  inbox
  verify
  agents [--json]
  spawns [--json] [-v]
  request --agent ID --role ROLE [--reason TEXT] [--skills a,b] [--work N]
          [--inputs a,b] [--outputs a,b] [--class NAME] [--parent-task ID]
          [--correlation ID] [--run] [--ticks N]
  watch [--every SECS] [--count N] [--once]
  why AGENT
  trace CORRELATION_ID
  pause AGENT
  resume AGENT
  terminate AGENT [reason]

Global:
  --root DIR       persistence directory (journal.db + side files). Default: var/run
  --budget N       max active agents (default 8)
  --workers N      max concurrent workers (default 2)
  --idle-ttl SECS  idle reap ttl (default 5)
  --wall           wall clock instead of virtual
  --version
  --help

Not in this milestone (parsed, then refused): accept, chaos-report, chaos-run.
";

const EX_USAGE: i32 = 64;
const EX_DEFERRED: i32 = 64;

#[derive(Debug, Clone)]
struct Globals {
    root: PathBuf,
    budget: i64,
    workers: i64,
    idle_ttl: f64,
    wall: bool,
}

impl Default for Globals {
    fn default() -> Self {
        Globals {
            root: PathBuf::from("var/run"),
            budget: 8,
            workers: 2,
            idle_ttl: 5.0,
            wall: false,
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Help,
    Version,
    Plan { text: String },
    Submit { text: String },
    Run { ticks: i64, resident: Option<f64> },
    Status,
    Board,
    Inbox,
    Verify,
    Agents { json: bool },
    Spawns { json: bool, verbose: bool },
    Request(RequestArgs),
    Watch { every: f64, count: i64 },
    Why { agent: String },
    Trace { cid: String },
    Pause { agent: String },
    Resume { agent: String },
    Terminate { agent: String, reason: String },
    Deferred { name: String },
}

#[derive(Debug, Clone)]
struct RequestArgs {
    agent: String,
    role: String,
    reason: String,
    skills: Vec<String>,
    work: f64,
    inputs: Vec<String>,
    outputs: Vec<String>,
    class: String,
    parent_task: String,
    correlation: String,
    run: bool,
    ticks: i64,
}

impl Default for RequestArgs {
    fn default() -> Self {
        RequestArgs {
            agent: String::new(),
            role: String::new(),
            reason: String::new(),
            skills: Vec::new(),
            work: 1.0,
            inputs: Vec::new(),
            outputs: Vec::new(),
            class: String::new(),
            parent_task: String::new(),
            correlation: String::new(),
            run: false,
            ticks: 40,
        }
    }
}

/// Entry used by `src/bin/arena.rs` and by tests.
pub fn run(args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let (g, op) = match parse(args) {
        Ok(v) => v,
        Err(e) => {
            let _ = writeln!(err, "{e}");
            let _ = writeln!(err, "{USAGE}");
            return EX_USAGE;
        }
    };
    match op {
        Op::Help => {
            let _ = write!(out, "{USAGE}");
            0
        }
        Op::Version => {
            let _ = writeln!(out, "arena {RUNTIME_VERSION}");
            0
        }
        Op::Deferred { name } => {
            let _ = writeln!(
                err,
                "{name} is not in this milestone (chaos/accept stay with the parity suite)"
            );
            EX_DEFERRED
        }
        Op::Plan { text } => cmd_plan(&text, out),
        Op::Submit { text } => match open_kernel(&g, false) {
            Ok(mut k) => cmd_submit(&mut k, &text, out, err),
            Err(e) => fail(err, &e),
        },
        Op::Run { ticks, resident } => match open_kernel(&g, false) {
            Ok(mut k) => cmd_run(&mut k, ticks, resident, out, err),
            Err(e) => fail(err, &e),
        },
        Op::Status => match open_kernel(&g, true) {
            Ok(k) => {
                let _ = writeln!(out, "{}", k.report());
                0
            }
            Err(e) => fail(err, &e),
        },
        Op::Board => match open_kernel(&g, true) {
            Ok(k) => cmd_board(&k, out),
            Err(e) => fail(err, &e),
        },
        Op::Inbox => match open_kernel(&g, true) {
            Ok(k) => cmd_inbox(&k, out),
            Err(e) => fail(err, &e),
        },
        Op::Verify => cmd_verify(&g, out, err),
        Op::Agents { json } => match open_kernel(&g, true) {
            Ok(k) => cmd_agents(&k, json, out),
            Err(e) => fail(err, &e),
        },
        Op::Spawns { json, verbose } => match open_kernel(&g, true) {
            Ok(k) => cmd_spawns(&k, json, verbose, out),
            Err(e) => fail(err, &e),
        },
        Op::Request(a) => match open_kernel(&g, false) {
            Ok(mut k) => cmd_request(&mut k, &a, out, err),
            Err(e) => fail(err, &e),
        },
        Op::Watch { every, count } => cmd_watch(&g, every, count, out, err),
        Op::Why { agent } => match open_kernel(&g, true) {
            Ok(k) => {
                let _ = writeln!(out, "{}", JValue::Obj(k.why(&agent)).to_pretty_string());
                0
            }
            Err(e) => fail(err, &e),
        },
        Op::Trace { cid } => match open_kernel(&g, true) {
            Ok(k) => {
                let rows: Vec<JValue> = k.trace(&cid).into_iter().map(JValue::Obj).collect();
                let _ = writeln!(out, "{}", JValue::Arr(rows).to_pretty_string());
                0
            }
            Err(e) => fail(err, &e),
        },
        Op::Pause { agent } => match open_kernel(&g, false) {
            Ok(mut k) => {
                let _ = k.pause(&agent);
                let _ = writeln!(out, "paused {agent}");
                0
            }
            Err(e) => fail(err, &e),
        },
        Op::Resume { agent } => match open_kernel(&g, false) {
            Ok(mut k) => {
                let _ = k.resume(&agent);
                let _ = writeln!(out, "resumed {agent}");
                0
            }
            Err(e) => fail(err, &e),
        },
        Op::Terminate { agent, reason } => match open_kernel(&g, false) {
            Ok(mut k) => {
                let _ = k.terminate_agent(&agent, &reason);
                let _ = writeln!(out, "terminated {agent}");
                0
            }
            Err(e) => fail(err, &e),
        },
    }
}

fn fail(err: &mut dyn Write, e: &str) -> i32 {
    let _ = writeln!(err, "{e}");
    1
}

fn journal_path(root: &Path) -> PathBuf {
    root.join("journal.db")
}

fn opts_from(g: &Globals) -> KernelOpts {
    let mut budget = SpawnBudget::default();
    budget.max_active_agents = g.budget;
    budget.max_concurrent_workers = g.workers;
    budget.idle_ttl = g.idle_ttl;
    KernelOpts {
        journal_path: Some(journal_path(&g.root)),
        root: Some(g.root.clone()),
        budget,
        clock_mode: if g.wall {
            "wall".into()
        } else {
            "virtual".into()
        },
        ..KernelOpts::default()
    }
}

/// Resume an existing journal (quiet projection) or start a new one.
/// `observe` is true for read commands: never create a journal just to look.
fn open_kernel(g: &Globals, observe: bool) -> Result<Kernel, String> {
    let _ = std::fs::create_dir_all(&g.root);
    let jp = journal_path(&g.root);
    if jp.exists() || observe {
        Kernel::from_journal(&jp, true, opts_from(g)).map_err(|e| e.to_string())
    } else {
        Kernel::new(opts_from(g)).map_err(|e| e.to_string())
    }
}

fn cmd_plan(text: &str, out: &mut dyn Write) -> i32 {
    let (tasks, rationale) = RuleBasedPlanner.plan(text);
    let mut roles: Vec<String> = tasks.iter().map(|t| t.role.clone()).collect();
    roles.sort();
    roles.dedup();
    let mut payload = JMap::new();
    payload.insert(
        "tasks".into(),
        JValue::Arr(tasks.iter().map(|t| JValue::Obj(t.snapshot())).collect()),
    );
    payload.insert("rationale".into(), JValue::Obj(rationale));
    payload.insert(
        "agents".into(),
        JValue::Arr(roles.into_iter().map(JValue::Str).collect()),
    );
    payload.insert("notes".into(), JValue::Arr(vec![]));
    let _ = writeln!(out, "{}", JValue::Obj(payload).to_pretty_string());
    0
}

fn cmd_submit(k: &mut Kernel, text: &str, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let r = k.submit(text);
    if r.get("error").and_then(|v| v.as_str()) == Some("plan cycle") {
        let _ = writeln!(err, "plan cycle");
        return 2;
    }
    let _ = writeln!(out, "{}", JValue::Obj(r).to_pretty_string());
    0
}

fn cmd_run(
    k: &mut Kernel,
    ticks: i64,
    resident: Option<f64>,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    if k.task_text().is_empty() && k.graph().tasks.is_empty() {
        let _ = writeln!(err, "nothing to run (submit a task first)");
        return 1;
    }
    let summary = match resident {
        Some(secs) => k.run_resident(secs, 0.05, ticks.max(1)),
        None => k.run(ticks),
    };
    let _ = writeln!(out, "{}", JValue::Obj(summary.clone()).to_pretty_string());
    if summary
        .get("done")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        0
    } else {
        1
    }
}

fn cmd_board(k: &Kernel, out: &mut dyn Write) -> i32 {
    let _ = writeln!(out, "BOARD");
    let _ = writeln!(out);
    for (tid, t) in &k.graph().tasks {
        let blocked = k.graph().blocked_by(&t.task_id);
        let blk: Vec<&str> = blocked.iter().map(|d| d.as_str()).collect();
        let owner = t
            .owner
            .as_ref()
            .map(|o| o.as_str().to_string())
            .unwrap_or_else(|| "-".into());
        let _ = writeln!(
            out,
            "  {:<22} {:<10} owner={:<16} blocked={:?}",
            tid.as_str(),
            t.status.as_str(),
            owner,
            blk
        );
    }
    0
}

fn cmd_inbox(k: &Kernel, out: &mut dyn Write) -> i32 {
    let path = k.inbox_file();
    let text = path
        .as_ref()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .unwrap_or_default();
    if text.trim().is_empty() {
        let _ = writeln!(out, "(empty)");
        return 0;
    }
    let _ = write!(out, "{text}");
    if !text.ends_with('\n') {
        let _ = writeln!(out);
    }
    0
}

fn cmd_verify(g: &Globals, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let jp = journal_path(&g.root);
    if !jp.exists() {
        let _ = writeln!(err, "no journal at {}", jp.display());
        return 1;
    }
    let k = match Kernel::from_journal(&jp, true, opts_from(g)) {
        Ok(k) => k,
        Err(e) => return fail(err, &e.to_string()),
    };
    let events = k.journal().count().unwrap_or(0);
    let (chain_ok, reason, _) = match k.journal().verify_chain() {
        Ok(t) => t,
        Err(e) => return fail(err, &e.to_string()),
    };
    let snap = k.snapshot();
    drop(k);
    let k2 = match Kernel::from_journal(&jp, true, opts_from(g)) {
        Ok(k) => k,
        Err(e) => return fail(err, &e.to_string()),
    };
    let snap2 = k2.snapshot();
    let replay_ok =
        JValue::Obj(snap.clone()).to_canon_string() == JValue::Obj(snap2).to_canon_string();
    let mut m = JMap::new();
    m.insert("chain_ok".into(), JValue::Bool(chain_ok));
    m.insert("chain_reason".into(), JValue::Str(reason));
    m.insert("events".into(), JValue::Int(events));
    m.insert("replay_ok".into(), JValue::Bool(replay_ok));
    m.insert("snapshot".into(), JValue::Obj(snap));
    let _ = writeln!(out, "{}", JValue::Obj(m).to_pretty_string());
    if chain_ok && replay_ok {
        0
    } else {
        1
    }
}

fn cmd_agents(k: &Kernel, json: bool, out: &mut dyn Write) -> i32 {
    if json {
        let _ = writeln!(out, "{}", JValue::Obj(k.metrics()).to_pretty_string());
        return 0;
    }
    let _ = writeln!(out, "AGENTS");
    let _ = writeln!(out);
    for rec in &k.registry().agents {
        let tid = rec
            .task_id
            .as_ref()
            .map(|t| t.as_str().to_string())
            .unwrap_or_else(|| "-".into());
        let waits = rec.pending_waits.len();
        let _ = writeln!(
            out,
            "  {:<16} {:<12} state={:<22} task={:<16} epoch={} waits={}",
            rec.agent_id.as_str(),
            rec.role,
            rec.state_str(),
            tid,
            rec.epoch,
            waits
        );
    }
    let working = k
        .registry()
        .agents
        .iter()
        .filter(|a| a.lifecycle.state == AgentState::Working)
        .count();
    let cap = k.registry().budget.max_concurrent_workers as usize;
    let slots = working.min(cap);
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "worker slots in use: {slots}/{}",
        k.registry().budget.max_concurrent_workers
    );
    0
}

fn cmd_spawns(k: &Kernel, json: bool, verbose: bool, out: &mut dyn Write) -> i32 {
    let entries = k.parent().ledger.newest_first();
    if json {
        let arr: Vec<JValue> = entries.iter().map(|e| JValue::Obj(e.to_dict())).collect();
        let _ = writeln!(out, "{}", JValue::Arr(arr).to_pretty_string());
        return 0;
    }
    let _ = writeln!(out, "SPAWNS  ({})", entries.len());
    let _ = writeln!(out);
    for e in entries {
        let owner = e.owner.as_deref().unwrap_or("-");
        let spawned = e.spawned_agent_id.as_deref().unwrap_or("-");
        let _ = writeln!(
            out,
            "  {:<16} {:<22} rule={:<28} owner={:<14} spawned={}",
            e.rid,
            e.state.as_str(),
            e.rule,
            owner,
            spawned
        );
        if verbose {
            let _ = writeln!(
                out,
                "      {}",
                JValue::Obj(e.request.to_dict()).to_canon_string()
            );
        }
    }
    0
}

fn cmd_request(k: &mut Kernel, a: &RequestArgs, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    if a.agent.is_empty() || a.role.is_empty() {
        let _ = writeln!(err, "request requires --agent and --role");
        return EX_USAGE;
    }
    let mut req = JMap::new();
    req.insert("from".into(), JValue::Str(a.agent.clone()));
    req.insert("requested_role".into(), JValue::Str(a.role.clone()));
    let reason = if a.reason.is_empty() {
        format!("{} asks for a {}", a.agent, a.role)
    } else {
        a.reason.clone()
    };
    req.insert("reason".into(), JValue::Str(reason));
    req.insert(
        "required_skills".into(),
        JValue::Arr(a.skills.iter().cloned().map(JValue::Str).collect()),
    );
    req.insert("estimated_work".into(), JValue::Float(a.work));
    req.insert(
        "required_inputs".into(),
        JValue::Arr(a.inputs.iter().cloned().map(JValue::Str).collect()),
    );
    req.insert(
        "expected_outputs".into(),
        JValue::Arr(a.outputs.iter().cloned().map(JValue::Str).collect()),
    );
    if !a.class.is_empty() {
        req.insert("capability_class".into(), JValue::Str(a.class.clone()));
    }
    if !a.parent_task.is_empty() {
        req.insert("parent_task_id".into(), JValue::Str(a.parent_task.clone()));
    }
    if !a.correlation.is_empty() {
        req.insert("correlation_id".into(), JValue::Str(a.correlation.clone()));
    }
    let msg = k.inject_spawn_request(req, "cli", true);
    let _ = writeln!(
        out,
        "queued spawn request from {} for {} (mid={})",
        a.agent, a.role, msg.mid
    );
    if a.run {
        let summary = k.run(a.ticks);
        let _ = writeln!(out, "{}", JValue::Obj(summary).to_pretty_string());
    }
    0
}

fn cmd_watch(g: &Globals, every: f64, count: i64, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let n = count.max(1);
    for i in 0..n {
        let k = match open_kernel(g, true) {
            Ok(k) => k,
            Err(e) => return fail(err, &e),
        };
        let s = k.summary();
        let counts = k.state_counts();
        let _ = writeln!(
            out,
            "tick={} done={} events={} stalled={} counts={}",
            s.get("tick").and_then(|v| v.as_int()).unwrap_or(0),
            s.get("done").and_then(|v| v.as_bool()).unwrap_or(false),
            s.get("events").and_then(|v| v.as_int()).unwrap_or(0),
            s.get("stalled").and_then(|v| v.as_bool()).unwrap_or(false),
            JValue::Obj(counts).to_canon_string()
        );
        drop(k);
        if i + 1 < n {
            std::thread::sleep(std::time::Duration::from_secs_f64(every.max(0.0)));
        }
    }
    0
}

fn parse(args: &[String]) -> Result<(Globals, Op), String> {
    let (g, rest, help, version) = extract_globals(args)?;
    if help {
        return Ok((g, Op::Help));
    }
    if version {
        return Ok((g, Op::Version));
    }
    if rest.is_empty() {
        return Err("missing command".into());
    }
    let cmd = rest[0].as_str();
    let tail = &rest[1..];
    let op = match cmd {
        "plan" => Op::Plan {
            text: tail.join(" "),
        },
        "submit" => Op::Submit {
            text: tail.join(" "),
        },
        "run" => {
            let (ticks, resident) = parse_run(tail)?;
            Op::Run { ticks, resident }
        }
        "status" => Op::Status,
        "board" => Op::Board,
        "inbox" => Op::Inbox,
        "verify" => Op::Verify,
        "agents" => Op::Agents {
            json: has_flag(tail, "--json"),
        },
        "spawns" => Op::Spawns {
            json: has_flag(tail, "--json"),
            verbose: has_flag(tail, "--verbose") || has_flag(tail, "-v"),
        },
        "request" => Op::Request(parse_request(tail)?),
        "watch" => {
            let (every, count) = parse_watch(tail)?;
            Op::Watch { every, count }
        }
        "why" => {
            let agent = tail.first().cloned().ok_or("why requires AGENT")?;
            Op::Why { agent }
        }
        "trace" => {
            let cid = tail
                .first()
                .cloned()
                .ok_or("trace requires CORRELATION_ID")?;
            Op::Trace { cid }
        }
        "pause" => {
            let agent = tail.first().cloned().ok_or("pause requires AGENT")?;
            Op::Pause { agent }
        }
        "resume" => {
            let agent = tail.first().cloned().ok_or("resume requires AGENT")?;
            Op::Resume { agent }
        }
        "terminate" => {
            let agent = tail.first().cloned().ok_or("terminate requires AGENT")?;
            let reason = if tail.len() > 1 {
                tail[1..].join(" ")
            } else {
                "cli".into()
            };
            Op::Terminate { agent, reason }
        }
        "accept" | "chaos-report" | "chaos-run" => Op::Deferred {
            name: cmd.to_string(),
        },
        other => return Err(format!("unknown command {other}")),
    };
    Ok((g, op))
}

fn extract_globals(args: &[String]) -> Result<(Globals, Vec<String>, bool, bool), String> {
    let mut g = Globals::default();
    let mut rest = Vec::new();
    let mut help = false;
    let mut version = false;
    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        match a {
            "--help" | "-h" => help = true,
            "--version" => version = true,
            "--wall" => g.wall = true,
            "--root" => {
                g.root = PathBuf::from(need_val(args, i, "--root")?);
                i += 1;
            }
            "--budget" => {
                g.budget = parse_i64(&need_val(args, i, "--budget")?, "--budget")?;
                i += 1;
            }
            "--workers" => {
                g.workers = parse_i64(&need_val(args, i, "--workers")?, "--workers")?;
                i += 1;
            }
            "--idle-ttl" => {
                g.idle_ttl = parse_f64(&need_val(args, i, "--idle-ttl")?, "--idle-ttl")?;
                i += 1;
            }
            _ if a.starts_with("--root=") => {
                g.root = PathBuf::from(&a["--root=".len()..]);
            }
            _ => rest.push(args[i].clone()),
        }
        i += 1;
    }
    Ok((g, rest, help, version))
}

fn need_val(args: &[String], i: usize, flag: &str) -> Result<String, String> {
    args.get(i + 1)
        .cloned()
        .ok_or_else(|| format!("{flag} needs a value"))
}

fn parse_i64(s: &str, flag: &str) -> Result<i64, String> {
    s.parse::<i64>()
        .map_err(|_| format!("{flag}: not an integer: {s}"))
}

fn parse_f64(s: &str, flag: &str) -> Result<f64, String> {
    s.parse::<f64>()
        .map_err(|_| format!("{flag}: not a number: {s}"))
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|a| a == flag)
}

fn parse_run(args: &[String]) -> Result<(i64, Option<f64>), String> {
    let mut ticks = 200i64;
    let mut resident = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--ticks" => {
                ticks = parse_i64(&need_val(args, i, "--ticks")?, "--ticks")?;
                i += 1;
            }
            "--resident" => {
                resident = Some(parse_f64(&need_val(args, i, "--resident")?, "--resident")?);
                i += 1;
            }
            other => return Err(format!("run: unexpected {other}")),
        }
        i += 1;
    }
    Ok((ticks, resident))
}

fn parse_watch(args: &[String]) -> Result<(f64, i64), String> {
    let mut every = 0.5;
    let mut count = 1i64;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--every" => {
                every = parse_f64(&need_val(args, i, "--every")?, "--every")?;
                i += 1;
            }
            "--count" => {
                count = parse_i64(&need_val(args, i, "--count")?, "--count")?;
                i += 1;
            }
            "--once" => count = 1,
            other => return Err(format!("watch: unexpected {other}")),
        }
        i += 1;
    }
    Ok((every, count))
}

fn parse_request(args: &[String]) -> Result<RequestArgs, String> {
    let mut a = RequestArgs::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--agent" => {
                a.agent = need_val(args, i, "--agent")?;
                i += 1;
            }
            "--role" => {
                a.role = need_val(args, i, "--role")?;
                i += 1;
            }
            "--reason" => {
                a.reason = need_val(args, i, "--reason")?;
                i += 1;
            }
            "--skills" => {
                a.skills = csv(&need_val(args, i, "--skills")?);
                i += 1;
            }
            "--work" => {
                a.work = parse_f64(&need_val(args, i, "--work")?, "--work")?;
                i += 1;
            }
            "--inputs" => {
                a.inputs = csv(&need_val(args, i, "--inputs")?);
                i += 1;
            }
            "--outputs" => {
                a.outputs = csv(&need_val(args, i, "--outputs")?);
                i += 1;
            }
            "--class" => {
                a.class = need_val(args, i, "--class")?;
                i += 1;
            }
            "--parent-task" => {
                a.parent_task = need_val(args, i, "--parent-task")?;
                i += 1;
            }
            "--correlation" => {
                a.correlation = need_val(args, i, "--correlation")?;
                i += 1;
            }
            "--run" => a.run = true,
            "--ticks" => {
                a.ticks = parse_i64(&need_val(args, i, "--ticks")?, "--ticks")?;
                i += 1;
            }
            other => return Err(format!("request: unexpected {other}")),
        }
        i += 1;
    }
    Ok(a)
}

fn csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn globals_may_sit_anywhere() {
        let (g, rest, _, _) = extract_globals(&s(&[
            "submit", "--root", "/tmp/a", "--budget", "3", "hello",
        ]))
        .unwrap();
        assert_eq!(g.root, PathBuf::from("/tmp/a"));
        assert_eq!(g.budget, 3);
        assert_eq!(rest, vec!["submit", "hello"]);
    }

    #[test]
    fn missing_command_is_usage() {
        let mut o = Vec::new();
        let mut e = Vec::new();
        let code = run(&[], &mut o, &mut e);
        assert_eq!(code, EX_USAGE);
        assert!(String::from_utf8_lossy(&e).contains("missing command"));
    }

    #[test]
    fn deferred_chaos_is_honest() {
        let mut o = Vec::new();
        let mut e = Vec::new();
        let code = run(&s(&["chaos-report"]), &mut o, &mut e);
        assert_eq!(code, EX_DEFERRED);
        assert!(String::from_utf8_lossy(&e).contains("not in this milestone"));
    }
}
