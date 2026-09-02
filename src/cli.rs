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

use crate::graph::DependencyGraph;
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
  --budget N       max active agents (default 6)
  --workers N      max concurrent workers (default 2)
  --idle-ttl SECS  idle reap ttl (default 5)
  --wall           wall clock instead of virtual
  --version
  --help

Not in this milestone (parsed, then refused): accept, chaos-report, chaos-run.
";

/// Python `arena` CLI: argparse usage = 2; operational failure = 1; success = 0.
/// Plan §7.3: submit cycle = 2; verify fail = 1. arena-code's 64 is not this binary.
const EX_USAGE: i32 = 2;
const EX_DEFERRED: i32 = 1;

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
            budget: 6,
            workers: 2,
            idle_ttl: 1e9,
            wall: false,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
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
    resident: Option<f64>,
    judge: bool,
}

impl Default for RequestArgs {
    fn default() -> Self {
        RequestArgs {
            agent: String::new(),
            role: String::new(),
            reason: String::new(),
            skills: Vec::new(),
            work: 3.0,
            inputs: Vec::new(),
            outputs: Vec::new(),
            class: String::new(),
            parent_task: String::new(),
            correlation: String::new(),
            run: true,
            ticks: 30,
            resident: None,
            judge: false,
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
                let _ = writeln!(out, "{}", JValue::Obj(k.status()).to_pretty_string());
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
                let w = k.why(&agent);
                let bad = w.contains_key("error");
                let _ = writeln!(out, "{}", JValue::Obj(w).to_pretty_string());
                if bad {
                    1
                } else {
                    0
                }
            }
            Err(e) => fail(err, &e),
        },
        Op::Trace { cid } => match open_kernel(&g, true) {
            Ok(k) => {
                let rows = k.trace(&cid);
                if rows.is_empty() {
                    let _ = writeln!(err, "no events for {cid}");
                    return 1;
                }
                for r in &rows {
                    let seq = r.get("seq").and_then(|v| v.as_int()).unwrap_or(0);
                    let ts = r.get("ts").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let et = r.get("etype").and_then(|v| v.as_str()).unwrap_or("");
                    let actor = r.get("actor").and_then(|v| v.as_str()).unwrap_or("");
                    let target = r.get("target").and_then(|v| v.as_str()).unwrap_or("");
                    let depth = r.get("depth").and_then(|v| v.as_int()).unwrap_or(0);
                    let body = r.get("body").and_then(|v| v.as_str()).unwrap_or("");
                    let body: String = body.chars().take(70).collect();
                    let _ = writeln!(
                        out,
                        "{seq:>5} {ts:>8.3} {et:<22} {actor:>14} -> {target:<14} d{depth} {body}"
                    );
                }
                0
            }
            Err(e) => fail(err, &e),
        },
        Op::Pause { agent } => match open_kernel(&g, false) {
            Ok(mut k) => cmd_control(&mut k, "pause", &agent, "", out),
            Err(e) => fail(err, &e),
        },
        Op::Resume { agent } => match open_kernel(&g, false) {
            Ok(mut k) => cmd_control(&mut k, "resume", &agent, "", out),
            Err(e) => fail(err, &e),
        },
        Op::Terminate { agent, reason } => match open_kernel(&g, false) {
            Ok(mut k) => cmd_control(&mut k, "terminate", &agent, &reason, out),
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
    let mut g = DependencyGraph::default();
    for t in &tasks {
        let _ = g.add(t.clone(), true);
    }
    let (generations, cycle) = match g.order() {
        Ok(o) => (
            JValue::Arr(
                o.iter()
                    .map(|gen| {
                        JValue::Arr(gen.iter().map(|t| JValue::Str(t.as_str().into())).collect())
                    })
                    .collect(),
            ),
            JValue::Null,
        ),
        Err(e) => (
            JValue::Null,
            JValue::Arr(e.0.into_iter().map(JValue::Str).collect()),
        ),
    };
    let mut roles: Vec<String> = tasks.iter().map(|t| t.role.clone()).collect();
    roles.sort();
    roles.dedup();
    let why = rationale
        .get("matched")
        .cloned()
        .unwrap_or(JValue::Obj(JMap::new()));
    let edges = rationale
        .get("artifact_edges")
        .cloned()
        .unwrap_or(JValue::Obj(JMap::new()));
    let mut payload = JMap::new();
    payload.insert(
        "roles_needed".into(),
        JValue::Arr(roles.into_iter().map(JValue::Str).collect()),
    );
    payload.insert(
        "tasks".into(),
        JValue::Arr(tasks.iter().map(|t| JValue::Obj(t.snapshot())).collect()),
    );
    payload.insert("generations".into(), generations);
    payload.insert("cycle".into(), cycle);
    payload.insert("why".into(), why);
    payload.insert("edges".into(), edges);
    let _ = writeln!(out, "{}", JValue::Obj(payload).to_pretty_string());
    0
}

fn cmd_submit(k: &mut Kernel, text: &str, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let r = k.submit(text);
    if r.get("error").and_then(|v| v.as_str()) == Some("plan cycle") {
        let _ = writeln!(err, "PLAN REJECTED (unschedulable): plan cycle");
        return 2;
    }
    let n_tasks = r.get("tasks").and_then(|v| v.as_int()).unwrap_or(0);
    let n_agents = match r.get("agents") {
        Some(JValue::Arr(a)) => a.len(),
        _ => 0,
    };
    let par = r.get("parallelism").cloned().unwrap_or(JValue::Arr(vec![]));
    let _ = writeln!(
        out,
        "planned {n_tasks} task(s); {n_agents} agent(s); generations={par}"
    );
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
        let _ = writeln!(
            err,
            "nothing submitted yet (empty journal); run `submit` first"
        );
        return 1;
    }
    let ticks = if ticks <= 0 { 60 } else { ticks };
    let summary = match resident {
        Some(secs) if secs > 0.0 => k.run_resident(secs, 0.05, ticks.max(1)),
        _ => k.run(ticks),
    };
    let _ = writeln!(out, "{}", k.report());
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
    let _ = writeln!(out, "{}", k.report());
    0
}

fn cmd_control(k: &mut Kernel, op: &str, agent: &str, reason: &str, out: &mut dyn Write) -> i32 {
    let ok = match op {
        "pause" => k.pause(agent),
        "resume" => k.resume(agent),
        _ => k.terminate_agent(agent, if reason.is_empty() { "cli" } else { reason }),
    };
    let _ = writeln!(out, "{op} {agent}: {}", if ok { "ok" } else { "refused" });
    if ok {
        0
    } else {
        1
    }
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
    let chain = if chain_ok {
        "VALID".into()
    } else {
        format!("BROKEN ({reason})")
    };
    let _ = writeln!(out, "journal rows: {events}  chain: {chain}");
    if !chain_ok {
        return 1;
    }
    let live = k.snapshot();
    drop(k);
    let k2 = match Kernel::from_journal(&jp, true, opts_from(g)) {
        Ok(k) => k,
        Err(e) => return fail(err, &e.to_string()),
    };
    let rp = k2.snapshot();
    let mut diffs = Vec::new();
    for key in ["agents", "tasks", "claims", "artifacts"] {
        let a = live.get(key).cloned().unwrap_or(JValue::Null);
        let b = rp.get(key).cloned().unwrap_or(JValue::Null);
        if a.to_canon_string() != b.to_canon_string() {
            diffs.push(key);
        }
    }
    if diffs.is_empty() {
        let _ = writeln!(out, "replay parity : OK");
        0
    } else {
        let _ = writeln!(out, "replay parity : DIVERGED {diffs:?}");
        1
    }
}

fn cmd_agents(k: &Kernel, json: bool, out: &mut dyn Write) -> i32 {
    if json {
        let mut roster = Vec::new();
        let mut recs: Vec<_> = k.registry().agents.iter().collect();
        recs.sort_by(|a, b| {
            a.epoch
                .cmp(&b.epoch)
                .then(a.agent_id.as_str().cmp(b.agent_id.as_str()))
        });
        for rec in recs {
            let mut row = JMap::new();
            row.insert("agent".into(), JValue::Str(rec.agent_id.as_str().into()));
            row.insert("role".into(), JValue::Str(rec.role.clone()));
            row.insert("state".into(), JValue::Str(rec.state_str().into()));
            row.insert(
                "task".into(),
                rec.task_id
                    .as_ref()
                    .map(|t| JValue::Str(t.as_str().into()))
                    .unwrap_or(JValue::Null),
            );
            row.insert("epoch".into(), JValue::Int(rec.epoch));
            row.insert("spawned_by".into(), JValue::Str(rec.spawned_by.clone()));
            row.insert("queued".into(), JValue::Int(rec.task_queue.len() as i64));
            row.insert("waits".into(), JValue::Int(rec.pending_waits.len() as i64));
            roster.push(JValue::Obj(row));
        }
        let mut m = JMap::new();
        m.insert("roster".into(), JValue::Arr(roster));
        m.insert("monitoring".into(), JValue::Obj(k.monitoring()));
        let _ = writeln!(out, "{}", JValue::Obj(m).to_pretty_string());
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
        "operator-requested capability".into()
    } else {
        a.reason.clone()
    };
    req.insert("reason".into(), JValue::Str(reason));
    req.insert("requires_judgment".into(), JValue::Bool(a.judge));
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
    let msg = k.inject_spawn_request(req.clone(), "cli", true);
    if a.run {
        match a.resident {
            Some(secs) if secs > 0.0 => {
                let _ = k.run_resident(secs, 0.05, a.ticks.max(1));
            }
            _ => {
                let _ = k.run(a.ticks);
            }
        }
    }
    let mut outm = JMap::new();
    outm.insert("injected".into(), JValue::Obj(req));
    outm.insert("mid".into(), JValue::Str(msg.mid));
    let ledger: Vec<JValue> = k
        .parent()
        .ledger
        .newest_first()
        .into_iter()
        .take(1)
        .map(|e| JValue::Obj(e.to_dict()))
        .collect();
    outm.insert("ledger".into(), JValue::Arr(ledger));
    outm.insert("monitoring".into(), JValue::Obj(k.monitoring()));
    let _ = writeln!(out, "{}", JValue::Obj(outm).to_pretty_string());
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
    let mut ticks = 60i64;
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
            "--skills" | "--skill" => {
                let v = need_val(args, i, "--skill")?;
                a.skills.extend(csv(&v));
                i += 1;
            }
            "--work" => {
                a.work = parse_f64(&need_val(args, i, "--work")?, "--work")?;
                i += 1;
            }
            "--inputs" | "--needs" => {
                let v = need_val(args, i, "--needs")?;
                a.inputs.extend(csv(&v));
                i += 1;
            }
            "--outputs" | "--produces" => {
                let v = need_val(args, i, "--produces")?;
                a.outputs.extend(csv(&v));
                i += 1;
            }
            "--class" | "--capability-class" => {
                a.class = need_val(args, i, "--capability-class")?;
                i += 1;
            }
            "--parent-task" | "--task" => {
                a.parent_task = need_val(args, i, "--task")?;
                i += 1;
            }
            "--correlation" | "--correlation-id" => {
                a.correlation = need_val(args, i, "--correlation-id")?;
                i += 1;
            }
            "--judge" => a.judge = true,
            "--run" => a.run = true,
            "--no-run" => a.run = false,
            "--resident" => {
                a.resident = Some(parse_f64(&need_val(args, i, "--resident")?, "--resident")?);
                i += 1;
            }
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
