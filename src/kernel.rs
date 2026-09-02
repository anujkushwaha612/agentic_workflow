//! Kernel: composition root and owner of mutable runtime state
//! (`arena/kernel.py`).
//!
//! ```text
//!     Clock  Journal  Graph  Registry  Bus  Actors
//!                         │
//!                      Kernel
//!                         │
//!              ActorRuntime (this impl)
//!                         │
//!                    AgentActor
//!                         │
//!                     Cognition   (proposes only)
//! ```
//!
//! Parent, Spawn and Tools are **seams**, not residents. Spawn requests land
//! in a queue; tool execution is behind [`ToolExecutor`]; cortex files are
//! applied here so a later Parent does not have to own the tick loop.
//!
//! Ownership: `&mut Kernel` flows through the tick. An actor is `remove`d
//! from the map, `run_step(self)`'d, then reinserted — no `Arc<Mutex<_>>`,
//! no kernel pointer on the actor.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::actor::{ActorRuntime, AgentActor, CompletionGate, VerifyVerdict, RUNNABLE};
use crate::bus::Bus;
use crate::clock::Clock;
use crate::cognition::{Cognition, ToolResultView};
use crate::graph::{CycleError, DependencyGraph, TaskSpec, TaskStatus};
use crate::ids::{AgentId, TaskId};
use crate::journal::{EventFilter, Journal, JournalError, JournalPath};
use crate::lifecycle::{AgentState, Lifecycle};
use crate::msg::{EventType, Message};
use crate::policy::{is_sleeping_action, make_policy};
use crate::registry::{emit_registered, emit_terminated, AgentRecord, AgentRegistry, SpawnBudget};
use crate::sys::json::{parse, py_round, JMap, JValue};

pub const CONFIG_FILE: &str = "kernel_config.json";

// ------------------------------------------------------------------ tools seam

/// Deferred M10 tool executor. Kernel never implements tool logic itself.
pub trait ToolExecutor {
    fn describe(&self, agent_id: &str) -> JMap;
    fn allowed(&self, agent_id: &str) -> Vec<String>;
    fn schemas(&self, agent_id: &str) -> Vec<JMap>;
    fn stats(&self) -> JMap;
    fn plan(
        &mut self,
        agent_id: &str,
        calls: &[JMap],
        task_id: Option<&str>,
        corr: &str,
    ) -> Vec<String>;
    fn execute(
        &mut self,
        agent_id: &str,
        tool: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
        corr: &str,
    ) -> ToolResultView;
}

/// Per-role factory so two agents of the same role get two brains.
/// The factory is given the agent id so fingerprints and transcripts
/// cannot accidentally alias.
type CognitionMaker = Box<dyn Fn(&str) -> Box<dyn Cognition>>;

pub struct CognitionFactory {
    inner: CognitionMaker,
}

impl CognitionFactory {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&str) -> Box<dyn Cognition> + 'static,
    {
        CognitionFactory { inner: Box::new(f) }
    }
    pub fn make(&self, agent_id: &str) -> Box<dyn Cognition> {
        (self.inner)(agent_id)
    }
}

// ------------------------------------------------------------------ artifacts

#[derive(Debug, Clone)]
pub struct ArtifactMeta {
    pub artifact: String,
    pub version: i64,
    pub producer: String,
    pub task_id: Option<String>,
    pub at: f64,
    pub digest: String,
    pub files: Vec<JMap>,
}

// ------------------------------------------------------------------ opts

/// Construction settings. Anything not set here is the Python dataclass default.
pub struct KernelOpts {
    pub journal_path: Option<PathBuf>,
    pub root: Option<PathBuf>,
    pub budget: SpawnBudget,
    pub clock_mode: String,
    pub clock_step: f64,
    pub work_unit: f64,
    pub default_policy: String,
    pub role_policies: BTreeMap<String, String>,
    pub role_overrides: BTreeMap<String, JMap>,
    pub policy_args: JMap,
    pub detect_deadlocks: bool,
    pub deadlock_action: String, // "resolve" | "report" | "off"
    pub auto_assign: bool,
    pub reap: bool,
    pub agent_subscriptions: Vec<String>,
    pub trace_path: String,
    pub inject_path: String,
    pub stall_limit: i64,
    pub transcript_dir: Option<String>,
}

impl Default for KernelOpts {
    fn default() -> Self {
        let mut role_policies = BTreeMap::new();
        for (k, v) in [
            ("frontend", "wait"),
            ("backend", "wait"),
            ("testing", "wait"),
            ("data", "wait"),
            ("evaluation", "wait"),
            ("cloud", "simulated"),
            ("security", "wait"),
        ] {
            role_policies.insert(k.into(), v.into());
        }
        KernelOpts {
            journal_path: None,
            root: None,
            budget: SpawnBudget::default(),
            clock_mode: "virtual".into(),
            clock_step: 0.01,
            work_unit: 0.25,
            default_policy: "simulated".into(),
            role_policies,
            role_overrides: BTreeMap::new(),
            policy_args: JMap::new(),
            detect_deadlocks: true,
            deadlock_action: "resolve".into(),
            auto_assign: true,
            reap: true,
            agent_subscriptions: vec!["resource.*".into()],
            trace_path: "var/events.jsonl".into(),
            inject_path: "var/inject.jsonl".into(),
            stall_limit: 6,
            transcript_dir: None,
        }
    }
}

impl KernelOpts {
    fn default_role_policies() -> BTreeMap<String, String> {
        KernelOpts::default().role_policies
    }
}

// ------------------------------------------------------------------ kernel

pub struct Kernel {
    pub journal: Journal,
    pub graph: DependencyGraph,
    pub registry: AgentRegistry,
    pub bus: Bus,
    pub clock: Clock,
    actors: BTreeMap<String, AgentActor>,
    pub artifacts: BTreeMap<String, ArtifactMeta>,
    pub tick: i64,
    pub task_text: String,
    pub work_unit: f64,
    pub default_policy: String,
    pub role_policies: BTreeMap<String, String>,
    pub role_overrides: BTreeMap<String, JMap>,
    pub policy_args: JMap,
    pub detect_deadlocks: bool,
    pub deadlock_action: String,
    pub auto_assign: bool,
    pub reap: bool,
    pub agent_subscriptions: Vec<String>,
    pub stall_limit: i64,
    pub stalled: bool,
    pub log_lines: Vec<String>,
    pub parent_inbox: Vec<JMap>,
    pub spawn_requests: Vec<Message>,
    pub feature_requests: Vec<JMap>,
    pub deadlocks: Vec<Vec<String>>,
    pub resident_loops: i64,
    pub injected_rows: i64,
    /// Folded spawn-request ledger (M9 Parent rehydrates from this).
    pub request_fold: JMap,
    pub cognition_sources: BTreeMap<String, CognitionFactory>,
    tools: Option<Box<dyn ToolExecutor>>,
    workspaces: BTreeMap<String, JMap>,
    pub transcript_dir: Option<String>,
    pub clock_mode: String,
    pub clock_step: f64,
    root: Option<PathBuf>,
    side_anchor: Option<PathBuf>,
    trace_rel: String,
    inject_rel: String,
    decision_rel: String,
    inbox_rel: String,
}

impl Kernel {
    pub fn new(opts: KernelOpts) -> Result<Kernel, JournalError> {
        let clock = Clock::make(&opts.clock_mode, opts.clock_step)
            .unwrap_or_else(|_| Clock::virtual_(opts.clock_step));
        let mut journal = match &opts.journal_path {
            None => Journal::open_memory()?,
            Some(p) if p.as_os_str() == ":memory:" => Journal::open_memory()?,
            Some(p) => Journal::open_file(p, Some(clock.now()))?,
        };
        journal.set_now_provider(clock.now());
        let registry = AgentRegistry::new(opts.budget);
        let bus = Bus::new();
        let side_anchor = compute_side_anchor(opts.root.as_deref(), journal.path.clone());
        let k = Kernel {
            journal,
            graph: DependencyGraph::default(),
            registry,
            bus,
            clock,
            actors: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            tick: 0,
            task_text: String::new(),
            work_unit: opts.work_unit,
            default_policy: opts.default_policy,
            role_policies: opts.role_policies,
            role_overrides: opts.role_overrides,
            policy_args: opts.policy_args,
            detect_deadlocks: opts.detect_deadlocks,
            deadlock_action: opts.deadlock_action,
            auto_assign: opts.auto_assign,
            reap: opts.reap,
            agent_subscriptions: opts.agent_subscriptions,
            stall_limit: opts.stall_limit,
            stalled: false,
            log_lines: Vec::new(),
            parent_inbox: Vec::new(),
            spawn_requests: Vec::new(),
            feature_requests: Vec::new(),
            deadlocks: Vec::new(),
            resident_loops: 0,
            injected_rows: 0,
            request_fold: JMap::new(),
            cognition_sources: BTreeMap::new(),
            tools: None,
            workspaces: BTreeMap::new(),
            transcript_dir: opts.transcript_dir,
            clock_mode: opts.clock_mode,
            clock_step: opts.clock_step,
            root: opts.root,
            side_anchor,
            trace_rel: opts.trace_path,
            inject_rel: opts.inject_path,
            decision_rel: "var/decisions.jsonl".into(),
            inbox_rel: "var/inbox.jsonl".into(),
        };
        if let Some(p) = k.anchor(&k.trace_rel.clone()) {
            let _ = std::fs::create_dir_all(p.parent().unwrap_or(p.as_path()));
        }
        Ok(k)
    }

    pub fn in_memory() -> Kernel {
        Kernel::new(KernelOpts::default()).expect("memory journal")
    }

    pub fn now(&self) -> f64 {
        self.clock.now()
    }

    fn sync_now(&mut self) {
        self.journal.set_now_provider(self.clock.now());
    }

    // ----------------------------------------------------- side-file anchoring

    fn anchor(&self, rel: &str) -> Option<PathBuf> {
        let q = PathBuf::from(rel);
        if q.is_absolute() {
            return Some(q);
        }
        self.side_anchor.as_ref().map(|a| a.join(q))
    }

    pub fn side_anchor(&self) -> Option<&Path> {
        self.side_anchor.as_deref()
    }

    pub fn root(&self) -> Option<&Path> {
        self.root.as_deref()
    }

    // --------------------------------------------------------------- logging

    pub fn log(&mut self, text: &str) {
        self.log_lines
            .push(format!("[{:7.2}] {text}", self.clock.now()));
    }

    fn emit(
        &mut self,
        etype: EventType,
        actor: &str,
        target: &str,
        body: &str,
        fields: JMap,
        task_id: Option<&str>,
    ) {
        self.sync_now();
        self.journal.emit(
            etype, actor, target, body, fields, task_id, None, None, None, 0,
        );
    }

    // ----------------------------------------------------------- lifecycle

    /// The ONLY sanctioned way to change an agent's lifecycle state.
    /// Accepted *and* rejected edges are journalled.
    pub fn transition(&mut self, agent_id: &str, to: AgentState, reason: &str) -> bool {
        self.sync_now();
        let Some(rec) = self.registry.get_mut(agent_id) else {
            return false;
        };
        let res = rec.lifecycle.request(to, reason, self.clock.now());
        if res.ok {
            self.registry.version += 1;
            let mut fields = JMap::new();
            fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
            fields.insert("frm".into(), JValue::Str(res.frm.as_str().into()));
            fields.insert("to".into(), JValue::Str(res.to.as_str().into()));
            fields.insert("reason".into(), JValue::Str(reason.into()));
            self.journal.emit(
                EventType::StateTransition,
                agent_id,
                "parent",
                &format!("{} -> {} ({reason})", res.frm.as_str(), res.to.as_str()),
                fields,
                None,
                None,
                None,
                None,
                0,
            );
            true
        } else {
            let mut fields = JMap::new();
            fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
            fields.insert("frm".into(), JValue::Str(res.frm.as_str().into()));
            fields.insert("to".into(), JValue::Str(res.to.as_str().into()));
            self.journal.emit(
                EventType::IllegalTransition,
                agent_id,
                "parent",
                &res.reason,
                fields,
                None,
                None,
                None,
                None,
                0,
            );
            false
        }
    }

    pub fn pause(&mut self, agent_id: &str) -> bool {
        if !self.transition(agent_id, AgentState::Paused, "paused") {
            return false;
        }
        let mut fields = JMap::new();
        fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
        self.emit(
            EventType::AgentPaused,
            "parent",
            agent_id,
            "paused",
            fields,
            None,
        );
        true
    }

    pub fn resume(&mut self, agent_id: &str) -> bool {
        if !self.transition(agent_id, AgentState::Idle, "resumed") {
            return false;
        }
        let mut fields = JMap::new();
        fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
        self.emit(
            EventType::AgentResumed,
            "parent",
            agent_id,
            "resumed",
            fields,
            None,
        );
        true
    }

    pub fn terminate_agent(&mut self, agent_id: &str, reason: &str) -> bool {
        self.transition(agent_id, AgentState::Terminated, reason);
        let Some(rec) = self.registry.terminate(agent_id, reason) else {
            return false;
        };
        emit_terminated(&mut self.journal, &rec, reason);
        self.actors.remove(agent_id);
        true
    }

    // ------------------------------------------------------- actor binding

    fn policy_for(&self, role: &str) -> Box<dyn crate::policy::Policy> {
        let name = self
            .role_policies
            .get(role)
            .cloned()
            .unwrap_or_else(|| self.default_policy.clone());
        let mut extra = self.policy_args.clone();
        if let Some(ov) = self.role_overrides.get(role) {
            for (k, v) in ov {
                extra.insert(k.clone(), v.clone());
            }
        }
        make_policy(&name, &extra).unwrap_or_else(|_| {
            make_policy("simulated", &JMap::new()).expect("simulated always exists")
        })
    }

    /// Bind a policy without touching lifecycle or the journal (recovery).
    pub fn bind_actor(&mut self, agent_id: &str) -> Option<&AgentActor> {
        let rec = self.registry.get(agent_id)?;
        let role = rec.role.clone();
        let policy = self.policy_for(&role);
        let actor = AgentActor::new(agent_id, policy);
        self.actors.insert(agent_id.to_string(), actor);
        self.bus.register_actor(agent_id);
        self.actors.get(agent_id)
    }

    /// Live bind: CREATED → INITIALIZING → IDLE, journalled.
    pub fn make_actor(&mut self, agent_id: &str) -> bool {
        if self.registry.get(agent_id).is_none() {
            return false;
        }
        self.bind_actor(agent_id);
        self.transition(agent_id, AgentState::Initializing, "bound to policy");
        self.transition(agent_id, AgentState::Idle, "ready");
        true
    }

    pub fn actor(&self, id: &str) -> Option<&AgentActor> {
        self.actors.get(id)
    }

    pub fn actor_mut(&mut self, id: &str) -> Option<&mut AgentActor> {
        self.actors.get_mut(id)
    }

    /// Attach a cognition source to one agent. `None` calls the role factory.
    /// Two agents of the same role must not share an instance.
    pub fn bind_cognition(
        &mut self,
        agent_id: &str,
        source: Option<Box<dyn Cognition>>,
    ) -> Result<JMap, String> {
        let rec = self
            .registry
            .get(agent_id)
            .ok_or_else(|| format!("unknown agent {agent_id}"))?;
        let role = rec.role.clone();
        let source = match source {
            Some(s) => s,
            None => {
                let factory = self
                    .cognition_sources
                    .get(&role)
                    .ok_or_else(|| format!("no cognition source for role {role:?}"))?;
                factory.make(agent_id)
            }
        };
        if !self.actors.contains_key(agent_id) {
            self.make_actor(agent_id);
        }
        let actor = self
            .actors
            .get_mut(agent_id)
            .ok_or_else(|| format!("no actor {agent_id}"))?;
        actor.bind_cognition(source);
        let fp = actor.cognition_fingerprint();
        if let Some(rec) = self.registry.get_mut(agent_id) {
            rec.cognition = fp.clone();
        }
        let mut fields = JMap::new();
        fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
        fields.insert(
            "source".into(),
            fp.get("source").cloned().unwrap_or(JValue::Null),
        );
        fields.insert(
            "kind".into(),
            fp.get("kind").cloned().unwrap_or(JValue::Null),
        );
        fields.insert(
            "model_id".into(),
            fp.get("model").cloned().unwrap_or(JValue::Str("".into())),
        );
        fields.insert(
            "prompt_sha256_16".into(),
            fp.get("prompt_sha256_16")
                .cloned()
                .unwrap_or(JValue::Str("".into())),
        );
        let body = format!(
            "cognition bound: {}",
            fp.get("source").and_then(|v| v.as_str()).unwrap_or("?")
        );
        self.emit(
            EventType::CognitionBound,
            "kernel",
            agent_id,
            &body,
            fields,
            None,
        );
        Ok(fp)
    }

    pub fn set_cognition_factory(&mut self, role: &str, factory: CognitionFactory) {
        self.cognition_sources.insert(role.to_string(), factory);
    }

    /// Record a workspace bind. Does not create an executor (M10).
    pub fn bind_workspace(&mut self, agent_id: &str, root: &str, writes: &[&str], reads: &[&str]) {
        let mut m = JMap::new();
        m.insert("root".into(), JValue::Str(root.into()));
        m.insert(
            "writes".into(),
            JValue::Arr(writes.iter().map(|s| JValue::Str((*s).into())).collect()),
        );
        m.insert(
            "reads".into(),
            JValue::Arr(reads.iter().map(|s| JValue::Str((*s).into())).collect()),
        );
        m.insert("bound".into(), JValue::Bool(true));
        self.workspaces.insert(agent_id.to_string(), m);
        let mut fields = JMap::new();
        fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
        fields.insert("root".into(), JValue::Str(root.into()));
        self.emit(
            EventType::WorkspaceBound,
            "kernel",
            agent_id,
            "workspace bound",
            fields,
            None,
        );
    }

    pub fn bind_tools_executor(&mut self, exec: Box<dyn ToolExecutor>) {
        self.tools = Some(exec);
    }

    pub fn cognition_report(&self) -> JMap {
        let mut rows: Vec<JMap> = Vec::new();
        let mut by_prompt: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for rec in &self.registry.agents {
            let aid = rec.agent_id.as_str().to_string();
            let actor = self.actors.get(&aid);
            let fp = rec.cognition.clone();
            let prompt = fp
                .get("prompt_sha256_16")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !prompt.is_empty() {
                by_prompt
                    .entry(prompt.clone())
                    .or_default()
                    .push(aid.clone());
            }
            let mut row = JMap::new();
            row.insert("agent_id".into(), JValue::Str(aid));
            row.insert("role".into(), JValue::Str(rec.role.clone()));
            row.insert(
                "source".into(),
                fp.get("source")
                    .cloned()
                    .unwrap_or_else(|| JValue::Str("?".into())),
            );
            row.insert(
                "kind".into(),
                fp.get("kind")
                    .cloned()
                    .unwrap_or_else(|| JValue::Str("policy".into())),
            );
            row.insert("prompt_sha256_16".into(), JValue::Str(prompt));
            row.insert(
                "transcript_len".into(),
                JValue::Int(actor.map(|a| a.transcript.len() as i64).unwrap_or(0)),
            );
            row.insert(
                "steps_run".into(),
                JValue::Int(actor.map(|a| a.steps_run).unwrap_or(0)),
            );
            rows.push(row);
        }
        let shared_prompt: Vec<JValue> = by_prompt
            .values()
            .filter(|v| v.len() > 1)
            .map(|v| JValue::Arr(v.iter().cloned().map(JValue::Str).collect()))
            .collect();
        for r in &mut rows {
            let p = r
                .get("prompt_sha256_16")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let shared = by_prompt.get(&p).map(|v| v.len() > 1).unwrap_or(false) && !p.is_empty();
            r.insert("shared_prompt".into(), JValue::Bool(shared));
            r.insert("shared_obj_id".into(), JValue::Bool(false));
        }
        let mut out = JMap::new();
        out.insert("agents".into(), JValue::Int(rows.len() as i64));
        out.insert(
            "distinct_prompts".into(),
            JValue::Int(by_prompt.len() as i64),
        );
        out.insert("shared_prompt_groups".into(), JValue::Arr(shared_prompt));
        out.insert(
            "rows".into(),
            JValue::Arr(rows.into_iter().map(JValue::Obj).collect()),
        );
        out
    }

    // ------------------------------------------------------- graph / agents

    pub fn register_agent(
        &mut self,
        agent_id: &str,
        role: &str,
        skills: &[&str],
        epoch: i64,
        spawned_by: &str,
    ) -> Result<AgentId, crate::registry::RegistryError> {
        self.sync_now();
        let id =
            self.registry
                .register(agent_id, role, skills, epoch, spawned_by, "", None, &[])?;
        if let Some(rec) = self.registry.get(agent_id) {
            let rec = rec.clone();
            emit_registered(&mut self.journal, &rec);
        }
        self.bus.register_actor(agent_id);
        let subs: Vec<&str> = self
            .agent_subscriptions
            .iter()
            .map(|s| s.as_str())
            .collect();
        self.bus.subscribe(&mut self.registry, agent_id, &subs);
        Ok(id)
    }

    pub fn add_task(&mut self, spec: TaskSpec) -> Result<(), String> {
        let snap = spec.snapshot();
        self.graph.add(spec, true)?;
        let mut fields = JMap::new();
        fields.insert("tasks".into(), JValue::Arr(vec![JValue::Obj(snap)]));
        self.emit(
            EventType::PlanCreated,
            "parent",
            "parent",
            "task added",
            fields,
            None,
        );
        Ok(())
    }

    /// Install a whole plan. Cycle ⇒ no agents, no tasks left behind.
    pub fn submit_plan(
        &mut self,
        text: &str,
        tasks: Vec<TaskSpec>,
        agents: &[(&str, &str, Vec<&str>)],
    ) -> Result<JMap, CycleError> {
        self.task_text = text.to_string();
        let before = self.graph.clone();
        let snaps: Vec<JValue> = tasks.iter().map(|t| JValue::Obj(t.snapshot())).collect();
        for t in tasks {
            if let Err(e) = self.graph.add(t, false) {
                self.graph = before;
                return Err(CycleError(vec![e]));
            }
        }
        self.graph.derive_edges();
        if let Err(e) = self.graph.validate() {
            self.graph = before;
            return Err(e);
        }
        let mut fields = JMap::new();
        fields.insert("tasks".into(), JValue::Arr(snaps));
        self.emit(
            EventType::PlanCreated,
            "parent",
            "parent",
            "planned",
            fields,
            None,
        );
        for (id, role, skills) in agents {
            let _ = self.register_agent(id, role, skills, 0, "parent");
            self.make_actor(id);
        }
        if self.auto_assign {
            self.schedule();
        }
        let n_tasks = self.graph.tasks.len() as i64;
        let gens = self.graph.order().unwrap_or_default();
        let mut out = JMap::new();
        out.insert("tasks".into(), JValue::Int(n_tasks));
        out.insert(
            "agents".into(),
            JValue::Arr(
                self.registry
                    .agents
                    .iter()
                    .map(|a| JValue::Str(a.agent_id.as_str().into()))
                    .collect(),
            ),
        );
        out.insert(
            "parallelism".into(),
            JValue::Arr(gens.iter().map(|g| JValue::Int(g.len() as i64)).collect()),
        );
        Ok(out)
    }

    /// `submit(text)` without a planner cannot invent an org. Returns empty.
    pub fn submit(&mut self, text: &str) -> JMap {
        self.task_text = text.to_string();
        let mut out = JMap::new();
        out.insert("tasks".into(), JValue::Int(0));
        out.insert("agents".into(), JValue::Arr(vec![]));
        out.insert(
            "detail".into(),
            JValue::Str("planner deferred to M9 Parent".into()),
        );
        out
    }

    pub fn assign(&mut self, task_id: &str, agent_id: &str) -> bool {
        let Some(t) = self.graph.tasks.get_mut(&TaskId::new(task_id)) else {
            return false;
        };
        if !t.is_open() {
            return false;
        }
        t.owner = Some(AgentId::new(agent_id));
        if t.status == TaskStatus::Pending {
            t.status = TaskStatus::Assigned;
        }
        let key = t.key();
        let _ = self
            .journal
            .claim(&key, agent_id, Some(task_id), self.clock.now());
        if let Some(rec) = self.registry.get_mut(agent_id) {
            match &rec.task_id {
                None => rec.task_id = Some(TaskId::new(task_id)),
                Some(cur)
                    if cur.as_str() != task_id
                        && !rec.task_queue.iter().any(|t| t.as_str() == task_id) =>
                {
                    rec.task_queue.push(TaskId::new(task_id));
                }
                _ => {}
            }
            self.registry.version += 1;
        }
        let mut fields = JMap::new();
        fields.insert("owner".into(), JValue::Str(agent_id.into()));
        self.emit(
            EventType::TaskAssigned,
            "parent",
            agent_id,
            &format!("{task_id} assigned"),
            fields,
            Some(task_id),
        );
        // COMPLETED → INITIALIZING re-queue edge
        if self.registry.get(agent_id).map(|r| r.lifecycle.state) == Some(AgentState::Completed) {
            self.transition(agent_id, AgentState::Initializing, "new assignment");
        }
        true
    }

    /// Assign unowned pending tasks to idle agents of the matching role.
    pub fn schedule(&mut self) {
        let pending: Vec<(String, String)> = self
            .graph
            .tasks
            .values()
            .filter(|t| t.status == TaskStatus::Pending && t.owner.is_none() && t.is_open())
            .map(|t| (t.task_id.as_str().to_string(), t.role.clone()))
            .collect();
        for (tid, role) in pending {
            let candidate = self
                .registry
                .agents
                .iter()
                .find(|a| {
                    a.role == role
                        && a.task_id.is_none()
                        && matches!(
                            a.lifecycle.state,
                            AgentState::Idle | AgentState::Initializing | AgentState::Created
                        )
                })
                .map(|a| a.agent_id.as_str().to_string());
            if let Some(aid) = candidate {
                self.assign(&tid, &aid);
            }
        }
    }

    // -------------------------------------------------------------- bus

    pub fn publish(&mut self, mut msg: Message) {
        self.sync_now();
        if msg.msg_type == EventType::FeatureRequest {
            let mut row = JMap::new();
            row.insert("from".into(), JValue::Str(msg.from_actor.as_str().into()));
            row.insert("to".into(), JValue::Str(msg.to_actor.as_str().into()));
            row.insert("body".into(), JValue::Str(msg.body.clone()));
            self.feature_requests.push(row);
        }
        let _ = self.bus.publish(&mut msg, &mut self.journal);
        if let Some(path) = self.anchor(&self.trace_rel.clone()) {
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
            {
                use std::io::Write;
                let _ = writeln!(f, "{}", JValue::Obj(msg.to_dict()).to_canon_string());
            }
        }
    }

    pub fn deliver(&mut self, actor_id: &str, msg: Message) -> bool {
        if self.bus.mailbox_len(actor_id) == 0 && !self.actors.contains_key(actor_id) {
            // mailbox may exist empty
        }
        // Bus has no public enqueue; publish a targeted message instead.
        let mut m = msg;
        m.to_actor = crate::ids::ActorName::new(actor_id);
        let before = self.bus.mailbox_len(actor_id);
        self.publish(m);
        self.bus.mailbox_len(actor_id) > before || before > 0
    }

    // ---------------------------------------------------------- artifacts

    pub fn artifact_exists(&self, artifact: &str) -> bool {
        self.artifacts.contains_key(artifact)
    }

    pub fn missing_artifacts(&self, arts: &[String]) -> Vec<String> {
        arts.iter()
            .filter(|a| !self.artifacts.contains_key(*a))
            .cloned()
            .collect()
    }

    fn condition_met(&self, condition: &str) -> bool {
        if let Some(a) = condition.strip_prefix("artifact:") {
            return self.artifacts.contains_key(a);
        }
        if let Some(tid) = condition.strip_prefix("task:") {
            return self
                .graph
                .tasks
                .get(&TaskId::new(tid))
                .map(|t| !t.is_open())
                .unwrap_or(false);
        }
        false
    }

    pub fn wake_ready(&mut self, reason: &str) -> Vec<String> {
        let mut woke = Vec::new();
        let ids: Vec<String> = self
            .registry
            .agents
            .iter()
            .map(|a| a.agent_id.as_str().to_string())
            .collect();
        for aid in ids {
            let state = match self.registry.get(&aid).map(|r| r.lifecycle.state) {
                Some(s) => s,
                None => continue,
            };
            if !matches!(
                state,
                AgentState::WaitingForDependency | AgentState::Blocked | AgentState::Escalated
            ) {
                continue;
            }
            let keep: Vec<bool> = self
                .registry
                .get(&aid)
                .map(|r| {
                    r.pending_waits
                        .iter()
                        .map(|w| {
                            w.get("condition")
                                .and_then(|v| v.as_str())
                                .map(|c| !self.condition_met(c))
                                .unwrap_or(true)
                        })
                        .collect()
                })
                .unwrap_or_default();
            if let Some(rec) = self.registry.get_mut(&aid) {
                let mut i = 0;
                rec.pending_waits.retain(|_| {
                    let k = keep.get(i).copied().unwrap_or(true);
                    i += 1;
                    k
                });
            }
            if self
                .registry
                .get(&aid)
                .map(|r| !r.pending_waits.is_empty())
                .unwrap_or(true)
            {
                continue;
            }
            let has_mail = self.bus.mailbox_len(&aid) > 0;
            if !has_mail {
                let tid = self
                    .registry
                    .get(&aid)
                    .and_then(|r| r.task_id.as_ref().map(|t| t.as_str().to_string()));
                let fields = JMap::new();
                self.emit(
                    EventType::DependencyReady,
                    "dependency_manager",
                    &aid,
                    &format!("unblocked: {reason}"),
                    fields,
                    tid.as_deref(),
                );
                let mut m = Message::new(
                    EventType::DependencyReady,
                    "dependency_manager",
                    aid.as_str(),
                );
                m.body = format!("unblocked: {reason}");
                m.task_id = tid.map(TaskId::new);
                self.publish(m);
                self.bus.stats.wakes += 1;
            }
            self.transition(&aid, AgentState::Working, &format!("unblocked: {reason}"));
            woke.push(aid);
        }
        woke
    }

    pub fn publish_artifact(&mut self, artifact: &str, producer: &str, task_id: Option<&str>) {
        let version = self.artifacts.get(artifact).map(|m| m.version).unwrap_or(0) + 1;
        self.artifacts.insert(
            artifact.to_string(),
            ArtifactMeta {
                artifact: artifact.to_string(),
                version,
                producer: producer.to_string(),
                task_id: task_id.map(|s| s.to_string()),
                at: self.clock.now(),
                digest: String::new(),
                files: Vec::new(),
            },
        );
        self.graph.known_artifacts.insert(artifact.to_string());
        if let Some(tid) = task_id {
            if let Some(t) = self.graph.tasks.get(&TaskId::new(tid)) {
                if !t.produces.is_empty()
                    && self.missing_artifacts(&t.produces).is_empty()
                    && t.is_open()
                {
                    let produces = t.produces.clone();
                    let owner = t.owner.clone();
                    if let Some(tt) = self.graph.tasks.get_mut(&TaskId::new(tid)) {
                        tt.status = TaskStatus::Done;
                        tt.finished_at = Some(self.clock.now());
                    }
                    let mut fields = JMap::new();
                    fields.insert(
                        "artifacts".into(),
                        JValue::Arr(produces.iter().cloned().map(JValue::Str).collect()),
                    );
                    fields.insert(
                        "owner".into(),
                        owner
                            .map(|o| JValue::Str(o.as_str().into()))
                            .unwrap_or(JValue::Null),
                    );
                    fields.insert("auto".into(), JValue::Bool(true));
                    self.emit(
                        EventType::TaskCompleted,
                        producer,
                        "parent",
                        &format!("{tid} auto-closed: all artifacts published"),
                        fields,
                        Some(tid),
                    );
                }
            }
        }
        self.wake_ready(&format!("artifact {artifact} v{version}"));
        self.registry.version += 1;
        let mut payload = JMap::new();
        payload.insert("artifact".into(), JValue::Str(artifact.into()));
        payload.insert("version".into(), JValue::Int(version));
        let mut fields = JMap::new();
        fields.insert("artifact".into(), JValue::Str(artifact.into()));
        fields.insert("version".into(), JValue::Int(version));
        fields.insert("payload".into(), JValue::Obj(payload));
        self.emit(
            EventType::ResourceUpdated,
            producer,
            "broadcast",
            &format!("{artifact} v{version} ready"),
            fields,
            task_id,
        );
        let _ = self.bus.resolve(
            &mut self.journal,
            &mut self.registry,
            &format!("artifact:{artifact}"),
            &format!("{producer} published {artifact} v{version}"),
            None,
        );
    }

    pub fn requeue(&mut self, task_id: &str, reason: &str) -> bool {
        let Some(t) = self.graph.tasks.get_mut(&TaskId::new(task_id)) else {
            return false;
        };
        if !t.is_open() {
            return false;
        }
        let owner = t.owner.take();
        t.status = TaskStatus::Pending;
        if let Some(o) = owner {
            if let Some(rec) = self.registry.get_mut(o.as_str()) {
                if rec.task_id.as_ref().map(|t| t.as_str()) == Some(task_id) {
                    rec.task_id = None;
                }
            }
        }
        self.registry.version += 1;
        self.log(&format!("requeued {task_id}: {reason}"));
        true
    }

    // ------------------------------------------------------- verify gate

    pub fn completion_gate_of(&self, agent_id: &str) -> CompletionGate {
        let Some(tid) = self.registry.get(agent_id).and_then(|r| r.task_id.clone()) else {
            return CompletionGate::default();
        };
        let Some(t) = self.graph.tasks.get(&tid) else {
            return CompletionGate::default();
        };
        if t.verify.is_empty() {
            return CompletionGate::default();
        }
        if t.verified {
            return CompletionGate {
                allow: true,
                verified: true,
                ..Default::default()
            };
        }
        CompletionGate {
            allow: false,
            verified: false,
            rule: "REJECT_UNVERIFIED".into(),
            detail: format!(
                "{} declares {} verify command(s); none has exited 0 for this task yet",
                t.task_id.as_str(),
                t.verify.len()
            ),
            task_id: Some(t.task_id.as_str().into()),
        }
    }

    pub fn run_verify(&mut self, agent_id: &str, task_id: Option<&str>) -> VerifyVerdict {
        let tid = task_id.map(|s| s.to_string()).or_else(|| {
            self.registry
                .get(agent_id)
                .and_then(|r| r.task_id.as_ref().map(|t| t.as_str().to_string()))
        });
        let Some(tid) = tid else {
            return VerifyVerdict {
                ok: false,
                ..Default::default()
            };
        };
        if !self.graph.tasks.contains_key(&TaskId::new(&tid)) {
            return VerifyVerdict {
                ok: false,
                ..Default::default()
            };
        }
        if self.tools.is_none() {
            return VerifyVerdict {
                ok: false,
                task_id: Some(tid),
                results: vec![],
            };
        }
        let argv_lists = self
            .graph
            .tasks
            .get(&TaskId::new(&tid))
            .map(|t| t.verify.clone())
            .unwrap_or_default();
        let mut results = Vec::new();
        let mut all_ok = !argv_lists.is_empty();
        for argv in argv_lists {
            let mut args = JMap::new();
            args.insert(
                "argv".into(),
                JValue::Arr(argv.iter().cloned().map(JValue::Str).collect()),
            );
            let res = if let Some(ex) = self.tools.as_mut() {
                ex.execute(agent_id, "run_command", &args, "", Some(&tid), "")
            } else {
                all_ok = false;
                break;
            };
            all_ok = all_ok && res.ok;
            let mut row = JMap::new();
            row.insert("tool".into(), JValue::Str(res.tool));
            row.insert("ok".into(), JValue::Bool(res.ok));
            row.insert(
                "exit".into(),
                res.exit_code.map(JValue::Int).unwrap_or(JValue::Null),
            );
            results.push(row);
        }
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(&tid)) {
            t.verified = all_ok;
            t.verified_at = Some(self.clock.now());
        }
        self.registry.version += 1;
        let mut fields = JMap::new();
        fields.insert("verified".into(), JValue::Bool(all_ok));
        fields.insert("agent_id".into(), JValue::Str(agent_id.into()));
        fields.insert(
            "rule".into(),
            JValue::Str(
                if all_ok {
                    "VERIFY_PASSED"
                } else {
                    "VERIFY_FAILED"
                }
                .into(),
            ),
        );
        fields.insert(
            "verify_results".into(),
            JValue::Arr(results.iter().cloned().map(JValue::Obj).collect()),
        );
        self.emit(
            EventType::TaskVerified,
            "kernel",
            "parent",
            &format!(
                "{tid} verify {} ({} command(s))",
                if all_ok { "PASSED" } else { "FAILED" },
                results.len()
            ),
            fields,
            Some(&tid),
        );
        VerifyVerdict {
            ok: all_ok,
            results,
            task_id: Some(tid),
        }
    }

    // ------------------------------------------------------- scheduling

    fn begin_if_assigned(&mut self, agent_id: &str) {
        let Some(rec) = self.registry.get(agent_id) else {
            return;
        };
        let Some(tid) = rec.task_id.clone() else {
            return;
        };
        let Some(t) = self.graph.tasks.get(&tid) else {
            return;
        };
        if t.started || !t.is_open() {
            return;
        }
        if let Some(tt) = self.graph.tasks.get_mut(&tid) {
            tt.started = true;
            tt.status = TaskStatus::Running;
            tt.started_at = Some(self.clock.now());
        }
        let mut fields = JMap::new();
        fields.insert("owner".into(), JValue::Str(agent_id.into()));
        self.emit(
            EventType::TaskStarted,
            agent_id,
            "parent",
            &format!("{} started", tid.as_str()),
            fields,
            Some(tid.as_str()),
        );
        if self.registry.get(agent_id).map(|r| r.lifecycle.state) == Some(AgentState::Idle) {
            self.transition(
                agent_id,
                AgentState::Working,
                &format!("started {}", tid.as_str()),
            );
        }
    }

    pub fn eligible(&mut self) -> Vec<String> {
        let mut run: Vec<(i64, i64, String)> = Vec::new();
        let ids: Vec<String> = self
            .registry
            .agents
            .iter()
            .map(|a| a.agent_id.as_str().to_string())
            .collect();
        for aid in ids {
            if !self.actors.contains_key(&aid) {
                self.make_actor(&aid);
            }
            let runnable = {
                let Some(actor) = self.actors.get(&aid) else {
                    continue;
                };
                actor.runnable(self)
            };
            if !runnable {
                continue;
            }
            let steps = self.actors.get(&aid).map(|a| a.steps_run).unwrap_or(0);
            let woke = if self.bus.mailbox_len(&aid) > 0 { 0 } else { 1 };
            run.push((steps, woke, aid));
        }
        run.sort();
        let cap = self.registry.budget.max_concurrent_workers.max(1) as usize;
        run.into_iter().take(cap).map(|(_, _, a)| a).collect()
    }

    fn step_actor(&mut self, aid: &str) {
        let Some(mut actor) = self.actors.remove(aid) else {
            return;
        };
        actor.run_step(self);
        self.actors.insert(aid.to_string(), actor);
    }

    fn all_done(&self) -> bool {
        if self.graph.tasks.is_empty() {
            return false;
        }
        !self.graph.tasks.values().any(|t| t.is_open())
    }

    pub fn reap_idle(&mut self) {
        let now = self.clock.now();
        let overdue: Vec<String> = self
            .registry
            .idle_overdue(now)
            .into_iter()
            .map(|a| a.agent_id.as_str().to_string())
            .collect();
        for aid in overdue {
            self.terminate_agent(&aid, "idle ttl");
        }
    }

    pub fn detect_deadlock(&self) -> Vec<Vec<String>> {
        let edges = self.registry.wait_for_edges(&self.graph);
        DependencyGraph::find_cycles(&edges)
    }

    pub fn resolve_deadlock(&mut self) -> Vec<Vec<String>> {
        let cycles = self.detect_deadlock();
        for cyc in &cycles {
            let mut fields = JMap::new();
            fields.insert(
                "cycle".into(),
                JValue::Arr(cyc.iter().cloned().map(JValue::Str).collect()),
            );
            self.emit(
                EventType::DeadlockDetected,
                "kernel",
                "parent",
                &format!("deadlock {}", cyc.join(" -> ")),
                fields,
                None,
            );
            if cyc.len() >= 2 {
                let victim = &cyc[cyc.len() - 2];
                if let Some(tid) = self
                    .registry
                    .get(victim)
                    .and_then(|r| r.task_id.as_ref().map(|t| t.as_str().to_string()))
                {
                    self.requeue(&tid, "deadlock victim");
                }
            }
        }
        cycles
    }

    fn apply_arena_decisions(&mut self) {
        let path = match self.anchor(&self.decision_rel.clone()) {
            Some(p) => p,
            None => return,
        };
        if !path.exists() {
            return;
        }
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let consumed = true;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = parse(line) else {
                continue;
            };
            let kind = v.str_or("kind", "");
            match kind.as_str() {
                "force_state" => {
                    let aid = v.str_or("agent_id", "");
                    let st = v.str_or("state", "");
                    if let Some(to) = AgentState::parse(&st) {
                        self.transition(&aid, to, "cortex force_state");
                    }
                }
                "force_terminate" => {
                    let aid = v.str_or("agent_id", "");
                    self.terminate_agent(&aid, "cortex force_terminate");
                }
                // approve_spawn / amend: Parent (M9). Leave the file if we
                // cannot honour them? No — unknown kinds are recorded and
                // consumed so a stale file cannot loop. The request stays
                // on spawn_requests for Parent.
                _ => {}
            }
        }
        if consumed {
            let _ = std::fs::write(&path, "");
        }
    }

    fn write_inbox_row(&mut self, row: &JMap) {
        let Some(path) = self.anchor(&self.inbox_rel.clone()) else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            use std::io::Write;
            let _ = writeln!(f, "{}", JValue::Obj(row.clone()).to_canon_string());
        }
    }

    // --------------------------------------------------------------- run

    pub fn run(&mut self, ticks: i64) -> JMap {
        for _ in 0..ticks {
            self.tick += 1;
            self.clock.tick();
            self.sync_now();
            self.bus.reset_tick();
            self.apply_arena_decisions();
            let expired = self
                .bus
                .expire_timeouts(&mut self.journal, &mut self.registry, self.clock.now())
                .unwrap_or_default();
            for row in expired {
                let mut note = JMap::new();
                note.insert("agent".into(), JValue::Str(row.agent_id.as_str().into()));
                note.insert(
                    "task_id".into(),
                    row.task_id.clone().map(JValue::Str).unwrap_or(JValue::Null),
                );
                note.insert(
                    "reason".into(),
                    JValue::Str(format!("WAIT_TIMEOUT on {}", row.condition)),
                );
                note.insert("at".into(), JValue::Float(self.clock.now()));
                note.insert("kind".into(), JValue::Str("timeout".into()));
                self.parent_inbox.push(note.clone());
                self.write_inbox_row(&note);
            }
            if self.detect_deadlocks && self.deadlock_action != "off" {
                let found = if self.deadlock_action == "resolve" {
                    self.resolve_deadlock()
                } else {
                    self.detect_deadlock()
                };
                self.deadlocks.extend(found);
            }
            if self.auto_assign {
                self.schedule();
            }
            let eligible = self.eligible();
            for aid in eligible {
                self.begin_if_assigned(&aid);
                while self.bus.mailbox_len(&aid) > 0 {
                    self.step_actor(&aid);
                    let last = self
                        .actors
                        .get(&aid)
                        .map(|a| a.last_action.clone())
                        .unwrap_or_default();
                    if is_sleeping_action(&last) {
                        break;
                    }
                }
                if self
                    .registry
                    .get(&aid)
                    .map(|r| RUNNABLE.contains(&r.lifecycle.state))
                    .unwrap_or(false)
                {
                    self.step_actor(&aid);
                }
            }
            if self.reap {
                self.reap_idle();
            }
            if self.all_done() {
                break;
            }
        }
        self.checkpoint("run boundary");
        self.trace_flush();
        self.summary()
    }

    pub fn run_resident(&mut self, seconds: f64, idle: f64, ticks_per_loop: i64) -> JMap {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f64(seconds);
        let mut loops = 0i64;
        let mut last = self.tick;
        while std::time::Instant::now() < deadline {
            loops += 1;
            self.drain_injection_file(true);
            let out = self.run(ticks_per_loop);
            self.checkpoint(&format!("resident loop {loops}"));
            let done = out.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
            if done && self.spawn_requests.is_empty() {
                break;
            }
            if self.tick == last && self.idle_predicate() {
                std::thread::sleep(std::time::Duration::from_secs_f64(idle));
            }
            last = self.tick;
            if out
                .get("stalled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
                && self.spawn_requests.is_empty()
            {
                break;
            }
        }
        self.resident_loops = loops;
        self.summary()
    }

    // ---------------------------------------------------------- config / snap

    pub fn config(&self) -> JMap {
        let mut m = JMap::new();
        m.insert(
            "default_policy".into(),
            JValue::Str(self.default_policy.clone()),
        );
        m.insert(
            "role_policies".into(),
            JValue::Obj(
                self.role_policies
                    .iter()
                    .map(|(k, v)| (k.clone(), JValue::Str(v.clone())))
                    .collect(),
            ),
        );
        m.insert(
            "cognition_roles".into(),
            JValue::Arr(
                self.cognition_sources
                    .keys()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        m.insert(
            "role_overrides".into(),
            JValue::Obj(
                self.role_overrides
                    .iter()
                    .map(|(k, v)| (k.clone(), JValue::Obj(v.clone())))
                    .collect(),
            ),
        );
        m.insert("policy_args".into(), JValue::Obj(self.policy_args.clone()));
        m.insert(
            "budget".into(),
            JValue::Obj(budget_to_map(&self.registry.budget)),
        );
        m.insert(
            "agent_subscriptions".into(),
            JValue::Arr(
                self.agent_subscriptions
                    .iter()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        m.insert(
            "detect_deadlocks".into(),
            JValue::Bool(self.detect_deadlocks),
        );
        m.insert(
            "deadlock_action".into(),
            JValue::Str(self.deadlock_action.clone()),
        );
        m.insert("auto_assign".into(), JValue::Bool(self.auto_assign));
        m.insert("reap".into(), JValue::Bool(self.reap));
        m.insert("stall_limit".into(), JValue::Int(self.stall_limit));
        m.insert("work_unit".into(), JValue::Float(self.work_unit));
        m.insert("clock_mode".into(), JValue::Str(self.clock_mode.clone()));
        m.insert("clock_step".into(), JValue::Float(self.clock_step));
        m.insert("task_text".into(), JValue::Str(self.task_text.clone()));
        m
    }

    fn config_path(&self) -> Option<PathBuf> {
        self.anchor(CONFIG_FILE)
    }

    pub fn write_config(&self) {
        let Some(path) = self.config_path() else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(path, JValue::Obj(self.config()).to_pretty_string());
    }

    pub fn read_config(root: &Path) -> JMap {
        let p = root.join(CONFIG_FILE);
        match std::fs::read_to_string(p) {
            Ok(s) => match parse(&s) {
                Ok(JValue::Obj(m)) => m,
                _ => JMap::new(),
            },
            Err(_) => JMap::new(),
        }
    }

    pub fn apply_config(&mut self, cfg: &JMap) {
        if let Some(s) = cfg.get("default_policy").and_then(|v| v.as_str()) {
            if self.default_policy.is_empty() {
                self.default_policy = s.to_string();
            }
        }
        if let Some(s) = cfg.get("task_text").and_then(|v| v.as_str()) {
            if self.task_text.is_empty() {
                self.task_text = s.to_string();
            }
        }
        if let Some(JValue::Obj(rp)) = cfg.get("role_policies") {
            if self.role_policies == KernelOpts::default_role_policies() {
                self.role_policies = rp
                    .iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect();
            }
        }
        if let Some(b) = cfg.get("detect_deadlocks").and_then(|v| v.as_bool()) {
            self.detect_deadlocks = b;
        }
        if let Some(s) = cfg.get("deadlock_action").and_then(|v| v.as_str()) {
            self.deadlock_action = s.to_string();
        }
        if let Some(b) = cfg.get("auto_assign").and_then(|v| v.as_bool()) {
            self.auto_assign = b;
        }
        if let Some(b) = cfg.get("reap").and_then(|v| v.as_bool()) {
            self.reap = b;
        }
        if let Some(w) = cfg.get("work_unit").and_then(|v| v.as_f64()) {
            self.work_unit = w;
        }
        if let Some(JValue::Obj(ov)) = cfg.get("role_overrides") {
            if self.role_overrides.is_empty() {
                self.role_overrides = ov
                    .iter()
                    .filter_map(|(k, v)| match v {
                        JValue::Obj(m) => Some((k.clone(), m.clone())),
                        _ => None,
                    })
                    .collect();
            }
        }
        if let Some(JValue::Obj(pa)) = cfg.get("policy_args") {
            if self.policy_args.is_empty() {
                self.policy_args = pa.clone();
            }
        }
        if let Some(JValue::Arr(subs)) = cfg.get("agent_subscriptions") {
            if self.agent_subscriptions == ["resource.*"] {
                self.agent_subscriptions = subs
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
            }
        }
        if let Some(JValue::Obj(b)) = cfg.get("budget") {
            apply_budget(&mut self.registry.budget, b);
        }
    }

    pub fn checkpoint(&mut self, reason: &str) -> JMap {
        let snap = self.snapshot();
        self.write_config();
        let mut fields = JMap::new();
        fields.insert("snapshot".into(), JValue::Obj(snap.clone()));
        fields.insert("tick".into(), JValue::Int(self.tick));
        fields.insert("reason".into(), JValue::Str(reason.into()));
        self.emit(
            EventType::Snapshot,
            "parent",
            "parent",
            &format!("checkpoint @tick {} ({reason})", self.tick),
            fields,
            None,
        );
        snap
    }

    fn trace_flush(&self) {
        let Some(path) = self.anchor(&self.trace_rel) else {
            return;
        };
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            use std::io::Write;
            let mut m = JMap::new();
            m.insert("kernel_snapshot".into(), JValue::Obj(self.state_counts()));
            m.insert("tick".into(), JValue::Int(self.tick));
            m.insert("done".into(), JValue::Bool(self.all_done()));
            let _ = writeln!(f, "{}", JValue::Obj(m).to_canon_string());
        }
    }

    // ---------------------------------------------------------- metrics

    pub fn state_counts(&self) -> JMap {
        let mut out = JMap::new();
        for s in AgentState::ALL {
            out.insert(s.as_str().into(), JValue::Int(0));
        }
        for rec in &self.registry.agents {
            let k = rec.lifecycle.state.as_str().to_string();
            let n = out.get(&k).and_then(|v| v.as_int()).unwrap_or(0) + 1;
            out.insert(k, JValue::Int(n));
        }
        out
    }

    pub fn spawn_stats_view(&self) -> JMap {
        // Parent recounts from the ledger (M9). Until then, zeros plus
        // whatever the fold restored.
        let keys = [
            "received",
            "approved",
            "rejected",
            "deduplicated",
            "escalated",
            "deferred",
            "reused",
            "spawned_by_agents",
            "cycles_rejected",
        ];
        let mut m = JMap::new();
        for k in keys {
            m.insert(k.into(), JValue::Int(0));
        }
        m
    }

    pub fn metrics(&self) -> JMap {
        let by_state = self.state_counts();
        let st = self.spawn_stats_view();
        let active = self.registry.active().len() as i64;
        let mut blocked = 0i64;
        for rec in self.registry.active() {
            if let Some(tid) = &rec.task_id {
                if !self.graph.blocked_by(tid).is_empty() {
                    blocked += 1;
                }
            }
        }
        let waiting: i64 = self
            .registry
            .agents
            .iter()
            .map(|r| r.pending_waits.len() as i64)
            .sum();
        let epochs: Vec<i64> = self.registry.agents.iter().map(|r| r.epoch).collect();
        let mut depths = JMap::new();
        for rec in &self.registry.agents {
            let k = rec.epoch.to_string();
            let n = depths.get(&k).and_then(|v| v.as_int()).unwrap_or(0) + 1;
            depths.insert(k, JValue::Int(n));
        }
        let cap = self.registry.budget.max_active_agents;
        let mut m = JMap::new();
        m.insert(
            "agents_total".into(),
            JValue::Int(self.registry.agents.len() as i64),
        );
        m.insert("active".into(), JValue::Int(active));
        m.insert(
            "idle".into(),
            by_state.get("IDLE").cloned().unwrap_or(JValue::Int(0)),
        );
        m.insert(
            "working".into(),
            by_state.get("WORKING").cloned().unwrap_or(JValue::Int(0)),
        );
        m.insert("waiting".into(), JValue::Int(waiting));
        m.insert("blocked".into(), JValue::Int(blocked));
        m.insert(
            "escalated".into(),
            by_state.get("ESCALATED").cloned().unwrap_or(JValue::Int(0)),
        );
        m.insert(
            "completed".into(),
            by_state.get("COMPLETED").cloned().unwrap_or(JValue::Int(0)),
        );
        m.insert(
            "requests_received".into(),
            st.get("received").cloned().unwrap_or(JValue::Int(0)),
        );
        m.insert(
            "remaining_capacity".into(),
            JValue::Int((cap - active).max(0)),
        );
        m.insert("agent_budget".into(), JValue::Int(cap));
        m.insert(
            "workers_max".into(),
            JValue::Int(self.registry.budget.max_concurrent_workers),
        );
        m.insert(
            "open_graph_tasks".into(),
            JValue::Int(self.graph.tasks.values().filter(|t| t.is_open()).count() as i64),
        );
        m.insert(
            "generation_epoch_max".into(),
            JValue::Int(epochs.iter().copied().max().unwrap_or(0)),
        );
        m.insert("spawn_depth_hist".into(), JValue::Obj(depths));
        m.insert("stalled".into(), JValue::Bool(self.stalled));
        m.insert("tick".into(), JValue::Int(self.tick));
        m
    }

    pub fn monitoring(&self) -> JMap {
        let m = self.metrics();
        let keys = [
            "active",
            "idle",
            "working",
            "waiting",
            "blocked",
            "escalated",
            "completed",
            "remaining_capacity",
            "agent_budget",
            "workers_max",
            "open_graph_tasks",
        ];
        let mut out = JMap::new();
        for k in keys {
            if let Some(v) = m.get(k) {
                out.insert(k.into(), v.clone());
            }
        }
        out
    }

    pub fn idle_predicate(&self) -> bool {
        if !self.spawn_requests.is_empty() || !self.feature_requests.is_empty() {
            return false;
        }
        if self
            .registry
            .agents
            .iter()
            .any(|a| !a.pending_waits.is_empty())
        {
            return false;
        }
        if self.actors.keys().any(|a| self.bus.mailbox_len(a) > 0) {
            return false;
        }
        if self
            .graph
            .tasks
            .values()
            .any(|t| t.is_open() && self.graph.is_ready(&t.task_id) && t.owner.is_none())
        {
            return false;
        }
        // eligible needs &mut; approximate: any runnable agent with work
        for rec in &self.registry.agents {
            if rec.task_id.is_some() && RUNNABLE.contains(&rec.lifecycle.state) {
                return false;
            }
        }
        true
    }

    pub fn inject_spawn_request(&mut self, req: JMap, source: &str, journal: bool) -> Message {
        let from = req
            .get("from")
            .and_then(|v| v.as_str())
            .or_else(|| req.get("requester_agent_id").and_then(|v| v.as_str()))
            .unwrap_or(source);
        let mut payload = JMap::new();
        for (k, v) in &req {
            if !matches!(
                k.as_str(),
                "from" | "requester_agent_id" | "correlation_id" | "task_id"
            ) {
                payload.insert(k.clone(), v.clone());
            }
        }
        let mut msg = Message::new(EventType::SpawnAgentRequest, from, "parent");
        msg.body = req
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(t) = req.get("task_id").and_then(|v| v.as_str()) {
            msg.task_id = Some(TaskId::new(t));
        }
        if let Some(c) = req.get("correlation_id").and_then(|v| v.as_str()) {
            msg.correlation_id = crate::ids::CorrelationId::new(c);
        }
        msg.payload = payload.clone();
        if journal {
            let mut fields = payload;
            fields.insert("source".into(), JValue::Str(source.into()));
            fields.insert("injected".into(), JValue::Bool(true));
            let tid = msg.task_id.as_ref().map(|t| t.as_str().to_string());
            self.emit(
                EventType::SpawnAgentRequest,
                msg.from_actor.as_str(),
                "parent",
                &msg.body,
                fields,
                tid.as_deref(),
            );
        }
        self.spawn_requests.push(msg.clone());
        msg
    }

    pub fn drain_injection_file(&mut self, consume: bool) -> i64 {
        let Some(path) = self.anchor(&self.inject_rel.clone()) else {
            return 0;
        };
        if !path.exists() {
            return 0;
        }
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let mut n = 0i64;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = parse(line) else {
                continue;
            };
            let row = match v.get("spawn_request") {
                Some(JValue::Obj(m)) => m.clone(),
                _ => match v {
                    JValue::Obj(m) => m,
                    _ => continue,
                },
            };
            self.inject_spawn_request(row, "external", true);
            n += 1;
        }
        self.injected_rows += n;
        if consume && n > 0 {
            let _ = std::fs::write(&path, "");
        }
        n
    }

    // ---------------------------------------------------------- reports

    pub fn summary(&self) -> JMap {
        let open: Vec<JValue> = self
            .graph
            .tasks
            .iter()
            .filter(|(_, t)| t.is_open())
            .map(|(id, _)| JValue::Str(id.as_str().into()))
            .collect();
        let chain_ok = self.journal.verify_chain().map(|t| t.0).unwrap_or(false);
        let mut m = JMap::new();
        m.insert("tick".into(), JValue::Int(self.tick));
        m.insert(
            "done".into(),
            JValue::Bool(open.is_empty() && !self.graph.tasks.is_empty()),
        );
        m.insert("stalled".into(), JValue::Bool(self.stalled));
        m.insert("open_tasks".into(), JValue::Arr(open));
        m.insert(
            "agents".into(),
            JValue::Int(self.registry.agents.len() as i64),
        );
        m.insert(
            "events".into(),
            JValue::Int(self.journal.count().unwrap_or(0)),
        );
        m.insert("artifacts".into(), JValue::Int(self.artifacts.len() as i64));
        m.insert("polls".into(), JValue::Int(self.bus.polls.polls));
        m.insert(
            "escalations".into(),
            JValue::Int(self.parent_inbox.len() as i64),
        );
        m.insert("deadlocks".into(), JValue::Int(self.deadlocks.len() as i64));
        m.insert("resident_loops".into(), JValue::Int(self.resident_loops));
        m.insert("injected".into(), JValue::Int(self.injected_rows));
        m.insert("chain_ok".into(), JValue::Bool(chain_ok));
        m
    }

    pub fn status(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("agents".into(), JValue::Obj(self.registry.snapshot()));
        m.insert("tasks".into(), JValue::Obj(self.graph.snapshot()));
        m.insert("counts".into(), JValue::Obj(self.state_counts()));
        m.insert("bus".into(), JValue::Obj(self.bus.snapshot()));
        m.insert("polls".into(), JValue::Int(self.bus.polls.polls));
        m.insert(
            "critical_path".into(),
            JValue::Int(self.graph.critical_path_length() as i64),
        );
        m.insert(
            "events".into(),
            JValue::Int(self.journal.count().unwrap_or(0)),
        );
        m
    }

    pub fn report(&self) -> String {
        let s = self.summary();
        let st = self.status();
        let mut lines = vec![
            "KERNEL".into(),
            String::new(),
            format!(
                "tick={} done={} events={} polls={} deadlocks={} chain_ok={}",
                s.get("tick").and_then(|v| v.as_int()).unwrap_or(0),
                s.get("done").and_then(|v| v.as_bool()).unwrap_or(false),
                s.get("events").and_then(|v| v.as_int()).unwrap_or(0),
                s.get("polls").and_then(|v| v.as_int()).unwrap_or(0),
                s.get("deadlocks").and_then(|v| v.as_int()).unwrap_or(0),
                s.get("chain_ok").and_then(|v| v.as_bool()).unwrap_or(false)
            ),
            format!(
                "counts: {}",
                JValue::Obj(self.state_counts()).to_canon_string()
            ),
        ];
        let _ = st;
        lines.push(format!(
            "artifacts: {}",
            self.artifacts
                .iter()
                .map(|(a, m)| format!("{a}=v{}", m.version))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        for (tid, t) in &self.graph.tasks {
            lines.push(format!(
                "  {:<20} {:<8} owner={:<14} deps={:?}",
                tid.as_str(),
                t.status.as_str(),
                t.owner
                    .as_ref()
                    .map(|o| o.as_str().to_string())
                    .unwrap_or_else(|| "None".into()),
                t.deps.iter().map(|d| d.as_str()).collect::<Vec<_>>()
            ));
        }
        lines.join("\n")
    }

    pub fn why(&self, agent_id: &str) -> JMap {
        let Some(rec) = self.registry.get(agent_id) else {
            let mut m = JMap::new();
            m.insert(
                "error".into(),
                JValue::Str(format!("unknown agent {agent_id}")),
            );
            return m;
        };
        let t = rec.task_id.as_ref().and_then(|id| self.graph.tasks.get(id));
        let mut m = JMap::new();
        m.insert("agent_id".into(), JValue::Str(agent_id.into()));
        m.insert("state".into(), JValue::Str(rec.state_str().into()));
        m.insert(
            "task".into(),
            rec.task_id
                .as_ref()
                .map(|t| JValue::Str(t.as_str().into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "task_status".into(),
            t.map(|x| JValue::Str(x.status.as_str().into()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "unmet_deps".into(),
            JValue::Arr(
                t.map(|x| {
                    self.graph
                        .unmet(&x.task_id)
                        .into_iter()
                        .map(JValue::Str)
                        .collect()
                })
                .unwrap_or_default(),
            ),
        );
        m.insert(
            "missing_artifacts".into(),
            JValue::Arr(
                t.map(|x| {
                    x.consumes
                        .iter()
                        .filter(|a| !self.artifact_exists(a))
                        .cloned()
                        .map(JValue::Str)
                        .collect()
                })
                .unwrap_or_default(),
            ),
        );
        m.insert(
            "durable_waits".into(),
            JValue::Arr(
                rec.pending_waits
                    .iter()
                    .map(|w| w.get("condition").cloned().unwrap_or(JValue::Null))
                    .collect(),
            ),
        );
        m.insert(
            "steps".into(),
            JValue::Int(self.actors.get(agent_id).map(|a| a.steps_run).unwrap_or(0)),
        );
        let recent = self
            .journal
            .events(&EventFilter::new().actor(agent_id).limit(6))
            .unwrap_or_default();
        m.insert(
            "recent".into(),
            JValue::Arr(recent.iter().map(|r| JValue::Obj(r.to_dict())).collect()),
        );
        m
    }

    pub fn trace(&self, correlation_id: &str) -> Vec<JMap> {
        self.journal.trace(correlation_id).unwrap_or_default()
    }

    pub fn snapshot(&self) -> JMap {
        let mut agents = JMap::new();
        let mut rows: Vec<&AgentRecord> = self.registry.agents.iter().collect();
        rows.sort_by(|a, b| a.agent_id.as_str().cmp(b.agent_id.as_str()));
        for a in rows {
            let aid = a.agent_id.as_str();
            let mut m = JMap::new();
            m.insert("role".into(), JValue::Str(a.role.clone()));
            m.insert("state".into(), JValue::Str(a.state_str().into()));
            m.insert(
                "task_id".into(),
                a.task_id
                    .as_ref()
                    .map(|t| JValue::Str(t.as_str().into()))
                    .unwrap_or(JValue::Null),
            );
            m.insert("epoch".into(), JValue::Int(a.epoch));
            let mut skills: Vec<String> = a.skills.iter().cloned().collect();
            skills.sort();
            m.insert(
                "skills".into(),
                JValue::Arr(skills.into_iter().map(JValue::Str).collect()),
            );
            m.insert("msgs_sent".into(), JValue::Int(a.msgs_sent));
            m.insert(
                "task_queue".into(),
                JValue::Arr(
                    a.task_queue
                        .iter()
                        .map(|t| JValue::Str(t.as_str().into()))
                        .collect(),
                ),
            );
            m.insert("work_done".into(), JValue::Float(py_round(a.work_done, 6)));
            m.insert(
                "steps_run".into(),
                JValue::Int(self.actors.get(aid).map(|x| x.steps_run).unwrap_or(0)),
            );
            m.insert(
                "policy_cursor".into(),
                JValue::Int(self.actors.get(aid).map(|x| x.policy_cursor()).unwrap_or(0)),
            );
            let mut waits: Vec<String> = a
                .pending_waits
                .iter()
                .filter_map(|w| {
                    w.get("condition")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            waits.sort();
            m.insert(
                "waits".into(),
                JValue::Arr(waits.into_iter().map(JValue::Str).collect()),
            );
            agents.insert(aid.to_string(), JValue::Obj(m));
        }
        let mut tasks = JMap::new();
        for (tid, t) in &self.graph.tasks {
            let mut m = JMap::new();
            m.insert("status".into(), JValue::Str(t.status.as_str().into()));
            m.insert(
                "owner".into(),
                t.owner
                    .as_ref()
                    .map(|o| JValue::Str(o.as_str().into()))
                    .unwrap_or(JValue::Null),
            );
            m.insert(
                "deps".into(),
                JValue::Arr(
                    t.deps
                        .iter()
                        .map(|d| JValue::Str(d.as_str().into()))
                        .collect(),
                ),
            );
            m.insert(
                "consumes".into(),
                JValue::Arr(t.consumes.iter().cloned().map(JValue::Str).collect()),
            );
            m.insert(
                "produces".into(),
                JValue::Arr(t.produces.iter().cloned().map(JValue::Str).collect()),
            );
            m.insert("est_work".into(), JValue::Float(t.est_work));
            m.insert(
                "claims".into(),
                JValue::Arr(t.claims.iter().cloned().map(JValue::Str).collect()),
            );
            tasks.insert(tid.as_str().to_string(), JValue::Obj(m));
        }
        let mut arts = JMap::new();
        for (a, meta) in &self.artifacts {
            arts.insert(a.clone(), JValue::Int(meta.version));
        }
        let mut claims = JMap::new();
        if let Ok(cs) = self.journal.claims() {
            for (k, v) in cs {
                let owner = v.get("owner").cloned().unwrap_or(JValue::Null);
                claims.insert(k.as_str().to_string(), owner);
            }
        }
        let mut out = JMap::new();
        out.insert("agents".into(), JValue::Obj(agents));
        out.insert("tasks".into(), JValue::Obj(tasks));
        out.insert("artifacts".into(), JValue::Obj(arts));
        out.insert("claims".into(), JValue::Obj(claims));
        out.insert("tick".into(), JValue::Int(self.tick));
        out.insert(
            "events".into(),
            JValue::Int(self.journal.count().unwrap_or(0)),
        );
        out
    }

    pub fn replay(&self) -> JMap {
        self.journal.fold().unwrap_or_default()
    }

    // ---------------------------------------------------------- recovery

    pub fn from_journal(
        path: &Path,
        quiet: bool,
        mut opts: KernelOpts,
    ) -> Result<Kernel, JournalError> {
        opts.journal_path = Some(path.to_path_buf());
        let mut k = Kernel::new(opts)?;
        let cfg_root = k.root.clone().or_else(|| k.side_anchor.clone());
        if let Some(root) = cfg_root {
            let cfg = Kernel::read_config(&root);
            if !cfg.is_empty() {
                k.apply_config(&cfg);
            }
        }
        let fold = k.journal.fold()?;
        let tick_ts = fold
            .get("meta")
            .and_then(|m| m.get("tick_ts"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        k.clock.advance(tick_ts);
        k.sync_now();

        if let Some(JValue::Obj(agents)) = fold.get("agents") {
            for (aid, spec) in agents {
                if k.registry.get(aid).is_some() {
                    continue;
                }
                let role = spec.str_or("role", "?");
                let skills: Vec<String> = match spec.get("skills") {
                    Some(JValue::Arr(v)) => v
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect(),
                    _ => vec![],
                };
                let skill_refs: Vec<&str> = skills.iter().map(|s| s.as_str()).collect();
                let epoch = spec.get("epoch").and_then(|v| v.as_int()).unwrap_or(0);
                let spawned = spec.str_or("spawned_by", "parent");
                let st =
                    AgentState::parse(&spec.str_or("state", "IDLE")).unwrap_or(AgentState::Idle);
                let since = spec
                    .get("state_since")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let _ = k.registry.register(
                    aid,
                    &role,
                    &skill_refs,
                    epoch,
                    &spawned,
                    "",
                    Some(Lifecycle::with_state(st, since)),
                    &[],
                );
                k.bind_actor(aid);
                if let Some(rec) = k.registry.get_mut(aid) {
                    rec.task_id = spec
                        .get("task_id")
                        .and_then(|v| v.as_str())
                        .map(TaskId::new);
                    rec.task_queue = match spec.get("task_queue") {
                        Some(JValue::Arr(v)) => v
                            .iter()
                            .filter_map(|x| x.as_str().map(TaskId::new))
                            .collect(),
                        _ => vec![],
                    };
                    rec.msgs_sent = spec.get("msgs_sent").and_then(|v| v.as_int()).unwrap_or(0);
                    rec.work_done = spec
                        .get("work_done")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    rec.lifecycle.state = st;
                    rec.lifecycle.since = since;
                }
            }
        }

        if let Some(JValue::Obj(tasks)) = fold.get("tasks") {
            for (tid, spec) in tasks {
                if k.graph.tasks.contains_key(&TaskId::new(tid)) {
                    continue;
                }
                let mut t = TaskSpec::new(
                    tid.as_str(),
                    &spec.str_or("title", ""),
                    &spec.str_or("role", ""),
                );
                t.skills = match spec.get("skills") {
                    Some(JValue::Arr(v)) => v
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect(),
                    _ => vec![],
                };
                t.est_work = spec.get("est_work").and_then(|v| v.as_f64()).unwrap_or(1.0);
                t.produces = jstr_list(spec.get("produces"));
                t.consumes = jstr_list(spec.get("consumes"));
                t.claims = jstr_list(spec.get("claims"));
                let _ = k.graph.add(t, false);
                if let Some(tt) = k.graph.tasks.get_mut(&TaskId::new(tid)) {
                    for art in tt.produces.clone() {
                        k.graph
                            .producers
                            .entry(art)
                            .or_default()
                            .insert(TaskId::new(tid));
                    }
                    tt.status = TaskStatus::parse(&spec.str_or("status", "pending"));
                    tt.owner = spec.get("owner").and_then(|v| v.as_str()).map(AgentId::new);
                    for d in jstr_list(spec.get("deps")) {
                        tt.deps.insert(TaskId::new(d));
                    }
                    tt.verified = spec
                        .get("verified")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    tt.started = spec
                        .get("started")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                        || matches!(tt.status, TaskStatus::Running | TaskStatus::Done);
                }
            }
        }
        let _ = k.graph.derive_edges();

        if let Some(JValue::Obj(arts)) = fold.get("artifacts") {
            for (art, ver) in arts {
                let version = ver.as_int().unwrap_or(1);
                k.artifacts.insert(
                    art.clone(),
                    ArtifactMeta {
                        artifact: art.clone(),
                        version,
                        producer: "replay".into(),
                        task_id: None,
                        at: 0.0,
                        digest: String::new(),
                        files: Vec::new(),
                    },
                );
                k.graph.known_artifacts.insert(art.clone());
            }
        }

        if let Some(JValue::Obj(reqs)) = fold.get("requests") {
            k.request_fold = reqs.clone();
        }

        if let Some(JValue::Obj(waits)) = fold.get("waits") {
            for w in waits.values() {
                let aid = w.str_or("agent_id", "");
                if let Some(rec) = k.registry.get_mut(&aid) {
                    if let JValue::Obj(m) = w {
                        rec.pending_waits.push(m.clone());
                    }
                }
            }
        }

        if let Some(JValue::Obj(agents)) = fold.get("agents") {
            for (aid, spec) in agents {
                if spec.str_or("state", "") == "WAITING_FOR_DEPENDENCY" {
                    if let Some(rec) = k.registry.get_mut(aid) {
                        rec.lifecycle.state = AgentState::WaitingForDependency;
                    }
                }
            }
        }

        let snap = k.journal.latest_snapshot().ok().flatten();
        if let Some(JValue::Obj(s)) = &snap {
            if let Some(t) = s.get("tick").and_then(|v| v.as_int()) {
                k.tick = t;
            }
        }
        let src = match &snap {
            Some(JValue::Obj(s)) => s.get("agents"),
            _ => None,
        };
        let src = src.or_else(|| fold.get("agents"));
        if let Some(JValue::Obj(agents)) = src {
            for (aid, a) in agents {
                if let Some(actor) = k.actors.get_mut(aid) {
                    let steps = a.get("steps_run").and_then(|v| v.as_int()).unwrap_or(0);
                    let cursor = a.get("policy_cursor").and_then(|v| v.as_int()).unwrap_or(0);
                    actor.restore_cursor(steps, cursor);
                }
                if let Some(rec) = k.registry.get_mut(aid) {
                    if let Some(w) = a.get("work_done").and_then(|v| v.as_f64()) {
                        rec.work_done = w;
                    }
                    if let Some(n) = a.get("msgs_sent").and_then(|v| v.as_int()) {
                        rec.msgs_sent = n;
                    }
                }
            }
        }

        if !quiet {
            let mut fields = JMap::new();
            fields.insert("agents".into(), JValue::Int(k.registry.agents.len() as i64));
            fields.insert("tasks".into(), JValue::Int(k.graph.tasks.len() as i64));
            let n = k.journal.count().unwrap_or(0);
            k.emit(
                EventType::ReplayComplete,
                "parent",
                "parent",
                &format!(
                    "rebuilt {} agents / {} tasks from {n} journal rows",
                    k.registry.agents.len(),
                    k.graph.tasks.len()
                ),
                fields,
                None,
            );
        }
        Ok(k)
    }
}

fn jstr_list(v: Option<&JValue>) -> Vec<String> {
    match v {
        Some(JValue::Arr(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect(),
        _ => vec![],
    }
}

fn budget_to_map(b: &SpawnBudget) -> JMap {
    let mut m = JMap::new();
    m.insert("max_active_agents".into(), JValue::Int(b.max_active_agents));
    m.insert(
        "max_concurrent_workers".into(),
        JValue::Int(b.max_concurrent_workers),
    );
    m.insert("max_spawn_epoch".into(), JValue::Int(b.max_spawn_epoch));
    m.insert(
        "min_share_of_remaining".into(),
        JValue::Float(b.min_share_of_remaining),
    );
    m.insert(
        "requester_overload_factor".into(),
        JValue::Float(b.requester_overload_factor),
    );
    m.insert("idle_ttl".into(), JValue::Float(b.idle_ttl));
    m
}

fn apply_budget(b: &mut SpawnBudget, m: &JMap) {
    if let Some(v) = m.get("max_active_agents").and_then(|v| v.as_int()) {
        b.max_active_agents = v;
    }
    if let Some(v) = m.get("max_concurrent_workers").and_then(|v| v.as_int()) {
        b.max_concurrent_workers = v;
    }
    if let Some(v) = m.get("max_spawn_epoch").and_then(|v| v.as_int()) {
        b.max_spawn_epoch = v;
    }
    if let Some(v) = m.get("min_share_of_remaining").and_then(|v| v.as_f64()) {
        b.min_share_of_remaining = v;
    }
    if let Some(v) = m.get("requester_overload_factor").and_then(|v| v.as_f64()) {
        b.requester_overload_factor = v;
    }
    if let Some(v) = m.get("idle_ttl").and_then(|v| v.as_f64()) {
        b.idle_ttl = v;
    }
}

fn compute_side_anchor(root: Option<&Path>, journal_path: JournalPath) -> Option<PathBuf> {
    let root_s = root
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    let journal_dir = match &journal_path {
        JournalPath::File(p) => p.parent().map(|d| d.to_path_buf()),
        JournalPath::Memory => None,
    };
    if !root_s.is_empty() && root_s != "." && root_s != "./" {
        return Some(PathBuf::from(root_s));
    }
    if let Some(jd) = journal_dir {
        let s = jd.to_string_lossy();
        if !s.is_empty() && s != "." {
            return Some(jd);
        }
    }
    None
}

fn current_open(k: &Kernel, id: &str) -> Option<TaskSpec> {
    let rec = k.registry.get(id)?;
    for tid in rec.pending_work() {
        if let Some(t) = k.graph.tasks.get(&tid) {
            if t.is_open() {
                return Some(t.clone());
            }
        }
    }
    None
}

impl ActorRuntime for Kernel {
    fn now(&self) -> f64 {
        self.clock.now()
    }
    fn tick(&self) -> i64 {
        self.tick
    }
    fn work_unit(&self) -> f64 {
        self.work_unit
    }
    fn task_text(&self) -> &str {
        &self.task_text
    }
    fn max_workers(&self) -> i64 {
        self.registry.budget.max_concurrent_workers
    }
    fn agent_state(&self, id: &str) -> Option<AgentState> {
        self.registry.get(id).map(|r| r.lifecycle.state)
    }
    fn agent_role(&self, id: &str) -> Option<String> {
        self.registry.get(id).map(|r| r.role.clone())
    }
    fn agent_notes_tail(&self, id: &str, n: usize) -> Vec<String> {
        self.registry
            .get(id)
            .map(|r| r.notes.iter().rev().take(n).rev().cloned().collect())
            .unwrap_or_default()
    }
    fn pending_work(&self, id: &str) -> Vec<TaskId> {
        self.registry
            .get(id)
            .map(AgentRecord::pending_work)
            .unwrap_or_default()
    }
    fn pending_waits(&self, id: &str) -> Vec<JMap> {
        self.registry
            .get(id)
            .map(|r| r.pending_waits.clone())
            .unwrap_or_default()
    }
    fn current_task(&self, id: &str) -> Option<TaskSpec> {
        current_open(self, id)
    }
    fn task_by_id(&self, id: &str) -> Option<TaskSpec> {
        self.graph.tasks.get(&TaskId::new(id)).cloned()
    }
    fn graph_view(&self) -> Vec<JMap> {
        self.graph
            .tasks
            .values()
            .map(|t| {
                let mut m = JMap::new();
                m.insert("task_id".into(), JValue::Str(t.task_id.as_str().into()));
                m.insert("title".into(), JValue::Str(t.title.clone()));
                m.insert("role".into(), JValue::Str(t.role.clone()));
                m.insert("status".into(), JValue::Str(t.status.as_str().into()));
                m.insert(
                    "owner".into(),
                    t.owner
                        .as_ref()
                        .map(|o| JValue::Str(o.as_str().into()))
                        .unwrap_or(JValue::Null),
                );
                m.insert(
                    "produces".into(),
                    JValue::Arr(t.produces.iter().cloned().map(JValue::Str).collect()),
                );
                m
            })
            .collect()
    }
    fn artifact_exists(&self, artifact: &str) -> bool {
        self.artifacts.contains_key(artifact)
    }
    fn unmet_artifacts(&self, id: &str) -> Vec<String> {
        match current_open(self, id) {
            Some(t) => t
                .consumes
                .into_iter()
                .filter(|a| !self.artifacts.contains_key(a))
                .collect(),
            None => vec![],
        }
    }
    fn unmet_upstream(&self, id: &str) -> Vec<String> {
        let Some(t) = current_open(self, id) else {
            return vec![];
        };
        let mut out = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        if let Some(spec) = self.graph.tasks.get(&t.task_id) {
            for dep in &spec.deps {
                if let Some(up) = self.graph.tasks.get(dep) {
                    for a in &up.produces {
                        if !self.artifacts.contains_key(a) && seen.insert(a.clone()) {
                            out.push(a.clone());
                        }
                    }
                }
            }
        }
        out.sort();
        out
    }
    fn consumed_ready(&self, id: &str) -> Vec<String> {
        match current_open(self, id) {
            Some(t) => t
                .consumes
                .into_iter()
                .filter(|a| self.artifacts.contains_key(a))
                .collect(),
            None => vec![],
        }
    }
    fn roster_roles(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .registry
            .active()
            .iter()
            .map(|a| a.role.clone())
            .collect();
        v.sort();
        v.dedup();
        v
    }
    fn producer_roles(&self, artifact: &str) -> Vec<String> {
        self.producer_index()
            .get(artifact)
            .cloned()
            .unwrap_or_default()
    }
    fn producer_index(&self) -> BTreeMap<String, Vec<String>> {
        let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for t in self.graph.tasks.values() {
            for a in &t.produces {
                m.entry(a.clone()).or_default().push(t.role.clone());
            }
        }
        m
    }
    fn workspace_view(&self, id: &str) -> JMap {
        if let Some(ex) = &self.tools {
            return ex.describe(id);
        }
        self.workspaces.get(id).cloned().unwrap_or_else(|| {
            let mut m = JMap::new();
            m.insert("bound".into(), JValue::Bool(false));
            m
        })
    }
    fn allowed_tools(&self, id: &str) -> Vec<String> {
        self.tools
            .as_ref()
            .map(|t| t.allowed(id))
            .unwrap_or_default()
    }
    fn tool_schemas(&self, id: &str) -> Vec<JMap> {
        self.tools
            .as_ref()
            .map(|t| t.schemas(id))
            .unwrap_or_default()
    }
    fn tool_stats(&self) -> JMap {
        self.tools.as_ref().map(|t| t.stats()).unwrap_or_default()
    }
    fn tools_bound(&self) -> bool {
        self.tools.is_some()
    }
    fn cognition_record(&self, id: &str) -> JMap {
        self.registry
            .get(id)
            .map(|r| r.cognition.clone())
            .unwrap_or_default()
    }
    fn transcript_dir(&self) -> Option<String> {
        self.transcript_dir.clone()
    }
    fn pop_inbox(&mut self, id: &str) -> Option<Message> {
        self.bus.drain(id, Some(1)).into_iter().next()
    }
    fn peek_inbox(&self, id: &str) -> Vec<Message> {
        self.bus.mailbox(id)
    }
    fn has_mail(&self, id: &str) -> bool {
        self.bus.mailbox_len(id) > 0
    }
    fn transition(&mut self, id: &str, to: AgentState, reason: &str) -> bool {
        Kernel::transition(self, id, to, reason)
    }
    fn journal_emit(
        &mut self,
        etype: EventType,
        actor: &str,
        target: &str,
        body: &str,
        fields: JMap,
        task_id: Option<&str>,
    ) {
        self.emit(etype, actor, target, body, fields, task_id);
    }
    fn publish(&mut self, msg: Message) {
        Kernel::publish(self, msg);
    }
    fn wait_for(
        &mut self,
        actor: &str,
        condition: &str,
        task_id: Option<&str>,
        correlation_id: &str,
        timeout: Option<f64>,
    ) {
        self.sync_now();
        let _ = self.bus.wait_for(
            &mut self.journal,
            &mut self.registry,
            self.clock.now(),
            actor,
            condition,
            task_id,
            correlation_id,
            timeout,
        );
    }
    fn publish_artifact(&mut self, artifact: &str, producer: &str, task_id: Option<&str>) {
        Kernel::publish_artifact(self, artifact, producer, task_id);
    }
    fn append_note(&mut self, id: &str, text: &str) {
        if let Some(rec) = self.registry.get_mut(id) {
            rec.notes.push(text.to_string());
        }
    }
    fn bump_msgs_sent(&mut self, id: &str) {
        if let Some(rec) = self.registry.get_mut(id) {
            rec.msgs_sent += 1;
        }
    }
    fn bump_work_done(&mut self, id: &str, amount: f64) {
        if let Some(rec) = self.registry.get_mut(id) {
            rec.work_done += amount;
            self.registry.version += 1;
        }
    }
    fn set_task_status_waiting(&mut self, tid: &str) {
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(tid)) {
            t.status = TaskStatus::Waiting;
        }
    }
    fn mark_task_done(&mut self, tid: &str, finished_at: f64) {
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(tid)) {
            t.status = TaskStatus::Done;
            t.finished_at = Some(finished_at);
        }
    }
    fn mark_task_verified(&mut self, tid: &str) {
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(tid)) {
            t.verified = true;
            t.verified_at = Some(self.clock.now());
        }
    }
    fn advance_backlog(&mut self, id: &str, finished_task: &str) {
        if let Some(rec) = self.registry.get_mut(id) {
            rec.task_queue.retain(|t| t.as_str() != finished_task);
            rec.task_id = rec.task_queue.first().cloned();
            if rec.task_id.is_some() {
                rec.task_queue.remove(0);
            }
            self.registry.version += 1;
        }
    }
    fn parent_escalate(&mut self, note: JMap) {
        self.parent_inbox.push(note.clone());
        self.write_inbox_row(&note);
    }
    fn parent_spawn_request(&mut self, req: JMap) {
        self.inject_spawn_request(req, "agent", true);
    }
    fn completion_gate(&self, id: &str) -> CompletionGate {
        self.completion_gate_of(id)
    }
    fn run_verify(&mut self, id: &str, task_id: Option<&str>) -> VerifyVerdict {
        Kernel::run_verify(self, id, task_id)
    }
    fn plan_tools(
        &mut self,
        id: &str,
        calls: &[JMap],
        task_id: Option<&str>,
        correlation_id: &str,
    ) -> Vec<String> {
        match self.tools.as_mut() {
            Some(ex) => ex.plan(id, calls, task_id, correlation_id),
            None => (0..calls.len()).map(|i| format!("r-{i:04}")).collect(),
        }
    }
    fn execute_tool(
        &mut self,
        id: &str,
        tool: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
        correlation_id: &str,
    ) -> ToolResultView {
        match self.tools.as_mut() {
            Some(ex) => ex.execute(id, tool, args, rid, task_id, correlation_id),
            None => ToolResultView {
                tool: tool.into(),
                ok: false,
                refused: "REFUSE_NO_EXECUTOR".into(),
                ..Default::default()
            },
        }
    }
    fn poll_hit(&mut self, id: &str) -> Result<i64, crate::bus::BusError> {
        self.bus.polls.hit(id)
    }
    fn bus_resolve(&mut self, condition: &str, reason: &str) {
        let _ = self.bus.resolve(
            &mut self.journal,
            &mut self.registry,
            condition,
            reason,
            None,
        );
    }
}

#[cfg(test)]
mod tests;
