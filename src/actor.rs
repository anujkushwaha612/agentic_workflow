//! The agent actor: one logical worker with a mailbox, a cognition source,
//! and no authority to mutate the kernel (`arena/actor.py`).
//!
//! ```text
//!     receive/observe
//!           ↓
//!     cognition.decide  (proposal only)
//!           ↓
//!     validate_intent   (runtime)
//!           ↓
//!     execute tools     (runtime)
//!           ↓
//!     apply control     (runtime: lifecycle / bus / journal)
//!           ↓
//!     observation update
//! ```
//!
//! Python's `AgentActor` held a live `kernel` pointer and dual-pathed
//! policy vs cognition. That is a prototype artifact. Here:
//!
//! * the actor owns **per-agent** state only (cursor, transcript, errors,
//!   cognition instance);
//! * every mutation goes through [`ActorRuntime`], which Kernel (M8)
//!   implements — cognition cannot name the kernel;
//! * every agent has a [`crate::cognition::Cognition`] source. A policy-only
//!   agent is just [`crate::cognition::PolicyCognition`]. There is no second
//!   execution loop.
//!
//! Logical agents are not OS processes. Many actors share one Kernel's
//! bounded worker slots.

use crate::cognition::{
    outcomes_from_results, validate_intent, Cognition, CognitionError, Control, Intent,
    Observation, PolicyCognition, ToolCall, ToolResultView,
};
use crate::graph::TaskSpec;
use crate::ids::TaskId;
use crate::lifecycle::AgentState;
use crate::msg::{EventType, Message};
use crate::policy::{is_sleeping_action, Act, Action, Policy, PolicyContext};
use crate::sys::json::{JMap, JValue};

// ----------------------------------------------------------------- constants

/// States in which an actor may consume a worker slot this tick (it must
/// also hold open work). Kernel scheduling uses the same set.
pub const RUNNABLE: [AgentState; 4] = [
    AgentState::Idle,
    AgentState::Working,
    AgentState::Initializing,
    AgentState::Completed,
];

// ------------------------------------------------------------- runtime types

/// Result of [`ActorRuntime::completion_gate`]. `allow = false` means the
/// actor asked to COMPLETE a task that still has unmet verify commands.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionGate {
    pub allow: bool,
    pub verified: bool,
    pub rule: String,
    pub detail: String,
    pub task_id: Option<String>,
}

impl Default for CompletionGate {
    fn default() -> Self {
        CompletionGate {
            allow: true,
            verified: false,
            rule: String::new(),
            detail: String::new(),
            task_id: None,
        }
    }
}

/// Result of [`ActorRuntime::run_verify`]. Only the runtime may set
/// `verified = true` on a task; this is the evidence it used.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VerifyVerdict {
    pub ok: bool,
    pub results: Vec<JMap>,
    pub task_id: Option<String>,
}

/// One actor turn, as an audit record (Python `run_step` return).
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    pub agent: String,
    pub action: String,
    pub detail: String,
    pub msg_in: Option<String>,
    pub calls: Vec<JMap>,
    pub log: Vec<String>,
}

impl Turn {
    fn noop(agent: &str, msg_in: Option<String>) -> Turn {
        Turn {
            agent: agent.to_string(),
            action: "noop".into(),
            detail: String::new(),
            msg_in,
            calls: Vec::new(),
            log: Vec::new(),
        }
    }
}

/// The entire surface an actor may use to read and (through sanctioned
/// methods) mutate runtime state.
///
/// Kernel is the only production implementor. Tests use a harness. Cognition
/// never sees this trait — it sees [`PolicyContext`] / [`Observation`].
pub trait ActorRuntime {
    fn now(&self) -> f64;
    fn tick(&self) -> i64;
    fn work_unit(&self) -> f64;
    fn task_text(&self) -> &str;
    fn max_workers(&self) -> i64;

    fn agent_state(&self, id: &str) -> Option<AgentState>;
    fn agent_role(&self, id: &str) -> Option<String>;
    fn agent_notes_tail(&self, id: &str, n: usize) -> Vec<String>;
    fn pending_work(&self, id: &str) -> Vec<TaskId>;
    fn pending_waits(&self, id: &str) -> Vec<JMap>;
    fn current_task(&self, id: &str) -> Option<TaskSpec>;
    fn task_by_id(&self, id: &str) -> Option<TaskSpec>;
    fn graph_view(&self) -> Vec<JMap>;
    fn artifact_exists(&self, artifact: &str) -> bool;
    fn unmet_artifacts(&self, id: &str) -> Vec<String>;
    fn unmet_upstream(&self, id: &str) -> Vec<String>;
    fn consumed_ready(&self, id: &str) -> Vec<String>;
    fn roster_roles(&self) -> Vec<String>;
    fn producer_roles(&self, artifact: &str) -> Vec<String>;
    /// artifact → producer roles (so PolicyContext can answer without a live borrow).
    fn producer_index(&self) -> std::collections::BTreeMap<String, Vec<String>>;
    fn workspace_view(&self, id: &str) -> JMap;
    fn allowed_tools(&self, id: &str) -> Vec<String>;
    fn tool_schemas(&self, id: &str) -> Vec<JMap>;
    fn tool_stats(&self) -> JMap;
    fn tools_bound(&self) -> bool;
    fn cognition_record(&self, id: &str) -> JMap;
    fn transcript_dir(&self) -> Option<String>;

    fn pop_inbox(&mut self, id: &str) -> Option<Message>;
    fn peek_inbox(&self, id: &str) -> Vec<Message>;
    fn has_mail(&self, id: &str) -> bool;

    fn transition(&mut self, id: &str, to: AgentState, reason: &str) -> bool;
    fn journal_emit(
        &mut self,
        etype: EventType,
        actor: &str,
        target: &str,
        body: &str,
        fields: JMap,
        task_id: Option<&str>,
    );
    fn publish(&mut self, msg: Message);
    fn wait_for(
        &mut self,
        actor: &str,
        condition: &str,
        task_id: Option<&str>,
        correlation_id: &str,
        timeout: Option<f64>,
    );
    fn publish_artifact(&mut self, artifact: &str, producer: &str, task_id: Option<&str>);
    fn append_note(&mut self, id: &str, text: &str);
    fn bump_msgs_sent(&mut self, id: &str);
    fn bump_work_done(&mut self, id: &str, amount: f64);
    fn set_task_status_waiting(&mut self, tid: &str);
    fn mark_task_done(&mut self, tid: &str, finished_at: f64);
    fn mark_task_verified(&mut self, tid: &str);
    fn advance_backlog(&mut self, id: &str, finished_task: &str);
    fn parent_escalate(&mut self, note: JMap);
    fn parent_spawn_request(&mut self, req: JMap);
    fn completion_gate(&self, id: &str) -> CompletionGate;
    fn run_verify(&mut self, id: &str, task_id: Option<&str>) -> VerifyVerdict;
    fn plan_tools(
        &mut self,
        id: &str,
        calls: &[JMap],
        task_id: Option<&str>,
        correlation_id: &str,
    ) -> Vec<String>;
    fn execute_tool(
        &mut self,
        id: &str,
        tool: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
        correlation_id: &str,
    ) -> ToolResultView;
    fn poll_hit(&mut self, id: &str) -> Result<i64, crate::bus::BusError>;
    fn bus_resolve(&mut self, condition: &str, reason: &str);
}

// --------------------------------------------------------------- actor ctx

/// Read-only (plus log/poll/builders) projection handed to cognition/policy.
/// Built at the start of a turn from live runtime state so a stale
/// observation cannot justify an action against a world that has moved on.
pub struct ActorCtx<'a> {
    rt: &'a mut dyn ActorRuntime,
    agent_id: String,
    msg: Option<Message>,
    step_index: i64,
    llm_available: bool,
    log: Vec<String>,
    task: Option<TaskSpec>,
    state: String,
    all_unmet: Vec<String>,
    unmet_arts: Vec<String>,
    consumed: Vec<String>,
    roster: Vec<String>,
    producer_index: std::collections::BTreeMap<String, Vec<String>>,
    role: String,
    goal: String,
    graph_view: Vec<JMap>,
    unread: Vec<Message>,
    workspace: JMap,
    max_workers: i64,
    tool_stats: JMap,
    notes: Vec<String>,
    tool_schemas: Vec<JMap>,
}

impl<'a> ActorCtx<'a> {
    fn open(rt: &'a mut dyn ActorRuntime, agent_id: &str, msg: Option<Message>, step: i64) -> Self {
        let task = rt.current_task(agent_id);
        let state = rt
            .agent_state(agent_id)
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        let unmet_arts = rt.unmet_artifacts(agent_id);
        let upstream = rt.unmet_upstream(agent_id);
        let mut all_unmet = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for a in unmet_arts.iter().chain(upstream.iter()) {
            if seen.insert(a.clone()) {
                all_unmet.push(a.clone());
            }
        }
        let consumed = rt.consumed_ready(agent_id);
        let roster = rt.roster_roles();
        ActorCtx {
            role: rt.agent_role(agent_id).unwrap_or_default(),
            goal: rt.task_text().to_string(),
            graph_view: rt.graph_view(),
            unread: rt.peek_inbox(agent_id),
            workspace: rt.workspace_view(agent_id),
            max_workers: rt.max_workers(),
            tool_stats: rt.tool_stats(),
            notes: rt.agent_notes_tail(agent_id, 3),
            tool_schemas: rt.tool_schemas(agent_id),
            producer_index: rt.producer_index(),
            rt,
            agent_id: agent_id.to_string(),
            msg,
            step_index: step,
            llm_available: false,
            log: Vec::new(),
            task,
            state,
            all_unmet,
            unmet_arts,
            consumed,
            roster,
        }
    }

    fn drain_log(&mut self) -> Vec<String> {
        std::mem::take(&mut self.log)
    }
}

impl PolicyContext for ActorCtx<'_> {
    fn agent_id(&self) -> &str {
        &self.agent_id
    }
    fn step_index(&self) -> i64 {
        self.step_index
    }
    fn state(&self) -> String {
        self.state.clone()
    }
    fn llm_available(&self) -> bool {
        self.llm_available
    }
    fn task(&self) -> Option<&TaskSpec> {
        self.task.as_ref()
    }
    fn task_id(&self) -> Option<String> {
        self.task.as_ref().map(|t| t.task_id.as_str().to_string())
    }
    fn all_unmet(&self) -> Vec<String> {
        self.all_unmet.clone()
    }
    fn unmet_artifacts(&self) -> Vec<String> {
        self.unmet_arts.clone()
    }
    fn consumed_ready(&self) -> Vec<String> {
        self.consumed.clone()
    }
    fn roster_roles(&self) -> Vec<String> {
        self.roster.clone()
    }
    fn producer_roles(&self, artifact: &str) -> Vec<String> {
        self.producer_index
            .get(artifact)
            .cloned()
            .unwrap_or_default()
    }
    fn log(&mut self, text: &str) {
        self.log.push(text.to_string());
        let note = format!("[{:.2}] {text}", self.rt.now());
        self.rt.append_note(&self.agent_id.clone(), &note);
    }
    fn poll_hit(&mut self) -> Result<i64, crate::bus::BusError> {
        let id = self.agent_id.clone();
        self.rt.poll_hit(&id)
    }
    fn self_msg(&self, msg_type: EventType, body: &str, payload: JMap) -> Message {
        let task_id = self.task_id();
        if let Some(base) = &self.msg {
            let mut m = base.child(msg_type, self.agent_id.as_str(), "parent");
            m.body = body.to_string();
            if let Some(t) = &task_id {
                m.task_id = Some(TaskId::new(t.clone()));
            }
            m.payload = payload;
            m
        } else {
            let mut m = Message::new(msg_type, self.agent_id.as_str(), "parent");
            m.body = body.to_string();
            m.task_id = task_id.map(TaskId::new);
            m.payload = payload;
            m
        }
    }
    fn request_specialist(
        &self,
        role: &str,
        reason: &str,
        skills: &[String],
        inputs: &[String],
        outputs: &[String],
        est_work: f64,
        capability_class: &str,
    ) -> Message {
        let title = self
            .task
            .as_ref()
            .map(|t| t.title.clone())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unspecified work".into());
        let reason = if reason.is_empty() {
            format!("{role} capability needed for {title}")
        } else {
            reason.to_string()
        };
        let mut p = JMap::new();
        p.insert("requested_role".into(), JValue::Str(role.into()));
        p.insert("reason".into(), JValue::Str(reason.clone()));
        p.insert(
            "required_skills".into(),
            JValue::Arr(skills.iter().cloned().map(JValue::Str).collect()),
        );
        p.insert(
            "required_inputs".into(),
            JValue::Arr(inputs.iter().cloned().map(JValue::Str).collect()),
        );
        p.insert(
            "expected_outputs".into(),
            JValue::Arr(outputs.iter().cloned().map(JValue::Str).collect()),
        );
        p.insert("estimated_work".into(), JValue::Float(est_work));
        p.insert(
            "parent_task_id".into(),
            self.task_id().map(JValue::Str).unwrap_or(JValue::Null),
        );
        p.insert(
            "capability_class".into(),
            JValue::Str(capability_class.into()),
        );
        p.insert("requires_judgment".into(), JValue::Bool(false));
        self.self_msg(EventType::SpawnAgentRequest, &reason, p)
    }
}

// ------------------------------------------------------------------- actor

/// One logical agent. Owns its cognition instance, transcript and cursor.
/// Does not own the journal, graph, registry or bus.
pub struct AgentActor {
    pub agent_id: String,
    /// Snapshot / `arena status` label. Not used for dispatch.
    pub policy_class: String,
    cognition: Box<dyn Cognition>,
    pub steps_run: i64,
    pub last_action: String,
    pub errors: Vec<String>,
    pub transcript: Vec<JMap>,
    all_results: Vec<ToolResultView>,
    last_results: Vec<ToolResultView>,
}

impl AgentActor {
    /// Policy-backed agent: wraps the policy in [`PolicyCognition`] so the
    /// execution loop is identical to a future LLM source.
    pub fn new(agent_id: impl Into<String>, policy: Box<dyn Policy>) -> AgentActor {
        let agent_id = agent_id.into();
        let policy_class = policy.class_name().to_string();
        let cognition = Box::new(PolicyCognition::new(policy, &agent_id));
        AgentActor {
            agent_id,
            policy_class,
            cognition,
            steps_run: 0,
            last_action: String::new(),
            errors: Vec::new(),
            transcript: Vec::new(),
            all_results: Vec::new(),
            last_results: Vec::new(),
        }
    }

    /// Bind an explicit cognition source (replaces the default policy adapter).
    /// Two agents must never share one instance — the kernel factory is
    /// responsible for that; the actor just stores what it is given.
    pub fn bind_cognition(&mut self, source: Box<dyn Cognition>) {
        self.cognition = source;
    }

    pub fn cognition_name(&self) -> &str {
        self.cognition.name()
    }

    pub fn cognition_fingerprint(&self) -> JMap {
        self.cognition.fingerprint()
    }

    pub fn policy_cursor(&self) -> i64 {
        self.cognition.policy_cursor()
    }

    pub fn restore_cursor(&mut self, steps_run: i64, policy_cursor: i64) {
        self.steps_run = steps_run;
        self.cognition.restore_cursor(policy_cursor);
    }

    // -------------------------------------------------------------- helpers

    pub(crate) fn transition(
        &mut self,
        rt: &mut dyn ActorRuntime,
        to: AgentState,
        reason: &str,
    ) -> bool {
        let ok = rt.transition(&self.agent_id, to, reason);
        if !ok {
            let frm = rt
                .agent_state(&self.agent_id)
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "?".into());
            self.errors
                .push(format!("illegal: {frm} -> {}", to.as_str()));
        }
        ok
    }

    fn rec_progress(&mut self, rt: &mut dyn ActorRuntime) {
        let tid = rt
            .current_task(&self.agent_id)
            .map(|t| t.task_id.as_str().to_string());
        // Python journals rec.task_id (current hat), which current_task
        // approximates via pending_work; Kernel will pass the record's
        // task_id through the same helper.
        let rec_tid = rt
            .pending_work(&self.agent_id)
            .first()
            .map(|t| t.as_str().to_string())
            .or(tid);
        let mut fields = JMap::new();
        fields.insert("steps_run".into(), JValue::Int(self.steps_run));
        fields.insert("policy_cursor".into(), JValue::Int(self.policy_cursor()));
        fields.insert("last_action".into(), JValue::Str(self.last_action.clone()));
        rt.journal_emit(
            EventType::TaskProgress,
            &self.agent_id,
            "parent",
            &format!("step {}", self.steps_run),
            fields,
            rec_tid.as_deref(),
        );
    }

    // --------------------------------------------------------------- observe

    /// What the agent is allowed to know this turn. Built from live runtime
    /// state — never from a cached Observation.
    pub fn observe(&self, ctx: &ActorCtx<'_>, msg: Option<&Message>) -> Observation {
        let task = ctx.task.as_ref().map(|t| t.snapshot()).unwrap_or_default();
        Observation {
            agent_id: self.agent_id.clone(),
            role: ctx.role.clone(),
            goal: ctx.goal.clone(),
            task,
            message: msg.cloned(),
            step_index: self.steps_run,
            unmet: ctx.all_unmet.clone(),
            consumed_ready: ctx.consumed.clone(),
            graph_view: ctx.graph_view.clone(),
            unread: ctx.unread.clone(),
            recent: outcomes_from_results(&self.last_results),
            history: outcomes_from_results(&self.all_results),
            workspace: ctx.workspace.clone(),
            budget: {
                let mut b = JMap::new();
                b.insert(
                    "steps_left".into(),
                    JValue::Int((400 - self.steps_run).max(0)),
                );
                b.insert("workers".into(), JValue::Int(ctx.max_workers));
                b.insert("tool_stats".into(), JValue::Obj(ctx.tool_stats.clone()));
                b
            },
            notes: ctx.notes.clone(),
            state: ctx.state.clone(),
            tool_schemas: ctx.tool_schemas.clone(),
            transcript_len: self.transcript.len() as i64,
        }
    }

    // ----------------------------------------------------------------- step

    /// One actor turn. Cognition proposes; the runtime validates and executes.
    pub fn run_step(&mut self, rt: &mut dyn ActorRuntime) -> Turn {
        let msg = rt.pop_inbox(&self.agent_id);
        let msg_in = msg.as_ref().map(|m| m.mid.clone());
        let mut ctx = ActorCtx::open(rt, &self.agent_id, msg.clone(), self.steps_run);
        let obs = self.observe(&ctx, msg.as_ref());
        let mut outcome = Turn::noop(&self.agent_id, msg_in);

        let intent = match self.cognition.decide(&obs, &mut ctx) {
            Ok(i) => i,
            Err(CognitionError::Capability(e)) => {
                self.errors.push(e.clone());
                let tid = rec_task_id(ctx.rt, &self.agent_id);
                let mut fields = JMap::new();
                fields.insert("kind".into(), JValue::Str("capability".into()));
                ctx.rt.journal_emit(
                    EventType::CognitionError,
                    &self.agent_id,
                    "parent",
                    &format!("cognition reached outside its seam: {e}"),
                    fields,
                    tid.as_deref(),
                );
                self.transition(ctx.rt, AgentState::Blocked, "cognition capability error");
                outcome.action = "error".into();
                outcome.detail = e;
                return outcome;
            }
            Err(CognitionError::Other(e)) => {
                self.errors.push(e.clone());
                let tid = rec_task_id(ctx.rt, &self.agent_id);
                let mut fields = JMap::new();
                fields.insert("kind".into(), JValue::Str("error".into()));
                ctx.rt.journal_emit(
                    EventType::CognitionError,
                    &self.agent_id,
                    "parent",
                    &format!("cognition failed: {e}"),
                    fields,
                    tid.as_deref(),
                );
                if ctx.rt.agent_state(&self.agent_id) != Some(AgentState::Terminated) {
                    self.transition(ctx.rt, AgentState::Blocked, "cognition error");
                }
                outcome.action = "error".into();
                outcome.detail = e;
                return outcome;
            }
        };

        let allowed = ctx.rt.allowed_tools(&self.agent_id);
        let (intent, notes) = validate_intent(Some(intent), &allowed, 4);
        if !notes.is_empty() {
            let tid = rec_task_id(ctx.rt, &self.agent_id);
            let fp = intent.fingerprint();
            for n in &notes {
                let mut fields = JMap::new();
                fields.insert("code".into(), JValue::Str(n.clone()));
                fields.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
                fields.insert("intent_fingerprint".into(), JValue::Str(fp.clone()));
                ctx.rt.journal_emit(
                    EventType::CognitionViolation,
                    &self.agent_id,
                    "parent",
                    &format!("intent refused: {n}"),
                    fields,
                    tid.as_deref(),
                );
            }
        }

        let source_name = self.cognition.name().to_string();
        self.record_turn(ctx.rt, {
            let mut row = JMap::new();
            row.insert("intent".into(), JValue::Obj(intent.to_dict()));
            row.insert(
                "notes".into(),
                JValue::Arr(notes.iter().cloned().map(JValue::Str).collect()),
            );
            row.insert("source".into(), JValue::Str(source_name));
            row
        });

        let results = self.execute_calls(ctx.rt, &intent, msg.as_ref());
        if !results.is_empty() {
            outcome.calls = results
                .iter()
                .map(|r| {
                    let mut m = JMap::new();
                    m.insert("tool".into(), JValue::Str(r.tool.clone()));
                    m.insert("ok".into(), JValue::Bool(r.ok));
                    m.insert(
                        "exit".into(),
                        r.exit_code.map(JValue::Int).unwrap_or(JValue::Null),
                    );
                    m.insert("refused".into(), JValue::Str(r.refused.clone()));
                    m
                })
                .collect();
        }

        let action = match self.intent_to_action(&intent, &ctx) {
            Ok(a) => a,
            Err(CognitionError::Capability(e)) => {
                self.errors.push(e.clone());
                let tid = rec_task_id(ctx.rt, &self.agent_id);
                let mut fields = JMap::new();
                fields.insert("kind".into(), JValue::Str("capability".into()));
                ctx.rt.journal_emit(
                    EventType::CognitionError,
                    &self.agent_id,
                    "parent",
                    &format!("cognition reached outside its seam: {e}"),
                    fields,
                    tid.as_deref(),
                );
                self.transition(ctx.rt, AgentState::Blocked, "cognition capability error");
                outcome.action = "error".into();
                outcome.detail = e;
                return outcome;
            }
            Err(CognitionError::Other(e)) => {
                outcome.action = "error".into();
                outcome.detail = e;
                return outcome;
            }
        };

        self.steps_run += 1;
        self.rec_progress(ctx.rt);
        // Apply BEFORE recording last_action. Python set last_action first,
        // which made the "repeated COMPLETE with no bound task" guard fire on
        // the *first* no-task complete (the comment in actor.py describes the
        // opposite intent). Classification: CHANGE — honour the comment.
        outcome.action = action.act.as_str().to_string();
        outcome.detail = action.reason.clone();
        self.apply_action(ctx.rt, &action, msg.as_ref(), &mut outcome);
        self.last_action = action.act.as_str().to_string();

        let state = ctx.rt.agent_state(&self.agent_id);
        if matches!(
            state,
            Some(AgentState::Initializing) | Some(AgentState::Created)
        ) && !ctx.rt.pending_work(&self.agent_id).is_empty()
        {
            self.transition(ctx.rt, AgentState::Working, "began task");
        }
        outcome.log = ctx.drain_log();
        outcome
    }

    fn execute_calls(
        &mut self,
        rt: &mut dyn ActorRuntime,
        intent: &Intent,
        msg: Option<&Message>,
    ) -> Vec<ToolResultView> {
        if intent.calls.is_empty() {
            self.last_results.clear();
            return Vec::new();
        }
        let tid = rec_task_id(rt, &self.agent_id);
        let corr = msg
            .map(|m| m.correlation_id.as_str().to_string())
            .unwrap_or_default();
        let mut results = Vec::new();
        if rt.tools_bound() {
            let call_dicts: Vec<JMap> = intent.calls.iter().map(ToolCall::to_dict).collect();
            let rids = rt.plan_tools(&self.agent_id, &call_dicts, tid.as_deref(), &corr);
            for (i, call) in intent.calls.iter().enumerate() {
                let rid = rids.get(i).cloned().unwrap_or_default();
                let res = rt.execute_tool(
                    &self.agent_id,
                    &call.tool,
                    &call.args,
                    &rid,
                    tid.as_deref(),
                    &corr,
                );
                self.record_turn(rt, {
                    let mut row = JMap::new();
                    row.insert("tool_result".into(), JValue::Str(res.block.clone()));
                    row.insert("rid".into(), JValue::Str(rid));
                    row
                });
                results.push(res);
            }
        } else {
            for call in &intent.calls {
                let mut res = ToolResultView {
                    tool: call.tool.clone(),
                    ok: false,
                    block: "this kernel has no tool executor bound; Kernel.bind_tools() is required before tools run".into(),
                    refused: "REFUSE_NO_EXECUTOR".into(),
                    ..Default::default()
                };
                res.data = JMap::new();
                let mut fields = JMap::new();
                fields.insert("tool".into(), JValue::Str(call.tool.clone()));
                fields.insert("code".into(), JValue::Str("REFUSE_NO_EXECUTOR".into()));
                fields.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
                rt.journal_emit(
                    EventType::ToolRefused,
                    &self.agent_id,
                    "kernel",
                    &format!("{}: REFUSE_NO_EXECUTOR", call.tool),
                    fields,
                    tid.as_deref(),
                );
                results.push(res);
            }
        }
        self.all_results.extend(results.iter().cloned());
        self.last_results = results.clone();
        results
    }

    fn intent_to_action(
        &self,
        intent: &Intent,
        ctx: &ActorCtx<'_>,
    ) -> Result<Action, CognitionError> {
        let c = intent.control.as_str();
        if c == Control::WAIT {
            let cond = if intent.wait_for.is_empty() {
                ctx.all_unmet.first().cloned().unwrap_or_default()
            } else {
                intent.wait_for.clone()
            };
            if cond.is_empty() {
                return Ok(Action {
                    act: Act::Noop,
                    reason: if intent.reason.is_empty() {
                        "nothing to wait on".into()
                    } else {
                        intent.reason.clone()
                    },
                    ..Default::default()
                });
            }
            let cond = if cond.contains(':') {
                cond
            } else {
                format!("artifact:{cond}")
            };
            return Ok(Action::wait(
                &cond,
                if intent.reason.is_empty() {
                    "waiting"
                } else {
                    &intent.reason
                },
                None,
            ));
        }
        if c == Control::ESCALATE {
            let mut extra = JMap::new();
            if let Some(JValue::Obj(e)) = intent.publish.get("extra") {
                extra = e.clone();
            }
            return Ok(Action::escalate_with(
                if intent.reason.is_empty() {
                    "escalation"
                } else {
                    &intent.reason
                },
                extra,
            ));
        }
        if c == Control::COMPLETE {
            let arts = match intent.publish.get("artifacts") {
                Some(JValue::Arr(items)) => items.clone(),
                _ => vec![],
            };
            let mut extra = JMap::new();
            extra.insert("artifacts".into(), JValue::Arr(arts));
            return Ok(Action::complete_with(
                if intent.reason.is_empty() {
                    "complete"
                } else {
                    &intent.reason
                },
                extra,
            ));
        }
        if c == Control::VERIFY {
            let mut extra = JMap::new();
            extra.insert("verify_now".into(), JValue::Bool(true));
            extra.insert("artifacts".into(), JValue::Arr(vec![]));
            extra.insert("intent_think".into(), JValue::Str(intent.think.clone()));
            return Ok(Action {
                act: Act::Complete,
                reason: if intent.reason.is_empty() {
                    "verify then complete".into()
                } else {
                    intent.reason.clone()
                },
                extra,
                ..Default::default()
            });
        }
        if c == Control::PUBLISH {
            let m = intent.msg.clone().unwrap_or_else(|| {
                ctx.self_msg(
                    EventType::StatusUpdate,
                    if intent.reason.is_empty() {
                        "update"
                    } else {
                        &intent.reason
                    },
                    JMap::new(),
                )
            });
            return Ok(Action::publish(
                m,
                if intent.reason.is_empty() {
                    "publish"
                } else {
                    &intent.reason
                },
            ));
        }
        if c == Control::SPAWN_REQUEST {
            let Some(m) = intent.msg.clone() else {
                return Err(CognitionError::Capability(
                    "SPAWN_REQUEST without a shaped message: use ctx.request_specialist() \
                     (or arena.cognition's ToolCall('request_specialist', ...)) so every field \
                     the Parent needs is present"
                        .into(),
                ));
            };
            return Ok(Action::publish(
                m,
                if intent.reason.is_empty() {
                    "spawn request"
                } else {
                    &intent.reason
                },
            ));
        }
        if c == Control::NOOP {
            return Ok(Action {
                act: Act::Noop,
                reason: if intent.reason.is_empty() {
                    "noop".into()
                } else {
                    intent.reason.clone()
                },
                ..Default::default()
            });
        }
        if !intent.reason.is_empty() {
            return Ok(Action::proceed(&intent.reason));
        }
        Ok(Action::proceed("worked"))
    }

    fn apply_action(
        &mut self,
        rt: &mut dyn ActorRuntime,
        action: &Action,
        msg: Option<&Message>,
        outcome: &mut Turn,
    ) {
        match action.act {
            Act::Wait => {
                let tid = rec_task_id(rt, &self.agent_id);
                let corr = msg
                    .map(|m| m.correlation_id.as_str().to_string())
                    .unwrap_or_default();
                rt.wait_for(
                    &self.agent_id,
                    &action.condition,
                    tid.as_deref(),
                    &corr,
                    action.timeout,
                );
                if matches!(
                    rt.agent_state(&self.agent_id),
                    Some(AgentState::Idle)
                        | Some(AgentState::Working)
                        | Some(AgentState::Escalated)
                        | Some(AgentState::Blocked)
                        | Some(AgentState::Paused)
                ) {
                    self.transition(
                        rt,
                        AgentState::WaitingForDependency,
                        if action.reason.is_empty() {
                            "waiting"
                        } else {
                            &action.reason
                        },
                    );
                }
                outcome.detail = format!("parked on {}", action.condition);
            }
            Act::Escalate => {
                self.transition(
                    rt,
                    AgentState::Escalated,
                    if action.reason.is_empty() {
                        "escalation"
                    } else {
                        &action.reason
                    },
                );
                let tid = rec_task_id(rt, &self.agent_id);
                let mut note = JMap::new();
                note.insert("agent".into(), JValue::Str(self.agent_id.clone()));
                note.insert(
                    "task_id".into(),
                    tid.clone().map(JValue::Str).unwrap_or(JValue::Null),
                );
                note.insert("kind".into(), JValue::Str("escalation".into()));
                note.insert("reason".into(), JValue::Str(action.reason.clone()));
                note.insert("extra".into(), JValue::Obj(action.extra.clone()));
                note.insert("at".into(), JValue::Float(rt.now()));
                rt.parent_escalate(note);
                outcome.detail = "escalated to parent".into();
            }
            Act::Complete => self.complete(rt, action),
            Act::Publish if action.msg.is_some() => {
                let m = action.msg.clone().expect("checked");
                if m.msg_type == EventType::SpawnAgentRequest {
                    let mut req = m.payload.clone();
                    req.insert("from".into(), JValue::Str(self.agent_id.clone()));
                    req.insert(
                        "task_id".into(),
                        m.task_id
                            .as_ref()
                            .map(|t| JValue::Str(t.as_str().into()))
                            .unwrap_or(JValue::Null),
                    );
                    req.insert(
                        "correlation_id".into(),
                        JValue::Str(m.correlation_id.as_str().into()),
                    );
                    req.insert("mid".into(), JValue::Str(m.mid.clone()));
                    rt.parent_spawn_request(req);
                }
                let detail = format!("{} -> {}", m.msg_type.as_str(), m.to_actor.as_str());
                rt.publish(m);
                rt.bump_msgs_sent(&self.agent_id);
                outcome.detail = detail;
            }
            _ => {
                let wu = rt.work_unit();
                rt.bump_work_done(&self.agent_id, wu);
                outcome.detail = if action.reason.is_empty() {
                    "proceeded".into()
                } else {
                    action.reason.clone()
                };
            }
        }
    }

    fn complete(&mut self, rt: &mut dyn ActorRuntime, action: &Action) {
        let rec_tid = rec_task_id(rt, &self.agent_id);
        let task = rec_tid.as_deref().and_then(|t| rt.task_by_id(t));
        let mut gate = rt.completion_gate(&self.agent_id);
        if !gate.allow {
            let ran = action
                .extra
                .get("verify_now")
                .and_then(JValue::as_bool)
                .unwrap_or(false);
            let verdict = if rt.tools_bound() {
                rt.run_verify(&self.agent_id, gate.task_id.as_deref())
            } else {
                VerifyVerdict {
                    ok: false,
                    ..Default::default()
                }
            };
            if verdict.ok {
                gate.allow = true;
                gate.verified = true;
            } else {
                let extra = if ran {
                    " (verify re-run by the runtime and it still failed)"
                } else {
                    ""
                };
                let body = format!("{}: {}{extra}", gate.rule, gate.detail);
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str(gate.rule.clone()));
                fields.insert("verified".into(), JValue::Bool(verdict.ok));
                fields.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
                fields.insert(
                    "verify_results".into(),
                    JValue::Arr(verdict.results.iter().cloned().map(JValue::Obj).collect()),
                );
                rt.journal_emit(
                    EventType::CompletionRefused,
                    &self.agent_id,
                    "parent",
                    &body,
                    fields,
                    gate.task_id.as_deref(),
                );
                rt.append_note(
                    &self.agent_id,
                    &format!("[{:.2}] completion refused: verification failed", rt.now()),
                );
                let mut payload = JMap::new();
                payload.insert("rule".into(), JValue::Str(gate.rule.clone()));
                payload.insert("percent".into(), JValue::Int(0));
                payload.insert(
                    "verify".into(),
                    JValue::Arr(
                        verdict
                            .results
                            .iter()
                            .take(2)
                            .cloned()
                            .map(JValue::Obj)
                            .collect(),
                    ),
                );
                let mut fb =
                    Message::new(EventType::TaskProgress, "parent", self.agent_id.as_str());
                fb.body = "runtime refused completion: verification did not pass".into();
                fb.task_id = gate.task_id.as_deref().map(TaskId::new);
                fb.payload = payload;
                rt.publish(fb);
                if let Some(tid) = &gate.task_id {
                    rt.set_task_status_waiting(tid);
                }
                self.transition(rt, AgentState::Idle, "verify failed; work remains");
                return;
            }
        }
        if let Some(t) = &task {
            if !t.verify.is_empty() && !t.verified {
                rt.mark_task_verified(t.task_id.as_str());
            }
        }
        if task.is_none() && self.last_action == Act::Complete.as_str() {
            self.errors
                .push("ignoring repeated COMPLETE with no bound task".into());
            return;
        }
        if task.is_none() {
            self.transition(rt, AgentState::Completed, "no task bound");
            rt.journal_emit(
                EventType::TaskCompleted,
                &self.agent_id,
                "parent",
                "agent finished with no bound task",
                JMap::new(),
                None,
            );
            return;
        }
        let task = task.expect("checked");
        let arts: Vec<String> = match action.extra.get("artifacts") {
            Some(JValue::Arr(items)) if !items.is_empty() => items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => task.produces.clone(),
        };
        for a in &arts {
            rt.publish_artifact(a, &self.agent_id, Some(task.task_id.as_str()));
        }
        let now = rt.now();
        rt.mark_task_done(task.task_id.as_str(), now);
        rt.advance_backlog(&self.agent_id, task.task_id.as_str());
        rt.bump_work_done(&self.agent_id, task.est_work);
        let already_done = rt.agent_state(&self.agent_id) == Some(AgentState::Completed);
        let mut fields = JMap::new();
        fields.insert(
            "artifacts".into(),
            JValue::Arr(task.produces.iter().cloned().map(JValue::Str).collect()),
        );
        fields.insert("owner".into(), JValue::Str(self.agent_id.clone()));
        rt.journal_emit(
            EventType::TaskCompleted,
            &self.agent_id,
            "parent",
            &format!("{} done", task.task_id.as_str()),
            fields,
            Some(task.task_id.as_str()),
        );
        if !already_done {
            self.transition(rt, AgentState::Completed, "task done");
        }
        let mut stats = JMap::new();
        stats.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
        stats.insert("steps_run".into(), JValue::Int(self.steps_run));
        rt.journal_emit(
            EventType::Stats,
            &self.agent_id,
            "parent",
            "task done",
            stats,
            None,
        );
        for a in &task.produces {
            rt.bus_resolve(
                &format!("artifact:{a}"),
                &format!("{} published {a}", task.task_id.as_str()),
            );
        }
    }

    fn record_turn(&mut self, rt: &mut dyn ActorRuntime, mut row: JMap) {
        row.insert("at".into(), JValue::Float(py_round4(rt.now())));
        row.insert("tick".into(), JValue::Int(rt.tick()));
        row.insert("step".into(), JValue::Int(self.steps_run));
        row.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
        self.transcript.push(row.clone());
        if let Some(dir) = rt.transcript_dir() {
            if let Err(e) = append_transcript(&dir, &self.agent_id, &row) {
                self.errors.push(format!("transcript write failed: {e}"));
            }
        }
    }

    // ------------------------------------------------------------------ loop

    pub fn has_mail(&self, rt: &dyn ActorRuntime) -> bool {
        rt.has_mail(&self.agent_id)
    }

    /// Runnable = in a runnable state with work, OR parked but holding mail.
    /// A COMPLETED agent with a backlog is runnable again.
    pub fn runnable(&self, rt: &dyn ActorRuntime) -> bool {
        let Some(state) = rt.agent_state(&self.agent_id) else {
            return false;
        };
        let pending = rt.pending_work(&self.agent_id);
        if !pending.is_empty()
            && matches!(
                state,
                AgentState::Completed | AgentState::Idle | AgentState::Initializing
            )
        {
            return true;
        }
        if RUNNABLE.contains(&state) && !pending.is_empty() {
            return true;
        }
        !rt.pending_waits(&self.agent_id).is_empty() && rt.has_mail(&self.agent_id)
    }

    pub fn run(&mut self, rt: &mut dyn ActorRuntime, max_steps: i64) -> Vec<Turn> {
        let mut out = Vec::new();
        for _ in 0..max_steps {
            let Some(state) = rt.agent_state(&self.agent_id) else {
                break;
            };
            if matches!(
                state,
                AgentState::Terminated
                    | AgentState::Completed
                    | AgentState::WaitingForDependency
                    | AgentState::Escalated
                    | AgentState::Paused
                    | AgentState::Blocked
            ) {
                break;
            }
            let r = self.run_step(rt);
            let stop = is_sleeping_action(&r.action) || r.action.eq_ignore_ascii_case("error");
            out.push(r);
            if stop {
                break;
            }
        }
        out
    }

    pub fn snapshot(&self, rt: &dyn ActorRuntime) -> JMap {
        let mut m = JMap::new();
        m.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
        m.insert(
            "state".into(),
            JValue::Str(
                rt.agent_state(&self.agent_id)
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
            ),
        );
        m.insert("steps_run".into(), JValue::Int(self.steps_run));
        m.insert(
            "cognition".into(),
            JValue::Obj(rt.cognition_record(&self.agent_id)),
        );
        m.insert(
            "transcript_len".into(),
            JValue::Int(self.transcript.len() as i64),
        );
        m.insert(
            "workspace".into(),
            JValue::Bool(!rt.workspace_view(&self.agent_id).is_empty()),
        );
        m.insert("last_action".into(), JValue::Str(self.last_action.clone()));
        m.insert(
            "errors".into(),
            JValue::Arr(self.errors.iter().cloned().map(JValue::Str).collect()),
        );
        m.insert(
            "task_id".into(),
            rec_task_id_ref(rt, &self.agent_id)
                .map(JValue::Str)
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "waits".into(),
            JValue::Arr(
                rt.pending_waits(&self.agent_id)
                    .iter()
                    .map(|w| w.get("condition").cloned().unwrap_or(JValue::Null))
                    .collect(),
            ),
        );
        m.insert("policy".into(), JValue::Str(self.policy_class.clone()));
        m
    }
}

fn rec_task_id(rt: &dyn ActorRuntime, id: &str) -> Option<String> {
    rt.pending_work(id)
        .first()
        .map(|t| t.as_str().to_string())
        .or_else(|| rt.current_task(id).map(|t| t.task_id.as_str().to_string()))
}

fn rec_task_id_ref(rt: &dyn ActorRuntime, id: &str) -> Option<String> {
    rec_task_id(rt, id)
}

fn py_round4(x: f64) -> f64 {
    crate::sys::json::py_round(x, 4)
}

fn append_transcript(dir: &str, agent_id: &str, row: &JMap) -> Result<(), String> {
    use std::io::Write;
    let p = std::path::Path::new(dir);
    std::fs::create_dir_all(p).map_err(|e| e.to_string())?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(p.join(format!("{agent_id}.jsonl")))
        .map_err(|e| e.to_string())?;
    let line = JValue::Obj(row.clone()).to_canon_string();
    writeln!(f, "{line}").map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests;
