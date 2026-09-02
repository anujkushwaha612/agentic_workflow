//! Agent policies + the pluggable cognition source (`arena/policy.py`).
//!
//! A policy is a pure function `(context, message) -> Action`. That indirection
//! is the whole point: Phase 1 ships deterministic heuristics, and a real model
//! can be dropped in behind the adapter without touching the kernel, the bus,
//! or the tests. A policy READS context and PRODUCES an action — it never
//! mutates kernel state; the runtime applies (or refuses) the action through
//! the lifecycle/journal/bus guards.
//!
//! ## Context projection
//! Python policies receive an `ActorContext` holding the kernel. Rust instead
//! defines exactly what a policy may know in the [`PolicyContext`] trait; the
//! actor (M7) implements it over kernel-owned state. Policies cannot name the
//! kernel, so they cannot mutate it.

use crate::bus::BusError;
use crate::graph::TaskSpec;
use crate::msg::{EventType, Message};
use crate::sys::json::{JMap, JValue};

// ----------------------------------------------------------------------- act

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Act {
    Proceed,
    Publish,
    Wait,
    Complete,
    Escalate,
    Noop,
}

impl Act {
    pub fn as_str(&self) -> &'static str {
        match self {
            Act::Proceed => "PROCEED",
            Act::Publish => "PUBLISH",
            Act::Wait => "WAIT",
            Act::Complete => "COMPLETE",
            Act::Escalate => "ESCALATE",
            Act::Noop => "NOOP",
        }
    }
}

/// The actions that end an actor's turn: after one of these the actor has
/// nothing more to do this tick, so the kernel stops draining its inbox.
///
/// Phase-2 defect D2: the actor stored `str(Act.X)` (UPPER CASE) while the run
/// loop compared against a lower-case literal tuple — the break could never
/// fire. One shared set, compared case-insensitively, so the two sites cannot
/// drift again.
pub const SLEEPING_ACTIONS: [&str; 4] = ["WAIT", "COMPLETE", "ESCALATE", "ERROR"];

/// True when this actor's previous turn ended in a state that should not be
/// followed by more work in the same tick. Tolerates both `str(Act.X)` and a
/// bare lower-case verb (Python `is_sleeping_action`).
pub fn is_sleeping_action(last_action: &str) -> bool {
    let up = last_action.to_uppercase();
    SLEEPING_ACTIONS.iter().any(|a| a.to_uppercase() == up)
}

// -------------------------------------------------------------------- action

/// A proposal. `msg` travels the control plane like any other event; `extra`
/// carries structured payloads (e.g. `artifacts` on COMPLETE).
#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    pub act: Act,
    pub msg: Option<Message>,
    pub condition: String,
    pub reason: String,
    pub timeout: Option<f64>,
    pub extra: JMap,
}

impl Default for Action {
    fn default() -> Self {
        Action {
            act: Act::Proceed,
            msg: None,
            condition: String::new(),
            reason: String::new(),
            timeout: None,
            extra: JMap::new(),
        }
    }
}

impl Action {
    pub fn proceed(reason: &str) -> Action {
        Action {
            act: Act::Proceed,
            reason: reason.to_string(),
            ..Default::default()
        }
    }
    pub fn publish(msg: Message, reason: &str) -> Action {
        Action {
            act: Act::Publish,
            msg: Some(msg),
            reason: reason.to_string(),
            ..Default::default()
        }
    }
    pub fn wait(condition: &str, reason: &str, timeout: Option<f64>) -> Action {
        Action {
            act: Act::Wait,
            condition: condition.to_string(),
            reason: reason.to_string(),
            timeout,
            ..Default::default()
        }
    }
    pub fn complete(reason: &str) -> Action {
        Action {
            act: Act::Complete,
            reason: reason.to_string(),
            ..Default::default()
        }
    }
    pub fn complete_with(reason: &str, extra: JMap) -> Action {
        Action {
            act: Act::Complete,
            reason: reason.to_string(),
            extra,
            ..Default::default()
        }
    }
    pub fn escalate(reason: &str) -> Action {
        Action {
            act: Act::Escalate,
            reason: reason.to_string(),
            ..Default::default()
        }
    }
    pub fn escalate_with(reason: &str, extra: JMap) -> Action {
        Action {
            act: Act::Escalate,
            reason: reason.to_string(),
            extra,
            ..Default::default()
        }
    }
}

// ------------------------------------------------------------------- context

/// The *entire* surface a policy gets. Deliberately read-only except for
/// log/poll-hit and the two message builders — no kernel access, no mutation.
///
/// `llm_available` / polls are observable seams: the chaos suite flips them to
/// prove that the default path never polls and that a model adapter would
/// actually be consulted if present.
pub trait PolicyContext {
    fn agent_id(&self) -> &str;
    fn step_index(&self) -> i64;
    fn state(&self) -> String;
    fn llm_available(&self) -> bool;
    /// The task the actor is gated on: current, else first queued one that is
    /// still open.
    fn task(&self) -> Option<&TaskSpec>;
    fn task_id(&self) -> Option<String>;
    /// Consumed-but-missing plus unmet upstream artifacts, deduped in order.
    fn all_unmet(&self) -> Vec<String>;
    /// Artifacts this task consumes that are not yet produced.
    fn unmet_artifacts(&self) -> Vec<String>;
    /// Artifacts this task consumes that already exist.
    fn consumed_ready(&self) -> Vec<String>;
    /// Distinct roles on the roster (for duplicate-capability checks).
    fn roster_roles(&self) -> Vec<String>;
    /// Roles of tasks that produce `artifact` (for detect_from).
    fn producer_roles(&self, artifact: &str) -> Vec<String>;
    /// Context-local note (also lands on the agent's record in the real impl).
    fn log(&mut self, text: &str);
    /// The polling anti-pattern measurement point.
    fn poll_hit(&mut self) -> Result<i64, BusError>;
    /// A message from this agent; implementations inherit causality from the
    /// message being handled (the ONLY sanctioned reply path) and default
    /// `task_id` to the current task.
    fn self_msg(&self, msg_type: EventType, body: &str, payload: JMap) -> Message;
    /// The ONLY sanctioned way for an agent to ask for a new colleague: it
    /// returns a Message (a proposal), never talks to the Parent directly.
    #[allow(clippy::too_many_arguments)]
    fn request_specialist(
        &self,
        role: &str,
        reason: &str,
        skills: &[String],
        inputs: &[String],
        outputs: &[String],
        est_work: f64,
        capability_class: &str,
    ) -> Message;
}

// -------------------------------------------------------------------- errors

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyError {
    /// A policy polled past its eagerness limit instead of declaring a wait.
    PollingForbidden(BusError),
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // the actor journals this as "policy crashed: PollingForbidden: ..."
            // exactly like the reference's exception path
            PolicyError::PollingForbidden(e) => write!(f, "PollingForbidden: {e}"),
        }
    }
}
impl std::error::Error for PolicyError {}

impl From<BusError> for PolicyError {
    fn from(e: BusError) -> Self {
        PolicyError::PollingForbidden(e)
    }
}

// -------------------------------------------------------------------- policy

pub trait Policy {
    /// the reference's `name` field, e.g. "simulated-work"
    fn name(&self) -> &'static str;
    /// the reference's class name, e.g. "SimulatedWork" (snapshot()/fingerprints use it)
    fn class_name(&self) -> &'static str;
    fn llm_available(&self) -> bool {
        false
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        msg: Option<&Message>,
    ) -> Result<Action, PolicyError>;
    /// How far the policy got inside the current task (the journal restores it).
    /// Reference: `_steps` if present, else `_worked`, else 0. NOTE: a wrapper
    /// (HybridPolicy) exposes NO cursor of its own — quirk kept.
    fn policy_cursor(&self) -> i64 {
        0
    }
    /// Restore counters after a replay. The reference sets every existing
    /// counter field (`_steps`, `_worked`, `_n`, `_asked`) to the SAME value.
    fn restore_cursor(&mut self, _cursor: i64) {}
}

// ------------------------------------------------------------------ llm seam

/// Optional tier B. Implemented by whoever has a model key; absent here.
pub trait LlmAdapter {
    fn name(&self) -> &'static str;
    fn decide(&self, prompt: &JMap) -> Result<Action, LlmError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmError(pub String);

impl std::fmt::Display for LlmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for LlmError {}

/// Default: never called, present so the seam is real code and not a comment.
pub struct NullLlm;

impl LlmAdapter for NullLlm {
    fn name(&self) -> &'static str {
        "null-llm"
    }
    fn decide(&self, _prompt: &JMap) -> Result<Action, LlmError> {
        Err(LlmError(
            "No LLM adapter configured. This sandbox has no model API key (verified: \
             env has 0 api/token/secret vars). Either pass llm=<adapter> to AgentActor or \
             leave policies deterministic and use the escalation inbox for Arena-side \
             judgment."
                .to_string(),
        ))
    }
}

// ------------------------------------------------------------------ builtins

/// Honest placeholder for "the agent does its job": fixed number of progress
/// steps. step_index comes from the actor (which the journal restores), never
/// from private state, so a recovered agent continues where it stopped.
#[derive(Debug, Clone)]
pub struct SimulatedWork {
    pub steps: i64,
}

impl Default for SimulatedWork {
    fn default() -> Self {
        SimulatedWork { steps: 3 }
    }
}

impl SimulatedWork {
    pub fn named() -> &'static str {
        "simulated-work"
    }
}

impl Policy for SimulatedWork {
    fn name(&self) -> &'static str {
        "simulated-work"
    }
    fn class_name(&self) -> &'static str {
        "SimulatedWork"
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        _msg: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        if ctx.step_index() <= self.steps {
            let denom = 1.max(self.steps);
            let pct = (100.0 * 1.0f64.min((ctx.step_index() - 1) as f64 / denom as f64)) as i64;
            let title = ctx.task().map(|t| t.title.clone()).unwrap_or_default();
            let mut payload = JMap::new();
            payload.insert("percent".into(), JValue::Int(pct));
            let m = ctx.self_msg(
                EventType::TaskProgress,
                &format!("{pct}% of {title}"),
                payload,
            );
            return Ok(Action::publish(m, ""));
        }
        let artifacts: Vec<JValue> = ctx
            .task()
            .map(|t| t.produces.iter().map(|a| JValue::Str(a.clone())).collect())
            .unwrap_or_default();
        let mut extra = JMap::new();
        extra.insert("artifacts".into(), JValue::Arr(artifacts));
        Ok(Action::complete_with("work finished", extra))
    }
}

/// The §6 pattern, correctly implemented: consume a dependency or park
/// durably. It does NOT ask "is schema ready?" — it asks the graph once for
/// what it still needs, and if something is missing it registers a durable
/// wait and releases its slot. Zero polls.
#[derive(Debug, Clone)]
pub struct WaitForArtifacts {
    pub timeout: Option<f64>,
    pub steps: i64,
}

impl Default for WaitForArtifacts {
    fn default() -> Self {
        WaitForArtifacts {
            timeout: Some(4.0),
            steps: 2,
        }
    }
}

impl Policy for WaitForArtifacts {
    fn name(&self) -> &'static str {
        "wait-for-artifacts"
    }
    fn class_name(&self) -> &'static str {
        "WaitForArtifacts"
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        msg: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        if let Some(m) = msg {
            if m.msg_type == EventType::DependencyReady {
                ctx.log(&format!("resumed by {}", m.body));
            }
        }
        let unmet = ctx.all_unmet();
        if let Some(cond) = unmet.first() {
            return Ok(Action::wait(
                &format!("artifact:{cond}"),
                &format!("needs {cond}"),
                self.timeout,
            ));
        }
        // NOTE: never gate on our own outputs — they are published at COMPLETE
        // by definition. An earlier draft did exactly that and the agent spun
        // in PROCEED forever.
        if ctx.step_index() <= self.steps {
            let mut payload = JMap::new();
            payload.insert(
                "consumed".into(),
                JValue::Arr(ctx.consumed_ready().into_iter().map(JValue::Str).collect()),
            );
            let tid = ctx.task_id().unwrap_or_default();
            let m = ctx.self_msg(
                EventType::TaskProgress,
                &format!("integrating {tid}"),
                payload,
            );
            return Ok(Action::publish(m, ""));
        }
        let artifacts: Vec<JValue> = ctx
            .task()
            .map(|t| t.produces.iter().map(|a| JValue::Str(a.clone())).collect())
            .unwrap_or_default();
        let mut extra = JMap::new();
        extra.insert("artifacts".into(), JValue::Arr(artifacts));
        Ok(Action::complete_with(
            "integrated against upstream artifacts",
            extra,
        ))
    }
}

/// DELIBERATE anti-pattern, used only by the chaos suite to prove polling is
/// detectable.
#[derive(Debug, Clone)]
pub struct PollUntilReady {
    pub limit: i64,
    n: i64,
}

impl Default for PollUntilReady {
    fn default() -> Self {
        PollUntilReady { limit: 5, n: 0 }
    }
}

impl PollUntilReady {
    pub fn with_limit(limit: i64) -> Self {
        PollUntilReady { limit, n: 0 }
    }
}

impl Policy for PollUntilReady {
    fn name(&self) -> &'static str {
        "poll-anti-pattern"
    }
    fn class_name(&self) -> &'static str {
        "PollUntilReady"
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        _msg: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        self.n += 1;
        ctx.poll_hit()?; // counted; the suite asserts on this
        if !ctx.unmet_artifacts().is_empty() {
            if self.n > self.limit {
                return Ok(Action::escalate(&format!("gave up after {} polls", self.n)));
            }
            return Ok(Action::proceed(&format!("checking again ({})", self.n)));
        }
        Ok(Action::complete(&format!(
            "dependency showed up on poll {}",
            self.n
        )))
    }
    fn policy_cursor(&self) -> i64 {
        self.n
    }
    fn restore_cursor(&mut self, cursor: i64) {
        self.n = cursor;
    }
}

/// §9 + §10: worker discovers scope creep, requests either a feature or a
/// specialist.
#[derive(Debug, Clone)]
pub struct EscalateOnComplexity {
    pub threshold: f64,
    pub request_spawns: i64,
    pub role: String,
    pub reason: String,
    pub skills: Vec<String>,
    pub est_work: f64,
    pub produces: Vec<String>,
    pub new_task_id: String,
    pub max_steps: i64,
    asked: i64,
    steps: i64,
}

impl Default for EscalateOnComplexity {
    fn default() -> Self {
        EscalateOnComplexity {
            threshold: 2.0,
            request_spawns: 1,
            role: "security engineer".to_string(),
            reason: "auth/authorization design needs specialized expertise".to_string(),
            skills: vec!["auth".to_string(), "crypto".to_string()],
            est_work: 3.0,
            produces: Vec::new(),
            new_task_id: String::new(),
            max_steps: 6,
            asked: 0,
            steps: 0,
        }
    }
}

impl EscalateOnComplexity {
    pub fn with_request_spawns(request_spawns: i64) -> Self {
        EscalateOnComplexity {
            request_spawns,
            ..Default::default()
        }
    }
}

impl Policy for EscalateOnComplexity {
    fn name(&self) -> &'static str {
        "escalate-on-complexity"
    }
    fn class_name(&self) -> &'static str {
        "EscalateOnComplexity"
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        msg: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        if let Some(m) = msg {
            if m.msg_type == EventType::SpawnApproved {
                ctx.log(&format!("parent approved: {}", m.body));
            }
            if m.msg_type == EventType::SpawnRejected {
                let rule = m
                    .payload
                    .get("rule")
                    .map(|v| v.to_canon_string())
                    .unwrap_or_else(|| "None".to_string());
                ctx.log(&format!("parent rejected spawn: {rule}"));
            }
            if m.msg_type == EventType::ApiContractReady {
                ctx.log("feature request satisfied upstream");
            }
        }
        if self.steps == 0
            && ctx
                .task()
                .map(|t| t.est_work >= self.threshold)
                .unwrap_or(true)
        {
            if self.asked < self.request_spawns {
                self.asked += 1;
                let m = ctx.request_specialist(
                    &self.role.clone(),
                    &self.reason.clone(),
                    &self.skills.clone(),
                    &[],
                    &self.produces.clone(),
                    self.est_work,
                    "",
                );
                return Ok(Action::publish(m, "request specialist"));
            }
            let mut extra = JMap::new();
            extra.insert("requested_role".into(), JValue::Str(self.role.clone()));
            return Ok(Action::escalate_with(
                "repeated specialist need cannot be resolved locally",
                extra,
            ));
        }
        if let Some(m) = msg {
            if m.to_actor.as_str() == ctx.agent_id()
                && m.msg_type == EventType::DependencyReady
                && self.asked < self.request_spawns
            {
                self.asked += 1;
                let m = ctx.request_specialist(
                    &self.role.clone(),
                    &self.reason.clone(),
                    &self.skills.clone(),
                    &[],
                    &self.produces.clone(),
                    self.est_work,
                    "",
                );
                return Ok(Action::publish(
                    m,
                    "request specialist after upstream handoff",
                ));
            }
        }
        self.steps += 1;
        if self.steps >= self.max_steps {
            let artifacts: Vec<JValue> = ctx
                .task()
                .map(|t| t.produces.iter().map(|a| JValue::Str(a.clone())).collect())
                .unwrap_or_default();
            let mut extra = JMap::new();
            extra.insert("artifacts".into(), JValue::Arr(artifacts));
            return Ok(Action::complete_with(
                "handled with available capability",
                extra,
            ));
        }
        Ok(Action::proceed("assessing complexity"))
    }
    fn policy_cursor(&self) -> i64 {
        self.steps
    }
    fn restore_cursor(&mut self, cursor: i64) {
        // the reference sets every existing counter field to the same value
        self.steps = cursor;
        self.asked = cursor;
    }
}

/// Tier D: heuristics first, model adapter second, inbox last. Default for
/// this build is (heuristic, NullLLM) so it runs unattended; wiring a real
/// adapter changes nothing else.
pub struct HybridPolicy {
    pub heuristic: Option<Box<dyn Policy>>,
    pub llm: Box<dyn LlmAdapter>,
    pub llm_available: bool,
}

impl Default for HybridPolicy {
    fn default() -> Self {
        HybridPolicy {
            heuristic: None,
            llm: Box::new(NullLlm),
            llm_available: false,
        }
    }
}

impl HybridPolicy {
    pub fn with(heuristic: Box<dyn Policy>) -> Self {
        HybridPolicy {
            heuristic: Some(heuristic),
            ..Default::default()
        }
    }
}

impl Policy for HybridPolicy {
    fn name(&self) -> &'static str {
        "hybrid"
    }
    fn class_name(&self) -> &'static str {
        "HybridPolicy"
    }
    fn llm_available(&self) -> bool {
        self.llm_available
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        msg: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        let a = match &mut self.heuristic {
            Some(h) => h.step(ctx, msg)?,
            None => Action::proceed(""),
        };
        let stalled = matches!(a.act, Act::Proceed | Act::Noop) && ctx.step_index() > 0;
        if stalled && ctx.llm_available() {
            // tier B — only reached when an adapter with real judgment was injected
            let mut prompt = JMap::new();
            prompt.insert("agent".into(), JValue::Str(ctx.agent_id().to_string()));
            prompt.insert(
                "task".into(),
                ctx.task_id().map(JValue::Str).unwrap_or(JValue::Null),
            );
            prompt.insert(
                "unmet".into(),
                JValue::Arr(ctx.unmet_artifacts().into_iter().map(JValue::Str).collect()),
            );
            prompt.insert("state".into(), JValue::Str(ctx.state()));
            match self.llm.decide(&prompt) {
                Ok(action) => return Ok(action),
                Err(e) => ctx.log(&format!("llm unavailable: {e}")),
            }
        }
        Ok(a)
    }
    // NOTE: no cursor of its own — the reference's getattr finds neither
    // `_steps` nor `_worked` on a HybridPolicy, so its cursor is always 0
    // even when the wrapped policy tracks one. Quirk preserved.
}

/// Phase 2 acceptance shape: work for a while, then discover the missing
/// capability mid-task.
///
/// The distinction that matters is *when* it asks: `after_steps > 0` means the
/// request is emitted while the org is already running. `detect_from` is the
/// honest version: ask only when the current task genuinely consumes an
/// artifact whose declared producer role is not represented on the roster.
#[derive(Debug, Clone)]
pub struct NeedsSpecialist {
    pub role: String,
    pub reason: String,
    pub skills: Vec<String>,
    pub est_work: f64,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    pub capability_class: String,
    /// 0 = ask on the first step (planning-adjacent); >0 = ask after N real
    /// work steps
    pub after_steps: i64,
    /// ask because the task demands a role nobody on the roster has
    pub detect_from: String,
    pub max_requests: i64,
    asked: i64,
    steps: i64,
}

impl Default for NeedsSpecialist {
    fn default() -> Self {
        NeedsSpecialist {
            role: "payment specialist".to_string(),
            reason: "provider-specific webhook handling is outside this agent's declared skills"
                .to_string(),
            skills: vec!["payments".to_string(), "webhooks".to_string()],
            est_work: 3.0,
            inputs: Vec::new(),
            outputs: vec!["artifacts/payments-webhooks.md".to_string()],
            capability_class: "payments".to_string(),
            after_steps: 2,
            detect_from: String::new(),
            max_requests: 1,
            asked: 0,
            steps: 0,
        }
    }
}

impl NeedsSpecialist {
    fn should_ask(&self, ctx: &mut dyn PolicyContext) -> bool {
        if self.asked >= self.max_requests || ctx.step_index() <= self.after_steps {
            return false;
        }
        if self.detect_from.is_empty() {
            return true;
        }
        let Some(task) = ctx.task().cloned() else {
            return false;
        };
        if ctx.roster_roles().iter().any(|r| r == &self.detect_from) {
            return false; // the capability already exists: never ask for a duplicate
        }
        for art in &task.consumes {
            for role in ctx.producer_roles(art) {
                if role == self.detect_from {
                    return true; // something I must consume is owned by a role nobody fills
                }
            }
        }
        false
    }
}

impl NeedsSpecialist {
    pub fn with_detect_from(detect_from: &str) -> Self {
        NeedsSpecialist {
            detect_from: detect_from.to_string(),
            ..Default::default()
        }
    }
}

impl Policy for NeedsSpecialist {
    fn name(&self) -> &'static str {
        "needs-specialist"
    }
    fn class_name(&self) -> &'static str {
        "NeedsSpecialist"
    }
    fn step(
        &mut self,
        ctx: &mut dyn PolicyContext,
        msg: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        // Counted first, on every turn: the journal's TASK_PROGRESS row carries
        // the cursor, and if a step that asks or inspects mail did not advance
        // the counter, a replay would restore a lower one (Phase 2 parity).
        self.steps += 1;
        if let Some(m) = msg {
            if m.msg_type == EventType::SpawnApproved {
                let aid = payload_str(&m.payload, "agent_id");
                ctx.log(&format!("parent approved spawn: {aid}"));
            } else if m.msg_type == EventType::SpawnRejected {
                let rule = payload_str(&m.payload, "rule");
                ctx.log(&format!("parent refused spawn ({rule}); continuing alone"));
            } else if m.msg_type == EventType::RequestRerouted {
                let owner = payload_str(&m.payload, "owner");
                ctx.log(&format!("work rerouted to {owner} instead of a new agent"));
            }
        }
        if self.should_ask(ctx) {
            self.asked += 1;
            // inputs = (self.inputs or task.consumes) if task else self.inputs
            // — the reference's operator precedence, preserved
            let inputs: Vec<String> = match ctx.task() {
                Some(_) => {
                    if !self.inputs.is_empty() {
                        self.inputs.clone()
                    } else {
                        ctx.task().map(|t| t.consumes.clone()).unwrap_or_default()
                    }
                }
                None => self.inputs.clone(),
            };
            let m = ctx.request_specialist(
                &self.role.clone(),
                &self.reason.clone(),
                &self.skills.clone(),
                &inputs,
                &self.outputs.clone(),
                self.est_work,
                &self.capability_class.clone(),
            );
            return Ok(Action::publish(m, "request specialist mid-run"));
        }
        if ctx.task().is_none() {
            return Ok(Action::proceed("no task; waiting for work"));
        }
        if ctx.step_index() > self.after_steps + 6 {
            let artifacts: Vec<JValue> = ctx
                .task()
                .map(|t| t.produces.iter().map(|a| JValue::Str(a.clone())).collect())
                .unwrap_or_default();
            let mut extra = JMap::new();
            extra.insert("artifacts".into(), JValue::Arr(artifacts));
            return Ok(Action::complete_with(
                "finished with the capability available",
                extra,
            ));
        }
        Ok(Action::proceed("working"))
    }
    fn policy_cursor(&self) -> i64 {
        self.steps
    }
    fn restore_cursor(&mut self, cursor: i64) {
        self.steps = cursor;
        self.asked = cursor;
    }
}

fn payload_str(payload: &JMap, key: &str) -> String {
    // Python's None renders as "None" in f-strings; missing key → "None"
    match payload.get(key) {
        Some(JValue::Str(s)) => s.clone(),
        Some(other) => other.to_canon_string(),
        None => "None".to_string(),
    }
}

// -------------------------------------------------------------- construction

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyError2 {
    UnknownPolicy(String),
}

impl std::fmt::Display for PolicyError2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyError2::UnknownPolicy(name) => write!(
                f,
                "unknown policy '{name}'; choose from ['escalate', 'poll', 'simulated', \
                 'specialist', 'wait']"
            ),
        }
    }
}
impl std::error::Error for PolicyError2 {}

impl PolicyError2 {
    /// Python's `str(KeyError(msg))` wraps the message in its repr — double
    /// quotes when the message already contains single ones. The kernel/CLI
    /// surfaces that string verbatim, so the quoting is parity-relevant.
    pub fn keyerror_repr(&self) -> String {
        let msg = self.to_string();
        if msg.contains('\'') && !msg.contains('"') {
            format!("\"{msg}\"")
        } else {
            format!("'{msg}'")
        }
    }
}

/// Built-in policy names, exactly the reference's `BUILTIN_POLICIES` keys.
/// NOTE: "hybrid" is deliberately absent (a KeyError in the reference too).
pub const BUILTIN_POLICIES: [&str; 5] = ["simulated", "wait", "poll", "escalate", "specialist"];

fn j_str(m: &JMap, k: &str) -> Option<String> {
    m.get(k).and_then(|v| v.as_str()).map(|s| s.to_string())
}
fn j_i64(m: &JMap, k: &str) -> Option<i64> {
    m.get(k).and_then(|v| v.as_int())
}
fn j_f64(m: &JMap, k: &str) -> Option<f64> {
    m.get(k).and_then(|v| v.as_f64())
}
fn j_strs(m: &JMap, k: &str) -> Option<Vec<String>> {
    match m.get(k) {
        Some(JValue::Arr(a)) => Some(
            a.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect(),
        ),
        _ => None,
    }
}

/// The reference's `make_policy`: construct by name with overrides. Unknown
/// override keys are absorbed (the reference stores them in `_ignored`;
/// ignoring them beats crashing on them).
pub fn make_policy(name: &str, overrides: &JMap) -> Result<Box<dyn Policy>, PolicyError2> {
    match name {
        "simulated" => {
            let mut p = SimulatedWork::default();
            if let Some(v) = j_i64(overrides, "steps") {
                p.steps = v;
            }
            Ok(Box::new(p))
        }
        "wait" => {
            let mut p = WaitForArtifacts::default();
            if let Some(v) = j_f64(overrides, "timeout") {
                p.timeout = Some(v);
            }
            if let Some(v) = j_i64(overrides, "steps") {
                p.steps = v;
            }
            Ok(Box::new(p))
        }
        "poll" => {
            let mut p = PollUntilReady::default();
            if let Some(v) = j_i64(overrides, "limit") {
                p.limit = v;
            }
            Ok(Box::new(p))
        }
        "escalate" => {
            let mut p = EscalateOnComplexity::default();
            if let Some(v) = j_f64(overrides, "threshold") {
                p.threshold = v;
            }
            if let Some(v) = j_i64(overrides, "request_spawns") {
                p.request_spawns = v;
            }
            if let Some(v) = j_str(overrides, "role") {
                p.role = v;
            }
            if let Some(v) = j_str(overrides, "reason") {
                p.reason = v;
            }
            if let Some(v) = j_strs(overrides, "skills") {
                p.skills = v;
            }
            if let Some(v) = j_f64(overrides, "est_work") {
                p.est_work = v;
            }
            if let Some(v) = j_strs(overrides, "produces") {
                p.produces = v;
            }
            if let Some(v) = j_str(overrides, "new_task_id") {
                p.new_task_id = v;
            }
            if let Some(v) = j_i64(overrides, "max_steps") {
                p.max_steps = v;
            }
            Ok(Box::new(p))
        }
        "specialist" => {
            let mut p = NeedsSpecialist::default();
            if let Some(v) = j_str(overrides, "role") {
                p.role = v;
            }
            if let Some(v) = j_str(overrides, "reason") {
                p.reason = v;
            }
            if let Some(v) = j_strs(overrides, "skills") {
                p.skills = v;
            }
            if let Some(v) = j_f64(overrides, "est_work") {
                p.est_work = v;
            }
            if let Some(v) = j_strs(overrides, "inputs") {
                p.inputs = v;
            }
            if let Some(v) = j_strs(overrides, "outputs") {
                p.outputs = v;
            }
            if let Some(v) = j_str(overrides, "capability_class") {
                p.capability_class = v;
            }
            if let Some(v) = j_i64(overrides, "after_steps") {
                p.after_steps = v;
            }
            if let Some(v) = j_str(overrides, "detect_from") {
                p.detect_from = v;
            }
            if let Some(v) = j_i64(overrides, "max_requests") {
                p.max_requests = v;
            }
            Ok(Box::new(p))
        }
        other => Err(PolicyError2::UnknownPolicy(other.to_string())),
    }
}

#[cfg(test)]
mod tests;
