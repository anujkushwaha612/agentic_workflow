//! The cognition seam (Phase 2.5 M0/M2): proposal structures + the swap
//! point, with no execution (`arena/cognition.py`).
//!
//! ```text
//!     Cognition decides.  The runtime validates and executes.
//!     Neither can impersonate the other.
//! ```
//!
//! What lives here: [`Observation`] (what an agent is allowed to know),
//! [`Intent`] (what it may propose), [`Control`] (the verbs the runtime
//! recognises), the [`Cognition`] trait, and [`PolicyCognition`] — the
//! adapter that makes every existing policy a cognition source without
//! changing a line of policy code.
//!
//! What deliberately does NOT live here: subprocesses, filesystems, the bus,
//! the graph, the registry. There is no import of tools/kernel/graph in this
//! module, and that is the enforcement: a cognition layer that cannot name
//! `open()` cannot write a file. `LLMCognition` / `ArenaCognition` /
//! `LocalModelCognition` will obey the same rule; the only thing that changes
//! between them and [`PolicyCognition`] is what `decide()` does with the
//! Observation. The boundary is drawn here rather than inside the actor:
//! [`PolicyCognition`] hands a policy a read-only projection
//! ([`crate::policy::PolicyContext`]) so a policy keeps working exactly as it
//! did — it returns an [`crate::policy::Action`], which was always a proposal
//! — and the runtime converts the proposal into an Intent. The conversion is
//! one-directional: nothing in this module can turn an Intent back into a
//! kernel mutation.

use crate::policy::{Act, Policy, PolicyContext};
use crate::sys::json::{py_repr, JMap, JValue};
use crate::sys::sha256::hex_digest;

// --------------------------------------------------------------------- verbs

/// The only ways an Intent can end a turn. Strings, not an enum, so a
/// provider that returns `{"control": "WAIT"}` needs no import path into our
/// type system to be validated.
pub struct Control;

impl Control {
    pub const NONE: &'static str = "";
    pub const WAIT: &'static str = "WAIT";
    pub const PUBLISH: &'static str = "PUBLISH";
    pub const SPAWN_REQUEST: &'static str = "SPAWN_REQUEST";
    pub const ESCALATE: &'static str = "ESCALATE";
    pub const COMPLETE: &'static str = "COMPLETE";
    pub const VERIFY: &'static str = "VERIFY";
    pub const NOOP: &'static str = "NOOP";

    pub const ALL: [&'static str; 7] = [
        Self::WAIT,
        Self::PUBLISH,
        Self::SPAWN_REQUEST,
        Self::ESCALATE,
        Self::COMPLETE,
        Self::VERIFY,
        Self::NOOP,
    ];
}

/// Verbs that mean "this agent is done acting this turn" — kept in sync with
/// the kernel's SLEEPING_ACTIONS so a cognition-driven turn and a
/// policy-driven turn cannot disagree about when the inbox drain stops.
/// (Phase-2 defect D2 was exactly this pair drifting.)
pub const YIELDING: [&str; 3] = [Control::WAIT, Control::ESCALATE, Control::COMPLETE];

// ----------------------------------------------------------------- outcome

/// A tool result as cognition is allowed to see it: text, digests, exit
/// codes. Never a file handle, never a path outside the workspace.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Outcome {
    pub tool: String,
    pub ok: bool,
    pub exit_code: Option<i64>,
    pub text: String,
    pub data: JMap,
    pub refused: String,
    pub rid: String,
}

impl Outcome {
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("tool".into(), JValue::Str(self.tool.clone()));
        m.insert("ok".into(), JValue::Bool(self.ok));
        m.insert(
            "exit_code".into(),
            self.exit_code.map(JValue::Int).unwrap_or(JValue::Null),
        );
        m.insert("refused".into(), JValue::Str(self.refused.clone()));
        m.insert("data".into(), JValue::Obj(self.data.clone()));
        m
    }
}

/// The fields `outcomes_from_results` reads off a ToolResult. Kept here (not
/// in tools) so the direction of knowledge is one-way: the runtime converts,
/// the cognition layer never imports the executor. The M10 ToolResult will
/// convert into this view.
#[derive(Debug, Clone, Default)]
pub struct ToolResultView {
    pub tool: String,
    pub ok: bool,
    pub exit_code: Option<i64>,
    pub block: String,
    pub data: JMap,
    pub refused: String,
    pub rid: String,
}

/// ToolResult -> Outcome, keeping only int/float/str/bool data values
/// (the reference's `isinstance` filter).
pub fn outcomes_from_results(results: &[ToolResultView]) -> Vec<Outcome> {
    results
        .iter()
        .map(|r| Outcome {
            // getattr(r, "tool", "?") — the "?" default only fires for objects
            // with NO tool attribute; the struct field always exists, so the
            // value passes through verbatim ("" included)
            tool: r.tool.clone(),
            ok: r.ok,
            exit_code: r.exit_code,
            text: r.block.clone(),
            data: r
                .data
                .iter()
                .filter(|(_, v)| {
                    matches!(
                        v,
                        JValue::Int(_) | JValue::Float(_) | JValue::Str(_) | JValue::Bool(_)
                    )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            refused: r.refused.clone(),
            rid: r.rid.clone(),
        })
        .collect()
}

// ------------------------------------------------------------- observation

/// Everything an agent may know at the start of a turn.
///
/// The read-only policy projection travels separately (the actor hands it to
/// `decide`); a provider cognition source reads the explicit fields instead,
/// because those are what a future serialisable prompt is built from.
#[derive(Debug, Clone, Default)]
pub struct Observation {
    pub agent_id: String,
    pub role: String,
    pub goal: String,
    pub task: JMap,
    pub message: Option<crate::msg::Message>,
    pub step_index: i64,
    pub unmet: Vec<String>,
    pub consumed_ready: Vec<String>,
    pub graph_view: Vec<JMap>,
    pub unread: Vec<crate::msg::Message>,
    pub recent: Vec<Outcome>,
    /// every tool result this agent has produced, newest last — the
    /// "reason again" half of the loop. Owned per agent: there is
    /// deliberately no shared store, and two agents cannot share a brain.
    pub history: Vec<Outcome>,
    pub workspace: JMap,
    pub budget: JMap,
    pub notes: Vec<String>,
    pub state: String,
    pub tool_schemas: Vec<JMap>,
    /// appended transcript of this agent's own turns, oldest first
    pub transcript_len: i64,
}

impl Observation {
    /// The provider-facing view: explicit fields only, no live references.
    pub fn prompt_seed(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
        m.insert("role".into(), JValue::Str(self.role.clone()));
        m.insert("goal".into(), JValue::Str(self.goal.clone()));
        m.insert("task".into(), JValue::Obj(self.task.clone()));
        m.insert("state".into(), JValue::Str(self.state.clone()));
        m.insert(
            "unmet".into(),
            JValue::Arr(self.unmet.iter().cloned().map(JValue::Str).collect()),
        );
        m.insert(
            "graph_view".into(),
            JValue::Arr(
                self.graph_view
                    .iter()
                    .map(|g| JValue::Obj(g.clone()))
                    .collect(),
            ),
        );
        m.insert("budget".into(), JValue::Obj(self.budget.clone()));
        m.insert("workspace".into(), JValue::Obj(self.workspace.clone()));
        m.insert(
            "recent".into(),
            JValue::Arr(
                self.recent
                    .iter()
                    .map(|o| JValue::Obj(o.to_dict()))
                    .collect(),
            ),
        );
        let tail: Vec<JValue> = self
            .history
            .iter()
            .rev()
            .take(6)
            .rev()
            .map(|o| JValue::Obj(o.to_dict()))
            .collect();
        m.insert("history_tail".into(), JValue::Arr(tail));
        m.insert(
            "notes".into(),
            JValue::Arr(self.notes.iter().cloned().map(JValue::Str).collect()),
        );
        m.insert(
            "tools".into(),
            JValue::Arr(
                self.tool_schemas
                    .iter()
                    .map(|s| JValue::Obj(s.clone()))
                    .collect(),
            ),
        );
        m
    }

    /// A compact text view, so a deterministic policy and a log reader see
    /// the same facts. Byte-parity with the reference's `render()`.
    pub fn render(&self) -> String {
        let get = |k: &str| -> String {
            match self.task.get(k) {
                Some(JValue::Str(s)) => s.clone(),
                Some(JValue::Null) | None => "None".to_string(),
                Some(other) => py_repr(other), // repr fallback
            }
        };
        let verify_len = match self.task.get("verify") {
            Some(JValue::Arr(a)) => a.len(),
            _ => 0,
        };
        // f-string of .get(): lists render as repr, strings raw, missing None
        let produces = match self.task.get("produces") {
            Some(v @ JValue::Arr(_)) => py_repr(v),
            Some(JValue::Str(s)) => s.clone(),
            Some(other) => py_repr(other),
            None => "None".to_string(),
        };
        let mut lines = vec![
            format!(
                "agent={} role={} state={} step={}",
                self.agent_id, self.role, self.state, self.step_index
            ),
            format!(
                "task={} status={} produces={} verify={}",
                get("task_id"),
                get("status"),
                produces,
                verify_len
            ),
        ];
        if !self.unmet.is_empty() {
            lines.push(format!(
                "unmet={}",
                py_repr(&JValue::Arr(
                    self.unmet.iter().cloned().map(JValue::Str).collect()
                ))
            ));
        }
        if !self.workspace.is_empty() {
            let files = self
                .workspace
                .get("files")
                .map(py_repr)
                .unwrap_or_else(|| "None".to_string());
            let dirty = self
                .workspace
                .get("dirty")
                .map(py_repr)
                .unwrap_or_else(|| "None".to_string());
            lines.push(format!("workspace files={files} dirty={dirty}"));
        }
        for o in self.recent.iter().rev().take(3).rev() {
            // the reference writes "exit=... " then appends "" or "refused=...",
            // so the trailing space is present even when there is no refusal
            let refused = if o.refused.is_empty() {
                String::new()
            } else {
                format!("refused={}", o.refused)
            };
            let exit = match o.exit_code {
                Some(e) => e.to_string(),
                None => "None".to_string(),
            };
            // f-string of a Python bool renders True/False
            let ok_txt = if o.ok { "True" } else { "False" };
            lines.push(format!(
                "last {}: ok={} exit={} {}",
                o.tool, ok_txt, exit, refused
            ));
        }
        lines.join("\n")
    }
}

// ------------------------------------------------------------------- intent

/// A tool call proposal. `args` is an open JSON object.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolCall {
    pub tool: String,
    pub args: JMap,
}

impl ToolCall {
    pub fn new(tool: &str, args: JMap) -> ToolCall {
        ToolCall {
            tool: tool.to_string(),
            args,
        }
    }
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("tool".into(), JValue::Str(self.tool.clone()));
        m.insert("args".into(), JValue::Obj(self.args.clone()));
        m
    }

    /// The reference's `Intent.of` dict path: `{"tool": ..., "args": {...}}`
    /// with a missing/None tool becoming "" and non-object args dropped.
    pub fn from_dict(v: &JValue) -> ToolCall {
        let mut tool = String::new();
        let mut args = JMap::new();
        if let JValue::Obj(m) = v {
            if let Some(JValue::Str(s)) = m.get("tool") {
                tool = s.clone();
            }
            if let Some(JValue::Obj(a)) = m.get("args") {
                args = a.clone();
            }
        }
        ToolCall { tool, args }
    }
}

/// A proposal. `calls` run before `control` is applied, and the runtime may
/// refuse either.
///
/// The reference carries the proposed Message *object* under
/// `publish["msg"]` / `spawn["msg"]`; here the typed [`Message`] rides
/// alongside and `to_dict()` materialises the same `{"msg": <view>}` shape
/// (the runtime reads the typed field back — a JSON view is lossless for
/// that, but the typed carrier avoids reconstructing identity fields).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Intent {
    pub calls: Vec<ToolCall>,
    pub control: String,
    pub think: String,
    pub reason: String,
    pub wait_for: String,
    pub publish: JMap,
    pub spawn: JMap,
    /// the message carrier when control is PUBLISH / SPAWN_REQUEST
    pub msg: Option<crate::msg::Message>,
    pub max_calls: i64,
}

impl Intent {
    pub fn new() -> Intent {
        Intent {
            max_calls: 4,
            ..Default::default()
        }
    }

    pub fn is_noop(&self) -> bool {
        self.calls.is_empty() && (self.control.is_empty() || self.control == Control::NOOP)
    }

    /// Key order fixed by the reference's `to_dict` (this order is what the
    /// fingerprint hashes). A carried message materialises as
    /// `{"msg": <message view>}` exactly like the reference's dict.
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert(
            "calls".into(),
            JValue::Arr(
                self.calls
                    .iter()
                    .map(|c| JValue::Obj(c.to_dict()))
                    .collect(),
            ),
        );
        m.insert("control".into(), JValue::Str(self.control.clone()));
        m.insert("think".into(), JValue::Str(self.think.clone()));
        m.insert("reason".into(), JValue::Str(self.reason.clone()));
        m.insert("wait_for".into(), JValue::Str(self.wait_for.clone()));
        let publish = self.verb_map(false);
        let spawn = self.verb_map(true);
        m.insert("publish".into(), JValue::Obj(publish));
        m.insert("spawn".into(), JValue::Obj(spawn));
        m
    }

    /// publish/spawn dict as the reference builds it: `{"msg": ...}` on the
    /// side the verb addresses, else the payload map as-is.
    fn verb_map(&self, spawn_side: bool) -> JMap {
        let carries_spawn = self.control == Control::SPAWN_REQUEST;
        if let Some(m) = &self.msg {
            if spawn_side == carries_spawn {
                let mut v = JMap::new();
                v.insert("msg".into(), JValue::Obj(m.to_dict()));
                return v;
            }
        }
        if spawn_side {
            self.spawn.clone()
        } else {
            self.publish.clone()
        }
    }

    /// sha256 of the CPython repr of `[to_dict()]`, truncated to 12 hex
    /// chars — byte-identical with the reference for the same semantic
    /// content (calls order, control/think/reason/wait_for strings,
    /// publish/spawn maps; a carried Message renders with its dataclass
    /// repr exactly as the reference's embedded object does).
    pub fn fingerprint(&self) -> String {
        use crate::sys::json::{py_repr, py_str_repr};
        // ToolCall.to_dict order is fixed by the reference (tool, args)
        let call_repr = |c: &ToolCall| {
            format!(
                "{{'tool': {}, 'args': {}}}",
                crate::sys::json::py_str_repr(&c.tool),
                py_repr(&JValue::Obj(c.args.clone()))
            )
        };
        let calls = format!(
            "[{}]",
            self.calls
                .iter()
                .map(call_repr)
                .collect::<Vec<_>>()
                .join(", ")
        );
        let dict_repr = |m: &JMap| py_repr(&JValue::Obj(m.clone()));
        // the carried message renders on the side its verb addresses (the
        // reference builds exactly one of publish={"msg": ...} / spawn={"msg": ...})
        let carries_spawn = self.control == Control::SPAWN_REQUEST;
        let msg_repr = self
            .msg
            .as_ref()
            .map(|m| format!("{{'msg': {}}}", m.py_repr()));
        let publish = if carries_spawn {
            dict_repr(&self.publish)
        } else {
            msg_repr.clone().unwrap_or_else(|| dict_repr(&self.publish))
        };
        let spawn = if carries_spawn {
            msg_repr.unwrap_or_else(|| dict_repr(&self.spawn))
        } else {
            dict_repr(&self.spawn)
        };
        let blob = format!(
            "[{{'calls': {}, 'control': {}, 'think': {}, 'reason': {}, 'wait_for': {}, 'publish': {}, 'spawn': {}}}]",
            calls,
            py_str_repr(&self.control),
            py_str_repr(&self.think),
            py_str_repr(&self.reason),
            py_str_repr(&self.wait_for),
            publish,
            spawn,
        );
        hex_digest(blob.as_bytes())[..12].to_string()
    }

    /// The reference's `Intent.of(*calls, control=..., **kw)` for
    /// dict-shaped provider output.
    pub fn of(call_dicts: &[JValue], control: &str, think: &str) -> Intent {
        Intent {
            calls: call_dicts.iter().map(ToolCall::from_dict).collect(),
            control: control.to_string(),
            think: think.to_string(),
            ..Intent::new()
        }
    }
}

// ------------------------------------------------------------- validation

/// Runtime-side validation. Returns the (possibly truncated) intent + refusal
/// notes. Never fails: an out-of-range proposal is data about the cognition
/// layer, and the journal is where that data belongs.
///
/// The reference also handles a non-Intent return (`INTENT_NOT_AN_INTENT`);
/// Rust's type system makes that case unrepresentable — a future provider
/// that parses model output builds its Intent via [`Intent::of`], where a
/// malformed call becomes a `""` tool that this function refuses with
/// `TOOL_NOT_ALLOWED` exactly as the reference does.
pub fn validate_intent(
    intent: Option<Intent>,
    allowed_tools: &[String],
    max_calls: i64,
) -> (Intent, Vec<String>) {
    let mut notes: Vec<String> = Vec::new();
    let intent = match intent {
        None => {
            return (
                Intent {
                    control: Control::NOOP.to_string(),
                    reason: "cognition returned nothing".to_string(),
                    ..Intent::new()
                },
                vec!["INTENT_MISSING".to_string()],
            );
        }
        Some(i) => i,
    };
    let mut intent = intent;
    if !intent.control.is_empty() && !Control::ALL.contains(&intent.control.as_str()) {
        notes.push(format!("CONTROL_UNKNOWN:{}", intent.control));
        // rebuild: calls and think survive; the verb, wait_for, publish,
        // spawn and the carried message do not
        intent = Intent {
            calls: intent.calls.clone(),
            control: Control::NOOP.to_string(),
            think: intent.think.clone(),
            reason: format!(
                "unknown control {}; treated as NOOP",
                crate::sys::json::py_str_repr(&intent.control)
            ),
            ..Intent::new()
        };
    }
    let mut calls: Vec<ToolCall> = Vec::new();
    let mut dropped = 0usize;
    for c in &intent.calls {
        if !allowed_tools.contains(&c.tool) {
            notes.push(format!("TOOL_NOT_ALLOWED:{}", c.tool));
            dropped += 1;
            continue;
        }
        calls.push(c.clone());
    }
    if calls.len() as i64 > max_calls {
        notes.push(format!("CALLS_TRUNCATED:{}>{}", calls.len(), max_calls));
        calls.truncate(max_calls as usize);
    }
    // NOTE: `intent.control` here is the possibly-rebuilt control, and NOOP
    // is a truthy verb in the reference — so a rebuilt intent never enters
    // the paper-trail branch even with every call refused.
    if dropped > 0 && calls.is_empty() && intent.control.is_empty() {
        return (
            Intent {
                control: Control::NOOP.to_string(),
                think: intent.think.clone(),
                reason: format!(
                    "all {} call(s) refused: {}",
                    dropped,
                    notes.iter().take(3).cloned().collect::<Vec<_>>().join(";")
                ),
                ..Intent::new()
            },
            notes,
        );
    }
    if calls.len() == intent.calls.len() {
        return (intent, notes);
    }
    (
        Intent {
            calls,
            control: intent.control.clone(),
            think: intent.think.clone(),
            reason: intent.reason.clone(),
            wait_for: intent.wait_for.clone(),
            publish: intent.publish.clone(),
            spawn: intent.spawn.clone(),
            msg: intent.msg.clone(),
            max_calls,
        },
        notes,
    )
}

// ---------------------------------------------------------------- protocol

/// A cognition failure. The actor journals capability breaches and ordinary
/// failures differently (`kind="capability"` vs `kind="error"`); neither ever
/// mutates the kernel.
#[derive(Debug, Clone, PartialEq)]
pub enum CognitionError {
    /// The source asked for something the runtime does not give it. In the
    /// reference this is `CognitionCapabilityError` (a policy reaching for a
    /// context member the read-only projection does not expose); in Rust the
    /// projection makes that unrepresentable at compile time, so this variant
    /// is raised by hand-built misuse and future providers.
    Capability(String),
    /// A model that is down, or a policy that crashed.
    Other(String),
}

impl std::fmt::Display for CognitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CognitionError::Capability(m) | CognitionError::Other(m) => m,
        })
    }
}
impl std::error::Error for CognitionError {}

/// The swap point. Nothing else in the kernel needs to know which
/// implementation is bound. Future implementations (`LLMCognition`,
/// `ArenaCognition`, `LocalModelCognition`) implement this same trait without
/// changing AgentActor; none of them exist yet.
pub trait Cognition {
    fn name(&self) -> &str;
    /// Propose an Intent from the observation. The read-only policy
    /// projection is handed alongside so the policy tier keeps working; a
    /// provider source reads the explicit observation fields instead.
    fn decide(
        &mut self,
        obs: &Observation,
        ctx: &mut dyn PolicyContext,
    ) -> Result<Intent, CognitionError>;
    /// Introspection that makes an *accidentally shared* brain visible rather
    /// than plausible.
    fn fingerprint(&self) -> JMap;
}

// ------------------------------------------------------------------ adapter

/// Adapter: an existing policy becomes a [`Cognition`] source. `Action` was
/// already a proposal the runtime applies, so this adapter is a translation,
/// not a privilege.
pub struct PolicyCognition {
    pub policy: Box<dyn Policy>,
    pub agent_id: String,
    pub name: String,
    pub model_id: String,
}

impl PolicyCognition {
    pub fn new(policy: Box<dyn Policy>, agent_id: &str) -> PolicyCognition {
        let name = format!("policy:{}", policy.name());
        PolicyCognition {
            policy,
            agent_id: agent_id.to_string(),
            name,
            model_id: "deterministic".to_string(),
        }
    }
}

impl Cognition for PolicyCognition {
    fn name(&self) -> &str {
        &self.name
    }

    fn fingerprint(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("source".into(), JValue::Str(self.name.clone()));
        m.insert("kind".into(), JValue::Str("policy".into()));
        m.insert("model".into(), JValue::Str(self.model_id.clone()));
        m.insert(
            "policy".into(),
            JValue::Str(self.policy.class_name().to_string()),
        );
        // NOTE: the reference also reports the object's memory address as
        // obj_id; Rust has no stable equivalent, so the shared-brain audit
        // relies on prompt_sha256_16 (deterministic) and the per-instance
        // transcript counters instead.
        let seed = format!("{}|{}", self.policy.class_name(), self.agent_id);
        m.insert(
            "prompt_sha256_16".into(),
            JValue::Str(hex_digest(seed.as_bytes())[..16].to_string()),
        );
        m
    }

    fn decide(
        &mut self,
        obs: &Observation,
        ctx: &mut dyn PolicyContext,
    ) -> Result<Intent, CognitionError> {
        let action = self
            .policy
            .step(ctx, obs.message.as_ref())
            .map_err(|e| CognitionError::Other(e.to_string()))?;
        Ok(self.translate(&action))
    }
}

impl PolicyCognition {
    /// Action -> Intent, the reference's `_translate` mapping verbatim.
    pub(crate) fn translate(&self, a: &crate::policy::Action) -> Intent {
        let mut i = Intent::new();
        match a.act {
            Act::Publish => {
                if let Some(msg) = &a.msg {
                    i.reason = if a.reason.is_empty() {
                        "publish".to_string()
                    } else {
                        a.reason.clone()
                    };
                    if msg.msg_type == crate::msg::EventType::SpawnAgentRequest {
                        i.control = Control::SPAWN_REQUEST.to_string();
                        if a.reason.is_empty() {
                            i.reason = "spawn request".to_string();
                        }
                        i.msg = Some(msg.clone());
                    } else {
                        i.control = Control::PUBLISH.to_string();
                        i.msg = Some(msg.clone());
                    }
                    return i;
                }
                i.control = Control::NONE.to_string();
                i.reason = if a.reason.is_empty() {
                    "proceed".to_string()
                } else {
                    a.reason.clone()
                };
                i
            }
            Act::Wait => {
                i.control = Control::WAIT.to_string();
                i.reason = if a.reason.is_empty() {
                    String::new()
                } else {
                    a.reason.clone()
                };
                i.wait_for = a.condition.clone();
                i
            }
            Act::Complete => {
                i.control = Control::COMPLETE.to_string();
                i.reason = if a.reason.is_empty() {
                    "complete".to_string()
                } else {
                    a.reason.clone()
                };
                // the reference does list(extra.get("artifacts") or []): a
                // missing/falsy value becomes [], and a plain string would be
                // exploded into characters — both reproduced
                let artifacts = match a.extra.get("artifacts") {
                    Some(JValue::Arr(items)) => items.clone(),
                    Some(JValue::Str(s)) if !s.is_empty() => {
                        s.chars().map(|c| JValue::Str(c.to_string())).collect()
                    }
                    _ => vec![],
                };
                let mut publish = JMap::new();
                publish.insert("artifacts".into(), JValue::Arr(artifacts));
                i.publish = publish;
                i
            }
            Act::Escalate => {
                i.control = Control::ESCALATE.to_string();
                i.reason = if a.reason.is_empty() {
                    "escalate".to_string()
                } else {
                    a.reason.clone()
                };
                let mut publish = JMap::new();
                publish.insert("extra".into(), JValue::Obj(a.extra.clone()));
                i.publish = publish;
                i
            }
            Act::Noop => {
                i.control = Control::NOOP.to_string();
                i.reason = if a.reason.is_empty() {
                    String::new()
                } else {
                    a.reason.clone()
                };
                i
            }
            Act::Proceed => {
                i.control = Control::NONE.to_string();
                i.reason = if a.reason.is_empty() {
                    "proceed".to_string()
                } else {
                    a.reason.clone()
                };
                i
            }
        }
    }
}

#[cfg(test)]
mod tests;
