//! Spawn request as a first-class object (`arena/spawn.py`).
//!
//! The request FSM is deliberately separate from the agent lifecycle: a spawned
//! agent is born the same way a planned one is. The ledger is an in-memory
//! index over the journal (dedup + monitoring) and is rebuilt by fold on
//! recovery so it can never outrun the log.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::graph::TaskSpec;
use crate::msg::Message;
use crate::sys::json::{JMap, JValue};
use crate::sys::sha256;

// --------------------------------------------------------------------------- vocabulary

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestState {
    Received,
    Evaluating,
    ApprovedCommitted,
    Rerouted,
    Rejected,
    Deduplicated,
    Escalated,
    Deferred,
}

impl RequestState {
    pub fn as_str(&self) -> &'static str {
        match self {
            RequestState::Received => "RECEIVED",
            RequestState::Evaluating => "EVALUATING",
            RequestState::ApprovedCommitted => "APPROVED_COMMITTED",
            RequestState::Rerouted => "REROUTED",
            RequestState::Rejected => "REJECTED",
            RequestState::Deduplicated => "DEDUPLICATED",
            RequestState::Escalated => "ESCALATED",
            RequestState::Deferred => "DEFERRED",
        }
    }
    pub fn parse(s: &str) -> Option<RequestState> {
        Some(match s {
            "RECEIVED" => RequestState::Received,
            "EVALUATING" => RequestState::Evaluating,
            "APPROVED_COMMITTED" => RequestState::ApprovedCommitted,
            "REROUTED" => RequestState::Rerouted,
            "REJECTED" => RequestState::Rejected,
            "DEDUPLICATED" => RequestState::Deduplicated,
            "ESCALATED" => RequestState::Escalated,
            "DEFERRED" => RequestState::Deferred,
            _ => return None,
        })
    }
}

/// Terminal states: a request in one of these never moves again, and a
/// duplicate arriving later is matched against them.
pub fn is_terminal(s: RequestState) -> bool {
    matches!(
        s,
        RequestState::ApprovedCommitted
            | RequestState::Rerouted
            | RequestState::Rejected
            | RequestState::Deduplicated
            | RequestState::Escalated
    )
}

pub fn allowed_transitions(from: RequestState) -> &'static [RequestState] {
    match from {
        RequestState::Received => &[RequestState::Evaluating],
        RequestState::Evaluating => &[
            RequestState::ApprovedCommitted,
            RequestState::Rerouted,
            RequestState::Rejected,
            RequestState::Deduplicated,
            RequestState::Escalated,
            RequestState::Deferred,
        ],
        RequestState::Deferred => &[
            RequestState::Evaluating,
            RequestState::ApprovedCommitted,
            RequestState::Rerouted,
            RequestState::Rejected,
        ],
        _ => &[],
    }
}

pub const RULES: &[&str] = &[
    "APPROVE",
    "REJECT_NOT_WORTH_IT",
    "REJECT_CAP",
    "REJECT_DUPLICATE_CAPABILITY",
    "REJECT_SPAWN_DEPTH",
    "REJECT_UNSUPPORTED",
    "REJECT_MALFORMED",
    "REJECT_CYCLE",
    "ESCALATE",
    "DEDUPLICATE",
    "DEFER_FOR_CAPACITY",
];

pub const REQUIRED_FIELDS: &[&str] = &[
    "requester_agent_id",
    "requested_role",
    "reason",
    "estimated_work",
    "expected_outputs",
    "correlation_id",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MalformedRequest(pub String);

impl std::fmt::Display for MalformedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for MalformedRequest {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IllegalRequestTransition {
    pub frm: RequestState,
    pub to: RequestState,
}

impl std::fmt::Display for IllegalRequestTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "illegal request transition: {} -> {}",
            self.frm.as_str(),
            self.to.as_str()
        )
    }
}
impl std::error::Error for IllegalRequestTransition {}

// --------------------------------------------------------------------------- catalog

#[derive(Debug, Clone)]
pub struct CapabilityCatalog {
    pub serviceable: BTreeMap<String, Vec<String>>,
    pub unserviceable: BTreeMap<String, String>,
}

impl Default for CapabilityCatalog {
    fn default() -> Self {
        let mut serviceable = BTreeMap::new();
        for (k, v) in [
            ("general", &["general", "documentation", "review"][..]),
            ("api", &["api", "backend", "service", "integration"][..]),
            ("frontend", &["frontend", "ui", "dashboard"][..]),
            ("data", &["data", "analytics", "pipeline", "etl"][..]),
            ("database", &["database", "schema", "migration"][..]),
            ("security", &["security", "auth", "crypto"][..]),
            (
                "payments",
                &["payments", "billing", "stripe", "webhooks"][..],
            ),
            ("cloud", &["cloud", "infra", "devops"][..]),
            ("ml", &["ml", "model", "training-script"][..]),
        ] {
            serviceable.insert(k.to_string(), v.iter().map(|s| (*s).to_string()).collect());
        }
        let mut unserviceable = BTreeMap::new();
        unserviceable.insert(
            "gpu-training".into(),
            "no accelerator on this host; produce a training plan instead".into(),
        );
        unserviceable.insert(
            "k8s-ops".into(),
            "no cluster credentials in the sandbox; produce manifests instead".into(),
        );
        unserviceable.insert(
            "db-admin".into(),
            "no live database; produce migrations instead".into(),
        );
        unserviceable.insert(
            "prod-deploy".into(),
            "no production access; produce a runbook instead".into(),
        );
        CapabilityCatalog {
            serviceable,
            unserviceable,
        }
    }
}

impl CapabilityCatalog {
    pub fn known(&self, capability_class: &str) -> bool {
        let c = capability_class.trim().to_lowercase();
        let c = if c.is_empty() {
            "general".to_string()
        } else {
            c
        };
        self.serviceable.contains_key(&c) || self.unserviceable.contains_key(&c)
    }

    pub fn is_serviceable(&self, capability_class: &str) -> bool {
        let c = capability_class.trim().to_lowercase();
        let c = if c.is_empty() {
            "general".to_string()
        } else {
            c
        };
        self.serviceable.contains_key(&c)
    }

    pub fn reason_for_unserviceable(&self, capability_class: &str) -> String {
        self.unserviceable
            .get(&capability_class.trim().to_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    /// Longest matching token wins.
    pub fn class_for(&self, role: &str, skills: &[String]) -> String {
        let mut tokens: BTreeSet<String> = role
            .to_lowercase()
            .replace(['/', '-'], " ")
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        for s in skills {
            let t = s.trim().to_lowercase();
            if !t.is_empty() {
                tokens.insert(t);
            }
        }
        let mut best = "general".to_string();
        let mut best_len = 0usize;
        for (cls, kws) in &self.serviceable {
            for kw in kws {
                if tokens.contains(kw) && kw.len() > best_len {
                    best = cls.clone();
                    best_len = kw.len();
                }
            }
        }
        best
    }

    pub fn as_dict(&self) -> JMap {
        let mut svc = JMap::new();
        for (k, v) in &self.serviceable {
            svc.insert(
                k.clone(),
                JValue::Arr(v.iter().cloned().map(JValue::Str).collect()),
            );
        }
        let mut uns = JMap::new();
        for (k, v) in &self.unserviceable {
            uns.insert(k.clone(), JValue::Str(v.clone()));
        }
        let mut m = JMap::new();
        m.insert("serviceable".into(), JValue::Obj(svc));
        m.insert("unserviceable".into(), JValue::Obj(uns));
        m
    }
}

// --------------------------------------------------------------------------- request

#[derive(Debug, Clone, PartialEq)]
pub struct SpawnRequest {
    pub requester_agent_id: String,
    pub requested_role: String,
    pub reason: String,
    pub required_skills: Vec<String>,
    pub estimated_work: f64,
    pub required_inputs: Vec<String>,
    pub expected_outputs: Vec<String>,
    pub parent_task_id: Option<String>,
    pub correlation_id: String,
    pub capability_class: String,
    pub requires_judgment: bool,
    pub preferred_owner: Option<String>,
    pub mid: String,
    pub rid: String,
    pub state: RequestState,
    pub rule: String,
    pub detail: String,
    pub at_tick: i64,
    pub created_at: f64,
}

impl Default for SpawnRequest {
    fn default() -> Self {
        SpawnRequest {
            requester_agent_id: String::new(),
            requested_role: String::new(),
            reason: String::new(),
            required_skills: Vec::new(),
            estimated_work: 0.0,
            required_inputs: Vec::new(),
            expected_outputs: Vec::new(),
            parent_task_id: None,
            correlation_id: String::new(),
            capability_class: "general".into(),
            requires_judgment: false,
            preferred_owner: None,
            mid: String::new(),
            rid: String::new(),
            state: RequestState::Received,
            rule: String::new(),
            detail: String::new(),
            at_tick: -1,
            created_at: 0.0,
        }
    }
}

impl SpawnRequest {
    pub fn from_message(msg: &Message, requester: &str) -> Result<SpawnRequest, MalformedRequest> {
        let p = &msg.payload;
        let from_actor = if !requester.is_empty() {
            requester.to_string()
        } else if !msg.from_actor.as_str().is_empty() {
            msg.from_actor.as_str().to_string()
        } else {
            p.get("from")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let role = p
            .get("requested_role")
            .and_then(|v| v.as_str())
            .or_else(|| p.get("role").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        let skills = {
            let s = str_list(p.get("required_skills"));
            if s.is_empty() {
                str_list(p.get("skills"))
            } else {
                s
            }
        };
        let outputs = {
            let o = str_list(p.get("expected_outputs"));
            if o.is_empty() {
                str_list(p.get("produces"))
            } else {
                o
            }
        };
        let est = match p
            .get("estimated_work")
            .or_else(|| p.get("work_estimate"))
            .and_then(|v| match v {
                JValue::Int(i) => Some(*i as f64),
                JValue::Float(f) => Some(*f),
                JValue::Str(s) => s.parse().ok(),
                JValue::Null => Some(0.0),
                _ => None,
            }) {
            Some(v) => v,
            None => {
                if p.get("estimated_work").is_none() && p.get("work_estimate").is_none() {
                    0.0
                } else {
                    return Err(MalformedRequest(format!(
                        "estimated_work is not a number: {:?}",
                        p.get("estimated_work")
                    )));
                }
            }
        };
        let parent_task_id = p
            .get("parent_task_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                p.get("task_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .or_else(|| msg.task_id.as_ref().map(|t| t.as_str().to_string()));
        let mut req = SpawnRequest {
            requester_agent_id: from_actor,
            requested_role: role,
            reason: p
                .get("reason")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| msg.body.clone()),
            required_skills: skills,
            estimated_work: est,
            required_inputs: str_list(p.get("required_inputs")),
            expected_outputs: outputs,
            parent_task_id,
            correlation_id: if !msg.correlation_id.as_str().is_empty() {
                msg.correlation_id.as_str().to_string()
            } else {
                p.get("correlation_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            },
            capability_class: p
                .get("capability_class")
                .and_then(|v| v.as_str())
                .unwrap_or("general")
                .to_string(),
            requires_judgment: p
                .get("requires_judgment")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            mid: String::new(),
            ..Default::default()
        };
        req.normalise();
        req.validate()?;
        Ok(req)
    }

    pub fn from_map(m: &JMap) -> Result<SpawnRequest, MalformedRequest> {
        let mut msg = Message::new(
            crate::msg::EventType::SpawnAgentRequest,
            m.get("from")
                .and_then(|v| v.as_str())
                .or_else(|| m.get("requester_agent_id").and_then(|v| v.as_str()))
                .unwrap_or(""),
            "parent",
        );
        msg.body = m
            .get("reason")
            .and_then(|v| v.as_str())
            .or_else(|| m.get("body").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        if let Some(t) = m.get("task_id").and_then(|v| v.as_str()) {
            msg.task_id = Some(crate::ids::TaskId::new(t));
        }
        if let Some(c) = m
            .get("correlation_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            msg.correlation_id = crate::ids::CorrelationId::new(c);
        } else if let Some(mid) = m.get("mid").and_then(|v| v.as_str()) {
            msg.correlation_id = crate::ids::CorrelationId::new(mid);
        }
        for (k, v) in m {
            if !matches!(
                k.as_str(),
                "from" | "requester_agent_id" | "correlation_id" | "task_id" | "body" | "mid"
            ) {
                msg.payload.insert(k.clone(), v.clone());
            }
        }
        if msg.correlation_id.as_str().is_empty() {
            // legacy dicts: a synthetic id so validate() can pass on inferable corr
            let synth = format!("legacy-{}", fingerprint_preview(&msg.payload));
            msg.correlation_id = crate::ids::CorrelationId::new(synth);
        }
        SpawnRequest::from_message(&msg, "")
    }

    pub fn validate(&self) -> Result<(), MalformedRequest> {
        let mut missing = Vec::new();
        if self.requester_agent_id.is_empty() {
            missing.push("requester_agent_id");
        }
        if self.requested_role.trim().is_empty() {
            missing.push("requested_role");
        }
        if self.reason.trim().is_empty() {
            missing.push("reason");
        }
        if self.estimated_work <= 0.0 {
            missing.push("estimated_work");
        }
        if self.expected_outputs.is_empty() {
            missing.push("expected_outputs");
        }
        if self.correlation_id.is_empty() && self.mid.is_empty() {
            missing.push("correlation_id");
        }
        let bad: Vec<&str> = missing
            .into_iter()
            .filter(|f| REQUIRED_FIELDS.contains(f))
            .collect();
        if bad.is_empty() {
            Ok(())
        } else {
            let mut sorted = bad;
            sorted.sort();
            Err(MalformedRequest(format!(
                "missing required field(s): {}",
                sorted.join(", ")
            )))
        }
    }

    /// Fill inferable fields. Returns the names that were guessed.
    pub fn normalise(&mut self) -> Vec<String> {
        let mut inferred = Vec::new();
        let slug = slugify(if self.requested_role.trim().is_empty() {
            "specialist"
        } else {
            self.requested_role.trim()
        });
        if self.reason.trim().is_empty() {
            self.reason = format!(
                "specialist work: {}",
                if self.requested_role.is_empty() {
                    "unspecified"
                } else {
                    &self.requested_role
                }
            );
            inferred.push("reason".into());
        }
        if self.expected_outputs.is_empty() {
            self.expected_outputs = vec![format!("artifacts/{slug}.md")];
            inferred.push("expected_outputs".into());
        }
        if self.required_skills.is_empty() {
            self.required_skills = if self.requested_role.is_empty() {
                vec!["general".into()]
            } else {
                vec![slug]
            };
            inferred.push("required_skills".into());
        }
        inferred
    }

    /// Dedup key. Not keyed on the requester: two agents asking for the same
    /// missing capability is still one capability.
    pub fn fingerprint(&self) -> String {
        let mut skills: Vec<String> = self
            .required_skills
            .iter()
            .map(|s| s.to_lowercase())
            .collect();
        skills.sort();
        skills.dedup();
        let mut outputs = self.expected_outputs.clone();
        outputs.sort();
        let mut core = JMap::new();
        core.insert(
            "class".into(),
            JValue::Str(
                if self.capability_class.is_empty() {
                    "general"
                } else {
                    &self.capability_class
                }
                .to_lowercase(),
            ),
        );
        core.insert(
            "outputs".into(),
            JValue::Arr(outputs.into_iter().map(JValue::Str).collect()),
        );
        core.insert(
            "role".into(),
            JValue::Str(self.requested_role.trim().to_lowercase()),
        );
        core.insert(
            "skills".into(),
            JValue::Arr(skills.into_iter().map(JValue::Str).collect()),
        );
        let dumped = dumps_spaced(&JValue::Obj(core));
        let hex = sha256::hex_digest(dumped.as_bytes());
        hex[..16].to_string()
    }

    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("rid".into(), JValue::Str(self.rid.clone()));
        m.insert(
            "requester_agent_id".into(),
            JValue::Str(self.requester_agent_id.clone()),
        );
        m.insert(
            "requested_role".into(),
            JValue::Str(self.requested_role.clone()),
        );
        m.insert("reason".into(), JValue::Str(self.reason.clone()));
        m.insert(
            "required_skills".into(),
            JValue::Arr(
                self.required_skills
                    .iter()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        m.insert("estimated_work".into(), JValue::Float(self.estimated_work));
        m.insert(
            "required_inputs".into(),
            JValue::Arr(
                self.required_inputs
                    .iter()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        m.insert(
            "expected_outputs".into(),
            JValue::Arr(
                self.expected_outputs
                    .iter()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        m.insert(
            "parent_task_id".into(),
            self.parent_task_id
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "correlation_id".into(),
            JValue::Str(self.correlation_id.clone()),
        );
        m.insert(
            "capability_class".into(),
            JValue::Str(self.capability_class.clone()),
        );
        m.insert(
            "requires_judgment".into(),
            JValue::Bool(self.requires_judgment),
        );
        m.insert("fingerprint".into(), JValue::Str(self.fingerprint()));
        m.insert("state".into(), JValue::Str(self.state.as_str().into()));
        m.insert("rule".into(), JValue::Str(self.rule.clone()));
        m.insert("detail".into(), JValue::Str(self.detail.clone()));
        m.insert("at_tick".into(), JValue::Int(self.at_tick));
        m.insert("mid".into(), JValue::Str(self.mid.clone()));
        m
    }

    /// Stable across a restart. Keyed on the fingerprint, not rid.
    pub fn task_id(&self) -> String {
        let base = slugify(if self.requested_role.trim().is_empty() {
            "specialist"
        } else {
            self.requested_role.trim()
        });
        let fp = self.fingerprint();
        format!("t_{base}_{}", &fp[..6.min(fp.len())])
    }

    pub fn to_task_spec(&self) -> TaskSpec {
        let mut t = TaskSpec::new(
            self.task_id(),
            if self.reason.len() > 80 {
                &self.reason[..80]
            } else if self.reason.is_empty() {
                // filled below
                ""
            } else {
                &self.reason
            },
            &self.requested_role,
        );
        if t.title.is_empty() {
            t.title = format!("{} work", self.requested_role);
        }
        t.skills = self.required_skills.clone();
        t.est_work = self.estimated_work;
        t.produces = self.expected_outputs.clone();
        t.consumes = self.required_inputs.clone();
        t.claims = vec![format!("spawn:{}", &self.fingerprint()[..8])];
        t
    }
}

fn slugify(s: &str) -> String {
    let mut out = String::new();
    let mut prev_us = false;
    for c in s.to_lowercase().chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            prev_us = false;
        } else if !prev_us {
            out.push('_');
            prev_us = true;
        }
    }
    out.trim_matches('_').to_string()
}

fn str_list(v: Option<&JValue>) -> Vec<String> {
    match v {
        Some(JValue::Arr(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .filter(|s| !s.is_empty())
            .collect(),
        Some(JValue::Str(s)) if !s.is_empty() => vec![s.clone()],
        _ => vec![],
    }
}

fn fingerprint_preview(p: &JMap) -> String {
    p.get("requested_role")
        .and_then(|v| v.as_str())
        .unwrap_or("x")
        .chars()
        .take(8)
        .collect()
}

/// Python `json.dumps(obj, sort_keys=True)` default separators `(", ", ": ")`.
fn dumps_spaced(v: &JValue) -> String {
    match v {
        JValue::Obj(m) => {
            if m.is_empty() {
                return "{}".into();
            }
            let parts: Vec<String> = m
                .iter()
                .map(|(k, val)| {
                    format!(
                        "{}: {}",
                        JValue::Str(k.clone()).to_canon_string(),
                        dumps_spaced(val)
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        JValue::Arr(a) => {
            if a.is_empty() {
                return "[]".into();
            }
            let parts: Vec<String> = a.iter().map(dumps_spaced).collect();
            format!("[{}]", parts.join(", "))
        }
        other => other.to_canon_string(),
    }
}

// --------------------------------------------------------------------------- ledger

#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub rid: String,
    pub request: SpawnRequest,
    pub state: RequestState,
    pub rule: String,
    pub owner: Option<String>,
    pub spawned_agent_id: Option<String>,
    pub task_id: Option<String>,
    pub attempts: i64,
    pub history: Vec<String>,
}

impl LedgerEntry {
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("rid".into(), JValue::Str(self.rid.clone()));
        m.insert("state".into(), JValue::Str(self.state.as_str().into()));
        m.insert("rule".into(), JValue::Str(self.rule.clone()));
        m.insert(
            "owner".into(),
            self.owner
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "spawned_agent_id".into(),
            self.spawned_agent_id
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "task_id".into(),
            self.task_id
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        m.insert("attempts".into(), JValue::Int(self.attempts));
        m.insert(
            "history".into(),
            JValue::Arr(self.history.iter().cloned().map(JValue::Str).collect()),
        );
        m.insert(
            "requester".into(),
            JValue::Str(self.request.requester_agent_id.clone()),
        );
        m.insert(
            "role".into(),
            JValue::Str(self.request.requested_role.clone()),
        );
        m.insert(
            "capability_class".into(),
            JValue::Str(self.request.capability_class.clone()),
        );
        m.insert(
            "estimated_work".into(),
            JValue::Float(self.request.estimated_work),
        );
        m.insert(
            "expected_outputs".into(),
            JValue::Arr(
                self.request
                    .expected_outputs
                    .iter()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        m.insert(
            "fingerprint".into(),
            JValue::Str(self.request.fingerprint()),
        );
        m.insert("at_tick".into(), JValue::Int(self.request.at_tick));
        m.insert(
            "correlation_id".into(),
            JValue::Str(self.request.correlation_id.clone()),
        );
        m
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpawnLedger {
    pub entries: HashMap<String, LedgerEntry>,
    pub by_fingerprint: HashMap<String, Vec<String>>,
    seq: i64,
}

impl SpawnLedger {
    pub fn open(&mut self, mut req: SpawnRequest) -> LedgerEntry {
        self.seq += 1;
        let rid = if req.rid.is_empty() {
            format!("rq-{:04}", self.seq)
        } else {
            req.rid.clone()
        };
        req.rid = rid.clone();
        let fp = req.fingerprint();
        let entry = LedgerEntry {
            rid: rid.clone(),
            request: req,
            state: RequestState::Received,
            rule: String::new(),
            owner: None,
            spawned_agent_id: None,
            task_id: None,
            attempts: 0,
            history: Vec::new(),
        };
        self.entries.insert(rid.clone(), entry.clone());
        self.by_fingerprint.entry(fp).or_default().push(rid.clone());
        entry
    }

    pub fn move_to(
        &mut self,
        rid: &str,
        to: RequestState,
        note: &str,
        strict: bool,
        rule: &str,
    ) -> Result<bool, IllegalRequestTransition> {
        let Some(entry) = self.entries.get_mut(rid) else {
            return Ok(false);
        };
        if !rule.is_empty() {
            entry.rule = rule.to_string();
            entry.request.rule = rule.to_string();
        }
        if entry.state == to {
            let h = format!("{to}: {note}", to = to.as_str()).trim().to_string();
            entry.history.push(h);
            return Ok(true);
        }
        if strict && !allowed_transitions(entry.state).contains(&to) {
            return Err(IllegalRequestTransition {
                frm: entry.state,
                to,
            });
        }
        entry.state = to;
        entry.request.state = to;
        let h = format!("{to}: {note}", to = to.as_str()).trim().to_string();
        entry.history.push(h);
        Ok(true)
    }

    pub fn close_as(&mut self, rid: &str, state: RequestState, rule: &str, detail: &str) -> bool {
        let Some(entry) = self.entries.get_mut(rid) else {
            return false;
        };
        entry.rule = rule.to_string();
        entry.request.rule = rule.to_string();
        entry.request.detail = detail.to_string();
        entry.state = state;
        entry.request.state = state;
        entry.history.push(format!("{}: {rule}", state.as_str()));
        true
    }

    pub fn inflight_for(&self, fingerprint: &str) -> Option<&LedgerEntry> {
        for rid in self.by_fingerprint.get(fingerprint).into_iter().flatten() {
            if let Some(e) = self.entries.get(rid) {
                if matches!(
                    e.state,
                    RequestState::Received | RequestState::Evaluating | RequestState::Deferred
                ) {
                    return Some(e);
                }
            }
        }
        None
    }

    pub fn resolved_for(&self, fingerprint: &str) -> Option<&LedgerEntry> {
        for rid in self.by_fingerprint.get(fingerprint).into_iter().flatten() {
            if let Some(e) = self.entries.get(rid) {
                if is_terminal(e.state) {
                    return Some(e);
                }
            }
        }
        None
    }

    pub fn newest_first(&self) -> Vec<&LedgerEntry> {
        let mut rids: Vec<&String> = self.entries.keys().collect();
        rids.sort_by(|a, b| b.cmp(a));
        rids.into_iter()
            .filter_map(|r| self.entries.get(r))
            .collect()
    }

    pub fn counts(&self) -> JMap {
        let mut out = JMap::new();
        for s in [
            RequestState::Received,
            RequestState::Evaluating,
            RequestState::ApprovedCommitted,
            RequestState::Rerouted,
            RequestState::Rejected,
            RequestState::Deduplicated,
            RequestState::Escalated,
            RequestState::Deferred,
        ] {
            out.insert(s.as_str().into(), JValue::Int(0));
        }
        for e in self.entries.values() {
            let k = e.state.as_str().to_string();
            let n = out.get(&k).and_then(|v| v.as_int()).unwrap_or(0) + 1;
            out.insert(k, JValue::Int(n));
        }
        out.insert("total".into(), JValue::Int(self.entries.len() as i64));
        out
    }

    pub fn by_rule(&self) -> BTreeMap<String, i64> {
        let mut out = BTreeMap::new();
        for e in self.entries.values() {
            if !e.rule.is_empty() {
                *out.entry(e.rule.clone()).or_insert(0) += 1;
            }
        }
        out
    }

    /// Rebuild from the journal. No transition validation: the log is
    /// authoritative. The fold key is the rid (CHANGE vs Python, which
    /// re-issued rq-NNNN and could collide with a spent request).
    pub fn rehydrate(
        &mut self,
        req: SpawnRequest,
        state: RequestState,
        rule: &str,
        owner: Option<String>,
        spawned: Option<String>,
        task_id: Option<String>,
    ) -> LedgerEntry {
        if req.rid.is_empty() {
            // should be set by caller from the fold key
        }
        let entry = self.open(req);
        if let Some(e) = self.entries.get_mut(&entry.rid) {
            e.state = state;
            e.request.state = state;
            e.rule = rule.to_string();
            e.owner = owner;
            e.spawned_agent_id = spawned;
            e.task_id = task_id;
            e.history.push(format!("replayed -> {}", state.as_str()));
            return e.clone();
        }
        entry
    }

    pub fn get(&self, rid: &str) -> Option<&LedgerEntry> {
        self.entries.get(rid)
    }
    pub fn get_mut(&mut self, rid: &str) -> Option<&mut LedgerEntry> {
        self.entries.get_mut(rid)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpawnStats {
    pub received: i64,
    pub approved: i64,
    pub rejected: i64,
    pub deduplicated: i64,
    pub escalated: i64,
    pub deferred: i64,
    pub reused: i64,
    pub spawned_by_agents: i64,
    pub cycles_rejected: i64,
}

impl SpawnStats {
    pub fn to_map(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("received".into(), JValue::Int(self.received));
        m.insert("approved".into(), JValue::Int(self.approved));
        m.insert("rejected".into(), JValue::Int(self.rejected));
        m.insert("deduplicated".into(), JValue::Int(self.deduplicated));
        m.insert("escalated".into(), JValue::Int(self.escalated));
        m.insert("deferred".into(), JValue::Int(self.deferred));
        m.insert("reused".into(), JValue::Int(self.reused));
        m.insert(
            "spawned_by_agents".into(),
            JValue::Int(self.spawned_by_agents),
        );
        m.insert("cycles_rejected".into(), JValue::Int(self.cycles_rejected));
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::EventType;

    fn req(role: &str, skills: &[&str], outputs: &[&str]) -> SpawnRequest {
        SpawnRequest {
            requester_agent_id: "backend_01".into(),
            requested_role: role.into(),
            reason: "need help".into(),
            required_skills: skills.iter().map(|s| (*s).to_string()).collect(),
            estimated_work: 3.0,
            expected_outputs: outputs.iter().map(|s| (*s).to_string()).collect(),
            correlation_id: "c-1".into(),
            capability_class: "payments".into(),
            ..Default::default()
        }
    }

    #[test]
    fn missing_load_bearing_field_is_refused() {
        let mut r = SpawnRequest {
            requester_agent_id: "a".into(),
            requested_role: "x".into(),
            reason: "y".into(),
            estimated_work: 0.0,
            expected_outputs: vec!["o".into()],
            correlation_id: "c".into(),
            ..Default::default()
        };
        let err = r.validate().unwrap_err();
        assert!(err.0.contains("estimated_work"), "{err}");
        r.estimated_work = 1.0;
        r.expected_outputs.clear();
        r.normalise(); // fills outputs
        assert!(r.validate().is_ok());
    }

    #[test]
    fn fingerprint_ignores_requester_and_field_order() {
        let a = req(
            "payment specialist",
            &["webhooks", "payments"],
            &["artifacts/payments-webhooks.md"],
        );
        let mut b = a.clone();
        b.requester_agent_id = "other_99".into();
        b.required_skills = vec!["payments".into(), "webhooks".into()];
        assert_eq!(a.fingerprint(), b.fingerprint());
        let mut c = a.clone();
        c.requested_role = "auth".into();
        assert_ne!(a.fingerprint(), c.fingerprint());
    }

    #[test]
    fn task_ids_are_stable_across_restart() {
        let r = req(
            "payment specialist",
            &["payments"],
            &["artifacts/payments-webhooks.md"],
        );
        let t1 = r.task_id();
        let t2 = r.task_id();
        assert_eq!(t1, t2);
        assert!(t1.starts_with("t_payment_specialist_"));
        assert_eq!(t1.len(), "t_payment_specialist_".len() + 6);
    }

    #[test]
    fn required_inputs_become_consumes() {
        let mut r = req("backend", &["api"], &["contracts/api.json"]);
        r.required_inputs = vec!["database/schema.sql".into()];
        let spec = r.to_task_spec();
        assert_eq!(spec.consumes, vec!["database/schema.sql"]);
        assert_eq!(spec.produces, vec!["contracts/api.json"]);
    }

    #[test]
    fn catalog_refuses_unserviceable() {
        let c = CapabilityCatalog::default();
        assert!(!c.is_serviceable("gpu-training"));
        assert!(c
            .reason_for_unserviceable("gpu-training")
            .contains("accelerator"));
        assert!(c.is_serviceable("payments"));
        assert_eq!(
            c.class_for("payment specialist", &["webhooks".into()]),
            "payments"
        );
    }

    #[test]
    fn catalog_can_be_extended() {
        let mut c = CapabilityCatalog::default();
        c.serviceable
            .insert("gpu-training".into(), vec!["gpu".into()]);
        c.unserviceable.remove("gpu-training");
        assert!(c.is_serviceable("gpu-training"));
    }

    #[test]
    fn ledger_dedups_inflight_and_resolved() {
        let mut led = SpawnLedger::default();
        let r1 = req("pay", &["p"], &["o.md"]);
        let e1 = led.open(r1.clone());
        led.move_to(&e1.rid, RequestState::Evaluating, "q", true, "")
            .unwrap();
        let mut r2 = r1.clone();
        r2.requester_agent_id = "other".into();
        let e2 = led.open(r2);
        assert_eq!(
            led.inflight_for(&e1.request.fingerprint()).unwrap().rid,
            e1.rid
        );
        assert_ne!(e2.rid, e1.rid);
        led.close_as(&e1.rid, RequestState::ApprovedCommitted, "APPROVE", "");
        assert!(led.resolved_for(&e1.request.fingerprint()).is_some());
    }

    #[test]
    fn request_fsm_rejects_illegal_edges() {
        let mut led = SpawnLedger::default();
        let e = led.open(req("pay", &["p"], &["o.md"]));
        let err = led
            .move_to(&e.rid, RequestState::ApprovedCommitted, "", true, "")
            .unwrap_err();
        assert_eq!(err.frm, RequestState::Received);
        led.move_to(&e.rid, RequestState::Evaluating, "", true, "")
            .unwrap();
        led.move_to(&e.rid, RequestState::Rejected, "", true, "REJECT_CAP")
            .unwrap();
        let err = led
            .move_to(&e.rid, RequestState::Evaluating, "", true, "")
            .unwrap_err();
        assert_eq!(err.frm, RequestState::Rejected);
    }

    #[test]
    fn deferred_is_the_only_way_back() {
        let mut led = SpawnLedger::default();
        let e = led.open(req("pay", &["p"], &["o.md"]));
        led.move_to(&e.rid, RequestState::Evaluating, "", true, "")
            .unwrap();
        led.move_to(
            &e.rid,
            RequestState::Deferred,
            "",
            true,
            "DEFER_FOR_CAPACITY",
        )
        .unwrap();
        led.move_to(&e.rid, RequestState::Evaluating, "", true, "")
            .unwrap();
        led.move_to(&e.rid, RequestState::Rejected, "", true, "REJECT_CAP")
            .unwrap();
        assert!(led
            .move_to(&e.rid, RequestState::Evaluating, "", true, "")
            .is_err());
    }

    #[test]
    fn from_message_round_trips() {
        let mut msg = Message::new(EventType::SpawnAgentRequest, "backend_01", "parent");
        msg.body = "need payments".into();
        msg.correlation_id = crate::ids::CorrelationId::new("c-abc");
        msg.payload.insert(
            "requested_role".into(),
            JValue::Str("payment specialist".into()),
        );
        msg.payload
            .insert("estimated_work".into(), JValue::Float(3.0));
        msg.payload.insert(
            "expected_outputs".into(),
            JValue::Arr(vec![JValue::Str("artifacts/p.md".into())]),
        );
        msg.payload.insert(
            "required_skills".into(),
            JValue::Arr(vec![JValue::Str("payments".into())]),
        );
        let r = SpawnRequest::from_message(&msg, "").unwrap();
        assert_eq!(r.requester_agent_id, "backend_01");
        assert_eq!(r.requested_role, "payment specialist");
        assert_eq!(r.estimated_work, 3.0);
        assert_eq!(r.correlation_id, "c-abc");
    }

    #[test]
    fn legacy_normalises_reason_and_outputs() {
        let mut msg = Message::new(EventType::SpawnAgentRequest, "be_01", "parent");
        msg.correlation_id = crate::ids::CorrelationId::new("c-1");
        msg.payload
            .insert("requested_role".into(), JValue::Str("security".into()));
        msg.payload
            .insert("estimated_work".into(), JValue::Float(2.0));
        let r = SpawnRequest::from_message(&msg, "").unwrap();
        assert!(r.reason.contains("security"));
        assert!(!r.expected_outputs.is_empty());
        assert!(!r.required_skills.is_empty());
    }
}
