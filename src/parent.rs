//! Parent / orchestrator (`arena/parent.py`).
//!
//! Parent is a Kernel collaborator: it owns the spawn ledger, catalog, planner
//! and counters. It does **not** hold a Kernel pointer. Funnel methods live on
//! Kernel and mutate `self.parent` plus graph/registry/journal.
//!
//! Safety property: intake (no mutation) → evaluate (pure read) → commit
//! (graph.amend first, then spawn/assign).

use std::collections::BTreeSet;

use crate::graph::{AmendResult, TaskSpec};
use crate::ids::TaskId;
use crate::journal::EventFilter;
use crate::kernel::Kernel;
use crate::lifecycle::AgentState;
use crate::msg::{EventType, Message};
use crate::registry::AgentRegistry;
use crate::spawn::{
    CapabilityCatalog, MalformedRequest, RequestState, SpawnLedger, SpawnRequest, SpawnStats,
};
use crate::sys::json::{py_round, JMap, JValue};

// ------------------------------------------------------------------------ planner

type PlanTask = (
    &'static str,
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
    f64,
);

#[derive(Debug, Clone)]
struct PlanRule {
    when: &'static [&'static str],
    task: PlanTask,
    extra: &'static [PlanTask],
}

const PLAN_RULES: &[PlanRule] = &[
    PlanRule {
        when: &["database", "postgres", "sql", "schema", "db"],
        task: (
            "t_db_schema",
            "design schema + migrations",
            "database",
            &["sql", "schema"],
            &["database/schema.sql", "database/migrations"],
            3.0,
        ),
        extra: &[],
    },
    PlanRule {
        when: &["api", "backend", "rest", "endpoint", "server", "graphql"],
        task: (
            "t_api_contract",
            "author API contract (OpenAPI)",
            "backend",
            &["api", "contracts"],
            &["contracts/api.json"],
            2.0,
        ),
        extra: &[(
            "t_api_impl",
            "implement API endpoints",
            "backend",
            &["api", "server"],
            &["backend/src"],
            4.0,
        )],
    },
    PlanRule {
        when: &["auth", "login", "session", "jwt", "oauth", "password"],
        task: (
            "t_auth",
            "authentication + session design",
            "auth",
            &["auth", "sessions", "jwt"],
            &["contracts/auth_api.json"],
            3.0,
        ),
        extra: &[],
    },
    PlanRule {
        when: &["frontend", "ui", "dashboard", "react", "web app", "screen"],
        task: (
            "t_fe_design",
            "UI architecture + design system",
            "frontend",
            &["ui", "react"],
            &["frontend/design"],
            2.0,
        ),
        extra: &[(
            "t_fe_integration",
            "wire UI to API",
            "frontend",
            &["ui", "integration"],
            &["frontend/src"],
            3.0,
        )],
    },
    PlanRule {
        when: &[
            "cloud",
            "deploy",
            "deployment",
            "aws",
            "infra",
            "terraform",
            "k8s",
        ],
        task: (
            "t_cloud_infra",
            "infrastructure + deployment spec",
            "cloud",
            &["iac", "deploy"],
            &["deploy/main.tf", "deploy/compose.yaml"],
            2.0,
        ),
        extra: &[],
    },
    PlanRule {
        when: &["payment", "stripe", "billing", "checkout", "invoice"],
        task: (
            "t_payments",
            "payment provider integration",
            "payments",
            &["stripe", "billing"],
            &["contracts/payments_api.json", "backend/payments"],
            4.0,
        ),
        extra: &[],
    },
    PlanRule {
        when: &["analytics", "event", "metrics", "tracking", "telemetry"],
        task: (
            "t_analytics",
            "event taxonomy + pipeline",
            "data",
            &["analytics", "etl"],
            &["analytics/schema.json"],
            3.0,
        ),
        extra: &[],
    },
    PlanRule {
        when: &["ml", "model", "train", "inference", "dataset"],
        task: (
            "t_ml_pipeline",
            "data pipeline + training loop",
            "ml-engineer",
            &["ml", "pipeline"],
            &["ml/pipeline"],
            4.0,
        ),
        extra: &[(
            "t_ml_eval",
            "evaluation harness",
            "evaluation",
            &["metrics", "eval"],
            &["ml/eval"],
            2.0,
        )],
    },
    PlanRule {
        when: &["test", "qa", "coverage", "e2e"],
        task: (
            "t_tests",
            "test strategy + suites",
            "testing",
            &["pytest", "e2e"],
            &["tests"],
            3.0,
        ),
        extra: &[],
    },
    PlanRule {
        when: &["security", "audit", "penetration", "encryption"],
        task: (
            "t_security",
            "threat model + controls",
            "security",
            &["threat-model", "crypto"],
            &["docs/security.md"],
            2.5,
        ),
        extra: &[],
    },
];

#[derive(Debug, Clone, Default)]
pub struct RuleBasedPlanner;

impl RuleBasedPlanner {
    pub fn name(&self) -> &'static str {
        "rule-based"
    }

    pub fn plan(&self, text: &str) -> (Vec<TaskSpec>, JMap) {
        let low = text.to_lowercase();
        let mut tasks: Vec<TaskSpec> = Vec::new();
        let mut hits = JMap::new();
        for rule in PLAN_RULES {
            let matched: Vec<&str> = rule
                .when
                .iter()
                .copied()
                .filter(|w| low.contains(w))
                .collect();
            if matched.is_empty() {
                continue;
            }
            let (tid, title, role, skills, produces, est) = rule.task;
            hits.insert(
                role.to_string(),
                JValue::Arr(matched.iter().map(|s| JValue::Str((*s).into())).collect()),
            );
            tasks.push(spec_from(tid, title, role, skills, produces, est));
            for (et_id, et_title, et_role, et_skills, et_prod, et_est) in rule.extra {
                let arr = match hits.get(*et_role) {
                    Some(JValue::Arr(v)) => {
                        let mut v = v.clone();
                        v.push(JValue::Str(format!("{tid}:sequel")));
                        v
                    }
                    _ => vec![JValue::Str(format!("{tid}:sequel"))],
                };
                hits.insert((*et_role).to_string(), JValue::Arr(arr));
                tasks.push(spec_from(
                    et_id, et_title, et_role, et_skills, et_prod, *et_est,
                ));
            }
        }
        let ids: BTreeSet<String> = tasks
            .iter()
            .map(|t| t.task_id.as_str().to_string())
            .collect();
        let produced: BTreeSet<String> = tasks.iter().flat_map(|t| t.produces.clone()).collect();
        for t in &mut tasks {
            let tid = t.task_id.as_str();
            if tid == "t_fe_integration" {
                t.consumes = [
                    "contracts/api.json",
                    "contracts/auth_api.json",
                    "contracts/payments_api.json",
                ]
                .iter()
                .filter(|a| produced.contains(**a))
                .map(|s| (*s).to_string())
                .collect();
            }
            if tid == "t_api_impl" {
                t.consumes = [
                    "database/schema.sql",
                    "contracts/auth_api.json",
                    "database/migrations",
                ]
                .iter()
                .filter(|a| produced.contains(**a))
                .map(|s| (*s).to_string())
                .collect();
            }
            if tid == "t_tests" {
                t.consumes = ["backend/src", "contracts/api.json", "frontend/src"]
                    .iter()
                    .filter(|a| produced.contains(**a))
                    .map(|s| (*s).to_string())
                    .collect();
            }
            if tid == "t_api_contract" {
                t.consumes = ["database/schema.sql"]
                    .iter()
                    .filter(|a| produced.contains(**a))
                    .map(|s| (*s).to_string())
                    .collect();
            }
            if tid == "t_ml_eval" {
                t.consumes = ["ml/pipeline"]
                    .iter()
                    .filter(|a| produced.contains(**a))
                    .map(|s| (*s).to_string())
                    .collect();
            }
            if tid == "t_security" {
                t.consumes = ["contracts/auth_api.json"]
                    .iter()
                    .filter(|a| produced.contains(**a))
                    .map(|s| (*s).to_string())
                    .collect();
            }
            if tid == "t_analytics" && ids.contains("t_api_contract") {
                t.consumes = vec!["contracts/api.json".into()];
            }
        }
        let mut roles: Vec<String> = tasks.iter().map(|t| t.role.clone()).collect();
        roles.sort();
        roles.dedup();
        let mut edges = JMap::new();
        for t in &tasks {
            if !t.consumes.is_empty() {
                edges.insert(
                    t.task_id.as_str().to_string(),
                    JValue::Arr(t.consumes.iter().cloned().map(JValue::Str).collect()),
                );
            }
        }
        let mut rationale = JMap::new();
        rationale.insert("matched".into(), JValue::Obj(hits));
        rationale.insert(
            "roles".into(),
            JValue::Arr(roles.into_iter().map(JValue::Str).collect()),
        );
        rationale.insert("planner".into(), JValue::Str(self.name().into()));
        rationale.insert("artifact_edges".into(), JValue::Obj(edges));
        (tasks, rationale)
    }
}

fn jstr(m: &JMap, k: &str) -> String {
    m.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn spec_from(
    tid: &str,
    title: &str,
    role: &str,
    skills: &[&str],
    produces: &[&str],
    est: f64,
) -> TaskSpec {
    let mut t = TaskSpec::new(tid, title, role);
    t.skills = skills.iter().map(|s| (*s).to_string()).collect();
    t.produces = produces.iter().map(|s| (*s).to_string()).collect();
    t.est_work = est;
    t
}

// ------------------------------------------------------------------- decision

#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    pub ok: bool,
    pub rule: String,
    pub detail: String,
    pub owner: Option<String>,
}

impl Decision {
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("ok".into(), JValue::Bool(self.ok));
        m.insert("rule".into(), JValue::Str(self.rule.clone()));
        m.insert("detail".into(), JValue::Str(self.detail.clone()));
        m.insert(
            "owner".into(),
            self.owner
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        m
    }
}

#[derive(Debug, Clone)]
pub struct Parent {
    pub ledger: SpawnLedger,
    pub catalog: CapabilityCatalog,
    pub planner: RuleBasedPlanner,
    pub escalated: BTreeSet<String>,
    pub spawn_stats: SpawnStats,
    pub decisions: Vec<JMap>,
    pub accepted_decisions: Vec<JMap>,
}

impl Default for Parent {
    fn default() -> Self {
        Parent {
            ledger: SpawnLedger::default(),
            catalog: CapabilityCatalog::default(),
            planner: RuleBasedPlanner,
            escalated: BTreeSet::new(),
            spawn_stats: SpawnStats::default(),
            decisions: Vec::new(),
            accepted_decisions: Vec::new(),
        }
    }
}

impl Parent {
    pub fn recount_spawn_stats(&mut self) -> SpawnStats {
        let mut out = SpawnStats::default();
        for e in self.ledger.entries.values() {
            out.received += 1;
            match e.state {
                RequestState::ApprovedCommitted => {
                    out.approved += 1;
                    if !e.request.requester_agent_id.is_empty() {
                        out.spawned_by_agents += 1;
                    }
                }
                RequestState::Rerouted => out.reused += 1,
                RequestState::Rejected => out.rejected += 1,
                RequestState::Deduplicated => out.deduplicated += 1,
                RequestState::Escalated => out.escalated += 1,
                RequestState::Deferred => out.deferred += 1,
                _ => {}
            }
            if e.rule == "REJECT_CYCLE" {
                out.cycles_rejected += 1;
            }
        }
        self.spawn_stats = out.clone();
        out
    }
}

impl Kernel {
    // ---------------------------------------------------------------- planning

    /// Staff a graph from task text via the rule-based planner. Empty match
    /// invents nothing.
    pub fn submit_text(&mut self, text: &str) -> JMap {
        self.task_text = text.to_string();
        let (tasks, rationale) = self.parent.planner.plan(text);
        if tasks.is_empty() {
            let mut fields = JMap::new();
            fields.insert("rationale".into(), JValue::Obj(rationale.clone()));
            fields.insert("tasks".into(), JValue::Arr(vec![]));
            self.emit(
                EventType::PlanCreated,
                "parent",
                "parent",
                "no task matched; nothing to spawn",
                fields,
                None,
            );
            let mut out = JMap::new();
            out.insert("tasks".into(), JValue::Int(0));
            out.insert("agents".into(), JValue::Arr(vec![]));
            out.insert("rationale".into(), JValue::Obj(rationale));
            return out;
        }
        let before = self.graph.clone();
        let snaps: Vec<JValue> = tasks.iter().map(|t| JValue::Obj(t.snapshot())).collect();
        for t in &tasks {
            if let Err(e) = self.graph.add(t.clone(), true) {
                self.graph = before;
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("PLAN_CYCLE".into()));
                fields.insert("cycle".into(), JValue::Arr(vec![JValue::Str(e)]));
                self.emit(
                    EventType::CycleRejected,
                    "parent",
                    "parent",
                    "plan rejected",
                    fields,
                    None,
                );
                let mut out = JMap::new();
                out.insert("tasks".into(), JValue::Int(0));
                out.insert("agents".into(), JValue::Arr(vec![]));
                out.insert("error".into(), JValue::Str("plan cycle".into()));
                return out;
            }
        }
        if let Err(e) = self.graph.validate() {
            self.graph = before;
            let mut fields = JMap::new();
            fields.insert("rule".into(), JValue::Str("PLAN_CYCLE".into()));
            fields.insert(
                "cycle".into(),
                JValue::Arr(e.0.into_iter().map(JValue::Str).collect()),
            );
            self.emit(
                EventType::CycleRejected,
                "parent",
                "parent",
                "plan rejected",
                fields,
                None,
            );
            let mut out = JMap::new();
            out.insert("tasks".into(), JValue::Int(0));
            out.insert("agents".into(), JValue::Arr(vec![]));
            return out;
        }
        let mut fields = JMap::new();
        fields.insert("tasks".into(), JValue::Arr(snaps));
        fields.insert("rationale".into(), JValue::Obj(rationale.clone()));
        self.emit(
            EventType::PlanCreated,
            "parent",
            "parent",
            &format!("planned {} tasks from task text", tasks.len()),
            fields,
            None,
        );
        let agents = self.spawn_for_plan(&tasks);
        let gens = self.graph.order().unwrap_or_default();
        let mut out = JMap::new();
        out.insert("tasks".into(), JValue::Int(tasks.len() as i64));
        out.insert(
            "agents".into(),
            JValue::Arr(agents.iter().cloned().map(JValue::Str).collect()),
        );
        out.insert("rationale".into(), JValue::Obj(rationale));
        out.insert(
            "order".into(),
            JValue::Arr(
                gens.iter()
                    .map(|g| {
                        JValue::Arr(g.iter().map(|t| JValue::Str(t.as_str().into())).collect())
                    })
                    .collect(),
            ),
        );
        out.insert(
            "parallelism".into(),
            JValue::Arr(gens.iter().map(|g| JValue::Int(g.len() as i64)).collect()),
        );
        out
    }

    pub fn spawn_for_plan(&mut self, tasks: &[TaskSpec]) -> Vec<String> {
        let mut made: Vec<(String, String)> = Vec::new();
        let cap = self.registry.budget.max_active_agents;
        let mut deferred: Vec<String> = Vec::new();
        for t in tasks {
            if self.registry.active().len() as i64 >= cap {
                deferred.push(t.task_id.as_str().to_string());
                continue;
            }
            let skills: Vec<&str> = t.skills.iter().map(|s| s.as_str()).collect();
            let existing = self.registry.cover(&t.role, &skills);
            let reuse = existing.into_iter().find(|a| a.epoch == 0);
            if let Some(a) = reuse {
                made.push((
                    t.task_id.as_str().to_string(),
                    a.agent_id.as_str().to_string(),
                ));
                continue;
            }
            if let Some(aid) = self.parent_spawn(
                &t.role,
                &t.skills,
                &format!("plan task {}", t.task_id),
                0,
                "parent",
                None,
            ) {
                made.push((t.task_id.as_str().to_string(), aid));
            }
        }
        for (tid, aid) in &made {
            self.assign_claimed(tid, aid, "planned", "");
        }
        if !deferred.is_empty() {
            let mut fields = JMap::new();
            fields.insert(
                "deferred".into(),
                JValue::Arr(deferred.iter().cloned().map(JValue::Str).collect()),
            );
            fields.insert("rule".into(), JValue::Str("DEFERRED_FOR_CAPACITY".into()));
            self.emit(
                EventType::StatusUpdate,
                "parent",
                "parent",
                &format!(
                    "plan under-staffed: {} task(s) deferred by MAX_ACTIVE_AGENTS={cap}",
                    deferred.len()
                ),
                fields,
                None,
            );
        }
        let mut agents: Vec<String> = made.into_iter().map(|(_, a)| a).collect();
        agents.sort();
        agents.dedup();
        agents
    }

    /// AGENT_REGISTERED before make_actor (fold needs the registration row first).
    pub fn parent_spawn(
        &mut self,
        role: &str,
        skills: &[String],
        reason: &str,
        epoch: i64,
        spawned_by: &str,
        agent_id: Option<&str>,
    ) -> Option<String> {
        if agent_id.is_none()
            && self.registry.active().len() as i64 >= self.registry.budget.max_active_agents
        {
            return None;
        }
        let aid = match agent_id {
            Some(a) => a.to_string(),
            None => self.registry.next_id(role).as_str().to_string(),
        };
        let skill_refs: Vec<&str> = skills.iter().map(|s| s.as_str()).collect();
        if self
            .register_agent_with_reason(&aid, role, &skill_refs, epoch, spawned_by, reason)
            .is_err()
        {
            return None;
        }
        self.make_actor(&aid);
        let mut fields = JMap::new();
        fields.insert("epoch".into(), JValue::Int(epoch));
        self.emit(
            EventType::StatusUpdate,
            "parent",
            &aid,
            &format!("agent {aid} ({role}) joined the arena"),
            fields,
            None,
        );
        Some(aid)
    }

    pub fn assign_claimed(
        &mut self,
        task_id: &str,
        agent_id: &str,
        reason: &str,
        correlation_id: &str,
    ) -> bool {
        let Some(task) = self.graph.tasks.get(&TaskId::new(task_id)) else {
            return false;
        };
        if !task.is_open() {
            return false;
        }
        let key = task.key();
        let (ok, incumbent) = self
            .journal
            .claim(&key, agent_id, Some(task_id), self.clock.now())
            .unwrap_or((false, String::new()));
        if !ok {
            let mut fields = JMap::new();
            fields.insert("owner".into(), JValue::Str(incumbent.clone()));
            fields.insert("rule".into(), JValue::Str("CLAIM_TAKEN".into()));
            self.emit_corr(
                EventType::DuplicateClaim,
                agent_id,
                "parent",
                &format!("{task_id} already claimed by {incumbent}"),
                fields,
                Some(task_id),
                if correlation_id.is_empty() {
                    None
                } else {
                    Some(correlation_id)
                },
            );
            return false;
        }
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(task_id)) {
            t.owner = Some(crate::ids::AgentId::new(agent_id));
            if t.status == crate::graph::TaskStatus::Pending {
                t.status = crate::graph::TaskStatus::Assigned;
            }
        }
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
        }
        if self.registry.get(agent_id).map(|r| r.lifecycle.state) == Some(AgentState::Completed) {
            self.transition(
                agent_id,
                AgentState::Initializing,
                &format!("re-queued {task_id}"),
            );
        }
        let produces = self
            .graph
            .tasks
            .get(&TaskId::new(task_id))
            .map(|t| t.produces.clone())
            .unwrap_or_default();
        let consumes = self
            .graph
            .tasks
            .get(&TaskId::new(task_id))
            .map(|t| t.consumes.clone())
            .unwrap_or_default();
        let role = self
            .graph
            .tasks
            .get(&TaskId::new(task_id))
            .map(|t| t.role.clone())
            .unwrap_or_default();
        let mut fields = JMap::new();
        fields.insert("owner".into(), JValue::Str(agent_id.into()));
        fields.insert("role".into(), JValue::Str(role));
        fields.insert(
            "produces".into(),
            JValue::Arr(produces.into_iter().map(JValue::Str).collect()),
        );
        fields.insert(
            "consumes".into(),
            JValue::Arr(consumes.into_iter().map(JValue::Str).collect()),
        );
        self.emit_corr(
            EventType::TaskAssigned,
            "parent",
            agent_id,
            if reason.is_empty() { task_id } else { reason },
            fields,
            Some(task_id),
            if correlation_id.is_empty() {
                None
            } else {
                Some(correlation_id)
            },
        );
        self.registry.version += 1;
        true
    }

    // ------------------------------------------------------------------ funnel

    fn as_request(&self, req: SpawnIn<'_>) -> Result<SpawnRequest, MalformedRequest> {
        match req {
            SpawnIn::Request(r) => {
                let mut r = r;
                r.normalise();
                r.validate()?;
                Ok(r)
            }
            SpawnIn::Message(m) => SpawnRequest::from_message(m, ""),
            SpawnIn::Map(m) => SpawnRequest::from_map(m),
        }
    }

    pub fn intake_spawn_request(&mut self, req: SpawnIn<'_>) -> Option<SpawnRequest> {
        self.parent.spawn_stats.received += 1;
        let r = match self.as_request(req) {
            Ok(r) => r,
            Err(e) => {
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("REJECT_MALFORMED".into()));
                self.emit(
                    EventType::SpawnRejected,
                    "parent",
                    "",
                    &format!("malformed spawn request: {e}"),
                    fields,
                    None,
                );
                self.parent.spawn_stats.rejected += 1;
                let mut d = JMap::new();
                d.insert("at".into(), JValue::Float(py_round(self.now(), 3)));
                d.insert("tick".into(), JValue::Int(self.tick));
                d.insert("rid".into(), JValue::Str("".into()));
                d.insert("ok".into(), JValue::Bool(false));
                d.insert("rule".into(), JValue::Str("REJECT_MALFORMED".into()));
                d.insert("detail".into(), JValue::Str(e.to_string()));
                self.parent.decisions.push(d);
                return None;
            }
        };
        let mut r = r;
        let inferred = r.normalise();
        if r.capability_class.is_empty() || r.capability_class == "general" {
            r.capability_class = self
                .parent
                .catalog
                .class_for(&r.requested_role, &r.required_skills);
        }
        r.at_tick = self.tick;
        r.created_at = self.now();
        if r.correlation_id.is_empty() {
            r.correlation_id = if r.mid.is_empty() {
                format!("spawn-{}", &r.fingerprint()[..8.min(r.fingerprint().len())])
            } else {
                r.mid.clone()
            };
        }
        let entry = self.parent.ledger.open(r.clone());
        r.rid = entry.rid.clone();
        let mut fields = JMap::new();
        fields.insert("rid".into(), JValue::Str(r.rid.clone()));
        fields.insert(
            "requester".into(),
            JValue::Str(r.requester_agent_id.clone()),
        );
        fields.insert(
            "requested_role".into(),
            JValue::Str(r.requested_role.clone()),
        );
        fields.insert(
            "capability_class".into(),
            JValue::Str(r.capability_class.clone()),
        );
        fields.insert("estimated_work".into(), JValue::Float(r.estimated_work));
        fields.insert("fingerprint".into(), JValue::Str(r.fingerprint()));
        fields.insert(
            "required_skills".into(),
            JValue::Arr(r.required_skills.iter().cloned().map(JValue::Str).collect()),
        );
        fields.insert(
            "required_inputs".into(),
            JValue::Arr(r.required_inputs.iter().cloned().map(JValue::Str).collect()),
        );
        fields.insert(
            "expected_outputs".into(),
            JValue::Arr(
                r.expected_outputs
                    .iter()
                    .cloned()
                    .map(JValue::Str)
                    .collect(),
            ),
        );
        fields.insert("at_tick".into(), JValue::Int(r.at_tick));
        fields.insert(
            "requires_judgment".into(),
            JValue::Bool(r.requires_judgment),
        );
        fields.insert(
            "inferred_fields".into(),
            JValue::Arr(inferred.into_iter().map(JValue::Str).collect()),
        );
        self.emit_corr(
            EventType::SpawnRequestReceived,
            "parent",
            "parent",
            &format!(
                "{} asks for a {}",
                if r.requester_agent_id.is_empty() {
                    "?"
                } else {
                    &r.requester_agent_id
                },
                r.requested_role
            ),
            fields,
            r.parent_task_id.as_deref(),
            Some(&r.correlation_id),
        );

        let fp = r.fingerprint();
        if let Some(inflight) = self.parent.ledger.inflight_for(&fp) {
            if inflight.rid != r.rid {
                let other = inflight.rid.clone();
                self.parent.ledger.close_as(
                    &r.rid,
                    RequestState::Deduplicated,
                    "DEDUPLICATE",
                    &format!("in flight as {other}"),
                );
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("DEDUPLICATE".into()));
                fields.insert("rid".into(), JValue::Str(r.rid.clone()));
                fields.insert("duplicate_of".into(), JValue::Str(other));
                fields.insert(
                    "requested_role".into(),
                    JValue::Str(r.requested_role.clone()),
                );
                self.emit_corr(
                    EventType::SpawnRejected,
                    "parent",
                    &r.requester_agent_id,
                    "identical request is already being evaluated; not queueing a second one",
                    fields,
                    None,
                    Some(&r.correlation_id),
                );
                self.parent.spawn_stats.deduplicated += 1;
                return None;
            }
        }
        if let Some(prior) = self.parent.ledger.resolved_for(&fp) {
            if prior.rid != r.rid {
                let why = format!(
                    "already resolved as {} ({})",
                    prior.rid,
                    prior.state.as_str()
                );
                let other = prior.rid.clone();
                self.parent.ledger.close_as(
                    &r.rid,
                    RequestState::Deduplicated,
                    "DEDUPLICATE",
                    &why,
                );
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("DEDUPLICATE".into()));
                fields.insert("rid".into(), JValue::Str(r.rid.clone()));
                fields.insert("duplicate_of".into(), JValue::Str(other));
                fields.insert(
                    "requested_role".into(),
                    JValue::Str(r.requested_role.clone()),
                );
                self.emit_corr(
                    EventType::SpawnRejected,
                    "parent",
                    &r.requester_agent_id,
                    &why,
                    fields,
                    None,
                    Some(&r.correlation_id),
                );
                self.parent.spawn_stats.deduplicated += 1;
                return None;
            }
        }
        let _ = self.parent.ledger.move_to(
            &r.rid,
            RequestState::Evaluating,
            "queued for evaluation",
            true,
            "",
        );
        Some(r)
    }

    pub fn evaluate_spawn(&self, r: &SpawnRequest) -> Decision {
        let b = &self.registry.budget;
        let requester = if r.requester_agent_id.is_empty() {
            None
        } else {
            self.registry.get(&r.requester_agent_id)
        };
        if let Some(req) = requester {
            if req.epoch >= b.max_spawn_epoch {
                return Decision {
                    ok: false,
                    rule: "REJECT_SPAWN_DEPTH".into(),
                    detail: format!(
                        "{} is at spawn epoch {} >= {}; only the parent may add agents below this depth",
                        r.requester_agent_id, req.epoch, b.max_spawn_epoch
                    ),
                    owner: None,
                };
            }
        }
        if self.registry.active().len() as i64 >= b.max_active_agents {
            return Decision {
                ok: false,
                rule: "REJECT_CAP".into(),
                detail: format!(
                    "registry is at {}/{}",
                    self.registry.active().len(),
                    b.max_active_agents
                ),
                owner: None,
            };
        }
        let total_spawns = self
            .journal
            .events(&EventFilter::new().etype("AGENT_REGISTERED"))
            .map(|v| v.len() as i64)
            .unwrap_or(0);
        if total_spawns > b.max_active_agents * 2 {
            return Decision {
                ok: false,
                rule: "REJECT_CAP".into(),
                detail: format!(
                    "{total_spawns} registrations for a {}-agent arena looks like churn, not scaling",
                    b.max_active_agents
                ),
                owner: None,
            };
        }
        if !r.capability_class.is_empty()
            && !self
                .parent
                .catalog
                .serviceable
                .contains_key(&r.capability_class)
        {
            let why = self
                .parent
                .catalog
                .reason_for_unserviceable(&r.capability_class);
            return Decision {
                ok: false,
                rule: "REJECT_UNSUPPORTED".into(),
                detail: format!(
                    "capability class '{}' cannot be staffed in this arena{}",
                    r.capability_class,
                    if why.is_empty() {
                        String::new()
                    } else {
                        format!(": {why}")
                    }
                ),
                owner: None,
            };
        }
        let skills: Vec<&str> = r.required_skills.iter().map(|s| s.as_str()).collect();
        let covered = self.registry.cover(&r.requested_role, &skills);
        if let Some(a) = covered.first() {
            return Decision {
                ok: false,
                rule: "REJECT_DUPLICATE_CAPABILITY".into(),
                detail: format!(
                    "{} ({}) already covers '{}'; assign to it instead of spawning",
                    a.agent_id, a.role, r.requested_role
                ),
                owner: Some(a.agent_id.as_str().to_string()),
            };
        }
        let remaining = {
            let rw = self.graph.remaining_work();
            if rw == 0.0 {
                if r.estimated_work == 0.0 {
                    1.0
                } else {
                    r.estimated_work
                }
            } else {
                rw
            }
        };
        if r.estimated_work > 0.0 && r.estimated_work < b.min_share_of_remaining * remaining {
            return Decision {
                ok: false,
                rule: "REJECT_NOT_WORTH_IT".into(),
                detail: format!(
                    "est work {} is < {:.0}% of remaining {:.1}; existing capacity absorbs this",
                    r.estimated_work,
                    b.min_share_of_remaining * 100.0,
                    remaining
                ),
                owner: None,
            };
        }
        if !r.requester_agent_id.is_empty()
            && self.registry.overloaded(&self.graph, &r.requester_agent_id)
        {
            return Decision {
                ok: false,
                rule: "REJECT_REQUESTER_OVERLOADED".into(),
                detail: format!(
                    "{} has more open work than it has completed; it must descope or delegate to a peer, not add headcount",
                    r.requester_agent_id
                ),
                owner: None,
            };
        }
        if r.requires_judgment {
            return Decision {
                ok: false,
                rule: "ESCALATE".into(),
                detail: format!(
                    "marginal call for '{}': the heuristics do not clearly favour spawning, and the requester flagged it as needing judgement, so the cortex decides",
                    r.requested_role
                ),
                owner: None,
            };
        }
        Decision {
            ok: true,
            rule: "APPROVE".into(),
            detail: format!(
                "no coverage for '{}', work estimate {} clears the bar, registry has room",
                r.requested_role, r.estimated_work
            ),
            owner: None,
        }
    }

    pub fn commit_spawn(&mut self, r: &SpawnRequest, d: Decision) -> Decision {
        let cid = r.correlation_id.clone();
        let rid = r.rid.clone();
        if d.rule == "ESCALATE" {
            let _ = self.parent.ledger.move_to(
                &rid,
                RequestState::Escalated,
                &d.detail,
                true,
                "ESCALATE",
            );
            self.parent.escalated.insert(rid.clone());
            let mut note = JMap::new();
            note.insert("agent".into(), JValue::Str(r.requester_agent_id.clone()));
            note.insert(
                "task_id".into(),
                r.parent_task_id
                    .as_ref()
                    .map(|s| JValue::Str(s.clone()))
                    .unwrap_or(JValue::Null),
            );
            note.insert("kind".into(), JValue::Str("spawn".into()));
            note.insert("reason".into(), JValue::Str(d.detail.clone()));
            note.insert("role".into(), JValue::Str(r.requested_role.clone()));
            note.insert("rid".into(), JValue::Str(rid.clone()));
            note.insert("at".into(), JValue::Float(py_round(self.now(), 3)));
            note.insert(
                "capability_class".into(),
                JValue::Str(r.capability_class.clone()),
            );
            self.record_escalation(note);
            let mut fields = JMap::new();
            fields.insert("rule".into(), JValue::Str("ESCALATE".into()));
            fields.insert("rid".into(), JValue::Str(rid));
            fields.insert(
                "requested_role".into(),
                JValue::Str(r.requested_role.clone()),
            );
            self.emit_corr(
                EventType::SpawnEscalated,
                "parent",
                &r.requester_agent_id,
                &d.detail,
                fields,
                None,
                Some(&cid),
            );
            self.parent.spawn_stats.escalated += 1;
            return d;
        }
        if !d.ok {
            if d.rule == "REJECT_DUPLICATE_CAPABILITY" && d.owner.is_some() {
                return self.reroute(r, d);
            }
            self.parent
                .ledger
                .close_as(&rid, RequestState::Rejected, &d.rule, &d.detail);
            self.record_decision(r, &d);
            let mut fields = JMap::new();
            fields.insert("rule".into(), JValue::Str(d.rule.clone()));
            fields.insert("rid".into(), JValue::Str(rid.clone()));
            fields.insert(
                "requested_role".into(),
                JValue::Str(r.requested_role.clone()),
            );
            if let Some(o) = &d.owner {
                fields.insert("owner".into(), JValue::Str(o.clone()));
            }
            self.emit_corr(
                EventType::SpawnRejected,
                "parent",
                &r.requester_agent_id,
                &d.detail,
                fields,
                None,
                Some(&cid),
            );
            self.parent.spawn_stats.rejected += 1;
            if !r.requester_agent_id.is_empty() {
                let mut payload = JMap::new();
                payload.insert("rule".into(), JValue::Str(d.rule.clone()));
                payload.insert("rid".into(), JValue::Str(rid));
                payload.insert(
                    "owner".into(),
                    d.owner
                        .as_ref()
                        .map(|s| JValue::Str(s.clone()))
                        .unwrap_or(JValue::Null),
                );
                payload.insert("do_it_yourself".into(), JValue::Bool(d.owner.is_some()));
                let mut msg = Message::new(
                    EventType::RequestDeclined,
                    "parent",
                    r.requester_agent_id.as_str(),
                );
                msg.body = format!("no new agent: {}", d.detail);
                msg.correlation_id = crate::ids::CorrelationId::new(&cid);
                msg.payload = payload;
                self.publish(msg);
            }
            return d;
        }

        let spec = r.to_task_spec();
        let mut added_deps: JMap = JMap::new();
        if self.graph.tasks.contains_key(&spec.task_id) {
            let mut fields = JMap::new();
            fields.insert("rid".into(), JValue::Str(rid.clone()));
            fields.insert("task_id".into(), JValue::Str(spec.task_id.as_str().into()));
            fields.insert("rule".into(), JValue::Str("REUSE_TASK".into()));
            self.emit_corr(
                EventType::SpawnRequestResolved,
                "parent",
                "parent",
                &format!("{} already exists; linking request {rid}", spec.task_id),
                fields,
                Some(spec.task_id.as_str()),
                Some(&cid),
            );
        } else {
            let res = self.graph.amend(vec![spec.clone()], &Default::default());
            if !res.ok {
                self.parent.ledger.close_as(
                    &rid,
                    RequestState::Rejected,
                    "REJECT_CYCLE",
                    &format!("amendment rejected: {:?}", res.cycle),
                );
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("REJECT_CYCLE".into()));
                fields.insert("rid".into(), JValue::Str(rid.clone()));
                fields.insert(
                    "added_tasks".into(),
                    JValue::Arr(vec![JValue::Obj(spec.snapshot())]),
                );
                fields.insert("restored".into(), JValue::Bool(res.restored));
                self.emit_corr(
                    EventType::GraphAmendRejected,
                    "parent",
                    "parent",
                    &format!(
                        "spawn-driven amendment for {} would create a cycle: {:?}",
                        spec.task_id, res.cycle
                    ),
                    fields,
                    None,
                    Some(&cid),
                );
                self.parent.spawn_stats.cycles_rejected += 1;
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("REJECT_CYCLE".into()));
                fields.insert("rid".into(), JValue::Str(rid.clone()));
                fields.insert(
                    "requested_role".into(),
                    JValue::Str(r.requested_role.clone()),
                );
                self.emit_corr(
                    EventType::SpawnRejected,
                    "parent",
                    &r.requester_agent_id,
                    "your request would create a dependency cycle; the graph was rolled back and no agent was created",
                    fields,
                    None,
                    Some(&cid),
                );
                self.parent.spawn_stats.rejected += 1;
                let d = Decision {
                    ok: false,
                    rule: "REJECT_CYCLE".into(),
                    detail: format!("cycle: {:?}", res.cycle),
                    owner: None,
                };
                self.record_decision(r, &d);
                return d;
            }
            let deps_after: Vec<String> = self
                .graph
                .tasks
                .get(&spec.task_id)
                .map(|t| t.deps.iter().map(|d| d.as_str().to_string()).collect())
                .unwrap_or_default();
            if !deps_after.is_empty() {
                added_deps.insert(
                    spec.task_id.as_str().into(),
                    JValue::Arr(deps_after.iter().cloned().map(JValue::Str).collect()),
                );
            }
            let mut snap = spec.snapshot();
            snap.insert(
                "claims".into(),
                JValue::Arr(spec.claims.iter().cloned().map(JValue::Str).collect()),
            );
            snap.insert(
                "skills".into(),
                JValue::Arr(spec.skills.iter().cloned().map(JValue::Str).collect()),
            );
            snap.insert(
                "consumes".into(),
                JValue::Arr(spec.consumes.iter().cloned().map(JValue::Str).collect()),
            );
            let mut fields = JMap::new();
            fields.insert("added_tasks".into(), JValue::Arr(vec![JValue::Obj(snap)]));
            fields.insert("added_deps".into(), JValue::Obj(added_deps));
            fields.insert(
                "order_after".into(),
                JValue::Arr(
                    self.graph
                        .order()
                        .unwrap_or_default()
                        .iter()
                        .map(|g| {
                            JValue::Arr(g.iter().map(|t| JValue::Str(t.as_str().into())).collect())
                        })
                        .collect(),
                ),
            );
            fields.insert("rid".into(), JValue::Str(rid.clone()));
            fields.insert(
                "requested_role".into(),
                JValue::Str(r.requested_role.clone()),
            );
            self.emit_corr(
                EventType::GraphAmended,
                "parent",
                "parent",
                &format!(
                    "graph amended by {}: +{} (re-topologically sorted, deps={:?})",
                    r.requester_agent_id, spec.task_id, deps_after
                ),
                fields,
                None,
                Some(&cid),
            );
            let _ = res;
        }

        let epoch = self
            .registry
            .get(&r.requester_agent_id)
            .map(|a| a.epoch + 1)
            .unwrap_or(1);
        let aid = self.parent_spawn(
            &r.requested_role,
            &r.required_skills,
            &r.reason,
            epoch,
            if r.requester_agent_id.is_empty() {
                "parent"
            } else {
                &r.requester_agent_id
            },
            None,
        );
        let Some(aid) = aid else {
            let _ = self.parent.ledger.move_to(
                &rid,
                RequestState::Deferred,
                "capacity freed later",
                true,
                "DEFER_FOR_CAPACITY",
            );
            if let Some(e) = self.parent.ledger.get_mut(&rid) {
                e.task_id = Some(spec.task_id.as_str().to_string());
            }
            let mut fields = JMap::new();
            fields.insert("rule".into(), JValue::Str("DEFER_FOR_CAPACITY".into()));
            fields.insert("rid".into(), JValue::Str(rid.clone()));
            fields.insert(
                "deferred_task_id".into(),
                JValue::Str(spec.task_id.as_str().into()),
            );
            fields.insert(
                "requested_role".into(),
                JValue::Str(r.requested_role.clone()),
            );
            self.emit_corr(
                EventType::DeferredForCapacity,
                "parent",
                &r.requester_agent_id,
                &format!(
                    "{} created but no agent slot; left pending for the next scheduling pass",
                    spec.task_id
                ),
                fields,
                Some(spec.task_id.as_str()),
                Some(&cid),
            );
            self.parent.spawn_stats.deferred += 1;
            let d = Decision {
                ok: false,
                rule: "DEFER_FOR_CAPACITY".into(),
                detail: format!(
                    "approved but {}/{} slots busy",
                    self.registry.active().len(),
                    self.registry.budget.max_active_agents
                ),
                owner: None,
            };
            self.record_decision(r, &d);
            return d;
        };
        self.assign_claimed(spec.task_id.as_str(), &aid, &r.reason, &cid);
        if let Some(e) = self.parent.ledger.get_mut(&rid) {
            e.spawned_agent_id = Some(aid.clone());
            e.task_id = Some(spec.task_id.as_str().to_string());
        }
        let _ = self.parent.ledger.move_to(
            &rid,
            RequestState::ApprovedCommitted,
            &format!("{aid} <- {}", spec.task_id),
            true,
            "APPROVE",
        );
        let mut fields = JMap::new();
        fields.insert("agent_id".into(), JValue::Str(aid.clone()));
        fields.insert(
            "approved_task_id".into(),
            JValue::Str(spec.task_id.as_str().into()),
        );
        fields.insert("rid".into(), JValue::Str(rid.clone()));
        fields.insert("epoch".into(), JValue::Int(epoch));
        fields.insert(
            "spawned_by".into(),
            JValue::Str(if r.requester_agent_id.is_empty() {
                "parent".into()
            } else {
                r.requester_agent_id.clone()
            }),
        );
        fields.insert("rule".into(), JValue::Str("APPROVE".into()));
        fields.insert(
            "requested_role".into(),
            JValue::Str(r.requested_role.clone()),
        );
        self.emit_corr(
            EventType::SpawnApproved,
            "parent",
            &aid,
            &format!(
                "spawned {aid} for {} (epoch {epoch})",
                if r.requester_agent_id.is_empty() {
                    "parent"
                } else {
                    &r.requester_agent_id
                }
            ),
            fields,
            Some(spec.task_id.as_str()),
            Some(&cid),
        );
        self.parent.spawn_stats.approved += 1;
        if !r.requester_agent_id.is_empty() {
            self.parent.spawn_stats.spawned_by_agents += 1;
            let mut payload = JMap::new();
            payload.insert("agent_id".into(), JValue::Str(aid.clone()));
            payload.insert("task_id".into(), JValue::Str(spec.task_id.as_str().into()));
            payload.insert("rid".into(), JValue::Str(rid));
            payload.insert(
                "expected_outputs".into(),
                JValue::Arr(
                    r.expected_outputs
                        .iter()
                        .cloned()
                        .map(JValue::Str)
                        .collect(),
                ),
            );
            let mut msg = Message::new(
                EventType::ApiContractReady,
                "parent",
                r.requester_agent_id.as_str(),
            );
            msg.body = format!("{aid} owns {} now", spec.task_id);
            msg.correlation_id = crate::ids::CorrelationId::new(&cid);
            msg.payload = payload;
            self.publish(msg);
        }
        self.record_decision(
            r,
            &Decision {
                ok: true,
                rule: "APPROVE".into(),
                detail: format!("{aid} <- {}", spec.task_id),
                owner: None,
            },
        );
        self.wake_ready(&format!("spawn of {aid}"));
        d
    }

    fn reroute(&mut self, r: &SpawnRequest, d: Decision) -> Decision {
        let spec = r.to_task_spec();
        let owner = d.owner.clone().unwrap_or_default();
        if !self.graph.tasks.contains_key(&spec.task_id) {
            let res = self.graph.amend(vec![spec.clone()], &Default::default());
            if !res.ok {
                self.parent.ledger.close_as(
                    &r.rid,
                    RequestState::Rejected,
                    "REJECT_CYCLE",
                    &format!("reuse amendment rejected: {:?}", res.cycle),
                );
                let mut fields = JMap::new();
                fields.insert("rule".into(), JValue::Str("REJECT_CYCLE".into()));
                fields.insert("rid".into(), JValue::Str(r.rid.clone()));
                self.emit_corr(
                    EventType::GraphAmendRejected,
                    "parent",
                    "parent",
                    &format!("reuse amendment would cycle: {:?}", res.cycle),
                    fields,
                    None,
                    Some(&r.correlation_id),
                );
                self.parent.spawn_stats.cycles_rejected += 1;
                return Decision {
                    ok: false,
                    rule: "REJECT_CYCLE".into(),
                    detail: format!("cycle: {:?}", res.cycle),
                    owner: None,
                };
            }
        }
        let ok = self.assign_claimed(
            spec.task_id.as_str(),
            &owner,
            &format!("reuse: {}", d.detail),
            &r.correlation_id,
        );
        if let Some(e) = self.parent.ledger.get_mut(&r.rid) {
            e.owner = Some(owner.clone());
            e.task_id = Some(spec.task_id.as_str().to_string());
        }
        let _ = self.parent.ledger.move_to(
            &r.rid,
            RequestState::Rerouted,
            &format!("{owner} takes {}", spec.task_id),
            true,
            "REUSE_EXISTING",
        );
        let mut fields = JMap::new();
        fields.insert("rid".into(), JValue::Str(r.rid.clone()));
        fields.insert("owner".into(), JValue::Str(owner.clone()));
        fields.insert(
            "rerouted_task_id".into(),
            JValue::Str(spec.task_id.as_str().into()),
        );
        fields.insert(
            "rule".into(),
            JValue::Str("REJECT_DUPLICATE_CAPABILITY".into()),
        );
        fields.insert("reused".into(), JValue::Bool(true));
        fields.insert("agent_spawned".into(), JValue::Bool(false));
        fields.insert(
            "requested_role".into(),
            JValue::Str(r.requested_role.clone()),
        );
        self.emit_corr(
            EventType::RequestRerouted,
            "parent",
            &owner,
            &format!(
                "no agent spawned: {owner} already covers '{}'; {} assigned to it (why: {})",
                r.requested_role, spec.task_id, d.detail
            ),
            fields,
            Some(spec.task_id.as_str()),
            Some(&r.correlation_id),
        );
        self.parent.spawn_stats.reused += 1;
        let d = Decision {
            ok: ok || d.ok,
            rule: "REUSE_EXISTING".into(),
            detail: format!("{owner} <- {}", spec.task_id),
            owner: Some(owner.clone()),
        };
        self.record_decision(r, &d);
        self.wake_ready(&format!("reroute to {owner}"));
        d
    }

    fn record_decision(&mut self, r: &SpawnRequest, d: &Decision) {
        let mut m = JMap::new();
        m.insert("at".into(), JValue::Float(py_round(self.now(), 3)));
        m.insert("tick".into(), JValue::Int(r.at_tick));
        m.insert("rid".into(), JValue::Str(r.rid.clone()));
        m.insert("from".into(), JValue::Str(r.requester_agent_id.clone()));
        m.insert("role".into(), JValue::Str(r.requested_role.clone()));
        m.insert(
            "task_id".into(),
            r.parent_task_id
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "capability_class".into(),
            JValue::Str(r.capability_class.clone()),
        );
        for (k, v) in d.to_dict() {
            m.insert(k, v);
        }
        self.parent.decisions.push(m);
        let mut fields = JMap::new();
        fields.insert("rule".into(), JValue::Str(d.rule.clone()));
        fields.insert("ok".into(), JValue::Bool(d.ok));
        fields.insert("rid".into(), JValue::Str(r.rid.clone()));
        fields.insert(
            "owner".into(),
            d.owner
                .as_ref()
                .map(|s| JValue::Str(s.clone()))
                .unwrap_or(JValue::Null),
        );
        self.emit_corr(
            EventType::SpawnRequestResolved,
            "parent",
            &r.requester_agent_id,
            &d.detail,
            fields,
            r.parent_task_id.as_deref(),
            Some(&r.correlation_id),
        );
    }

    pub fn decide_spawn_msg(&mut self, msg: &Message) -> Decision {
        self.decide_spawn(SpawnIn::Message(msg))
    }

    pub fn decide_spawn_map(&mut self, m: &JMap) -> Decision {
        self.decide_spawn(SpawnIn::Map(m))
    }

    pub fn decide_spawn(&mut self, req: SpawnIn<'_>) -> Decision {
        let Some(r) = self.intake_spawn_request(req) else {
            return Decision {
                ok: false,
                rule: "REJECT_INTAKE".into(),
                detail: "refused at intake (duplicate or malformed); see SPAWN_REJECTED".into(),
                owner: None,
            };
        };
        let d = self.evaluate_spawn(&r);
        let out = self.commit_spawn(&r, d);
        self.registry.version += 1;
        out
    }

    pub fn honour_pending_escalations(&mut self) -> Vec<Decision> {
        let mut out = Vec::new();
        let rids: Vec<String> = self.parent.escalated.iter().cloned().collect();
        for rid in rids {
            let state = self.parent.ledger.get(&rid).map(|e| e.state);
            if state != Some(RequestState::Escalated) {
                self.parent.escalated.remove(&rid);
                continue;
            }
            let approved = self.parent.decisions.iter().any(|d| {
                d.get("rid").and_then(|v| v.as_str()) == Some(rid.as_str())
                    && d.get("rule").and_then(|v| v.as_str()) == Some("APPROVE")
            });
            if !approved {
                continue;
            }
            self.parent.escalated.remove(&rid);
            let _ = self.parent.ledger.move_to(
                &rid,
                RequestState::Deferred,
                "answered by the arena cortex",
                false,
                "",
            );
            let _ = self.parent.ledger.move_to(
                &rid,
                RequestState::Evaluating,
                "re-committed after escalation",
                true,
                "",
            );
            let req = self
                .parent
                .ledger
                .get(&rid)
                .map(|e| e.request.clone())
                .expect("entry");
            let d = self.commit_spawn(
                &req,
                Decision {
                    ok: true,
                    rule: "APPROVE".into(),
                    detail: "escalation answered in the affirmative".into(),
                    owner: None,
                },
            );
            out.push(d);
        }
        out
    }

    pub fn drain_spawn_requests(&mut self) -> Vec<Decision> {
        let mut out = Vec::new();
        while !self.spawn_requests.is_empty() {
            let msg = self.spawn_requests.remove(0);
            out.push(self.decide_spawn(SpawnIn::Message(&msg)));
        }
        out
    }

    pub fn resolve_amendments(&mut self) -> Vec<JMap> {
        let mut out = Vec::new();
        while !self.feature_requests.is_empty() {
            let fr = self.feature_requests.remove(0);
            let target = jstr(&fr, "to");
            let payload = match fr.get("payload") {
                Some(JValue::Obj(m)) => m.clone(),
                _ => JMap::new(),
            };
            let feature = payload
                .get("feature")
                .and_then(|v| v.as_str())
                .unwrap_or("unnamed")
                .to_string();
            let from = jstr(&fr, "from");
            let corr = jstr(&fr, "correlation_id");
            let mut res = JMap::new();
            res.insert("feature".into(), JValue::Str(feature.clone()));
            res.insert("from".into(), JValue::Str(from.clone()));
            res.insert("to".into(), JValue::Str(target.clone()));
            res.insert("correlation_id".into(), JValue::Str(corr.clone()));
            let Some(trec_state) = self.registry.get(&target).map(|r| r.lifecycle.state) else {
                res.insert("option".into(), JValue::Str("C".into()));
                res.insert(
                    "why".into(),
                    JValue::Str(format!("unknown agent {target}; parent must decide")),
                );
                let mut note = JMap::new();
                note.insert("agent".into(), JValue::Str(from));
                note.insert(
                    "reason".into(),
                    JValue::Str(format!(
                        "feature '{feature}' addressed to unknown agent {target}"
                    )),
                );
                note.insert("kind".into(), JValue::Str("unknown_target".into()));
                note.insert("at".into(), JValue::Float(self.now()));
                self.record_escalation(note);
                out.push(res);
                continue;
            };
            let arts: Vec<String> = match payload.get("required_artifacts") {
                Some(JValue::Arr(a)) => a
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
                _ => vec![],
            };
            if let Some(art) = arts.first() {
                if self.artifact_exists(art) {
                    res.insert("option".into(), JValue::Str("A".into()));
                    res.insert(
                        "why".into(),
                        JValue::Str(format!("{art} already published")),
                    );
                    out.push(res);
                    continue;
                }
            }
            if matches!(
                trec_state,
                AgentState::WaitingForDependency
                    | AgentState::Blocked
                    | AgentState::Escalated
                    | AgentState::Paused
            ) {
                res.insert("option".into(), JValue::Str("C".into()));
                res.insert(
                    "why".into(),
                    JValue::Str(format!(
                        "{target} is {}; escalated to parent",
                        trec_state.as_str()
                    )),
                );
                let mut note = JMap::new();
                note.insert("agent".into(), JValue::Str(target.clone()));
                note.insert(
                    "reason".into(),
                    JValue::Str(format!(
                        "FEATURE_REQUEST '{feature}' arrived while {}",
                        trec_state.as_str()
                    )),
                );
                note.insert("kind".into(), JValue::Str("feature_while_sleeping".into()));
                note.insert("at".into(), JValue::Float(self.now()));
                self.record_escalation(note);
                out.push(res);
                continue;
            }
            let need: BTreeSet<String> = match payload.get("skills") {
                Some(JValue::Arr(a)) => a
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
                _ => BTreeSet::new(),
            };
            let rec = self.registry.get(&target).unwrap();
            let have_skills: Vec<&str> = rec.skills.iter().map(|s| s.as_str()).collect();
            let have = AgentRegistry::tokens(&rec.role, &have_skills);
            if !need.is_empty() && need.is_disjoint(&have) {
                res.insert("option".into(), JValue::Str("B".into()));
                res.insert(
                    "why".into(),
                    JValue::Str(format!(
                        "{target} skills {:?} do not cover {:?}: out of scope",
                        have, need
                    )),
                );
                let mut msg =
                    Message::new(EventType::RequestDeclined, target.as_str(), from.as_str());
                msg.body = jstr(&res, "why");
                msg.correlation_id = crate::ids::CorrelationId::new(&corr);
                self.publish(msg);
                out.push(res);
                continue;
            }
            let tid = format!(
                "t_feat_{}_{}",
                feature.replace(' ', "_"),
                corr.chars().rev().take(4).collect::<String>()
            );
            let mut new = TaskSpec::new(
                tid.as_str(),
                &format!("{feature} (requested by {from})"),
                &rec.role,
            );
            let mut skills: Vec<String> = have.union(&need).cloned().collect();
            skills.sort();
            new.skills = skills;
            new.est_work = payload
                .get("est_work")
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            new.produces = if arts.is_empty() {
                vec![format!("docs/{tid}.md")]
            } else {
                arts
            };
            new.consumes = match payload.get("consumes") {
                Some(JValue::Arr(a)) => a
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
                _ => vec![],
            };
            new.claims = vec![format!("feature:{feature}")];
            let amend = self.graph.amend(vec![new], &Default::default());
            if !amend.ok {
                res.insert("option".into(), JValue::Str("B".into()));
                res.insert(
                    "why".into(),
                    JValue::Str(format!(
                        "would create a cycle {:?}; rolled back",
                        amend.cycle
                    )),
                );
                let mut msg =
                    Message::new(EventType::RequestDeclined, target.as_str(), from.as_str());
                msg.body = jstr(&res, "why");
                self.publish(msg);
                out.push(res);
                continue;
            }
            self.assign_claimed(
                &tid,
                &target,
                &format!("feature request from {from}"),
                &corr,
            );
            res.insert("option".into(), JValue::Str("A".into()));
            res.insert(
                "why".into(),
                JValue::Str(format!("accepted as {tid} on {target}")),
            );
            res.insert("task_id".into(), JValue::Str(tid));
            out.push(res);
        }
        out
    }

    /// Drain spawn queue, resolve feature amendments, assign unowned pending.
    pub fn parent_schedule(&mut self) -> Vec<(String, String)> {
        let mut assigned = Vec::new();
        self.drain_spawn_requests();
        self.resolve_amendments();
        let unowned: Vec<String> = self
            .graph
            .tasks
            .values()
            .filter(|t| t.owner.is_none() && t.status == crate::graph::TaskStatus::Pending)
            .map(|t| t.task_id.as_str().to_string())
            .collect();
        for tid in unowned {
            let (role, skills) = match self.graph.tasks.get(&TaskId::new(&tid)) {
                Some(t) => (t.role.clone(), t.skills.clone()),
                None => continue,
            };
            let skill_refs: Vec<&str> = skills.iter().map(|s| s.as_str()).collect();
            let mut cand: Vec<(usize, f64, String)> = self
                .registry
                .cover(&role, &skill_refs)
                .into_iter()
                .filter(|a| {
                    matches!(
                        a.lifecycle.state,
                        AgentState::Idle | AgentState::Working | AgentState::WaitingForDependency
                    ) && !a.pending_work().iter().any(|t| t.as_str() == tid)
                })
                .map(|a| (a.load(), a.work_done, a.agent_id.as_str().to_string()))
                .collect();
            cand.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .then(a.2.cmp(&b.2))
            });
            if cand.is_empty() {
                cand = self
                    .registry
                    .with_state(&[AgentState::Idle])
                    .into_iter()
                    .filter(|a| !a.pending_work().iter().any(|t| t.as_str() == tid))
                    .map(|a| (a.load(), 0.0, a.agent_id.as_str().to_string()))
                    .collect();
                cand.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
            }
            if let Some((_, _, aid)) = cand.first() {
                if self.assign_claimed(&tid, aid, "scheduled", "") {
                    assigned.push((tid, aid.clone()));
                }
            } else if (self.registry.active().len() as i64) < self.registry.budget.max_active_agents
            {
                if let Some(aid) = self.parent_spawn(
                    &role,
                    &skills,
                    &format!("no free agent for {tid}"),
                    0,
                    "parent",
                    None,
                ) {
                    if self.assign_claimed(&tid, &aid, "scheduled-after-spawn", "") {
                        assigned.push((tid, aid));
                    }
                }
            }
        }
        assigned
    }

    pub fn honour_approve_spawn(&mut self, rid: &str, reason: &str) {
        if let Some(e) = self.parent.ledger.get(rid) {
            if e.state == RequestState::Escalated {
                let mut d = JMap::new();
                d.insert("at".into(), JValue::Float(py_round(self.now(), 3)));
                d.insert("rid".into(), JValue::Str(rid.into()));
                d.insert("from".into(), JValue::Str("arena".into()));
                d.insert("role".into(), JValue::Str(e.request.requested_role.clone()));
                d.insert("ok".into(), JValue::Bool(true));
                d.insert("rule".into(), JValue::Str("APPROVE".into()));
                d.insert(
                    "detail".into(),
                    JValue::Str(if reason.is_empty() {
                        "approved by the cortex".into()
                    } else {
                        reason.into()
                    }),
                );
                self.parent.decisions.push(d);
                return;
            }
        }
        let mut m = JMap::new();
        m.insert("from".into(), JValue::Str("parent".into()));
        m.insert("requested_role".into(), JValue::Str("specialist".into()));
        m.insert(
            "reason".into(),
            JValue::Str(if reason.is_empty() {
                "approved by the arena cortex".into()
            } else {
                reason.into()
            }),
        );
        m.insert("estimated_work".into(), JValue::Float(1.0));
        self.decide_spawn(SpawnIn::Map(&m));
    }

    pub fn honour_amend(
        &mut self,
        tasks: Vec<TaskSpec>,
        deps: &JMap,
        correlation_id: &str,
    ) -> AmendResult {
        let mut new_deps = std::collections::BTreeMap::new();
        for (k, v) in deps {
            if let JValue::Arr(a) = v {
                new_deps.insert(
                    k.clone(),
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect(),
                );
            }
        }
        let res = self.graph.amend(tasks.clone(), &new_deps);
        if res.ok {
            let mut fields = JMap::new();
            fields.insert(
                "added_tasks".into(),
                JValue::Arr(tasks.iter().map(|t| JValue::Obj(t.snapshot())).collect()),
            );
            self.emit_corr(
                EventType::PlanAmended,
                "parent",
                "parent",
                &format!("amend: +{:?}", res.added),
                fields,
                None,
                if correlation_id.is_empty() {
                    None
                } else {
                    Some(correlation_id)
                },
            );
            let _ = self.spawn_for_plan(&tasks);
        } else {
            let mut fields = JMap::new();
            fields.insert(
                "cycle".into(),
                JValue::Arr(res.cycle.iter().cloned().map(JValue::Str).collect()),
            );
            self.emit_corr(
                EventType::CycleRejected,
                "parent",
                "parent",
                &format!("amend rejected, rolled back {:?}", res.rolled_back),
                fields,
                None,
                if correlation_id.is_empty() {
                    None
                } else {
                    Some(correlation_id)
                },
            );
        }
        res
    }

    pub fn parent(&self) -> &Parent {
        &self.parent
    }
}

/// Input to the spawn funnel.
#[allow(clippy::large_enum_variant)]
pub enum SpawnIn<'a> {
    Request(SpawnRequest),
    Message(&'a Message),
    Map(&'a JMap),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_saas_hits_roles() {
        let p = RuleBasedPlanner;
        let (tasks, rat) = p.plan(
            "Build a SaaS app with authentication, dashboard, API backend, PostgreSQL database, and cloud deployment",
        );
        let roles: BTreeSet<_> = tasks.iter().map(|t| t.role.as_str()).collect();
        assert!(roles.contains("database"));
        assert!(roles.contains("backend"));
        assert!(roles.contains("frontend"));
        assert!(roles.contains("auth"));
        assert!(roles.contains("cloud"));
        assert!(rat.contains_key("matched"));
        let fe = tasks
            .iter()
            .find(|t| t.task_id.as_str() == "t_fe_integration")
            .unwrap();
        assert!(fe.consumes.contains(&"contracts/api.json".to_string()));
    }

    #[test]
    fn planner_empty_on_unrelated_text() {
        let p = RuleBasedPlanner;
        let (tasks, _) = p.plan("rename the landing page title");
        assert!(tasks.is_empty());
    }
}
