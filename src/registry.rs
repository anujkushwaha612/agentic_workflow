//! Agent Registry / Agent Pool with spawn-lineage accounting (`arena/registry.py`).
//!
//! Two separate limits, because they mean different things:
//! - `max_active_agents` — organisational ambition: how many engineers the
//!   Parent will manage;
//! - `max_concurrent_workers` — hardware reality: how many may hold an
//!   execution slot.
//!
//! Spawn lineage (epoch + parent) is what makes "infinite spawning"
//! structurally impossible rather than merely discouraged: agents at max epoch
//! cannot request agents, and total spawns are also bounded by a
//! work-estimate budget, so churning short-lived agents to dodge a cap does
//! not work.
//!
//! ## Ownership translation
//! The Python registry holds a live `graph` reference set by the kernel. In
//! Rust the kernel is the sole owner of mutable state, so the two graph-aware
//! queries ([`AgentRegistry::overloaded`], [`AgentRegistry::wait_for_edges`])
//! borrow the [`DependencyGraph`] per call instead. Semantics are unchanged.
//!
//! ## Ordering fidelity
//! Python dicts are insertion-ordered, and the iteration order of `active()` /
//! `idle_overdue()` is observable (reap order → journal row order). Agents are
//! therefore kept in a `Vec` in insertion order; every method whose output
//! order is defined by sorting (`snapshot`, `status_lines`, `cover`) sorts
//! exactly as the reference does.

use std::collections::{BTreeMap, BTreeSet};

use crate::graph::DependencyGraph;
use crate::ids::{AgentId, TaskId};
use crate::journal::Journal;
use crate::lifecycle::{AgentState, Lifecycle};
use crate::msg::EventType;
use crate::sys::json::{py_round, JMap, JValue};

// ------------------------------------------------------------------ errors

/// Typed registry failures (the reference raises `ValueError` / `KeyError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    AlreadyRegistered(String),
    UnknownAgent(String),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::AlreadyRegistered(id) => {
                write!(f, "agent {id} already registered")
            }
            RegistryError::UnknownAgent(id) => write!(f, "unknown agent '{id}'"),
        }
    }
}
impl std::error::Error for RegistryError {}

// ------------------------------------------------------------------ record

/// One agent's row in the pool. Field-for-field the Python `AgentRecord`
/// (15 fields incl. `task_queue`, `cognition`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgentRecord {
    pub agent_id: AgentId,
    pub role: String,
    pub skills: BTreeSet<String>,
    pub epoch: i64,
    pub spawned_by: String,
    pub spawn_reason: String,
    pub task_id: Option<TaskId>,
    pub lifecycle: Lifecycle,
    /// Durable waits as open JSON objects (reference: `list[dict]`).
    pub pending_waits: Vec<JMap>,
    /// Backlog: a specialist given two tasks must not be forced to drop one.
    pub task_queue: Vec<TaskId>,
    pub subscriptions: Vec<String>,
    pub msgs_sent: i64,
    pub work_done: f64,
    pub progress_steps: i64,
    pub notes: Vec<String>,
    /// How this agent decides, as data. Journaled at bind time and projected
    /// by fold(), so `arena status` on a *replayed* kernel still reports
    /// whether the org had real cognition.
    pub cognition: JMap,
}

impl AgentRecord {
    pub fn state(&self) -> AgentState {
        self.lifecycle.state
    }

    pub fn state_str(&self) -> &'static str {
        self.lifecycle.state.as_str()
    }

    /// `[task_id] + queue-without-task_id` — hats first, then the backlog.
    pub fn pending_work(&self) -> Vec<TaskId> {
        let mut out: Vec<TaskId> = Vec::new();
        if let Some(t) = &self.task_id {
            out.push(t.clone());
        }
        for t in &self.task_queue {
            if Some(t) != self.task_id.as_ref() {
                out.push(t.clone());
            }
        }
        out
    }

    pub fn load(&self) -> usize {
        self.pending_work().len()
    }

    /// JSON-safe projection (exact key set of the reference `snapshot()`).
    pub fn snapshot(&self) -> JMap {
        let mut m = JMap::new();
        m.insert(
            "agent_id".into(),
            JValue::Str(self.agent_id.as_str().into()),
        );
        m.insert("role".into(), JValue::Str(self.role.clone()));
        m.insert(
            "skills".into(),
            JValue::Arr(self.skills.iter().map(|s| JValue::Str(s.clone())).collect()),
        );
        m.insert("state".into(), JValue::Str(self.state_str().into()));
        m.insert(
            "state_since".into(),
            JValue::Float(py_round(self.lifecycle.since, 4)),
        );
        m.insert("epoch".into(), JValue::Int(self.epoch));
        m.insert("spawned_by".into(), JValue::Str(self.spawned_by.clone()));
        m.insert("task_id".into(), opt_task(&self.task_id));
        m.insert("msgs_sent".into(), JValue::Int(self.msgs_sent));
        m.insert(
            "work_done".into(),
            JValue::Float(py_round(self.work_done, 6)),
        );
        m.insert(
            "queue".into(),
            JValue::Arr(
                self.task_queue
                    .iter()
                    .map(|t| JValue::Str(t.as_str().into()))
                    .collect(),
            ),
        );
        m.insert("load".into(), JValue::Int(self.load() as i64));
        m.insert("cognition".into(), JValue::Obj(self.cognition.clone()));
        // `w.get("condition")` — an absent key projects as null
        m.insert(
            "waiting_on".into(),
            JValue::Arr(
                self.pending_waits
                    .iter()
                    .map(|w| w.get("condition").cloned().unwrap_or(JValue::Null))
                    .collect(),
            ),
        );
        m
    }
}

fn opt_task(t: &Option<TaskId>) -> JValue {
    match t {
        Some(t) => JValue::Str(t.as_str().to_string()),
        None => JValue::Null,
    }
}

// ------------------------------------------------------------------ budget

/// organisational ambition vs hardware reality (+ the anti-churn dials).
#[derive(Debug, Clone, PartialEq)]
pub struct SpawnBudget {
    pub max_active_agents: i64,
    pub max_concurrent_workers: i64,
    pub max_spawn_epoch: i64,
    /// a new agent must be worth at least this fraction of remaining work
    pub min_share_of_remaining: f64,
    /// an agent may not ask for help while itself this far over its estimate
    pub requester_overload_factor: f64,
    /// idle/completed agents are reaped after this much unproductive time
    pub idle_ttl: f64,
}

impl Default for SpawnBudget {
    fn default() -> Self {
        SpawnBudget {
            max_active_agents: 8,
            max_concurrent_workers: 2,
            max_spawn_epoch: 3,
            min_share_of_remaining: 0.10,
            requester_overload_factor: 2.0,
            idle_ttl: 5.0,
        }
    }
}

// ---------------------------------------------------------------- registry

/// Insertion-ordered agent pool. `version` bumps on every mutation; replay
/// tests assert `live.version == replay.version`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgentRegistry {
    pub agents: Vec<AgentRecord>,
    pub budget: SpawnBudget,
    _seq: BTreeMap<String, i64>,
    /// bumped on every mutation; replay tests assert live == replay
    pub version: i64,
}

impl AgentRegistry {
    pub fn new(budget: SpawnBudget) -> AgentRegistry {
        AgentRegistry {
            budget,
            ..Default::default()
        }
    }

    // ------------------------------------------------------------ creation
    /// `<slug>_<NN>` — lowercased, spaces and dashes to underscores. Note the
    /// reference does NOT fold `/` here (unlike `_tokens`).
    pub fn next_id(&mut self, role: &str) -> AgentId {
        let slug = role.trim().to_lowercase().replace([' ', '-'], "_");
        let n = self._seq.get(&slug).copied().unwrap_or(0) + 1;
        self._seq.insert(slug.clone(), n);
        AgentId::new(format!("{slug}_{n:02}"))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        agent_id: &str,
        role: &str,
        skills: &[&str],
        epoch: i64,
        spawned_by: &str,
        spawn_reason: &str,
        lifecycle: Option<Lifecycle>,
        subscriptions: &[&str],
    ) -> Result<AgentId, RegistryError> {
        if self.get(agent_id).is_some() {
            return Err(RegistryError::AlreadyRegistered(agent_id.to_string()));
        }
        let mut rec = AgentRecord {
            agent_id: AgentId::new(agent_id),
            role: role.to_string(),
            skills: skills.iter().map(|s| s.to_string()).collect(),
            epoch,
            spawned_by: spawned_by.to_string(),
            spawn_reason: spawn_reason.to_string(),
            lifecycle: lifecycle.unwrap_or_default(),
            subscriptions: subscriptions.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        };
        let note = if spawn_reason.is_empty() {
            format!("registered by {spawned_by}: planned")
        } else {
            format!("registered by {spawned_by}: {spawn_reason}")
        };
        rec.notes.push(note);
        self.agents.push(rec);
        self.version += 1;
        Ok(AgentId::new(agent_id))
    }

    pub fn get(&self, agent_id: &str) -> Option<&AgentRecord> {
        self.agents.iter().find(|a| a.agent_id.as_str() == agent_id)
    }

    pub fn get_mut(&mut self, agent_id: &str) -> Option<&mut AgentRecord> {
        self.agents
            .iter_mut()
            .find(|a| a.agent_id.as_str() == agent_id)
    }

    pub fn require(&self, agent_id: &str) -> Result<&AgentRecord, RegistryError> {
        self.get(agent_id)
            .ok_or_else(|| RegistryError::UnknownAgent(agent_id.to_string()))
    }

    pub fn require_mut(&mut self, agent_id: &str) -> Result<&mut AgentRecord, RegistryError> {
        self.get_mut(agent_id)
            .ok_or_else(|| RegistryError::UnknownAgent(agent_id.to_string()))
    }

    /// Remove from the pool entirely (unlike COMPLETED, which still holds a
    /// slot in `snapshot`). Returns the record so callers can journal it.
    pub fn terminate(&mut self, agent_id: &str, reason: &str) -> Option<AgentRecord> {
        let idx = self
            .agents
            .iter()
            .position(|a| a.agent_id.as_str() == agent_id)?;
        let mut rec = self.agents.remove(idx);
        let note = if reason.is_empty() {
            "terminated: unspecified".to_string()
        } else {
            format!("terminated: {reason}")
        };
        rec.notes.push(note);
        self.version += 1;
        Some(rec)
    }

    // ------------------------------------------------------------- queries
    /// Everyone still holding a pool slot (insertion order, like the dict).
    pub fn active(&self) -> Vec<&AgentRecord> {
        self.agents
            .iter()
            .filter(|a| a.lifecycle.state != AgentState::Terminated)
            .collect()
    }

    pub fn with_state(&self, states: &[AgentState]) -> Vec<&AgentRecord> {
        self.agents
            .iter()
            .filter(|a| states.contains(&a.lifecycle.state))
            .collect()
    }

    /// Role tokens (length > 2 only) plus lowercased skills (any length —
    /// exactly the reference's asymmetry: `"go"` counts as a skill token but
    /// never as a role token). `-` and `/` both split; case is preserved for
    /// role tokens here, while [`AgentRegistry::cover`] compares role tokens
    /// lowercased. Quirk kept deliberately.
    pub fn tokens(role: &str, skills: &[&str]) -> BTreeSet<String> {
        let mut toks = BTreeSet::new();
        for t in role.replace(['-', '/'], "_").split('_') {
            if t.chars().count() > 2 {
                toks.insert(t.to_string());
            }
        }
        for s in skills {
            toks.insert(s.to_lowercase());
        }
        toks
    }

    fn role_tokens_lower(role: &str) -> BTreeSet<String> {
        role.to_lowercase()
            .replace(['-', '/'], "_")
            .split('_')
            .filter(|t| t.chars().count() > 2)
            .map(|t| t.to_string())
            .collect()
    }

    /// Agents whose skill set covers the ask, best-matching first. Backs the
    /// "can an existing agent do this?" question in spawn evaluation.
    ///
    /// Scoring is the reference's: `-(overlap + 2*name_hit)` then `agent_id`
    /// (the `load` field in the reference tuple is *not* part of its sort
    /// key). Raw substring matching is deliberately not used: `"" in
    /// "backend"` is True in Python, so an unskilled role would silently
    /// "cover" everything.
    pub fn cover(&self, role: &str, skills: &[&str]) -> Vec<&AgentRecord> {
        let need = AgentRegistry::tokens(role, skills);
        let need_toks = AgentRegistry::role_tokens_lower(role);
        let mut scored: Vec<(i64, &AgentRecord)> = Vec::new();
        for a in self.active() {
            let have_skills: Vec<&str> = a.skills.iter().map(|s| s.as_str()).collect();
            let have = AgentRegistry::tokens(&a.role, &have_skills);
            let overlap = need.intersection(&have).count() as i64;
            let a_toks = AgentRegistry::role_tokens_lower(&a.role);
            let name_hit = !need_toks.is_disjoint(&a_toks) as i64;
            if overlap > 0 || name_hit > 0 {
                scored.push((-(overlap + 2 * name_hit), a));
            }
        }
        scored.sort_by(|x, y| {
            x.0.cmp(&y.0)
                .then_with(|| x.1.agent_id.as_str().cmp(y.1.agent_id.as_str()))
        });
        scored.into_iter().map(|(_, a)| a).collect()
    }

    /// True when the agent's open assigned work exceeds
    /// `requester_overload_factor × max(work_done, 0.25)`.
    pub fn overloaded(&self, graph: &DependencyGraph, agent_id: &str) -> bool {
        let Some(a) = self.get(agent_id) else {
            return false;
        };
        let mine: f64 = graph
            .tasks
            .values()
            .filter(|t| t.owner.as_ref().map(|o| o.as_str()) == Some(agent_id) && t.is_open())
            .map(|t| t.est_work)
            .sum();
        mine > self.budget.requester_overload_factor * a.work_done.max(0.25)
    }

    /// IDLE/COMPLETED agents past `idle_ttl` (insertion order).
    pub fn idle_overdue(&self, now: f64) -> Vec<&AgentRecord> {
        self.active()
            .into_iter()
            .filter(|a| {
                matches!(a.lifecycle.state, AgentState::Idle | AgentState::Completed)
                    && now - a.lifecycle.since >= self.budget.idle_ttl
            })
            .collect()
    }

    pub fn working_count(&self) -> usize {
        self.with_state(&[AgentState::Working]).len()
    }

    // ----------------------------------------------------------- rendering
    /// `{agent_id: snapshot}` sorted by agent id.
    pub fn snapshot(&self) -> JMap {
        let mut rows: Vec<&AgentRecord> = self.agents.iter().collect();
        rows.sort_by(|a, b| a.agent_id.as_str().cmp(b.agent_id.as_str()));
        let mut out = JMap::new();
        for a in rows {
            out.insert(a.agent_id.as_str().to_string(), JValue::Obj(a.snapshot()));
        }
        out
    }

    /// One line per agent, sorted by `(epoch, agent_id)`.
    pub fn status_lines(&self) -> Vec<String> {
        let glyph = |s: AgentState| -> &'static str {
            match s {
                AgentState::Working => "*",
                AgentState::Idle => "o",
                AgentState::WaitingForDependency => "~",
                AgentState::Blocked => "!",
                AgentState::Escalated => "^",
                AgentState::Paused => "=",
                AgentState::Completed => "+",
                AgentState::Created | AgentState::Initializing => ".",
                AgentState::Terminated => "x",
            }
        };
        let mut rows: Vec<&AgentRecord> = self.agents.iter().collect();
        rows.sort_by(|a, b| {
            a.epoch
                .cmp(&b.epoch)
                .then_with(|| a.agent_id.as_str().cmp(b.agent_id.as_str()))
        });
        rows.iter()
            .map(|a| {
                let suffix = match &a.task_id {
                    Some(t) => format!("  [{}]", t.as_str()),
                    None => String::new(),
                };
                format!(
                    "{:<22} {:<18} {} {}{}",
                    a.agent_id.as_str(),
                    a.role,
                    glyph(a.lifecycle.state),
                    a.state_str(),
                    suffix
                )
            })
            .collect()
    }

    /// agent -> agents it is directly blocked on, for deadlock detection.
    /// A waiting agent blocks on a task; that task's owner blocks it.
    pub fn wait_for_edges(&self, graph: &DependencyGraph) -> BTreeMap<AgentId, BTreeSet<AgentId>> {
        let mut agents: BTreeMap<AgentId, crate::graph::AgentWaitView> = BTreeMap::new();
        for a in &self.agents {
            let waits = a
                .pending_waits
                .iter()
                .map(|w| crate::graph::WaitRef {
                    task_id: w.get("task_id").and_then(|v| v.as_str()).map(TaskId::new),
                })
                .collect();
            agents.insert(
                a.agent_id.clone(),
                crate::graph::AgentWaitView {
                    state: a.state_str().to_string(),
                    waits,
                },
            );
        }
        DependencyGraph::wait_for_edges(&agents, graph)
    }

    // -------------------------------------------------------- journal sync
    /// Adopt a replayed registry (used by the crash/replay test).
    pub fn sync_from(&mut self, other: &AgentRegistry) {
        self.agents = other.agents.clone();
        self._seq = other._seq.clone();
        self.version = other.version;
    }

    /// The highest issued per-slug counter (replay reconstruction aid).
    pub fn seq_for(&self, slug: &str) -> i64 {
        self._seq.get(slug).copied().unwrap_or(0)
    }
}

// ------------------------------------------------------------- emit helpers

/// `AGENT_REGISTERED` row, exactly the reference's field set.
pub fn emit_registered(journal: &mut Journal, rec: &AgentRecord) {
    let mut fields = JMap::new();
    fields.insert("agent_id".into(), JValue::Str(rec.agent_id.as_str().into()));
    fields.insert("role".into(), JValue::Str(rec.role.clone()));
    fields.insert(
        "skills".into(),
        JValue::Arr(rec.skills.iter().map(|s| JValue::Str(s.clone())).collect()),
    );
    fields.insert("epoch".into(), JValue::Int(rec.epoch));
    fields.insert("spawned_by".into(), JValue::Str(rec.spawned_by.clone()));
    fields.insert("reason".into(), JValue::Str(rec.spawn_reason.clone()));
    journal.emit(
        EventType::AgentRegistered,
        "parent",
        rec.agent_id.as_str(),
        "",
        fields,
        None,
        None,
        None,
        None,
        0,
    );
}

/// `AGENT_TERMINATED` row.
pub fn emit_terminated(journal: &mut Journal, rec: &AgentRecord, reason: &str) {
    let mut fields = JMap::new();
    fields.insert("agent_id".into(), JValue::Str(rec.agent_id.as_str().into()));
    fields.insert("reason".into(), JValue::Str(reason.into()));
    journal.emit(
        EventType::AgentTerminated,
        "parent",
        rec.agent_id.as_str(),
        "",
        fields,
        None,
        None,
        None,
        None,
        0,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{TaskSpec, TaskStatus};

    fn fixture_reg() -> (AgentRegistry, DependencyGraph) {
        let mut g = DependencyGraph::default();
        g.add(
            TaskSpec {
                est_work: 10.0,
                ..TaskSpec::new("big", "lots of work", "backend")
            },
            true,
        )
        .unwrap();
        let r = AgentRegistry::new(SpawnBudget {
            max_active_agents: 3,
            max_spawn_epoch: 2,
            min_share_of_remaining: 0.25,
            idle_ttl: 1.0,
            ..Default::default()
        });
        (r, g)
    }

    fn add(
        r: &mut AgentRegistry,
        aid: &str,
        role: &str,
        skills: &[&str],
        epoch: i64,
        state: AgentState,
        since: f64,
    ) -> AgentId {
        r.register(
            aid,
            role,
            skills,
            epoch,
            "parent",
            "",
            Some(Lifecycle::with_state(state, since)),
            &[],
        )
        .unwrap()
    }

    #[test]
    fn ids_are_stable_and_slugged() {
        let (mut r, _) = fixture_reg();
        assert_eq!(
            r.next_id("Frontend Engineer").as_str(),
            "frontend_engineer_01"
        );
        assert_eq!(
            r.next_id("Frontend Engineer").as_str(),
            "frontend_engineer_02"
        );
        assert_eq!(r.next_id("ml engineer").as_str(), "ml_engineer_01");
    }

    #[test]
    fn duplicate_registration_is_rejected() {
        let (mut r, _) = fixture_reg();
        add(&mut r, "a1", "backend", &[], 0, AgentState::Idle, 0.0);
        let err = r.register("a1", "backend", &[], 0, "parent", "", None, &[]);
        assert_eq!(
            err.unwrap_err(),
            RegistryError::AlreadyRegistered("a1".into())
        );
    }

    #[test]
    fn cover_needs_real_skill_overlap_or_a_real_role_match() {
        let (mut r, _) = fixture_reg();
        add(
            &mut r,
            "be",
            "backend",
            &["api", "http"],
            0,
            AgentState::Idle,
            0.0,
        );
        let ids = |v: Vec<&AgentRecord>| -> Vec<String> {
            v.iter().map(|a| a.agent_id.as_str().to_string()).collect()
        };
        assert_eq!(ids(r.cover("backend", &[])), vec!["be"]); // role-token match
        assert_eq!(ids(r.cover("api design", &["api"])), vec!["be"]);
        // a nameless / tiny role must NOT be treated as covering everything
        assert!(
            r.cover("crew_0", &[]).is_empty(),
            "unskilled role silently covered everything"
        );
        assert!(r.cover("", &[]).is_empty());
        assert!(r.cover("payments", &["stripe"]).is_empty());
    }

    #[test]
    fn cover_ranks_best_match_first() {
        let (mut r, _) = fixture_reg();
        add(
            &mut r,
            "generalist",
            "backend",
            &["api"],
            0,
            AgentState::Idle,
            0.0,
        );
        add(
            &mut r,
            "specialist",
            "auth engineer",
            &["auth", "oauth", "jwt"],
            0,
            AgentState::Idle,
            0.0,
        );
        let best = r.cover("oauth auth flow", &["auth", "oauth"]);
        assert_eq!(best[0].agent_id.as_str(), "specialist");
    }

    #[test]
    fn terminated_agents_stop_counting() {
        let (mut r, _) = fixture_reg();
        let a = add(&mut r, "a1", "backend", &["api"], 0, AgentState::Idle, 0.0);
        assert_eq!(r.active().len(), 1);
        r.get_mut(a.as_str()).unwrap().lifecycle.state = AgentState::Terminated;
        assert!(r.active().is_empty());
        assert!(r.cover("backend", &[]).is_empty());
    }

    #[test]
    fn load_and_pending_work() {
        let (mut r, _) = fixture_reg();
        let a = add(&mut r, "a1", "backend", &[], 0, AgentState::Idle, 0.0);
        {
            let rec = r.get_mut(a.as_str()).unwrap();
            rec.task_id = Some(TaskId::new("big"));
            rec.task_queue = vec![TaskId::new("extra")];
        }
        let rec = r.get(a.as_str()).unwrap();
        assert_eq!(rec.load(), 2);
        assert_eq!(
            rec.pending_work()
                .iter()
                .map(|t| t.as_str().to_string())
                .collect::<Vec<_>>(),
            vec!["big", "extra"]
        );
    }

    #[test]
    fn overloaded_needs_actual_progress() {
        let (mut r, mut g) = fixture_reg();
        let a = add(&mut r, "be", "backend", &["api"], 0, AgentState::Idle, 0.0);
        g.tasks.get_mut(&TaskId::new("big")).unwrap().owner = Some(a.clone());
        r.get_mut(a.as_str()).unwrap().task_id = Some(TaskId::new("big"));
        assert_eq!(r.get(a.as_str()).unwrap().work_done, 0.0);
        assert!(
            r.overloaded(&g, "be"),
            "10 units of open work vs no progress must read as overloaded"
        );
        r.get_mut(a.as_str()).unwrap().work_done = 9.0;
        assert!(!r.overloaded(&g, "be"), "nearly done is not overloaded");
        // unknown agent / no open work → false
        assert!(!r.overloaded(&g, "nobody"));
    }

    #[test]
    fn idle_ttl_only_touches_sleepers() {
        let (mut r, _) = fixture_reg();
        add(&mut r, "idle", "x", &[], 0, AgentState::Idle, 0.0);
        add(&mut r, "busy", "y", &[], 0, AgentState::Working, 0.0);
        let ids = |v: Vec<&AgentRecord>| -> Vec<String> {
            v.iter().map(|a| a.agent_id.as_str().to_string()).collect()
        };
        assert!(r.idle_overdue(0.5).is_empty());
        assert_eq!(ids(r.idle_overdue(1.5)), vec!["idle"]);
    }

    #[test]
    fn terminate_bumps_version_for_replay_checks() {
        let (mut r, _) = fixture_reg();
        add(&mut r, "a1", "x", &[], 0, AgentState::Idle, 0.0);
        let v0 = r.version;
        let rec = r.terminate("a1", "done").unwrap();
        assert!(r.version > v0);
        assert!(r.get("a1").is_none());
        assert_eq!(rec.notes.last().unwrap(), "terminated: done");
        // terminating an unknown agent is a no-op (version untouched)
        let v1 = r.version;
        assert!(r.terminate("ghost", "").is_none());
        assert_eq!(r.version, v1);
    }

    #[test]
    fn wait_for_edges_uses_pending_waits() {
        let (mut r, mut g) = fixture_reg();
        g.add(
            TaskSpec {
                owner: Some(AgentId::new("b1")),
                ..TaskSpec::new("t2", "other", "b")
            },
            true,
        )
        .unwrap();
        add(
            &mut r,
            "a1",
            "a",
            &[],
            0,
            AgentState::WaitingForDependency,
            0.0,
        );
        add(
            &mut r,
            "b1",
            "b",
            &[],
            0,
            AgentState::WaitingForDependency,
            0.0,
        );
        {
            let rec = r.get_mut("a1").unwrap();
            let mut w = JMap::new();
            w.insert("task_id".into(), JValue::Str("t2".into()));
            rec.pending_waits.push(w);
        }
        {
            let rec = r.get_mut("b1").unwrap();
            let mut w = JMap::new();
            w.insert("task_id".into(), JValue::Str("big".into()));
            rec.pending_waits.push(w);
        }
        g.tasks.get_mut(&TaskId::new("big")).unwrap().owner = Some(AgentId::new("a1"));
        g.tasks.get_mut(&TaskId::new("t2")).unwrap().owner = Some(AgentId::new("b1"));
        let edges = r.wait_for_edges(&g);
        let fmt = |e: &BTreeMap<AgentId, BTreeSet<AgentId>>| {
            e.iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_string(),
                        v.iter().map(|x| x.as_str().to_string()).collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let got = fmt(&edges);
        assert_eq!(
            got,
            vec![
                ("a1".into(), vec!["b1".into()]),
                ("b1".into(), vec!["a1".into()])
            ]
        );
    }

    #[test]
    fn status_lines_are_sorted_by_epoch() {
        let (mut r, _) = fixture_reg();
        add(&mut r, "zz_01", "z", &[], 1, AgentState::Idle, 0.0);
        add(&mut r, "aa_01", "a", &[], 0, AgentState::Idle, 0.0);
        let lines = r.status_lines();
        assert!(lines[0].starts_with("aa_01"), "{lines:?}");
        assert!(lines[0].contains("IDLE"));
        // idle glyph in the state column region
        assert!(lines[0][27..].contains('o'), "{}", lines[0]);
    }

    #[test]
    fn snapshot_is_json_safe() {
        let (mut r, _) = fixture_reg();
        add(&mut r, "a1", "x", &["y"], 0, AgentState::Idle, 0.0);
        let snap = JValue::Obj(r.snapshot());
        // canonical serialization must succeed (JSON-safe values only)
        let s = snap.to_canon_string();
        assert!(s.contains("\"agent_id\":\"a1\""));
        assert!(s.contains("\"skills\":[\"y\"]"));
    }

    #[test]
    fn spawn_budget_defaults_are_finite() {
        let b = SpawnBudget::default();
        assert!(0 < b.max_active_agents && b.max_active_agents < 64);
        assert!(b.max_concurrent_workers >= 1);
        assert!(
            b.max_spawn_epoch <= 3,
            "unbounded spawn depth invites recursion"
        );
    }

    // ---------------------- extras: exact-quirk regression tests ----------

    #[test]
    fn next_id_keeps_slashes_but_tokens_split_them() {
        let (mut r, _) = fixture_reg();
        // next_id only folds space and dash — '/' survives (reference quirk)
        assert_eq!(r.next_id("dev ops/infra").as_str(), "dev_ops/infra_01");
        // tokens split on '/' for coverage purposes
        let toks = AgentRegistry::tokens("dev/ops", &[]);
        assert!(toks.contains("dev") && toks.contains("ops"));
    }

    #[test]
    fn cover_name_hit_is_weighted_double_but_skills_count() {
        let (mut r, _) = fixture_reg();
        add(
            &mut r,
            "role_match",
            "database",
            &[],
            0,
            AgentState::Idle,
            0.0,
        );
        add(
            &mut r,
            "skill_match",
            "misc",
            &["database", "extra", "more"],
            0,
            AgentState::Idle,
            0.0,
        );
        // role_match: name_hit=1 → score -(0+2)=-2; skill_match: overlap=1 → -1.
        // Lower score wins, so the role match ranks first (verified vs Python).
        let order: Vec<String> = r
            .cover("database", &[])
            .iter()
            .map(|a| a.agent_id.as_str().to_string())
            .collect();
        assert_eq!(order, vec!["role_match", "skill_match"]);
    }

    #[test]
    fn snapshot_rounds_like_python() {
        let (mut r, _) = fixture_reg();
        let a = add(&mut r, "a1", "x", &[], 0, AgentState::Idle, 0.0);
        {
            let rec = r.get_mut(a.as_str()).unwrap();
            rec.lifecycle.since = 0.123456789;
            rec.work_done = 1.23456789;
            let mut w = JMap::new();
            w.insert(
                "condition".into(),
                JValue::Str("artifact:contracts/api.json".into()),
            );
            rec.pending_waits.push(w);
            let w2 = JMap::new(); // no condition key → null
            rec.pending_waits.push(w2);
        }
        let snap = r.get(a.as_str()).unwrap().snapshot();
        assert_eq!(snap.get("state_since").unwrap().as_f64(), Some(0.1235));
        assert_eq!(snap.get("work_done").unwrap().as_f64(), Some(1.234568));
        let waiting = snap.get("waiting_on").unwrap();
        let s = waiting.to_canon_string();
        assert_eq!(s, "[\"artifact:contracts/api.json\",null]");
    }

    #[test]
    fn status_line_formats_and_suffix() {
        let (mut r, _) = fixture_reg();
        let a = add(
            &mut r,
            "database_01",
            "database engineer",
            &[],
            0,
            AgentState::Working,
            0.0,
        );
        r.get_mut(a.as_str()).unwrap().task_id = Some(TaskId::new("t_db"));
        let lines = r.status_lines();
        assert_eq!(
            lines[0],
            "database_01            database engineer  * WORKING  [t_db]"
        );
    }

    #[test]
    fn registration_notes_and_version() {
        let (mut r, _) = fixture_reg();
        let v0 = r.version;
        let a = add(&mut r, "a1", "backend", &["api"], 0, AgentState::Idle, 0.0);
        assert_eq!(r.version, v0 + 1);
        let rec = r.get(a.as_str()).unwrap();
        assert_eq!(rec.notes, vec!["registered by parent: planned"]);
        r.register("a2", "x", &[], 1, "backend_01", "help me", None, &[])
            .unwrap();
        assert_eq!(
            r.get("a2").unwrap().notes,
            vec!["registered by backend_01: help me"]
        );
        assert_eq!(r.get("a2").unwrap().epoch, 1);
    }

    #[test]
    fn sync_from_adopts_replayed_state() {
        let (mut r, _) = fixture_reg();
        add(&mut r, "a1", "backend", &["api"], 0, AgentState::Idle, 0.0);
        r.next_id("backend");
        let mut other = AgentRegistry::default();
        add(
            &mut other,
            "z9",
            "ml",
            &["torch"],
            2,
            AgentState::Working,
            3.0,
        );
        other.version = 42;
        r.sync_from(&other);
        assert_eq!(r.version, 42);
        assert!(r.get("a1").is_none());
        assert!(r.get("z9").is_some());
        assert_eq!(r.seq_for("backend"), 0, "seq must be adopted too");
    }

    #[test]
    fn require_unknown_is_typed_error() {
        let (r, _) = fixture_reg();
        assert_eq!(
            r.require("ghost").unwrap_err(),
            RegistryError::UnknownAgent("ghost".into())
        );
    }

    #[test]
    fn open_task_filter_in_overload() {
        // only OPEN tasks count toward overload
        let (mut r, mut g) = fixture_reg();
        g.add(TaskSpec::new("done1", "finished", "backend"), true)
            .unwrap();
        g.tasks.get_mut(&TaskId::new("done1")).unwrap().status = TaskStatus::Done;
        let a = add(&mut r, "be", "backend", &[], 0, AgentState::Idle, 0.0);
        g.tasks.get_mut(&TaskId::new("done1")).unwrap().owner = Some(a.clone());
        g.tasks.get_mut(&TaskId::new("big")).unwrap().owner = Some(a.clone());
        assert!(r.overloaded(&g, "be")); // only "big" (10.0) counts; done1 excluded
        g.tasks.get_mut(&TaskId::new("big")).unwrap().status = TaskStatus::Done;
        assert!(!r.overloaded(&g, "be"));
    }

    #[test]
    fn emit_helpers_write_reference_payloads() {
        let mut j = Journal::open_memory().unwrap();
        let (mut r, _) = fixture_reg();
        let a = add(
            &mut r,
            "database_01",
            "database",
            &["sql", "schema"],
            0,
            AgentState::Idle,
            0.0,
        );
        emit_registered(&mut j, r.get(a.as_str()).unwrap());
        let rows = j.iterate().unwrap();
        let reg_payload = rows[0].payload_text.clone();
        assert_eq!(
            reg_payload,
            "{\"agent_id\":\"database_01\",\"body\":\"\",\"epoch\":0,\"reason\":\"\",\"role\":\"database\",\"skills\":[\"schema\",\"sql\"],\"spawned_by\":\"parent\"}"
        );
        let rec = r.terminate("database_01", "idle reap").unwrap();
        emit_terminated(&mut j, &rec, "idle reap");
        let rows = j.iterate().unwrap();
        assert_eq!(rows[1].etype, "AGENT_TERMINATED");
        assert_eq!(
            rows[1].payload_text,
            "{\"agent_id\":\"database_01\",\"body\":\"\",\"reason\":\"idle reap\"}"
        );
    }
}
