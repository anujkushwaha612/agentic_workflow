//! Dependency DAG + the agent wait-for graph (`arena/graph.py`).
//!
//! Task edges are DERIVED, not hand-written: a task declares the artifacts it
//! produces/consumes; an edge B→A appears iff A consumes an artifact B produces.
//! That is what makes "dynamic" real: the parent never hardcodes dependencies.

use crate::ids::{AgentId, TaskId};
use crate::sys::json::{py_round, JMap, JValue};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskStatus {
    #[default]
    Pending,
    Assigned,
    Running,
    Waiting,
    Done,
    Failed,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Assigned => "assigned",
            TaskStatus::Running => "running",
            TaskStatus::Waiting => "waiting",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> TaskStatus {
        match s {
            "assigned" => TaskStatus::Assigned,
            "running" => TaskStatus::Running,
            "waiting" => TaskStatus::Waiting,
            "done" => TaskStatus::Done,
            "failed" => TaskStatus::Failed,
            _ => TaskStatus::Pending,
        }
    }
    pub fn is_open(&self) -> bool {
        matches!(
            self,
            TaskStatus::Pending | TaskStatus::Assigned | TaskStatus::Running | TaskStatus::Waiting
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskSpec {
    pub task_id: TaskId,
    pub title: String,
    pub role: String,
    pub skills: Vec<String>,
    pub est_work: f64,
    pub produces: Vec<String>,
    pub consumes: Vec<String>,
    pub claims: Vec<String>,
    pub deps: BTreeSet<TaskId>,
    pub owner: Option<AgentId>,
    pub status: TaskStatus,
    /// M2: verification is part of the task — argv lists that must exit 0.
    pub verify: Vec<Vec<String>>,
    pub verified: bool,
    pub verified_at: Option<f64>,
    /// started exactly once, so TASK_STARTED cannot be double-journalled.
    pub started: bool,
    pub started_at: Option<f64>,
    pub finished_at: Option<f64>,
    pub notes: Vec<String>,
}

impl TaskSpec {
    pub fn new(task_id: impl Into<TaskId>, title: &str, role: &str) -> TaskSpec {
        TaskSpec {
            task_id: task_id.into(),
            title: title.to_string(),
            role: role.to_string(),
            est_work: 1.0,
            status: TaskStatus::Pending,
            ..Default::default()
        }
    }

    /// Dedup/ownership key for the claim table: what work, on what resources.
    pub fn key(&self) -> String {
        let mut parts = vec![self.role.clone()];
        let mut produces: Vec<&String> = self.produces.iter().collect();
        produces.sort();
        produces.dedup();
        parts.extend(produces.into_iter().cloned());
        let mut claims = self.claims.clone();
        claims.sort();
        parts.extend(claims);
        parts.join("|")
    }

    pub fn is_open(&self) -> bool {
        self.status.is_open()
    }

    pub fn snapshot(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("task_id".into(), JValue::Str(self.task_id.as_str().into()));
        m.insert("title".into(), JValue::Str(self.title.clone()));
        m.insert("role".into(), JValue::Str(self.role.clone()));
        m.insert("status".into(), JValue::Str(self.status.as_str().into()));
        m.insert(
            "owner".into(),
            self.owner
                .as_ref()
                .map(|o| JValue::Str(o.as_str().to_string()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "deps".into(),
            JValue::Arr(
                self.deps
                    .iter()
                    .map(|d| JValue::Str(d.as_str().into()))
                    .collect(),
            ),
        );
        m.insert(
            "produces".into(),
            JValue::Arr(
                self.produces
                    .iter()
                    .map(|p| JValue::Str(p.clone()))
                    .collect(),
            ),
        );
        m.insert(
            "consumes".into(),
            JValue::Arr(
                self.consumes
                    .iter()
                    .map(|c| JValue::Str(c.clone()))
                    .collect(),
            ),
        );
        m.insert("est_work".into(), JValue::Float(self.est_work));
        m.insert("started".into(), JValue::Bool(self.started));
        m.insert(
            "verify".into(),
            JValue::Arr(
                self.verify
                    .iter()
                    .map(|v| JValue::Arr(v.iter().map(|a| JValue::Str(a.clone())).collect()))
                    .collect(),
            ),
        );
        m.insert("verified".into(), JValue::Bool(self.verified));
        m
    }
}

#[derive(Debug)]
pub struct CycleError(pub Vec<String>);

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "circular dependency: {}", self.0.join(" -> "))
    }
}
impl std::error::Error for CycleError {}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DependencyGraph {
    pub tasks: BTreeMap<TaskId, TaskSpec>,
    /// artifact -> task ids that produce it
    pub producers: BTreeMap<String, BTreeSet<TaskId>>,
    /// artifacts known to exist (kernel keeps in sync; gating is artifact-based)
    pub known_artifacts: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct AmendResult {
    pub ok: bool,
    pub added: Vec<TaskId>,
    pub new_edges: BTreeMap<String, Vec<String>>,
    pub rolled_back: Vec<TaskId>,
    pub cycle: Vec<String>,
    pub restored: bool,
}

impl AmendResult {
    fn ok(added: Vec<TaskId>, new_edges: BTreeMap<String, Vec<String>>) -> AmendResult {
        AmendResult {
            ok: true,
            added,
            new_edges,
            rolled_back: vec![],
            cycle: vec![],
            restored: false,
        }
    }
}

impl DependencyGraph {
    // ------------------------------------------------------------ mutation
    pub fn add(&mut self, spec: TaskSpec, derive: bool) -> Result<(), String> {
        if self.tasks.contains_key(&spec.task_id) {
            return Err(format!("duplicate task_id {}", spec.task_id));
        }
        for a in &spec.produces {
            self.producers
                .entry(a.clone())
                .or_default()
                .insert(spec.task_id.clone());
        }
        let id = spec.task_id.clone();
        self.tasks.insert(id, spec);
        if derive {
            self.derive_edges();
        }
        Ok(())
    }

    pub fn remove(&mut self, task_id: &TaskId) -> Option<TaskSpec> {
        let spec = self.tasks.remove(task_id)?;
        for a in &spec.produces {
            if let Some(set) = self.producers.get_mut(a) {
                set.remove(task_id);
            }
        }
        for t in self.tasks.values_mut() {
            t.deps.remove(task_id);
        }
        Some(spec)
    }

    /// B depends on A iff B consumes an artifact A produces. Returns edges added.
    pub fn derive_edges(&mut self) -> BTreeMap<String, Vec<String>> {
        let mut added: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let ids: Vec<TaskId> = self.tasks.keys().cloned().collect();
        for tid in ids {
            let consumes = self.tasks[&tid].consumes.clone();
            for artifact in consumes {
                let producers: Vec<TaskId> = self
                    .producers
                    .get(&artifact)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                for producer in producers {
                    if producer == tid {
                        continue;
                    }
                    let spec = self.tasks.get_mut(&tid).expect("task present");
                    if !spec.deps.contains(&producer) {
                        spec.deps.insert(producer.clone());
                        added
                            .entry(tid.as_str().to_string())
                            .or_default()
                            .push(producer.as_str().to_string());
                    }
                }
            }
        }
        added
    }

    /// Validated edge insert; rolls back if it creates a cycle.
    pub fn add_edge(&mut self, dependent: &TaskId, dependency: &TaskId) -> Result<(), CycleError> {
        {
            let spec = self
                .tasks
                .get_mut(dependent)
                .ok_or_else(|| CycleError(vec![format!("no task {dependent}")]))?;
            if spec.deps.contains(dependency) {
                return Ok(());
            }
            spec.deps.insert(dependency.clone());
        }
        match self.validate() {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Some(spec) = self.tasks.get_mut(dependent) {
                    spec.deps.remove(dependency);
                }
                Err(e)
            }
        }
    }

    fn adj(&self) -> BTreeMap<TaskId, BTreeSet<TaskId>> {
        self.tasks
            .iter()
            .map(|(t, s)| {
                (
                    t.clone(),
                    s.deps
                        .iter()
                        .filter(|d| self.tasks.contains_key(*d))
                        .cloned()
                        .collect(),
                )
            })
            .collect()
    }

    pub fn validate(&self) -> Result<(), CycleError> {
        match topo_generations(&self.adj()) {
            Ok(_) => Ok(()),
            Err(cycle) => Err(CycleError(cycle)),
        }
    }

    // ------------------------------------------------------------- reading
    /// Topological generations: each inner list can run in parallel (sorted).
    pub fn order(&self) -> Result<Vec<Vec<TaskId>>, CycleError> {
        topo_generations(&self.adj()).map_err(CycleError)
    }

    pub fn predecessors(&self, task_id: &TaskId) -> BTreeSet<TaskId> {
        let mut out = BTreeSet::new();
        for (p, s) in &self.tasks {
            if s.deps.contains(task_id) {
                out.insert(p.clone());
            }
        }
        out
    }

    pub fn successors(&self, task_id: &TaskId) -> BTreeSet<TaskId> {
        self.tasks
            .get(task_id)
            .map(|s| s.deps.clone())
            .unwrap_or_default()
    }

    pub fn blocked_by(&self, task_id: &TaskId) -> Vec<TaskId> {
        self.successors(task_id)
            .iter()
            .filter(|d| self.tasks.get(*d).map(|t| t.is_open()).unwrap_or(false))
            .cloned()
            .collect()
    }

    pub fn ready(&self, require_owner: bool) -> Vec<TaskId> {
        let mut out = Vec::new();
        for (tid, s) in &self.tasks {
            if s.status != TaskStatus::Pending {
                continue;
            }
            if require_owner && s.owner.is_none() {
                continue;
            }
            if s.deps
                .iter()
                .all(|d| self.tasks.get(d).map(|t| !t.is_open()).unwrap_or(true))
            {
                out.push(tid.clone());
            }
        }
        out
    }

    pub fn is_ready(&self, task_id: &TaskId) -> bool {
        match self.tasks.get(task_id) {
            None => false,
            Some(s) => s
                .deps
                .iter()
                .all(|d| self.tasks.get(d).map(|t| !t.is_open()).unwrap_or(true)),
        }
    }

    /// Upstream tasks whose outputs are not all present yet.
    pub fn unmet_artifact_producers(&self, task_id: &TaskId) -> Vec<TaskId> {
        let mut out = Vec::new();
        let deps: Vec<TaskId> = self
            .tasks
            .get(task_id)
            .map(|s| s.deps.iter().cloned().collect())
            .unwrap_or_default();
        for d in deps {
            if let Some(dep) = self.tasks.get(&d) {
                if dep
                    .produces
                    .iter()
                    .any(|a| !self.known_artifacts.contains(a))
                {
                    out.push(d);
                }
            }
        }
        out
    }

    /// What this task is waiting on, by name — backs `arena why`.
    pub fn unmet(&self, task_id: &TaskId) -> Vec<String> {
        match self.tasks.get(task_id) {
            None => vec![],
            Some(s) => s
                .deps
                .iter()
                .filter(|d| self.tasks.contains_key(*d))
                .filter(|d| self.tasks[*d].is_open())
                .map(|d| format!("{}:{}", d.as_str(), self.tasks[d].status.as_str()))
                .collect(),
        }
    }

    pub fn remaining_work(&self) -> f64 {
        self.tasks
            .values()
            .filter(|t| t.is_open())
            .map(|t| t.est_work)
            .sum()
    }

    pub fn remaining_work_for(&self, role: &str) -> f64 {
        self.tasks
            .values()
            .filter(|t| t.is_open() && t.role == role)
            .map(|t| t.est_work)
            .sum()
    }

    pub fn all_artifacts(&self) -> BTreeMap<String, Vec<String>> {
        self.producers
            .iter()
            .map(|(a, t)| {
                let mut v: Vec<String> = t.iter().map(|x| x.as_str().to_string()).collect();
                v.sort();
                (a.clone(), v)
            })
            .collect()
    }

    pub fn snapshot(&self) -> JMap {
        let mut m = JMap::new();
        m.insert(
            "order".into(),
            match self.order() {
                Ok(gens) => JValue::Arr(
                    gens.iter()
                        .map(|g| {
                            JValue::Arr(g.iter().map(|t| JValue::Str(t.as_str().into())).collect())
                        })
                        .collect(),
                ),
                Err(_) => JValue::Null,
            },
        );
        let mut tasks = JMap::new();
        for (t, s) in &self.tasks {
            tasks.insert(t.as_str().to_string(), JValue::Obj(s.snapshot()));
        }
        m.insert("tasks".into(), JValue::Obj(tasks));
        m.insert(
            "artifacts".into(),
            JValue::Obj(map_to_jmap(&self.all_artifacts())),
        );
        m
    }

    pub fn has_cycle(&self) -> bool {
        self.validate().is_err()
    }

    // ------------------------------------------------------------ amendment
    /// Mutate the graph, revalidate, roll back *everything* on a cycle.
    pub fn amend(
        &mut self,
        new_tasks: Vec<TaskSpec>,
        new_deps: &BTreeMap<String, Vec<String>>,
    ) -> AmendResult {
        let before = self.clone();
        let mut added: Vec<TaskId> = Vec::new();
        // validate before touching edges: an added task can close a cycle by
        // itself, purely through what it consumes.
        for spec in new_tasks {
            if !self.tasks.contains_key(&spec.task_id) {
                if let Err(e) = self.add(spec.clone(), true) {
                    let _ = e;
                    let cycle = self.validate().err().map(|c| c.0).unwrap_or_default();
                    *self = before;
                    return AmendResult {
                        ok: false,
                        added: added.clone(),
                        new_edges: BTreeMap::new(),
                        rolled_back: added.clone(),
                        cycle,
                        restored: true,
                    };
                }
                added.push(spec.task_id.clone());
            }
        }
        if let Err(e) = self.validate() {
            *self = before;
            return AmendResult {
                ok: false,
                added: added.clone(),
                new_edges: BTreeMap::new(),
                rolled_back: added.clone(),
                cycle: e.0,
                restored: true,
            };
        }
        let mut touched: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (dependent, deps) in new_deps {
            for d in deps {
                let dep_id = TaskId::new(d.clone());
                let dep_dep = TaskId::new(dependent.clone());
                if self.tasks.contains_key(&dep_dep) && self.tasks.contains_key(&dep_id) {
                    let pre = self.tasks[&dep_dep].deps.clone();
                    if let Err(e) = self.add_edge(&dep_dep, &dep_id) {
                        *self = before;
                        return AmendResult {
                            ok: false,
                            added: added.clone(),
                            new_edges: BTreeMap::new(),
                            rolled_back: added.clone(),
                            cycle: e.0,
                            restored: true,
                        };
                    }
                    let post = &self.tasks[&dep_dep].deps;
                    let mut diff = BTreeSet::new();
                    for x in post.symmetric_difference(&pre) {
                        diff.insert(x.as_str().to_string());
                    }
                    if !diff.is_empty() {
                        touched.entry(dependent.clone()).or_default().extend(diff);
                    }
                }
            }
        }
        if let Err(e) = self.validate() {
            *self = before;
            return AmendResult {
                ok: false,
                added: added.clone(),
                new_edges: BTreeMap::new(),
                rolled_back: added.clone(),
                cycle: e.0,
                restored: true,
            };
        }
        let new_edges: BTreeMap<String, Vec<String>> = touched
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
            .collect();
        AmendResult::ok(added, new_edges)
    }

    // -------------------------------------------------- agent wait-for graph
    /// agent -> agents it is directly blocked on (owner of the task it waits on).
    pub fn wait_for_edges(
        agents: &BTreeMap<AgentId, AgentWaitView>,
        graph: &DependencyGraph,
    ) -> BTreeMap<AgentId, BTreeSet<AgentId>> {
        let mut edges: BTreeMap<AgentId, BTreeSet<AgentId>> = BTreeMap::new();
        for (aid, a) in agents {
            if !matches!(a.state.as_str(), "WAITING_FOR_DEPENDENCY" | "BLOCKED") {
                continue;
            }
            for wait in &a.waits {
                let Some(tid) = &wait.task_id else { continue };
                let Some(owner) = graph.tasks.get(tid).and_then(|t| t.owner.clone()) else {
                    continue;
                };
                if owner != *aid && agents.contains_key(&owner) {
                    edges.entry(aid.clone()).or_default().insert(owner);
                }
            }
        }
        edges
    }

    /// DFS cycle enumeration (self-edges ignored), deterministic like Python's.
    pub fn find_cycles(edges: &BTreeMap<AgentId, BTreeSet<AgentId>>) -> Vec<Vec<String>> {
        let mut cycles: Vec<Vec<String>> = Vec::new();
        let mut seen: Vec<BTreeSet<String>> = Vec::new();
        let mut state: BTreeMap<String, i32> = BTreeMap::new();
        let mut stack: Vec<String> = Vec::new();

        fn dfs(
            u: &str,
            edges: &BTreeMap<AgentId, BTreeSet<AgentId>>,
            state: &mut BTreeMap<String, i32>,
            stack: &mut Vec<String>,
            cycles: &mut Vec<Vec<String>>,
            seen: &mut Vec<BTreeSet<String>>,
        ) {
            state.insert(u.to_string(), 1);
            stack.push(u.to_string());
            if let Some(neighbors) = edges.get(&AgentId::new(u)) {
                let sorted: Vec<&AgentId> = neighbors.iter().collect();
                for v in sorted {
                    if v.as_str() == u {
                        continue;
                    }
                    let st = *state.get(v.as_str()).unwrap_or(&0);
                    if st == 1 {
                        let i = stack.iter().position(|x| x == v.as_str()).unwrap_or(0);
                        let cyc: Vec<String> = stack[i..].to_vec();
                        let mut closed = cyc.clone();
                        closed.push(v.as_str().to_string());
                        let key: BTreeSet<String> = cyc.iter().cloned().collect();
                        if !seen.contains(&key) {
                            seen.push(key);
                            cycles.push(closed);
                        }
                    } else if st == 0 {
                        dfs(v.as_str(), edges, state, stack, cycles, seen);
                    }
                }
            }
            stack.pop();
            state.insert(u.to_string(), 2);
        }

        let roots: Vec<String> = edges.keys().map(|k| k.as_str().to_string()).collect();
        for node in roots {
            if *state.get(&node).unwrap_or(&0) == 0 {
                dfs(&node, edges, &mut state, &mut stack, &mut cycles, &mut seen);
            }
        }
        cycles
    }

    /// Longest chain of open tasks — the parent's bottleneck signal.
    pub fn critical_path_length(&self) -> usize {
        fn depth(tid: &TaskId, g: &DependencyGraph, memo: &mut BTreeMap<TaskId, usize>) -> usize {
            if let Some(v) = memo.get(tid) {
                return *v;
            }
            let spec = match g.tasks.get(tid) {
                Some(s) => s,
                None => return 0,
            };
            let v = if !spec.is_open() {
                0
            } else {
                let subs: Vec<usize> = spec
                    .deps
                    .iter()
                    .filter(|d| g.tasks.contains_key(*d))
                    .map(|d| depth(d, g, memo))
                    .collect();
                1 + subs.iter().copied().max().unwrap_or(0)
            };
            memo.insert(tid.clone(), v);
            v
        }
        let mut memo: BTreeMap<TaskId, usize> = BTreeMap::new();
        self.tasks
            .keys()
            .map(|t| depth(t, self, &mut memo))
            .max()
            .unwrap_or(0)
    }
}

/// The registry's projection for wait-for edges.
#[derive(Debug, Clone)]
pub struct AgentWaitView {
    pub state: String,
    pub waits: Vec<WaitRef>,
}

#[derive(Debug, Clone)]
pub struct WaitRef {
    pub task_id: Option<TaskId>,
}

fn map_to_jmap(m: &BTreeMap<String, Vec<String>>) -> JMap {
    let mut out = JMap::new();
    for (k, v) in m {
        out.insert(
            k.clone(),
            JValue::Arr(v.iter().map(|s| JValue::Str(s.clone())).collect()),
        );
    }
    out
}

/// Kahn's algorithm reproducing `graphlib.TopologicalSorter` semantics, with
/// each generation sorted for determinism. Returns the cycle nodes on failure
/// (mirroring `graphlib.CycleError.args[1]`).
pub fn topo_generations(
    adj: &BTreeMap<TaskId, BTreeSet<TaskId>>,
) -> Result<Vec<Vec<TaskId>>, Vec<String>> {
    let mut indegree: BTreeMap<TaskId, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<TaskId, BTreeSet<TaskId>> = BTreeMap::new();
    // `deps` are predecessors (B.deps ⊆ {producers B waits on}): indegree counts
    // a node's unprocessed predecessors, and dep -> dependents points forward.
    for (node, deps) in adj {
        indegree.entry(node.clone()).or_insert(0);
        for d in deps {
            *indegree.entry(node.clone()).or_insert(0) += 1;
            dependents
                .entry(d.clone())
                .or_default()
                .insert(node.clone());
        }
    }
    let mut ready: BTreeSet<TaskId> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| n.clone())
        .collect();
    let mut out: Vec<Vec<TaskId>> = Vec::new();
    let mut visited = 0usize;
    while !ready.is_empty() {
        let batch: Vec<TaskId> = ready.iter().cloned().collect();
        for b in &batch {
            ready.remove(b);
            visited += 1;
            if let Some(deps) = dependents.get(b) {
                for d in deps {
                    if let Some(rem) = indegree.get_mut(d) {
                        *rem -= 1;
                        if *rem == 0 {
                            ready.insert(d.clone());
                        }
                    }
                }
            }
        }
        out.push(batch);
    }
    if visited != indegree.len() {
        // cycle: report the nodes still stuck (deterministic order)
        let stuck: Vec<String> = indegree
            .iter()
            .filter(|(_, d)| **d > 0)
            .map(|(n, _)| n.as_str().to_string())
            .collect();
        return Err(stuck);
    }
    Ok(out)
}

pub fn py_round6(x: f64) -> f64 {
    py_round(x, 6)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str, role: &str, produces: Vec<&str>, consumes: Vec<&str>) -> TaskSpec {
        TaskSpec {
            task_id: TaskId::new(id),
            title: format!("{id} title"),
            role: role.into(),
            produces: produces.into_iter().map(String::from).collect(),
            consumes: consumes.into_iter().map(String::from).collect(),
            est_work: 2.0,
            ..TaskSpec::new(id, "", role)
        }
    }

    #[test]
    fn derived_edges_and_generations() {
        let mut g = DependencyGraph::default();
        g.add(
            spec("t_db", "database", vec!["database/schema.sql"], vec![]),
            true,
        )
        .unwrap();
        g.add(
            spec(
                "t_api",
                "backend",
                vec!["contracts/api.json"],
                vec!["database/schema.sql"],
            ),
            true,
        )
        .unwrap();
        g.add(
            spec("t_fe", "frontend", vec![], vec!["contracts/api.json"]),
            true,
        )
        .unwrap();
        assert_eq!(
            g.tasks[&TaskId::new("t_api")].deps,
            BTreeSet::from([TaskId::new("t_db")])
        );
        let gens = g.order().unwrap();
        assert_eq!(gens.len(), 3);
        assert_eq!(gens[0], vec![TaskId::new("t_db")]);
        assert_eq!(gens[2], vec![TaskId::new("t_fe")]);
    }

    #[test]
    fn explicit_cycle_rejected_and_rolled_back() {
        let mut g = DependencyGraph::default();
        g.add(spec("t_db", "database", vec!["s"], vec![]), true)
            .unwrap();
        g.add(spec("t_fe", "frontend", vec![], vec!["s"]), true)
            .unwrap();
        let err = g.add_edge(&TaskId::new("t_db"), &TaskId::new("t_fe"));
        assert!(err.is_err());
        assert!(!g.tasks[&TaskId::new("t_db")]
            .deps
            .contains(&TaskId::new("t_fe")));
    }

    #[test]
    fn amend_rolls_back_everything_on_cycle() {
        // clean graph: a produces x, b consumes x (b deps a)
        let mut g = DependencyGraph::default();
        g.add(spec("a", "r1", vec!["x"], vec![]), true).unwrap();
        g.add(spec("b", "r2", vec![], vec!["x"]), true).unwrap();
        let before = g.clone();

        // acyclic amend succeeds: c produces y (nothing consumes it yet)
        let res = g.amend(vec![spec("c", "r3", vec!["y"], vec![])], &BTreeMap::new());
        assert!(res.ok);
        assert_eq!(res.added, vec![TaskId::new("c")]);
        assert!(!res.restored);

        // an added task can close a cycle by itself via what it consumes:
        // d consumes y (c produces) and produces x... no cycle; instead make
        // a consume d's product: new task d consumes x (a produces) and a
        // gains dep on d via new_deps -> a->d->a.
        let mut g1 = DependencyGraph::default();
        g1.add(spec("a", "r1", vec!["x"], vec![]), true).unwrap();
        let before1 = g1.clone();
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), vec!["d".to_string()]);
        let res1 = g1.amend(vec![spec("d", "r2", vec![], vec!["x"])], &m);
        assert!(!res1.ok);
        assert_eq!(res1.rolled_back, vec![TaskId::new("d")]);
        assert!(res1.restored);
        // graph is byte-identical to before the failed amend
        assert_eq!(g1, before1);

        // amend onto an already-cyclic graph must fail and restore
        let mut g2 = DependencyGraph::default();
        g2.add(spec("p", "r", vec!["art1"], vec!["art2"]), true)
            .unwrap();
        g2.add(spec("q", "r", vec!["art2"], vec!["art1"]), true)
            .unwrap();
        assert!(g2.has_cycle());
        let before2 = g2.clone();
        let res2 = g2.amend(vec![spec("r", "r", vec![], vec![])], &BTreeMap::new());
        assert!(!res2.ok);
        assert!(res2.restored);
        assert_eq!(g2, before2);
        let _ = (before, g);
    }

    #[test]
    fn wait_for_cycles_detectable() {
        let mut agents = BTreeMap::new();
        agents.insert(
            AgentId::new("a1"),
            AgentWaitView {
                state: "WAITING_FOR_DEPENDENCY".into(),
                waits: vec![WaitRef {
                    task_id: Some(TaskId::new("t_x")),
                }],
            },
        );
        agents.insert(
            AgentId::new("a2"),
            AgentWaitView {
                state: "WAITING_FOR_DEPENDENCY".into(),
                waits: vec![WaitRef {
                    task_id: Some(TaskId::new("t_y")),
                }],
            },
        );
        let mut g = DependencyGraph::default();
        let mut t1 = spec("t_x", "r1", vec![], vec![]);
        t1.owner = Some(AgentId::new("a2"));
        let mut t2 = spec("t_y", "r2", vec![], vec![]);
        t2.owner = Some(AgentId::new("a1"));
        g.add(t1, true).unwrap();
        g.add(t2, true).unwrap();
        let edges = DependencyGraph::wait_for_edges(&agents, &g);
        let cycles = DependencyGraph::find_cycles(&edges);
        assert_eq!(cycles.len(), 1, "{cycles:?}");
        assert_eq!(cycles[0], vec!["a1", "a2", "a1"]);
    }

    #[test]
    fn critical_path() {
        let mut g = DependencyGraph::default();
        g.add(spec("a", "r", vec!["x"], vec![]), true).unwrap();
        g.add(spec("b", "r", vec![], vec!["x"]), true).unwrap();
        g.add(spec("c", "r", vec![], vec![]), true).unwrap();
        assert_eq!(g.critical_path_length(), 2);
        g.tasks.get_mut(&TaskId::new("a")).unwrap().status = TaskStatus::Done;
        assert_eq!(g.critical_path_length(), 1);
    }

    #[test]
    fn task_key_format() {
        let t = spec("t", "backend", vec!["b.md", "a.md"], vec![]);
        assert_eq!(t.key(), "backend|a.md|b.md");
    }
}
