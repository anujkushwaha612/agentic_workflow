//! M7 actor tests — safety properties, cognition boundary, recovery cursor,
//! verification gate, and per-agent isolation.

use super::*;
use crate::bus::Bus;
use crate::clock::Clock;
use crate::cognition::CognitionError;
use crate::graph::{DependencyGraph, TaskStatus};
use crate::ids::AgentId;
use crate::journal::{EventFilter, Journal};
use crate::lifecycle::Lifecycle;
use crate::policy::{SimulatedWork, WaitForArtifacts};
use crate::registry::{AgentRecord, AgentRegistry};
use std::collections::BTreeMap;

// ------------------------------------------------------------------ harness

/// A Kernel-shaped test double: owns the same modules Kernel will own, and
/// implements [`WorldView`] + [`RuntimeEffects`] so actor tests do not wait on Kernel.
struct Harness {
    journal: Journal,
    registry: AgentRegistry,
    graph: DependencyGraph,
    bus: Bus,
    clock: Clock,
    artifacts: BTreeMap<String, i64>,
    parent_inbox: Vec<JMap>,
    spawn_requests: Vec<JMap>,
    work_unit: f64,
    tick: i64,
    task_text: String,
    tools: Option<StubTools>,
    transcript_dir: Option<String>,
    verify_pass: bool,
}

struct StubTools {
    allowed: Vec<String>,
    schemas: Vec<JMap>,
    executions: i64,
    fail: bool,
}

impl Default for Harness {
    fn default() -> Self {
        let mut journal = Journal::open_memory().expect("journal");
        journal.set_now_provider(0.0);
        Harness {
            journal,
            registry: AgentRegistry::default(),
            graph: DependencyGraph::default(),
            bus: Bus::new(),
            clock: Clock::virtual_(0.01),
            artifacts: BTreeMap::new(),
            parent_inbox: Vec::new(),
            spawn_requests: Vec::new(),
            work_unit: 0.25,
            tick: 0,
            task_text: "build it".into(),
            tools: None,
            transcript_dir: None,
            verify_pass: true,
        }
    }
}

impl Harness {
    fn register_agent(&mut self, id: &str, role: &str, state: AgentState) {
        self.registry
            .register(
                id,
                role,
                &[],
                0,
                "parent",
                "",
                Some(Lifecycle::with_state(state, 0.0)),
                &[],
            )
            .unwrap();
        self.bus.register_actor(id);
    }

    fn add_task(&mut self, spec: TaskSpec) {
        self.graph.add(spec, true).unwrap();
    }

    fn assign(&mut self, agent: &str, task: &str) {
        let rec = self.registry.get_mut(agent).unwrap();
        rec.task_id = Some(TaskId::new(task));
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(task)) {
            t.owner = Some(AgentId::new(agent));
            t.status = TaskStatus::Assigned;
        }
    }

    fn events(&self, etype: &str) -> Vec<crate::journal::EventRow> {
        self.journal
            .events(&EventFilter::new().etype(etype))
            .unwrap()
    }

    fn bind_tools(&mut self, allowed: &[&str]) {
        self.tools = Some(StubTools {
            allowed: allowed.iter().map(|s| s.to_string()).collect(),
            schemas: allowed
                .iter()
                .map(|t| {
                    let mut m = JMap::new();
                    m.insert("tool".into(), JValue::Str((*t).into()));
                    m
                })
                .collect(),
            executions: 0,
            fail: false,
        });
    }
}

fn current_open(h: &Harness, id: &str) -> Option<TaskSpec> {
    let rec = h.registry.get(id)?;
    for tid in rec.pending_work() {
        if let Some(t) = h.graph.tasks.get(&tid) {
            if t.is_open() {
                return Some(t.clone());
            }
        }
    }
    None
}

impl WorldView for Harness {
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
    fn workspace_view(&self, _id: &str) -> JMap {
        if self.tools.is_none() {
            let mut m = JMap::new();
            m.insert("bound".into(), JValue::Bool(false));
            return m;
        }
        let mut m = JMap::new();
        m.insert("bound".into(), JValue::Bool(true));
        m.insert("files".into(), JValue::Arr(vec![]));
        m.insert("dirty".into(), JValue::Bool(false));
        m
    }
    fn allowed_tools(&self, _id: &str) -> Vec<String> {
        self.tools
            .as_ref()
            .map(|t| t.allowed.clone())
            .unwrap_or_default()
    }
    fn tool_schemas(&self, _id: &str) -> Vec<JMap> {
        self.tools
            .as_ref()
            .map(|t| t.schemas.clone())
            .unwrap_or_default()
    }
    fn tool_stats(&self) -> JMap {
        let mut m = JMap::new();
        m.insert(
            "executions".into(),
            JValue::Int(self.tools.as_ref().map(|t| t.executions).unwrap_or(0)),
        );
        m
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
    fn peek_inbox(&self, id: &str) -> Vec<Message> {
        self.bus.mailbox(id)
    }
    fn has_mail(&self, id: &str) -> bool {
        self.bus.mailbox_len(id) > 0
    }
    fn completion_gate(&self, id: &str) -> CompletionGate {
        let Some(tid) = self.registry.get(id).and_then(|r| r.task_id.clone()) else {
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
}

impl RuntimeEffects for Harness {
    fn pop_inbox(&mut self, id: &str) -> Option<Message> {
        self.bus.drain(id, Some(1)).into_iter().next()
    }
    fn transition(&mut self, id: &str, to: AgentState, reason: &str) -> bool {
        let Some(rec) = self.registry.get_mut(id) else {
            return false;
        };
        let res = rec.lifecycle.request(to, reason, self.clock.now());
        if res.ok {
            self.registry.version += 1;
            let mut fields = JMap::new();
            fields.insert("agent_id".into(), JValue::Str(id.into()));
            fields.insert("frm".into(), JValue::Str(res.frm.as_str().into()));
            fields.insert("to".into(), JValue::Str(res.to.as_str().into()));
            fields.insert("reason".into(), JValue::Str(reason.into()));
            self.journal.emit(
                EventType::StateTransition,
                id,
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
            fields.insert("agent_id".into(), JValue::Str(id.into()));
            fields.insert("frm".into(), JValue::Str(res.frm.as_str().into()));
            fields.insert("to".into(), JValue::Str(res.to.as_str().into()));
            self.journal.emit(
                EventType::IllegalTransition,
                id,
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
    fn journal_emit(
        &mut self,
        etype: EventType,
        actor: &str,
        target: &str,
        body: &str,
        fields: JMap,
        task_id: Option<&str>,
    ) {
        self.journal.emit(
            etype, actor, target, body, fields, task_id, None, None, None, 0,
        );
    }
    fn publish(&mut self, mut msg: Message) {
        let _ = self.bus.publish(&mut msg, &mut self.journal);
    }
    fn wait_for(
        &mut self,
        actor: &str,
        condition: &str,
        task_id: Option<&str>,
        correlation_id: &str,
        timeout: Option<f64>,
    ) {
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
        let v = self.artifacts.get(artifact).copied().unwrap_or(0) + 1;
        self.artifacts.insert(artifact.to_string(), v);
        self.graph.known_artifacts.insert(artifact.to_string());
        let mut fields = JMap::new();
        fields.insert("artifact".into(), JValue::Str(artifact.into()));
        fields.insert("version".into(), JValue::Int(v));
        self.journal.emit(
            EventType::ResourceUpdated,
            producer,
            "broadcast",
            &format!("{artifact} v{v} ready"),
            fields,
            task_id,
            Some(artifact),
            None,
            None,
            0,
        );
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
    fn record_escalation(&mut self, note: JMap) {
        self.parent_inbox.push(note);
    }
    fn enqueue_spawn_request(&mut self, req: JMap) {
        self.spawn_requests.push(req);
    }
    fn run_verify(&mut self, id: &str, task_id: Option<&str>) -> VerifyVerdict {
        let tid = task_id.map(|s| s.to_string()).or_else(|| {
            self.registry
                .get(id)
                .and_then(|r| r.task_id.as_ref().map(|t| t.as_str().to_string()))
        });
        let Some(tid) = tid else {
            return VerifyVerdict {
                ok: false,
                ..Default::default()
            };
        };
        let ok = self.verify_pass;
        if let Some(t) = self.graph.tasks.get_mut(&TaskId::new(&tid)) {
            t.verified = ok;
            t.verified_at = Some(self.clock.now());
        }
        let mut row = JMap::new();
        row.insert("ok".into(), JValue::Bool(ok));
        let mut fields = JMap::new();
        fields.insert("verified".into(), JValue::Bool(ok));
        fields.insert("agent_id".into(), JValue::Str(id.into()));
        fields.insert(
            "rule".into(),
            JValue::Str(if ok { "VERIFY_PASSED" } else { "VERIFY_FAILED" }.into()),
        );
        self.journal.emit(
            EventType::TaskVerified,
            "kernel",
            "parent",
            &format!(
                "{tid} verify {} (1 command(s))",
                if ok { "PASSED" } else { "FAILED" }
            ),
            fields,
            Some(&tid),
            None,
            None,
            None,
            0,
        );
        VerifyVerdict {
            ok,
            results: vec![row],
            task_id: Some(tid),
        }
    }
    fn plan_tools(
        &mut self,
        _id: &str,
        calls: &[JMap],
        _task_id: Option<&str>,
        _corr: &str,
    ) -> Vec<String> {
        (0..calls.len()).map(|i| format!("r-{i:04}")).collect()
    }
    fn execute_tool(
        &mut self,
        _id: &str,
        tool: &str,
        _args: &JMap,
        rid: &str,
        _task_id: Option<&str>,
        _corr: &str,
    ) -> ToolResultView {
        if let Some(t) = self.tools.as_mut() {
            t.executions += 1;
        }
        let fail = self.tools.as_ref().map(|t| t.fail).unwrap_or(false);
        ToolResultView {
            tool: tool.into(),
            ok: !fail,
            exit_code: Some(if fail { 1 } else { 0 }),
            block: format!("== {tool} =="),
            rid: rid.into(),
            ..Default::default()
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

fn spec(id: &str, role: &str, produces: &[&str], consumes: &[&str]) -> TaskSpec {
    let mut t = TaskSpec::new(id, id, role);
    t.produces = produces.iter().map(|s| s.to_string()).collect();
    t.consumes = consumes.iter().map(|s| s.to_string()).collect();
    t.est_work = 2.0;
    t
}

fn working_agent(h: &mut Harness, id: &str) {
    h.register_agent(id, "backend", AgentState::Working);
}

// ----------------------------------------------------------------- tests

#[test]
fn simulated_work_progresses_then_completes_through_cognition() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &[]));
    h.assign("be_01", "t_api");
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork { steps: 1 }));
    assert_eq!(actor.cognition_name(), "policy:simulated-work");

    let t1 = actor.run_step(&mut h);
    assert_eq!(t1.action, "PUBLISH");
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Working
    );

    let t2 = actor.run_step(&mut h);
    assert_eq!(t2.action, "PUBLISH");

    let t3 = actor.run_step(&mut h);
    assert_eq!(t3.action, "COMPLETE");
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Completed
    );
    assert!(h.artifacts.contains_key("api.md"));
    assert_eq!(
        h.graph.tasks[&TaskId::new("t_api")].status,
        TaskStatus::Done
    );
    assert!(!h.events("TASK_COMPLETED").is_empty());
    assert!(!h.events("STATE_TRANSITION").is_empty());
    // cognition path journals a TASK_PROGRESS cursor row per turn
    assert!(h.events("TASK_PROGRESS").len() >= 3);
}

#[test]
fn wait_parks_and_releases_the_worker_slot() {
    let mut h = Harness::default();
    working_agent(&mut h, "fe_01");
    h.add_task(spec("t_fe", "frontend", &["ui.md"], &["api.md"]));
    h.assign("fe_01", "t_fe");
    let mut actor = AgentActor::new("fe_01", Box::new(WaitForArtifacts::default()));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "WAIT");
    assert!(turn.detail.contains("artifact:api.md"));
    assert_eq!(
        h.registry.get("fe_01").unwrap().lifecycle.state,
        AgentState::WaitingForDependency
    );
    assert_eq!(h.registry.get("fe_01").unwrap().pending_waits.len(), 1);
    assert!(
        !actor.runnable(&h),
        "waiting without mail must not hold a worker slot"
    );
}

#[test]
fn waiting_agent_with_mail_is_runnable() {
    let mut h = Harness::default();
    h.register_agent("fe_01", "frontend", AgentState::WaitingForDependency);
    h.add_task(spec("t_fe", "frontend", &[], &["api.md"]));
    h.assign("fe_01", "t_fe");
    let rec = h.registry.get_mut("fe_01").unwrap();
    let mut w = JMap::new();
    w.insert("condition".into(), JValue::Str("artifact:api.md".into()));
    rec.pending_waits.push(w);
    let mut wake = Message::new(EventType::DependencyReady, "dependency_manager", "fe_01");
    wake.body = "unblocked".into();
    h.bus.publish(&mut wake, &mut h.journal).unwrap();
    let actor = AgentActor::new("fe_01", Box::new(WaitForArtifacts::default()));
    assert!(actor.runnable(&h));
}

#[test]
fn completed_agent_with_backlog_is_runnable() {
    let mut h = Harness::default();
    h.register_agent("be_01", "backend", AgentState::Completed);
    h.add_task(spec("t2", "backend", &["b.md"], &[]));
    let rec = h.registry.get_mut("be_01").unwrap();
    rec.task_id = Some(TaskId::new("t2"));
    let actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    assert!(actor.runnable(&h));
}

#[test]
fn repeated_complete_with_no_task_is_ignored() {
    let mut h = Harness::default();
    h.register_agent("idle_01", "backend", AgentState::Working);
    let mut actor = AgentActor::new("idle_01", Box::new(SimulatedWork { steps: -1 }));
    let t1 = actor.run_step(&mut h);
    assert_eq!(t1.action, "COMPLETE");
    assert_eq!(
        h.registry.get("idle_01").unwrap().lifecycle.state,
        AgentState::Completed
    );
    // force another complete while already COMPLETED and still WORKING-path
    // last_action is COMPLETE; no task bound
    h.registry.get_mut("idle_01").unwrap().lifecycle.state = AgentState::Working;
    let before = h.journal.count().unwrap();
    let _t2 = actor.run_step(&mut h);
    assert!(actor.errors.iter().any(|e| e.contains("repeated COMPLETE")));
    let extra_completed = h.events("TASK_COMPLETED").len();
    assert_eq!(extra_completed, 1, "second COMPLETE must not journal again");
    assert!(h.journal.count().unwrap() >= before); // progress rows ok, no second TASK_COMPLETED
}

#[test]
fn verification_gate_refuses_unverified_complete() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    let mut t = spec("t_api", "backend", &["api.md"], &[]);
    t.verify = vec![vec!["pytest".into()]];
    h.add_task(t);
    h.assign("be_01", "t_api");
    h.bind_tools(&["run_command"]);
    h.verify_pass = false;
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork { steps: -1 }));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "COMPLETE");
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Idle,
        "failed verify must return WORKING→IDLE, not BLOCKED"
    );
    assert_eq!(
        h.graph.tasks[&TaskId::new("t_api")].status,
        TaskStatus::Waiting
    );
    assert!(!h.graph.tasks[&TaskId::new("t_api")].verified);
    assert!(!h.events("COMPLETION_REFUSED").is_empty());
    assert!(!h.events("TASK_VERIFIED").is_empty());
    assert!(h.artifacts.is_empty(), "refused complete must not publish");
}

#[test]
fn verification_gate_allows_complete_after_runtime_verify_passes() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    let mut t = spec("t_api", "backend", &["api.md"], &[]);
    t.verify = vec![vec!["pytest".into()]];
    h.add_task(t);
    h.assign("be_01", "t_api");
    h.bind_tools(&["run_command"]);
    h.verify_pass = true;
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork { steps: 0 }));
    // VERIFY control: custom cognition
    struct VerifyNow;
    impl Cognition for VerifyNow {
        fn name(&self) -> &str {
            "verify-now"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _obs: &Observation,
            _ctx: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            let mut i = Intent::new();
            i.control = Control::VERIFY.into();
            Ok(i)
        }
    }
    actor.bind_cognition(Box::new(VerifyNow));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "COMPLETE");
    assert!(h.graph.tasks[&TaskId::new("t_api")].verified);
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Completed
    );
    assert!(h.events("COMPLETION_REFUSED").is_empty());
}

#[test]
fn cognition_cannot_bypass_tools_or_lifecycle() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &[]));
    h.assign("be_01", "t_api");

    struct Sneaky;
    impl Cognition for Sneaky {
        fn name(&self) -> &str {
            "sneaky"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _obs: &Observation,
            ctx: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            // the context has no kernel, no filesystem, no journal. The
            // only "write" is log(), which appends a note.
            ctx.log("thinking");
            let mut i = Intent::new();
            i.calls = vec![ToolCall::new("rm", JMap::new())];
            i.control = Control::COMPLETE.into();
            Ok(i)
        }
    }
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    actor.bind_cognition(Box::new(Sneaky));
    let turn = actor.run_step(&mut h);
    assert!(
        h.events("COGNITION_VIOLATION")
            .iter()
            .any(|r| r.body().contains("TOOL_NOT_ALLOWED:rm")),
        "unauthorized tool must be journalled, not executed"
    );
    // no executor bound + refused tool: REFUSE_NO_EXECUTOR is not reached
    // because validate_intent drops the call. COMPLETE still goes through the gate.
    assert_eq!(turn.action, "COMPLETE");
    assert!(h.artifacts.contains_key("api.md"));
}

#[test]
fn two_agents_do_not_share_a_brain() {
    let mut h = Harness::default();
    working_agent(&mut h, "a");
    working_agent(&mut h, "b");
    h.add_task(spec("t_a", "backend", &["a.md"], &[]));
    h.add_task(spec("t_b", "backend", &["b.md"], &[]));
    h.assign("a", "t_a");
    h.assign("b", "t_b");
    let mut a = AgentActor::new("a", Box::new(SimulatedWork { steps: 0 }));
    let b = AgentActor::new("b", Box::new(SimulatedWork { steps: 0 }));
    a.run_step(&mut h);
    assert_eq!(a.transcript.len(), 1);
    assert_eq!(b.transcript.len(), 0, "b must not see a's turns");
    assert_ne!(
        a.cognition_fingerprint().get("prompt_sha256_16"),
        b.cognition_fingerprint().get("prompt_sha256_16"),
        "per-agent prompt digest must differ"
    );
}

#[test]
fn restore_cursor_preserves_policy_progress() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &[]));
    h.assign("be_01", "t_api");
    let mut ns = crate::policy::NeedsSpecialist::default();
    ns.after_steps = 10;
    let mut actor = AgentActor::new("be_01", Box::new(ns));
    actor.run_step(&mut h);
    actor.run_step(&mut h);
    let steps = actor.steps_run;
    let cursor = actor.policy_cursor();
    assert!(cursor >= 2);
    let mut ns2 = crate::policy::NeedsSpecialist::default();
    ns2.after_steps = 10;
    let mut revived = AgentActor::new("be_01", Box::new(ns2));
    revived.restore_cursor(steps, cursor);
    assert_eq!(revived.steps_run, steps);
    assert_eq!(revived.policy_cursor(), cursor);
}

#[test]
fn illegal_transition_is_journalled_and_does_not_mutate() {
    let mut h = Harness::default();
    h.register_agent("be_01", "backend", AgentState::Completed);
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    let ok = actor.transition(&mut h, AgentState::Working, "chaos");
    assert!(!ok);
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Completed
    );
    assert!(!h.events("ILLEGAL_TRANSITION").is_empty());
    assert!(actor.errors.iter().any(|e| e.starts_with("illegal:")));
}

#[test]
fn observation_is_rebuilt_each_turn_from_live_state() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &["db.sql"]));
    h.assign("be_01", "t_api");
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    use std::sync::{Arc, Mutex};
    struct Cap2(Arc<Mutex<Vec<String>>>);
    impl Cognition for Cap2 {
        fn name(&self) -> &str {
            "cap2"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            obs: &Observation,
            _ctx: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            *self.0.lock().unwrap() = obs.unmet.clone();
            let mut i = Intent::new();
            i.control = Control::NOOP.into();
            Ok(i)
        }
    }
    let seen = Arc::new(Mutex::new(vec![]));
    actor.bind_cognition(Box::new(Cap2(seen.clone())));
    actor.run_step(&mut h);
    assert_eq!(*seen.lock().unwrap(), vec!["db.sql".to_string()]);
    h.artifacts.insert("db.sql".into(), 1);
    actor.run_step(&mut h);
    assert!(
        seen.lock().unwrap().is_empty(),
        "stale unmet must not survive a new observe()"
    );
}

#[test]
fn spawn_request_is_a_message_not_a_parent_call() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &[]));
    h.assign("be_01", "t_api");
    let mut ns = crate::policy::NeedsSpecialist::default();
    ns.after_steps = -1;
    ns.detect_from = String::new();
    let mut actor = AgentActor::new("be_01", Box::new(ns));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "PUBLISH");
    assert_eq!(h.spawn_requests.len(), 1);
    assert_eq!(
        h.spawn_requests[0]
            .get("requested_role")
            .and_then(|v| v.as_str()),
        Some("payment specialist")
    );
    // parent is not mutated beyond the request queue — no new agent
    assert_eq!(h.registry.active().len(), 1);
}

#[test]
fn escalate_lands_in_parent_inbox() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    struct Boom;
    impl Cognition for Boom {
        fn name(&self) -> &str {
            "boom"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _o: &Observation,
            _c: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            let mut i = Intent::new();
            i.control = Control::ESCALATE.into();
            i.reason = "stuck".into();
            Ok(i)
        }
    }
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    actor.bind_cognition(Box::new(Boom));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "ESCALATE");
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Escalated
    );
    assert_eq!(h.parent_inbox.len(), 1);
}

#[test]
fn cognition_crash_blocks_without_kernel_panic() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    struct Dead;
    impl Cognition for Dead {
        fn name(&self) -> &str {
            "dead"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _o: &Observation,
            _c: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            Err(CognitionError::Other("model down".into()))
        }
    }
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    actor.bind_cognition(Box::new(Dead));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "error");
    assert_eq!(
        h.registry.get("be_01").unwrap().lifecycle.state,
        AgentState::Blocked
    );
    assert!(!h.events("COGNITION_ERROR").is_empty());
}

#[test]
fn tools_execute_only_through_runtime() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &[]));
    h.assign("be_01", "t_api");
    h.bind_tools(&["read_file"]);
    struct Reader;
    impl Cognition for Reader {
        fn name(&self) -> &str {
            "reader"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _o: &Observation,
            _c: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            let mut i = Intent::new();
            i.calls = vec![ToolCall::new("read_file", JMap::new())];
            i.control = Control::NOOP.into();
            Ok(i)
        }
    }
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    actor.bind_cognition(Box::new(Reader));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.calls.len(), 1);
    assert_eq!(
        turn.calls[0].get("tool").and_then(|v| v.as_str()),
        Some("read_file")
    );
    assert_eq!(h.tools.as_ref().unwrap().executions, 1);
    assert_eq!(actor.transcript.len(), 2); // intent + tool_result
}

#[test]
fn run_stops_on_sleeping_action() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    h.add_task(spec("t_api", "backend", &["api.md"], &[]));
    h.assign("be_01", "t_api");
    let mut actor = AgentActor::new("be_01", Box::new(SimulatedWork { steps: -1 }));
    let turns = actor.run(&mut h, 12);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].action, "COMPLETE");
}

#[test]
fn from_cognition_does_not_require_a_policy() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    struct Quiet;
    impl Cognition for Quiet {
        fn name(&self) -> &str {
            "quiet"
        }
        fn fingerprint(&self) -> JMap {
            JMap::new()
        }
        fn decide(
            &mut self,
            _o: &Observation,
            _c: &mut dyn PolicyContext,
        ) -> Result<Intent, CognitionError> {
            let mut i = Intent::new();
            i.control = Control::NOOP.into();
            Ok(i)
        }
    }
    let mut actor = AgentActor::from_cognition("be_01", Box::new(Quiet));
    let turn = actor.run_step(&mut h);
    assert_eq!(turn.action, "NOOP");
    assert_eq!(actor.cognition_name(), "quiet");
}

#[test]
fn actor_snapshot_is_json_safe() {
    let mut h = Harness::default();
    working_agent(&mut h, "be_01");
    let actor = AgentActor::new("be_01", Box::new(SimulatedWork::default()));
    let snap = actor.snapshot(&h);
    assert_eq!(snap.get("agent_id").and_then(|v| v.as_str()), Some("be_01"));
    assert_eq!(
        snap.get("policy").and_then(|v| v.as_str()),
        Some("SimulatedWork")
    );
    let _ = JValue::Obj(snap).to_canon_string();
}
