//! Policy unit tests. The mock context mirrors the facts an ActorContext
//! exposes; golden parity against a real Python kernel context lives in
//! `tests/policy_parity.rs`.

use super::*;

/// Minimal deterministic context: one task, a set of existing artifacts, a
/// roster, and optional inheritance base message.
struct MockCtx {
    agent: String,
    task: Option<TaskSpec>,
    artifacts: Vec<String>,
    roster: Vec<String>,
    producers: Vec<(String, String)>, // (artifact, role)
    step: i64,
    llm: bool,
    base: Option<Message>,
    logs: Vec<String>,
    polls: i64,
    poll_limit: i64,
}

impl MockCtx {
    fn new(task: Option<TaskSpec>) -> MockCtx {
        MockCtx {
            agent: "be_01".to_string(),
            task,
            artifacts: Vec::new(),
            roster: vec!["backend".to_string()],
            producers: Vec::new(),
            step: 0,
            llm: false,
            base: None,
            logs: Vec::new(),
            polls: 0,
            poll_limit: 0,
        }
    }
}

impl PolicyContext for MockCtx {
    fn agent_id(&self) -> &str {
        &self.agent
    }
    fn step_index(&self) -> i64 {
        self.step
    }
    fn state(&self) -> String {
        "WORKING".to_string()
    }
    fn llm_available(&self) -> bool {
        self.llm
    }
    fn task(&self) -> Option<&TaskSpec> {
        self.task.as_ref()
    }
    fn task_id(&self) -> Option<String> {
        self.task.as_ref().map(|t| t.task_id.as_str().to_string())
    }
    fn all_unmet(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        if let Some(t) = &self.task {
            for a in &t.consumes {
                if !self.artifacts.contains(a) && !out.contains(a) {
                    out.push(a.clone());
                }
            }
        }
        out
    }
    fn unmet_artifacts(&self) -> Vec<String> {
        self.all_unmet()
    }
    fn consumed_ready(&self) -> Vec<String> {
        self.task
            .as_ref()
            .map(|t| {
                t.consumes
                    .iter()
                    .filter(|a| self.artifacts.contains(a))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
    fn roster_roles(&self) -> Vec<String> {
        self.roster.clone()
    }
    fn producer_roles(&self, artifact: &str) -> Vec<String> {
        self.producers
            .iter()
            .filter(|(a, _)| a == artifact)
            .map(|(_, r)| r.clone())
            .collect()
    }
    fn log(&mut self, text: &str) {
        self.logs.push(text.to_string());
    }
    fn poll_hit(&mut self) -> Result<i64, BusError> {
        self.polls += 1;
        if self.poll_limit > 0 && self.polls > self.poll_limit {
            return Err(BusError::PollingForbidden {
                actor: self.agent.clone(),
                hits: self.polls,
                limit: self.poll_limit,
            });
        }
        Ok(self.polls)
    }
    fn self_msg(&self, msg_type: EventType, body: &str, payload: JMap) -> Message {
        let mut m = match &self.base {
            Some(base) => {
                let mut c = base.child(msg_type, self.agent.clone(), "parent");
                c.body = body.to_string();
                c
            }
            None => {
                let mut fresh = Message::new(msg_type, self.agent.clone(), "parent");
                fresh.body = body.to_string();
                fresh
            }
        };
        m.task_id = self.task_id().map(crate::ids::TaskId::new);
        m.payload = payload;
        m
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
        let mut payload = JMap::new();
        payload.insert("requested_role".into(), JValue::Str(role.to_string()));
        payload.insert("reason".into(), JValue::Str(reason.to_string()));
        payload.insert(
            "required_skills".into(),
            JValue::Arr(skills.iter().cloned().map(JValue::Str).collect()),
        );
        payload.insert(
            "required_inputs".into(),
            JValue::Arr(inputs.iter().cloned().map(JValue::Str).collect()),
        );
        payload.insert(
            "expected_outputs".into(),
            JValue::Arr(outputs.iter().cloned().map(JValue::Str).collect()),
        );
        payload.insert("estimated_work".into(), JValue::Float(est_work));
        payload.insert(
            "parent_task_id".into(),
            self.task_id().map(JValue::Str).unwrap_or(JValue::Null),
        );
        payload.insert(
            "capability_class".into(),
            JValue::Str(capability_class.to_string()),
        );
        payload.insert("requires_judgment".into(), JValue::Bool(false));
        let body = if reason.is_empty() {
            format!(
                "{role} capability needed for {}",
                self.task
                    .as_ref()
                    .map(|t| t.title.clone())
                    .unwrap_or_default()
            )
        } else {
            reason.to_string()
        };
        self.self_msg(EventType::SpawnAgentRequest, &body, payload)
    }
}

fn task(id: &str, est: f64) -> TaskSpec {
    let mut t = TaskSpec::new(id, &format!("{id} title"), "backend");
    t.est_work = est;
    t.produces = vec!["out/api.md".to_string()];
    t.consumes = vec!["in/schema.sql".to_string()];
    t
}

#[test]
fn sleeping_actions_are_case_insensitive() {
    assert!(is_sleeping_action("WAIT"));
    assert!(is_sleeping_action("wait"));
    assert!(is_sleeping_action("Complete"));
    assert!(is_sleeping_action("ESCALATE"));
    assert!(is_sleeping_action("ERROR"));
    assert!(!is_sleeping_action("")); // "" is not in the set (Python: "".upper() in {...} is False)
    assert!(!is_sleeping_action("PROCEED"));
    assert!(!is_sleeping_action("NOOP"));
    assert!(!is_sleeping_action("PUBLISH"));
}

#[test]
fn simulated_work_progress_curve_matches_reference_math() {
    // pct = int(100 * min(1.0, (step_index-1)/max(1,steps))) — including the
    // negative first reading at step_index 0 (reference quirk)
    let mut p = SimulatedWork { steps: 3 };
    let bodies = [
        "-33% of t title",
        "0% of t title",
        "33% of t title",
        "66% of t title",
    ];
    for (i, want) in bodies.iter().enumerate() {
        let mut ctx = MockCtx::new(Some(task("t", 1.0)));
        ctx.step = i as i64;
        let a = p.step(&mut ctx, None).unwrap();
        assert_eq!(a.act, Act::Publish, "step {i}");
        assert_eq!(a.msg.as_ref().unwrap().body, *want);
        assert_eq!(
            a.msg
                .as_ref()
                .unwrap()
                .payload
                .get("percent")
                .unwrap()
                .as_int(),
            Some(match i {
                0 => -33,
                1 => 0,
                2 => 33,
                _ => 66,
            })
        );
    }
    // step_index 4 > steps 3 -> complete with the task's produces
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.step = 4;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Complete);
    assert_eq!(a.reason, "work finished");
    assert_eq!(
        a.extra.get("artifacts").unwrap().to_canon_string(),
        r#"["out/api.md"]"#
    );
    // no task -> empty artifacts, still completes
    let mut ctx = MockCtx::new(None);
    ctx.step = 4;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.extra.get("artifacts").unwrap().to_canon_string(), "[]");
    assert_eq!(p.class_name(), "SimulatedWork");
    assert_eq!(p.name(), "simulated-work");
    assert_eq!(p.policy_cursor(), 0, "SimulatedWork tracks no cursor");
}

#[test]
fn wait_for_artifacts_parks_durably_then_integrates() {
    let mut p = WaitForArtifacts::default();
    // missing consume -> WAIT artifact:<cond> with the default 4s timeout
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Wait);
    assert_eq!(a.condition, "artifact:in/schema.sql");
    assert_eq!(a.reason, "needs in/schema.sql");
    assert_eq!(a.timeout, Some(4.0));
    // dependency ready message -> logged resume
    let mut ready = Message::new(EventType::DependencyReady, "dependency_manager", "be_01");
    ready.body = "in/schema.sql is ready (published)".to_string();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.artifacts = vec!["in/schema.sql".to_string()];
    let a = p.step(&mut ctx, Some(&ready)).unwrap();
    assert_eq!(
        ctx.logs,
        vec!["resumed by in/schema.sql is ready (published)"]
    );
    assert_eq!(a.act, Act::Publish);
    assert_eq!(a.msg.as_ref().unwrap().body, "integrating t");
    assert_eq!(
        a.msg
            .as_ref()
            .unwrap()
            .payload
            .get("consumed")
            .unwrap()
            .to_canon_string(),
        r#"["in/schema.sql"]"#
    );
    // steps 1..2 progress, then complete
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.artifacts = vec!["in/schema.sql".to_string()];
    ctx.step = 1;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Publish);
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.artifacts = vec!["in/schema.sql".to_string()];
    ctx.step = 3;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Complete);
    assert_eq!(a.reason, "integrated against upstream artifacts");
    assert_eq!(
        a.extra.get("artifacts").unwrap().to_canon_string(),
        r#"["out/api.md"]"#
    );
}

#[test]
fn poll_until_ready_is_the_measurable_antipattern() {
    let mut p = PollUntilReady { limit: 2, n: 0 };
    let mk = || MockCtx::new(Some(task("t", 1.0)));
    let mut ctx = mk();
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(
        (a.act, a.reason.clone()),
        (Act::Proceed, "checking again (1)".to_string())
    );
    assert_eq!(ctx.polls, 1);
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.reason, "checking again (2)");
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Escalate);
    assert_eq!(a.reason, "gave up after 3 polls");
    assert_eq!(ctx.polls, 3);
    // dependency appears -> complete on the next poll
    ctx.artifacts = vec!["in/schema.sql".to_string()];
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Complete);
    assert_eq!(a.reason, "dependency showed up on poll 4");
    assert_eq!(p.policy_cursor(), 4);
    // cursor restore
    let mut p2 = PollUntilReady::default();
    p2.restore_cursor(7);
    assert_eq!(p2.policy_cursor(), 7);
}

#[test]
fn poll_past_eagerness_limit_is_a_typed_error() {
    let mut p = PollUntilReady { limit: 99, n: 0 };
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.poll_limit = 2;
    assert!(p.step(&mut ctx, None).is_ok());
    assert!(p.step(&mut ctx, None).is_ok());
    let err = p.step(&mut ctx, None).unwrap_err();
    assert_eq!(
        err.to_string(),
        "PollingForbidden: be_01 polled 3 times (limit 2): use kernel.wait_for() instead of re-checking"
    );
}

#[test]
fn escalate_on_complexity_asks_then_escalates() {
    let mut p = EscalateOnComplexity::default(); // threshold 2.0, request_spawns 1
    let mut ctx = MockCtx::new(Some(task("t", 2.0))); // est == threshold -> ask
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Publish);
    assert_eq!(a.reason, "request specialist");
    let m = a.msg.as_ref().unwrap();
    assert_eq!(m.msg_type, EventType::SpawnAgentRequest);
    assert_eq!(m.to_actor.as_str(), "parent");
    assert_eq!(
        m.payload.get("requested_role").unwrap().as_str(),
        Some("security engineer")
    );
    assert_eq!(
        m.payload.get("required_skills").unwrap().to_canon_string(),
        r#"["auth","crypto"]"#
    );
    assert_eq!(m.payload.get("estimated_work").unwrap().as_f64(), Some(3.0));
    assert_eq!(
        m.body,
        "auth/authorization design needs specialized expertise"
    );
    // second call: already asked once -> escalate
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Escalate);
    assert_eq!(
        a.reason,
        "repeated specialist need cannot be resolved locally"
    );
    assert_eq!(
        a.extra.get("requested_role").unwrap().as_str(),
        Some("security engineer")
    );
}

#[test]
fn escalate_on_complexity_low_complexity_proceeds_to_completion() {
    let mut p = EscalateOnComplexity::default();
    let mut ctx = MockCtx::new(Some(task("t", 1.0))); // below threshold
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(
        (a.act, a.reason.clone()),
        (Act::Proceed, "assessing complexity".to_string())
    );
    // run to max_steps (6): steps increments each non-ask turn
    let mut last = a;
    for _ in 0..10 {
        last = p.step(&mut ctx, None).unwrap();
        if last.act == Act::Complete {
            break;
        }
    }
    assert_eq!(last.act, Act::Complete);
    assert_eq!(last.reason, "handled with available capability");
}

#[test]
fn escalate_on_complexity_reasks_after_upstream_handoff() {
    let mut p = EscalateOnComplexity::with_request_spawns(2);
    let mut ctx = MockCtx::new(Some(task("t", 1.0))); // low complexity: no first-step ask
    let mut m = Message::new(EventType::DependencyReady, "dependency_manager", "be_01");
    m.body = "unblocked".into();
    let a = p.step(&mut ctx, Some(&m)).unwrap();
    assert_eq!(a.act, Act::Publish);
    assert_eq!(a.reason, "request specialist after upstream handoff");
    // msg logs
    let mut app = Message::new(EventType::SpawnApproved, "parent", "be_01");
    app.body = "approved".into();
    let mut ctx2 = MockCtx::new(Some(task("t", 1.0)));
    let _ = p.step(&mut ctx2, Some(&app)).unwrap();
    assert!(ctx2.logs.contains(&"parent approved: approved".to_string()));
}

#[test]
fn hybrid_policy_prefers_heuristic_and_survives_llm_failure() {
    let mut p = HybridPolicy::with(Box::new(SimulatedWork::default()));
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.step = 0;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Publish); // heuristic's progress step
                                     // stalled (PROCEED with step_index > 0) + llm_available but NullLLM ->
                                     // the failure is logged and the heuristic action returned
    let mut p = HybridPolicy::with(Box::new(EscalateOnComplexity::default()));
    let mut ctx = MockCtx::new(Some(task("t", 1.0))); // below threshold -> PROCEED
    ctx.step = 1;
    ctx.llm = true;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Proceed);
    assert_eq!(a.reason, "assessing complexity");
    assert_eq!(ctx.logs.len(), 1);
    assert!(ctx.logs[0].starts_with("llm unavailable: No LLM adapter configured"));
    // not stalled (COMPLETE) -> llm never consulted
    let mut p = HybridPolicy::with(Box::new(SimulatedWork { steps: 0 }));
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.step = 1;
    ctx.llm = true;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Complete);
    assert!(ctx.logs.is_empty());
    // no heuristic -> plain PROCEED; hybrid exposes no cursor (reference quirk)
    let mut p = HybridPolicy::default();
    let mut ctx = MockCtx::new(None);
    ctx.step = 5;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Proceed);
    assert_eq!(p.policy_cursor(), 0);
}

#[test]
fn needs_specialist_asks_mid_run() {
    let mut p = NeedsSpecialist::default(); // after_steps 2, max_requests 1
    let mk = |step: i64| {
        let mut ctx = MockCtx::new(Some(task("t", 1.0)));
        ctx.step = step;
        ctx
    };
    let mut ctx = mk(0);
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Proceed);
    let mut ctx = mk(1);
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Proceed);
    let mut ctx = mk(3);
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Publish);
    assert_eq!(a.reason, "request specialist mid-run");
    let m = a.msg.as_ref().unwrap();
    assert_eq!(m.msg_type, EventType::SpawnAgentRequest);
    assert_eq!(
        m.payload.get("capability_class").unwrap().as_str(),
        Some("payments")
    );
    assert_eq!(
        m.payload.get("expected_outputs").unwrap().to_canon_string(),
        r#"["artifacts/payments-webhooks.md"]"#
    );
    // asked already -> keeps working; task completes past after_steps + 6
    let mut ctx = mk(4);
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Proceed);
    let mut ctx = mk(9);
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Complete);
    assert_eq!(a.reason, "finished with the capability available");
}

#[test]
fn needs_specialist_detect_from_requires_an_unfilled_producer_role() {
    let mut p = NeedsSpecialist::with_detect_from("database");
    // roster HAS a database agent -> never ask
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.roster = vec!["backend".to_string(), "database".to_string()];
    ctx.step = 3;
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Proceed);
    // producer role absent from roster AND we consume its artifact -> ask
    let mut p = NeedsSpecialist::with_detect_from("database");
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.roster = vec!["backend".to_string()];
    ctx.producers = vec![("in/schema.sql".to_string(), "database".to_string())];
    ctx.step = 3;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Publish);
    // inputs fall back to task consumes when self.inputs empty
    assert_eq!(
        a.msg
            .unwrap()
            .payload
            .get("required_inputs")
            .unwrap()
            .to_canon_string(),
        r#"["in/schema.sql"]"#
    );
    // producer role represented on roster -> no ask
    let mut p = NeedsSpecialist::with_detect_from("database");
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.roster = vec!["backend".to_string(), "database".to_string()];
    ctx.producers = vec![("in/schema.sql".to_string(), "database".to_string())];
    ctx.step = 3;
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Proceed);
    // no task -> no ask, "waiting for work"
    let mut p = NeedsSpecialist::with_detect_from("database");
    let mut ctx = MockCtx::new(None);
    ctx.step = 3;
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(
        (a.act, a.reason.clone()),
        (Act::Proceed, "no task; waiting for work".to_string())
    );
}

#[test]
fn needs_specialist_logs_spawn_outcomes() {
    let mut p = NeedsSpecialist::default();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    let mut m = Message::new(EventType::SpawnApproved, "parent", "be_01");
    m.payload
        .insert("agent_id".into(), JValue::Str("sec_01".into()));
    let _ = p.step(&mut ctx, Some(&m)).unwrap();
    assert_eq!(ctx.logs, vec!["parent approved spawn: sec_01".to_string()]);

    let mut p = NeedsSpecialist::default();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    let mut m = Message::new(EventType::SpawnRejected, "parent", "be_01");
    m.payload.insert("rule".into(), JValue::Str("CAP".into()));
    let _ = p.step(&mut ctx, Some(&m)).unwrap();
    assert_eq!(
        ctx.logs,
        vec!["parent refused spawn (CAP); continuing alone".to_string()]
    );

    let mut p = NeedsSpecialist::default();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    let mut m = Message::new(EventType::RequestRerouted, "parent", "be_01");
    m.payload
        .insert("owner".into(), JValue::Str("db_01".into()));
    let _ = p.step(&mut ctx, Some(&m)).unwrap();
    assert_eq!(
        ctx.logs,
        vec!["work rerouted to db_01 instead of a new agent".to_string()]
    );
    // missing payload fields render as "None" (reference f-string of None)
    let mut p = NeedsSpecialist::default();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    let m = Message::new(EventType::SpawnRejected, "parent", "be_01");
    let _ = p.step(&mut ctx, Some(&m)).unwrap();
    assert_eq!(
        ctx.logs,
        vec!["parent refused spawn (None); continuing alone".to_string()]
    );
}

#[test]
fn cursors_restore_like_the_reference() {
    // NeedsSpecialist/EscalateOnComplexity: _steps AND _asked both restored
    let mut p = NeedsSpecialist::default();
    p.restore_cursor(4);
    assert_eq!(p.policy_cursor(), 4);
    // asked == 4 means max_requests reached -> no more asking
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.step = 9;
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Complete);
    let mut p = EscalateOnComplexity::default();
    p.restore_cursor(2);
    assert_eq!(p.policy_cursor(), 2);
    // WaitForArtifacts/SimulatedWork/Hybrid: no cursor fields at all
    let mut w = WaitForArtifacts::default();
    w.restore_cursor(9);
    assert_eq!(w.policy_cursor(), 0);
}

#[test]
fn make_policy_builds_builtins_and_absorbs_overrides() {
    let empty = JMap::new();
    assert_eq!(
        make_policy("simulated", &empty).unwrap().name(),
        "simulated-work"
    );
    assert_eq!(
        make_policy("wait", &empty).unwrap().name(),
        "wait-for-artifacts"
    );
    assert_eq!(
        make_policy("poll", &empty).unwrap().name(),
        "poll-anti-pattern"
    );
    assert_eq!(
        make_policy("escalate", &empty).unwrap().name(),
        "escalate-on-complexity"
    );
    assert_eq!(
        make_policy("specialist", &empty).unwrap().name(),
        "needs-specialist"
    );
    // hybrid is NOT a builtin (KeyError in the reference too)
    let hybrid_err = match make_policy("hybrid", &empty) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("hybrid must not be a builtin"),
    };
    assert_eq!(
        hybrid_err,
        "unknown policy 'hybrid'; choose from ['escalate', 'poll', 'simulated', 'specialist', 'wait']"
    );
    // overrides apply; unknown keys are absorbed, not errors
    let mut ov = JMap::new();
    ov.insert("steps".into(), JValue::Int(5));
    ov.insert("nonsense".into(), JValue::Bool(true));
    let mut p = make_policy("simulated", &ov).unwrap();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.step = 5;
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Publish); // 5 <= 5 boundary
    ctx.step = 6;
    assert_eq!(p.step(&mut ctx, None).unwrap().act, Act::Complete);
    let mut ov = JMap::new();
    ov.insert("after_steps".into(), JValue::Int(0));
    ov.insert("role".into(), JValue::Str("ml engineer".into()));
    let mut p = make_policy("specialist", &ov).unwrap();
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    ctx.step = 1; // after_steps=0 -> ask on the first counted step
    let a = p.step(&mut ctx, None).unwrap();
    assert_eq!(a.act, Act::Publish);
    assert_eq!(
        a.msg
            .unwrap()
            .payload
            .get("requested_role")
            .unwrap()
            .as_str(),
        Some("ml engineer")
    );
}

#[test]
fn self_msg_inherits_causality_from_the_handled_message() {
    let mut ctx = MockCtx::new(Some(task("t", 1.0)));
    let base = Message::new(EventType::TaskAssigned, "parent", "be_01");
    ctx.base = Some(base);
    let m = ctx.self_msg(EventType::TaskProgress, "50%", JMap::new());
    assert_eq!(m.causal_depth, 1);
    assert_eq!(m.task_id.as_ref().map(|t| t.as_str()), Some("t"));
    // fresh (no base) -> depth 0
    let ctx2 = MockCtx::new(Some(task("t", 1.0)));
    let m2 = ctx2.self_msg(EventType::TaskProgress, "50%", JMap::new());
    assert_eq!(m2.causal_depth, 0);
}
