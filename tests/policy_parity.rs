//! Policy parity: replicate the Python golden scenario
//! (`tests/parity/policy_capture.py` → `tests/parity/fixtures/policy_golden.json`)
//! — all six built-ins driven through a context holding the same facts a real
//! Python ActorContext exposed — and compare every decision, log line,
//! message shape, and error string.
//!
//! The one field not compared is the `task_id` of the raw `self_msg` API probe
//! (Python's kwargs semantics make an *explicitly passed* None override the
//! default; no policy ever does that — policies pass `task_id=ctx.task_id` or
//! nothing, both of which the trait models and the policy cases cover).

use arena::graph::TaskSpec;
use arena::msg::{EventType, Message};
use arena::policy::{
    is_sleeping_action, make_policy, Act, EscalateOnComplexity, HybridPolicy, LlmAdapter, LlmError,
    NeedsSpecialist, Policy, PolicyContext, PollUntilReady, SimulatedWork, WaitForArtifacts,
};
use arena::sys::json::{parse, JMap, JValue};
use std::path::PathBuf;

fn golden() -> JValue {
    let p: PathBuf = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/parity/fixtures/policy_golden.json"
    )
    .into();
    parse(&std::fs::read_to_string(p).expect("golden file present")).expect("golden parses")
}

fn g<'a>(v: &'a JValue, key: &str) -> &'a JValue {
    v.get(key).unwrap_or_else(|| panic!("missing key {key}"))
}

/// Same facts the Python capture's kernel exposed: three tasks, one agent
/// (role backend), an artifacts set, and producers by artifact.
struct TestCtx {
    tasks: Vec<TaskSpec>,
    task_id: Option<String>,
    artifacts: Vec<String>,
    roster: Vec<String>,
    producers: Vec<(String, String)>,
    step: i64,
    llm: bool,
    base: Option<Message>,
    logs: Vec<String>,
}

impl TestCtx {
    fn new() -> TestCtx {
        let mut t_be = TaskSpec::new("t_be", "backend API", "backend");
        t_be.est_work = 2.0;
        t_be.produces = vec!["out/api.md".to_string()];
        t_be.consumes = vec!["in/schema.sql".to_string()];
        let mut t_small = TaskSpec::new("t_small", "small job", "backend");
        t_small.est_work = 1.0;
        t_small.produces = vec!["out/small.md".to_string()];
        let mut t_schema = TaskSpec::new("t_schema", "schema", "data");
        t_schema.est_work = 1.0;
        t_schema.produces = vec!["in/schema.sql".to_string()];
        TestCtx {
            tasks: vec![t_be, t_small, t_schema],
            task_id: Some("t_be".to_string()),
            artifacts: Vec::new(),
            roster: vec!["backend".to_string()],
            producers: vec![("in/schema.sql".to_string(), "data".to_string())],
            step: 0,
            llm: false,
            base: None,
            logs: Vec::new(),
        }
    }

    fn task(&self) -> Option<&TaskSpec> {
        let tid = self.task_id.as_ref()?;
        self.tasks.iter().find(|t| t.task_id.as_str() == tid)
    }
}

impl PolicyContext for TestCtx {
    fn agent_id(&self) -> &str {
        "be_01"
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
        self.task()
    }
    fn task_id(&self) -> Option<String> {
        self.task().map(|t| t.task_id.as_str().to_string())
    }
    fn all_unmet(&self) -> Vec<String> {
        match self.task() {
            Some(t) => t
                .consumes
                .iter()
                .filter(|a| !self.artifacts.contains(a))
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }
    fn unmet_artifacts(&self) -> Vec<String> {
        self.all_unmet()
    }
    fn consumed_ready(&self) -> Vec<String> {
        match self.task() {
            Some(t) => t
                .consumes
                .iter()
                .filter(|a| self.artifacts.contains(a))
                .cloned()
                .collect(),
            None => Vec::new(),
        }
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
    fn poll_hit(&mut self) -> Result<i64, arena::bus::BusError> {
        // the capture's kernel had no eagerness limit: plain counting
        use std::cell::Cell;
        thread_local! {
            static POLLS: Cell<i64> = const { Cell::new(0) };
        }
        POLLS.with(|p| {
            p.set(p.get() + 1);
            Ok(p.get())
        })
    }
    fn self_msg(&self, msg_type: EventType, body: &str, payload: JMap) -> Message {
        let mut m = match &self.base {
            Some(base) => {
                let mut c = base.child(msg_type, "be_01", "parent");
                c.body = body.to_string();
                c
            }
            None => {
                let mut fresh = Message::new(msg_type, "be_01", "parent");
                fresh.body = body.to_string();
                fresh
            }
        };
        m.task_id = self.task_id().map(arena::ids::TaskId::new);
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
                self.task().map(|t| t.title.clone()).unwrap_or_default()
            )
        } else {
            reason.to_string()
        };
        self.self_msg(EventType::SpawnAgentRequest, &body, payload)
    }
}

fn msg_view(m: Option<&Message>) -> JValue {
    match m {
        None => JValue::Null,
        Some(m) => {
            let mut d = JMap::new();
            d.insert("type".into(), JValue::Str(m.msg_type.as_str().to_string()));
            d.insert(
                "from".into(),
                JValue::Str(m.from_actor.as_str().to_string()),
            );
            d.insert("to".into(), JValue::Str(m.to_actor.as_str().to_string()));
            d.insert("body".into(), JValue::Str(m.body.clone()));
            d.insert(
                "task_id".into(),
                m.task_id
                    .as_ref()
                    .map(|t| JValue::Str(t.as_str().to_string()))
                    .unwrap_or(JValue::Null),
            );
            d.insert("depth".into(), JValue::Int(m.causal_depth));
            let mut pl = m.payload.clone();
            for k in ["correlation_id", "caused_by"] {
                pl.remove(k);
            }
            d.insert("payload".into(), JValue::Obj(pl));
            JValue::Obj(d)
        }
    }
}

fn action_view(a: &arena::policy::Action) -> JMap {
    let mut d = JMap::new();
    d.insert("act".into(), JValue::Str(a.act.as_str().to_string()));
    d.insert("reason".into(), JValue::Str(a.reason.clone()));
    d.insert("condition".into(), JValue::Str(a.condition.clone()));
    d.insert(
        "timeout".into(),
        a.timeout.map(JValue::Float).unwrap_or(JValue::Null),
    );
    d.insert("msg".into(), msg_view(a.msg.as_ref()));
    d.insert("extra".into(), JValue::Obj(a.extra.clone()));
    d
}

struct Boom;
impl LlmAdapter for Boom {
    fn name(&self) -> &'static str {
        "boom"
    }
    fn decide(&self, _prompt: &JMap) -> Result<arena::policy::Action, LlmError> {
        Err(LlmError("model is down".to_string()))
    }
}

fn fresh(t: EventType, to: &str, body: &str) -> Message {
    let mut m = Message::new(t, "dependency_manager", to);
    if t == EventType::SpawnApproved || t == EventType::SpawnRejected {
        m = Message::new(t, "parent", to);
    }
    m.body = body.to_string();
    m
}

#[test]
fn policy_decisions_match_python_golden() {
    let want = golden();
    let want_decisions = match g(&want, "decisions") {
        JValue::Arr(a) => a.clone(),
        _ => panic!("decisions not a list"),
    };
    let want_logs = match g(&want, "logs") {
        JValue::Arr(a) => a.clone(),
        _ => panic!("logs not a list"),
    };

    let mut got_decisions: Vec<JValue> = Vec::new();
    let mut got_logs: Vec<JValue> = Vec::new();

    macro_rules! run {
        ($case:expr, $pol:expr, $ctx:expr) => {{
            let case = $case;
            let base_clone = $ctx.base.clone();
            match $pol.step(&mut $ctx, base_clone.as_ref()) {
                Ok(a) => {
                    let mut d = action_view(&a);
                    d.insert("case".into(), JValue::Str(case.to_string()));
                    d.insert("ok".into(), JValue::Bool(true));
                    got_decisions.push(JValue::Obj(d));
                }
                Err(e) => {
                    let mut d = JMap::new();
                    d.insert("case".into(), JValue::Str(case.to_string()));
                    d.insert("ok".into(), JValue::Bool(false));
                    d.insert("error".into(), JValue::Str(e.to_string()));
                    got_decisions.push(JValue::Obj(d));
                }
            }
            let mut l = JMap::new();
            l.insert("case".into(), JValue::Str(case.to_string()));
            l.insert(
                "logs".into(),
                JValue::Arr($ctx.logs.iter().cloned().map(JValue::Str).collect()),
            );
            got_logs.push(JValue::Obj(l));
        }};
    }

    // ---- SimulatedWork ----------------------------------------------------
    let mut p = SimulatedWork::default();
    for s in 0..5 {
        let mut ctx = TestCtx::new();
        ctx.step = s;
        run!("sim/step", p, ctx);
    }
    // patch the per-step case names (macro takes a literal tag)
    {
        let mut n: i64 = -1;
        for d in got_decisions.iter_mut() {
            if let JValue::Obj(m) = d {
                if m.get("case").and_then(|c| c.as_str()) == Some("sim/step") {
                    n += 1;
                    m.insert("case".into(), JValue::Str(format!("sim/step{n}")));
                }
            }
        }
        let mut n2: i64 = -1;
        for l in got_logs.iter_mut() {
            if let JValue::Obj(m) = l {
                if m.get("case").and_then(|c| c.as_str()) == Some("sim/step") {
                    n2 += 1;
                    m.insert("case".into(), JValue::Str(format!("sim/step{n2}")));
                }
            }
        }
    }

    {
        let mut ctx = TestCtx::new();
        ctx.step = 4;
        ctx.task_id = None;
        let a = p.step(&mut ctx, None).unwrap();
        let mut d = action_view(&a);
        d.insert("case".into(), JValue::Str("sim/no_task".into()));
        d.insert("ok".into(), JValue::Bool(true));
        got_decisions.push(JValue::Obj(d));
        // the capture's run() helper does not log for this case either
    }

    // ---- WaitForArtifacts -------------------------------------------------
    let mut p = WaitForArtifacts::default();
    {
        let mut ctx = TestCtx::new();
        run!("wait/missing", p, ctx);
    }
    {
        let mut ctx = TestCtx::new();
        ctx.artifacts = vec!["in/schema.sql".to_string()];
        ctx.base = Some(fresh(
            EventType::DependencyReady,
            "be_01",
            "in/schema.sql is ready (published)",
        ));
        ctx.step = 1;
        run!("wait/resumed", p, ctx);
    }
    {
        let mut ctx = TestCtx::new();
        ctx.artifacts = vec!["in/schema.sql".to_string()];
        ctx.step = 2;
        run!("wait/integrate2", p, ctx);
    }
    {
        let mut ctx = TestCtx::new();
        ctx.artifacts = vec!["in/schema.sql".to_string()];
        ctx.step = 3;
        run!("wait/complete", p, ctx);
    }

    // ---- PollUntilReady (artifacts still present, then cleared) -----------
    let mut p = PollUntilReady::with_limit(2);
    for s in 1..4 {
        let mut ctx = TestCtx::new();
        ctx.artifacts = vec!["in/schema.sql".to_string()];
        ctx.step = s;
        run!("poll/step", p, ctx);
    }
    {
        let mut n = 0;
        for d in got_decisions.iter_mut() {
            if let JValue::Obj(m) = d {
                if m.get("case").and_then(|c| c.as_str()) == Some("poll/step") {
                    n += 1;
                    m.insert("case".into(), JValue::Str(format!("poll/step{n}")));
                }
            }
        }
        let mut n = 0;
        for l in got_logs.iter_mut() {
            if let JValue::Obj(m) = l {
                if m.get("case").and_then(|c| c.as_str()) == Some("poll/step") {
                    n += 1;
                    m.insert("case".into(), JValue::Str(format!("poll/step{n}")));
                }
            }
        }
    }
    {
        let mut ctx = TestCtx::new(); // artifacts cleared
        ctx.step = 4;
        run!("poll/clear", p, ctx);
    }

    // ---- EscalateOnComplexity ----------------------------------------------
    let mut p = EscalateOnComplexity::default();
    {
        let mut ctx = TestCtx::new();
        run!("esc/ask", p, ctx);
    }
    {
        let mut ctx = TestCtx::new();
        run!("esc/escalate", p, ctx);
    }
    let mut p2 = EscalateOnComplexity::default();
    {
        let mut ctx = TestCtx::new();
        ctx.task_id = Some("t_small".to_string());
        run!("esc/low", p2, ctx);
    }
    let mut p3 = EscalateOnComplexity::with_request_spawns(2);
    {
        let mut ctx = TestCtx::new();
        ctx.task_id = Some("t_small".to_string());
        ctx.base = Some(fresh(EventType::DependencyReady, "be_01", "unblocked"));
        run!("esc/handoff", p3, ctx);
    }
    {
        let mut ctx = TestCtx::new();
        ctx.task_id = Some("t_small".to_string());
        ctx.step = 1;
        ctx.base = Some(fresh(EventType::SpawnApproved, "be_01", "ok"));
        run!("esc/approved_log", p3, ctx);
    }

    // ---- NeedsSpecialist ----------------------------------------------------
    let mut p = NeedsSpecialist::default();
    for (case, s) in [
        ("ns/early1", 1i64),
        ("ns/early2", 2),
        ("ns/ask", 3),
        ("ns/after", 4),
        ("ns/complete", 9),
    ] {
        let mut ctx = TestCtx::new();
        ctx.step = s;
        run!(case, p, ctx);
    }
    {
        let mut nsd = NeedsSpecialist::with_detect_from("data");
        let mut ctx = TestCtx::new();
        ctx.step = 3;
        run!("ns/detect_ask", nsd, ctx);
    }
    {
        let mut nsd = NeedsSpecialist::with_detect_from("backend");
        let mut ctx = TestCtx::new();
        ctx.step = 3;
        run!("ns/detect_self", nsd, ctx);
    }
    {
        let mut ns = NeedsSpecialist::default();
        let mut ctx = TestCtx::new();
        ctx.step = 1;
        let mut rej = fresh(EventType::SpawnRejected, "be_01", "no");
        rej.payload.insert("rule".into(), JValue::Str("CAP".into()));
        ctx.base = Some(rej);
        run!("ns/rejected_log", ns, ctx);
    }

    // ---- Hybrid with a down adapter ----------------------------------------
    {
        let mut ph = HybridPolicy {
            heuristic: Some(Box::new(EscalateOnComplexity::default())),
            llm: Box::new(Boom),
            llm_available: true,
        };
        let mut ctx = TestCtx::new();
        ctx.task_id = Some("t_small".to_string());
        ctx.step = 1;
        ctx.llm = true;
        run!("hybrid/llm_down", ph, ctx);
    }
    {
        let mut ph = HybridPolicy {
            heuristic: Some(Box::new(EscalateOnComplexity::default())),
            llm: Box::new(Boom),
            llm_available: false,
        };
        let mut ctx = TestCtx::new();
        ctx.task_id = Some("t_small".to_string());
        ctx.step = 1;
        ctx.llm = false;
        run!("hybrid/no_llm", ph, ctx);
    }

    // compare decisions and logs case by case
    assert_eq!(
        got_decisions.len(),
        want_decisions.len(),
        "decision count {} vs {}",
        got_decisions.len(),
        want_decisions.len()
    );
    for (got, want_d) in got_decisions.iter().zip(want_decisions.iter()) {
        let case = want_d.get("case").unwrap().as_str().unwrap().to_string();
        assert_eq!(
            got,
            want_d,
            "decision {case}:\n got {}\n want {}",
            got.to_canon_string(),
            want_d.to_canon_string()
        );
    }
    for (got, want_l) in got_logs.iter().zip(want_logs.iter()) {
        let case = want_l.get("case").unwrap().as_str().unwrap().to_string();
        assert_eq!(got, want_l, "logs {case}");
    }

    // ---- self_msg causality (depth-1 inheritance) ---------------------------
    let inherit = g(&want, "self_msg_inherit");
    {
        let base = fresh(EventType::TaskAssigned, "be_01", "go");
        let mut base = base;
        base.task_id = Some(arena::ids::TaskId::new("t_be"));
        // a fresh context-level self_msg with a base: depth 1, same to/from
        let mut c = TestCtx::new();
        c.base = Some(base);
        let mut payload = JMap::new();
        payload.insert("percent".into(), JValue::Int(50));
        let m = c.self_msg(EventType::TaskProgress, "50%", payload);
        let view = match msg_view(Some(&m)) {
            JValue::Obj(o) => o,
            _ => unreachable!(),
        };
        assert_eq!(view.get("depth"), inherit.get("depth"), "inherit depth");
        assert_eq!(view.get("from"), inherit.get("from"));
        assert_eq!(view.get("to"), inherit.get("to"));
        assert_eq!(view.get("body"), inherit.get("body"));
        assert_eq!(view.get("payload"), inherit.get("payload"));
        assert_eq!(view.get("type"), inherit.get("type"));
        // task_id is the ctx-API kwargs quirk (explicit None), not a policy
        // behavior; policies are covered by the request_specialist cases above
    }

    // ---- is_sleeping_action surface ----------------------------------------
    let sleeping = match g(&want, "sleeping") {
        JValue::Obj(m) => m.clone(),
        _ => panic!("sleeping not an object"),
    };
    for (k, v) in &sleeping {
        let want_b = v.as_bool().unwrap();
        assert_eq!(is_sleeping_action(k), want_b, "is_sleeping_action({k:?})");
    }

    // ---- make_policy --------------------------------------------------------
    let mp = match g(&want, "make_policy") {
        JValue::Obj(m) => m.clone(),
        _ => panic!("make_policy not an object"),
    };
    let names = match mp.get("names").unwrap() {
        JValue::Obj(m) => m.clone(),
        _ => panic!(),
    };
    let empty = JMap::new();
    for (k, v) in &names {
        assert_eq!(
            JValue::Str(make_policy(k, &empty).unwrap().name().to_string()),
            *v,
            "make_policy({k}).name"
        );
    }
    for bad in ["hybrid", "nope"] {
        let err = match make_policy(bad, &empty) {
            Err(e) => JValue::Str(e.keyerror_repr()),
            Ok(_) => panic!("{bad} must be an error"),
        };
        assert_eq!(err, *mp.get(bad).unwrap(), "make_policy({bad}) error");
    }
    assert_eq!(
        *mp.get("sim_steps5_complete_at").unwrap(),
        JValue::Bool(true)
    );
    {
        let mut ov = JMap::new();
        ov.insert("after_steps".into(), JValue::Int(0));
        ov.insert("role".into(), JValue::Str("ml engineer".into()));
        let mut p = make_policy("specialist", &ov).unwrap();
        let mut ctx = TestCtx::new();
        ctx.step = 1;
        let a = p.step(&mut ctx, None).unwrap();
        let mut view = action_view(&a);
        view.remove("case");
        assert_eq!(
            JValue::Obj(view),
            *mp.get("specialist_override").unwrap(),
            "specialist override decision"
        );
    }

    // sanity: the Act surface used above
    assert_eq!(Act::Wait.as_str(), "WAIT");
}
