//! M6 cognition tests — the ten cases the milestone names, plus the
//! outcome-conversion contract.

use super::*;
use crate::msg::{EventType, Message, Plane};
use crate::policy::{Act, Action, Policy, PolicyContext, PolicyError};
use crate::sys::json::{py_str_repr, JValue};

fn jmap(pairs: &[(&str, JValue)]) -> JMap {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ------------------------------------------------------------------ context

/// A read-only PolicyContext stand-in — the same surface the actor's real
/// context exposes to policies.
#[derive(Default)]
struct Ctx {
    task: Option<crate::graph::TaskSpec>,
    artifacts: Vec<String>,
    roster: Vec<String>,
    producers: Vec<(String, String)>, // (artifact, role)
    step: i64,
}

fn spec(
    task_id: &str,
    title: &str,
    consumes: &[&str],
    produces: &[&str],
    est: f64,
) -> crate::graph::TaskSpec {
    let mut t = crate::graph::TaskSpec::new(task_id, title, "worker");
    t.est_work = est;
    t.consumes = consumes.iter().map(|s| s.to_string()).collect();
    t.produces = produces.iter().map(|s| s.to_string()).collect();
    t
}

impl PolicyContext for Ctx {
    fn agent_id(&self) -> &str {
        "a_01"
    }
    fn step_index(&self) -> i64 {
        self.step
    }
    fn state(&self) -> String {
        "WORKING".to_string()
    }
    fn llm_available(&self) -> bool {
        false
    }
    fn task(&self) -> Option<&crate::graph::TaskSpec> {
        self.task.as_ref()
    }
    fn task_id(&self) -> Option<String> {
        self.task.as_ref().map(|t| t.task_id.as_str().to_string())
    }
    fn all_unmet(&self) -> Vec<String> {
        match &self.task {
            Some(t) => t
                .consumes
                .iter()
                .filter(|a| !self.artifacts.contains(a))
                .cloned()
                .collect(),
            None => vec![],
        }
    }
    fn unmet_artifacts(&self) -> Vec<String> {
        self.all_unmet()
    }
    fn consumed_ready(&self) -> Vec<String> {
        match &self.task {
            Some(t) => t
                .consumes
                .iter()
                .filter(|a| self.artifacts.contains(a))
                .cloned()
                .collect(),
            None => vec![],
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
    fn log(&mut self, _text: &str) {}
    fn poll_hit(&mut self) -> Result<i64, crate::bus::BusError> {
        Ok(0)
    }
    fn self_msg(&self, msg_type: EventType, body: &str, payload: JMap) -> Message {
        let mut m = Message::new(msg_type, "a_01", "a_01");
        m.body = body.to_string();
        m.payload = payload;
        m
    }
    fn request_specialist(
        &self,
        _role: &str,
        _reason: &str,
        _skills: &[String],
        _inputs: &[String],
        _outputs: &[String],
        _est_work: f64,
        _capability_class: &str,
    ) -> Message {
        Message::new(EventType::SpawnAgentRequest, "a_01", "broadcast")
    }
}

fn obs_with_message(msg: Option<Message>) -> Observation {
    Observation {
        agent_id: "a_01".into(),
        role: "worker".into(),
        goal: "build the thing".into(),
        message: msg,
        step_index: 2,
        ..Default::default()
    }
}

// ---------------------------------------------------------------- 1. valid intent

#[test]
fn valid_intent_passes_through_untouched() {
    let mut i = Intent::new();
    i.calls = vec![
        ToolCall::new(
            "bash",
            jmap(&[("argv", JValue::Arr(vec![JValue::Str("ls".into())]))]),
        ),
        ToolCall::new("read", jmap(&[])),
    ];
    i.control = Control::PUBLISH.to_string();
    i.think = "checking".into();
    i.reason = "publish after check".into();
    let allowed = vec!["bash".to_string(), "read".to_string()];
    let (out, notes) = validate_intent(Some(i.clone()), &allowed, 4);
    assert!(
        notes.is_empty(),
        "no refusals for a valid intent: {notes:?}"
    );
    assert_eq!(
        out, i,
        "valid intent returned as-is (same instance in the reference)"
    );
    assert!(!out.is_noop());
}

// ------------------------------------------------------------- 2. missing intent

#[test]
fn missing_intent_becomes_noop_with_note() {
    let (out, notes) = validate_intent(None, &[], 4);
    assert_eq!(notes, vec!["INTENT_MISSING"]);
    assert_eq!(out.control, Control::NOOP);
    assert_eq!(out.reason, "cognition returned nothing");
    assert!(out.is_noop());
}

// ------------------------------------------------------------ 3. unknown control

#[test]
fn unknown_control_is_downgraded_not_trusted() {
    let mut i = Intent::new();
    i.control = "run".into();
    i.think = "original thought".into();
    i.reason = "full speed".into();
    i.wait_for = "x".into();
    let (out, notes) = validate_intent(Some(i), &[], 4);
    assert_eq!(notes, vec!["CONTROL_UNKNOWN:run"]);
    assert_eq!(out.control, Control::NOOP);
    assert_eq!(
        out.reason,
        format!("unknown control {}; treated as NOOP", py_str_repr("run"))
    );
    // calls and think survive the rebuild; the verb, wait_for and payloads do not
    assert_eq!(out.think, "original thought");
    assert_eq!(out.wait_for, "");
    assert!(out.publish.is_empty() && out.spawn.is_empty() && out.msg.is_none());
}

// ------------------------------------------------------------ 4. unauthorized tool

#[test]
fn unauthorized_tool_refused_with_paper_trail() {
    let mut i = Intent::new(); // control stays "" (NONE)
    i.calls = vec![
        ToolCall::new("rm", jmap(&[])),
        ToolCall::new("curl", jmap(&[])),
    ];
    let allowed = vec!["bash".to_string()];
    let (out, notes) = validate_intent(Some(i), &allowed, 4);
    assert_eq!(notes, vec!["TOOL_NOT_ALLOWED:rm", "TOOL_NOT_ALLOWED:curl"]);
    // every call refused and no control verb: NOOP with a paper trail
    assert_eq!(out.control, Control::NOOP);
    assert_eq!(
        out.reason,
        "all 2 call(s) refused: TOOL_NOT_ALLOWED:rm;TOOL_NOT_ALLOWED:curl"
    );
    assert!(out.calls.is_empty());
}

// --------------------------------------------------------- 5. excessive tool calls

#[test]
fn excessive_tool_calls_truncated_after_filtering() {
    let mut i = Intent::new();
    i.calls = (0..6)
        .map(|n| ToolCall::new(&format!("t{n}"), jmap(&[])))
        .collect();
    let allowed: Vec<String> = (0..6).map(|n| format!("t{n}")).collect();
    let (out, notes) = validate_intent(Some(i), &allowed, 4);
    assert_eq!(notes, vec!["CALLS_TRUNCATED:6>4"]);
    assert_eq!(out.calls.len(), 4);
    assert_eq!(out.calls[0].tool, "t0", "the FIRST max_calls survive");
    assert_eq!(out.max_calls, 4);
    // mixed case: one refused (t6), six allowed -> truncated to 4, one note each
    let mut i2 = Intent::new();
    i2.calls = (0..7)
        .map(|n| ToolCall::new(&format!("t{n}"), jmap(&[])))
        .collect();
    let (out2, notes2) = validate_intent(Some(i2), &allowed, 4);
    assert_eq!(notes2, vec!["TOOL_NOT_ALLOWED:t6", "CALLS_TRUNCATED:6>4"]);
    assert_eq!(out2.calls.len(), 4);
}

// ---------------------------------------------------------- 6. malformed tool call

#[test]
fn malformed_tool_call_dict_becomes_empty_tool_and_is_refused() {
    // Intent::of is the dict-shaped provider path: a missing/None tool -> ""
    let dict = JValue::Obj(jmap(&[(
        "args",
        JValue::Obj(jmap(&[("x", JValue::Int(1))])),
    )]));
    let i = Intent::of(
        &[dict.clone(), JValue::Str("garbage".into())],
        Control::NONE,
        "",
    );
    assert_eq!(i.calls.len(), 2);
    assert_eq!(i.calls[0].tool, "");
    assert_eq!(i.calls[0].args.get("x"), Some(&JValue::Int(1)));
    // a non-dict call contributes an empty ToolCall, never a crash
    assert_eq!(i.calls[1].tool, "");
    let allowed = vec!["bash".to_string()];
    let (out, notes) = validate_intent(Some(i), &allowed, 4);
    assert_eq!(notes, vec!["TOOL_NOT_ALLOWED:", "TOOL_NOT_ALLOWED:"]);
    assert!(out.calls.is_empty());
    assert_eq!(
        out.reason,
        "all 2 call(s) refused: TOOL_NOT_ALLOWED:;TOOL_NOT_ALLOWED:"
    );
}

// ------------------------------------------------------- 7. policy adapter compat

#[test]
fn policy_adapter_proposes_valid_intents_through_the_same_path() {
    // WAIT via WaitForArtifacts
    let mut ctx = Ctx {
        task: Some(spec("t_01", "needs schema", &["db/schema.sql"], &[], 1.0)),
        artifacts: vec![],
        step: 0,
        ..Default::default()
    };
    let pol = crate::policy::WaitForArtifacts::default();
    let mut cog = PolicyCognition::new(Box::new(pol), "a_01");
    let obs = obs_with_message(None);
    let intent = cog.decide(&obs, &mut ctx).expect("wait intent");
    assert_eq!(intent.control, Control::WAIT);
    assert_eq!(intent.wait_for, "artifact:db/schema.sql");
    let (v, notes) = validate_intent(Some(intent.clone()), &[], 4);
    assert!(notes.is_empty());
    assert_eq!(
        v, intent,
        "a policy proposal needs no leniency to pass validation"
    );

    // COMPLETE via SimulatedWork with a finished task (est_work 1 -> done at step 1)
    let mut ctx = Ctx {
        task: Some(spec("t_01", "one step", &[], &["t_01.done"], 1.0)),
        step: 4,
        ..Default::default()
    };
    let pol = crate::policy::SimulatedWork::default();
    let mut cog = PolicyCognition::new(Box::new(pol), "a_01");
    let obs = Observation {
        step_index: 4,
        ..obs_with_message(None)
    };
    let intent = cog.decide(&obs, &mut ctx).expect("complete intent");
    assert_eq!(
        intent.control,
        Control::COMPLETE,
        "est_work=1 completes at step 4"
    );
    assert_eq!(
        intent.publish.get("artifacts"),
        Some(&JValue::Arr(vec![JValue::Str("t_01.done".into())]))
    );
    let (v, notes) = validate_intent(Some(intent), &[], 4);
    assert!(notes.is_empty());
    assert_eq!(v.control, Control::COMPLETE);

    // SPAWN_REQUEST via NeedsSpecialist (producer role absent from roster)
    let mut ctx = Ctx {
        task: Some(spec(
            "t_02",
            "load the database",
            &["db/schema.sql"],
            &[],
            1.0,
        )),
        roster: vec!["worker".into()],
        producers: vec![("db/schema.sql".to_string(), "database".to_string())],
        ..Default::default()
    };
    let ns = crate::policy::NeedsSpecialist::with_detect_from("database");
    let mut cog = PolicyCognition::new(Box::new(ns), "a_01");
    let obs = Observation {
        step_index: 3,
        ..obs_with_message(None)
    };
    ctx.step = 3;
    let intent = cog.decide(&obs, &mut ctx).expect("spawn intent");
    assert_eq!(intent.control, Control::SPAWN_REQUEST);
    let msg = intent.msg.clone().expect("typed message carrier");
    assert_eq!(msg.msg_type, EventType::SpawnAgentRequest);
    let (v, notes) = validate_intent(Some(intent), &[], 4);
    assert!(notes.is_empty());
    assert_eq!(v.control, Control::SPAWN_REQUEST);
    assert!(v.msg.is_some(), "the carrier survives validation");
}

// -------------------------------------------------------- 8. fingerprint stability

#[test]
fn fingerprint_is_stable_and_sensitive() {
    let mut a = Intent::new();
    a.control = Control::WAIT.to_string();
    a.wait_for = "db/schema.sql".into();
    let b = a.clone();
    assert_eq!(
        a.fingerprint(),
        b.fingerprint(),
        "same content, same digest"
    );
    assert_eq!(a.fingerprint().len(), 12);
    // changing ANY surface the digest covers changes it
    let mut c = a.clone();
    c.reason = "tiny change".into();
    assert_ne!(a.fingerprint(), c.fingerprint());
    let mut d = a.clone();
    d.control = Control::NOOP.to_string();
    assert_ne!(a.fingerprint(), d.fingerprint());
    // order matters: calls are positional
    let x = ToolCall::new("bash", jmap(&[]));
    let y = ToolCall::new("read", jmap(&[]));
    let mut p1 = Intent::new();
    p1.calls = vec![x.clone(), y.clone()];
    let mut p2 = Intent::new();
    p2.calls = vec![y, x];
    assert_ne!(p1.fingerprint(), p2.fingerprint());
}

#[test]
fn fingerprint_known_vector_matches_cpython() {
    // computed in Python: sha256(repr([Intent(control="WAIT",
    // wait_for="db/schema.sql").to_dict()]))[:12]
    let mut i = Intent::new();
    i.control = "WAIT".into();
    i.wait_for = "db/schema.sql".into();
    assert_eq!(i.fingerprint(), "7cbe514f9472");
}

#[test]
fn fingerprint_of_message_carrier_uses_dataclass_repr() {
    // computed in Python for the same Message field values (fixed mid/ts)
    let mut m = Message::new(EventType::ArtifactPublished, "a_01", "broadcast");
    m.mid = "m-fixed000001".into();
    m.correlation_id = "c-fixed000001".into();
    m.ts = 1.25;
    let want_repr = "Message(msg_type=<MessageType.ARTIFACT_PUBLISHED: 'ARTIFACT_PUBLISHED'>, from_actor='a_01', to_actor='broadcast', body='', payload={}, topic='resource.artifact.published', resource=None, task_id=None, plane=<Plane.RESOURCE: 'resource'>, correlation_id='c-fixed000001', caused_by=None, causal_depth=0, seq=-1, ts=1.25, mid='m-fixed000001')";
    assert_eq!(m.py_repr(), want_repr);
    let mut i = Intent::new();
    i.control = Control::PUBLISH.to_string();
    i.msg = Some(m);
    assert_eq!(i.fingerprint(), "82db38e7cb55");
}

// ---------------------------------------------------- 9. observation rendering

#[test]
fn observation_render_is_byte_exact() {
    let obs = Observation {
        agent_id: "a_01".into(),
        role: "worker".into(),
        state: "WORKING".into(),
        step_index: 3,
        task: jmap(&[
            ("task_id", JValue::Str("t_09".into())),
            ("status", JValue::Str("OPEN".into())),
            (
                "produces",
                JValue::Arr(vec![JValue::Str("db/schema.sql".into())]),
            ),
            (
                "verify",
                JValue::Arr(vec![
                    JValue::Str("pytest".into()),
                    JValue::Str("ruff".into()),
                ]),
            ),
        ]),
        unmet: vec!["api/openapi.json".into()],
        workspace: jmap(&[
            (
                "files",
                JValue::Arr(vec![JValue::Str("a.py".into()), JValue::Str("b.py".into())]),
            ),
            ("dirty", JValue::Bool(true)),
        ]),
        recent: vec![
            Outcome {
                tool: "bash".into(),
                ok: true,
                exit_code: Some(0),
                ..Default::default()
            },
            Outcome {
                tool: "write".into(),
                ok: false,
                exit_code: None,
                refused: "path outside workspace".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let want = "agent=a_01 role=worker state=WORKING step=3\n\
                task=t_09 status=OPEN produces=['db/schema.sql'] verify=2\n\
                unmet=['api/openapi.json']\n\
                workspace files=['a.py', 'b.py'] dirty=True\n\
                last bash: ok=True exit=0 \n\
                last write: ok=False exit=None refused=path outside workspace";
    assert_eq!(obs.render(), want);
}

#[test]
fn prompt_seed_carries_explicit_fields_and_six_tail_entries() {
    let mk = |tool: &str| Outcome {
        tool: tool.into(),
        ok: true,
        ..Default::default()
    };
    let obs = Observation {
        agent_id: "a_02".into(),
        role: "coder".into(),
        goal: "ship it".into(),
        state: "WORKING".into(),
        history: (0..9).map(|n| mk(&format!("h{n}"))).collect(),
        recent: vec![mk("r0")],
        notes: vec!["note one".into()],
        tool_schemas: vec![jmap(&[("tool", JValue::Str("bash".into()))])],
        graph_view: vec![jmap(&[("task_id", JValue::Str("t_1".into()))])],
        budget: jmap(&[("steps_left", JValue::Int(3))]),
        workspace: jmap(&[("files", JValue::Arr(vec![]))]),
        unmet: vec!["x".into()],
        task: jmap(&[("task_id", JValue::Str("t_1".into()))]),
        ..Default::default()
    };
    let seed = obs.prompt_seed();
    assert_eq!(seed.len(), 13);
    assert_eq!(seed.get("agent_id"), Some(&JValue::Str("a_02".into())));
    let tail = seed
        .get("history_tail")
        .and_then(|v| match v {
            JValue::Arr(a) => Some(a.clone()),
            _ => None,
        })
        .expect("array");
    assert_eq!(tail.len(), 6, "exactly the last six");
    assert_eq!(
        tail[0].get("tool").and_then(|t| t.as_str()),
        Some("h3"),
        "oldest of the tail"
    );
    assert_eq!(tail[5].get("tool").and_then(|t| t.as_str()), Some("h8"));
    assert!(
        matches!(seed.get("recent"), Some(JValue::Arr(a)) if a[0].get("tool") == Some(&JValue::Str("r0".into())))
    );
    // Outcome.to_dict drops text and rid
    let od = tail[0].clone();
    assert!(od.get("text").is_none() && od.get("rid").is_none());
    assert!(od.get("data").is_some() && od.get("refused").is_some());
}

// ------------------------------------------------- 10. cognition error handling

/// A policy whose step() always fails — the "model down" stand-in.
struct Boom;
impl Policy for Boom {
    fn name(&self) -> &'static str {
        "boom"
    }
    fn class_name(&self) -> &'static str {
        "Boom"
    }
    fn step(
        &mut self,
        _ctx: &mut dyn PolicyContext,
        _m: Option<&Message>,
    ) -> Result<Action, PolicyError> {
        Err(PolicyError::PollingForbidden(
            crate::bus::BusError::PollingForbidden {
                actor: "a_01".into(),
                hits: 9,
                limit: 4,
            },
        ))
    }
}

#[test]
fn crashing_policy_maps_to_cognition_error_not_kernel_mutation() {
    let mut ctx = Ctx::default();
    let mut cog = PolicyCognition::new(Box::new(Boom), "a_01");
    let obs = obs_with_message(None);
    let err = cog.decide(&obs, &mut ctx).expect_err("must fail");
    match err {
        CognitionError::Other(msg) => {
            assert!(
                msg.starts_with("PollingForbidden:"),
                "raw policy error text: {msg}"
            );
        }
        CognitionError::Capability(_) => panic!("an ordinary crash is not a capability breach"),
    }
    // the reference distinguishes capability breaches for a DIFFERENT journal
    // kind; in Rust the read-only projection makes that breach unrepresentable
    // at compile time — the variant exists so future provider sources can
    // still raise it and be journaled distinctly.
    let cap = CognitionError::Capability(
        "policy X used a context member that the read-only projection does not expose".into(),
    );
    assert_ne!(cap, CognitionError::Other("x".into()));
    assert!(cap.to_string().contains("read-only projection"));
}

#[test]
fn cognition_fingerprint_reports_policy_class_and_prompt_digest() {
    let cog = PolicyCognition::new(
        Box::new(crate::policy::SimulatedWork::default()),
        "coder_01",
    );
    assert_eq!(cog.name(), "policy:simulated-work");
    let fp = cog.fingerprint();
    assert_eq!(
        fp.get("source"),
        Some(&JValue::Str("policy:simulated-work".into()))
    );
    assert_eq!(fp.get("kind"), Some(&JValue::Str("policy".into())));
    assert_eq!(fp.get("model"), Some(&JValue::Str("deterministic".into())));
    assert_eq!(fp.get("policy"), Some(&JValue::Str("SimulatedWork".into())));
    // sha256("SimulatedWork|coder_01")[:16] — the shared-brain audit digest
    let want = crate::sys::sha256::hex_digest(b"SimulatedWork|coder_01")[..16].to_string();
    assert_eq!(fp.get("prompt_sha256_16"), Some(&JValue::Str(want)));
    assert!(
        !fp.contains_key("obj_id"),
        "no fake object id (documented deviation)"
    );
}

// ------------------------------------------------------- outcome conversion

#[test]
fn outcomes_from_results_keeps_scalars_and_defaults() {
    let rs = vec![
        ToolResultView {
            tool: "bash".into(),
            ok: true,
            exit_code: Some(2),
            block: "== bash ==\nls\n".into(),
            data: jmap(&[
                ("n", JValue::Int(3)),
                ("ratio", JValue::Float(0.5)),
                ("name", JValue::Str("x".into())),
                ("flag", JValue::Bool(false)),
                ("nested", JValue::Obj(jmap(&[]))), // dropped: not a scalar
                ("listy", JValue::Arr(vec![])),     // dropped: not a scalar
            ]),
            ..Default::default()
        },
        ToolResultView {
            // tool missing -> "?"; refused None -> ""
            block: "hidden".into(),
            data: jmap(&[("keep", JValue::Str("v".into()))]),
            ..Default::default()
        },
    ];
    let out = outcomes_from_results(&rs);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].tool, "bash");
    assert!(out[0].ok);
    assert_eq!(out[0].exit_code, Some(2));
    assert_eq!(out[0].text, "== bash ==\nls\n");
    assert_eq!(out[0].data.len(), 4, "only int/float/str/bool survive");
    assert!(!out[0].data.contains_key("nested") && !out[0].data.contains_key("listy"));
    // getattr(r, "tool", "?") — the field exists ("" default), so the "?"
    // fallback is unreachable on the Rust side
    assert_eq!(out[1].tool, "");
    assert_eq!(out[1].refused, "");
    // refused is passed through when present
    let rs2 = vec![ToolResultView {
        tool: "write".into(),
        refused: "outside workspace".into(),
        rid: "r-1".into(),
        ..Default::default()
    }];
    assert_eq!(outcomes_from_results(&rs2)[0].refused, "outside workspace");
}

// -------------------------------------------------------------- verb sets

#[test]
fn control_and_yielding_sets_match_the_kernel_contract() {
    assert_eq!(Control::ALL.len(), 7);
    for c in Control::ALL {
        assert!(!c.is_empty());
    }
    // D2 guard: yielding = the kernel's sleeping actions
    for y in YIELDING {
        assert!(Control::ALL.contains(&y));
    }
    assert_eq!(YIELDING.len(), 3);
    // the empty verb is not a verb
    assert!(!Control::ALL.contains(&Control::NONE));
    let mut i = Intent::new();
    assert!(i.is_noop(), "no calls + empty control is a no-op");
    i.control = Control::NOOP.into();
    assert!(i.is_noop(), "explicit NOOP is a no-op");
    i.calls = vec![ToolCall::new("bash", jmap(&[]))];
    assert!(!i.is_noop(), "a call makes it real even with NOOP control");
}

// -------------------------------------------------------------- translate

#[test]
fn translate_covers_every_act_with_reference_defaults() {
    let cog = PolicyCognition::new(Box::new(crate::policy::SimulatedWork::default()), "a_01");
    // PROCEED
    let a = Action::proceed("steady");
    let i = cog.translate(&a);
    assert_eq!(i.control, Control::NONE);
    assert_eq!(i.reason, "steady");
    // PROCEED with empty reason -> "proceed"
    let i = cog.translate(&Action::proceed(""));
    assert_eq!(i.reason, "proceed");
    // WAIT keeps condition and blank-able reason
    let mut a = Action::wait("artifact:x", "parked", None);
    let i = cog.translate(&a);
    assert_eq!(
        (i.control.as_str(), i.wait_for.as_str(), i.reason.as_str()),
        (Control::WAIT, "artifact:x", "parked")
    );
    a = Action::wait("c", "", None);
    assert_eq!(cog.translate(&a).reason, "");
    // NOOP
    let i = cog.translate(&Action {
        act: Act::Noop,
        reason: "nothing to do".into(),
        ..Default::default()
    });
    assert_eq!(
        (i.control.as_str(), i.reason.as_str()),
        (Control::NOOP, "nothing to do")
    );
    // ESCALATE -> publish.extra
    let mut a = Action::escalate("stuck");
    a.extra = jmap(&[("requested_role", JValue::Str("dba".into()))]);
    let i = cog.translate(&a);
    assert_eq!(i.control, Control::ESCALATE);
    assert_eq!(i.reason, "stuck");
    assert_eq!(
        i.publish.get("extra").and_then(|e| e.get("requested_role")),
        Some(&JValue::Str("dba".into()))
    );
    // COMPLETE -> publish.artifacts (missing key -> [])
    let a = Action::complete("");
    let i = cog.translate(&a);
    assert_eq!(i.reason, "complete");
    assert_eq!(i.publish.get("artifacts"), Some(&JValue::Arr(vec![])));
    // PUBLISH without a message degrades to NONE/proceed
    let a = Action {
        act: Act::Publish,
        msg: None,
        ..Default::default()
    };
    let i = cog.translate(&a);
    assert_eq!(
        (i.control.as_str(), i.reason.as_str()),
        (Control::NONE, "proceed")
    );
    // PUBLISH with a plain message -> PUBLISH + carrier
    let mut m = Message::new(EventType::ArtifactPublished, "a_01", "broadcast");
    m.mid = "m-carrier00001".into();
    let a = Action::publish(m, "");
    let i = cog.translate(&a);
    assert_eq!(i.control, Control::PUBLISH);
    assert_eq!(i.reason, "publish");
    assert!(i.msg.is_some());
    // PUBLISH whose message is a spawn request -> SPAWN_REQUEST + carrier
    let sm = Message::new(EventType::SpawnAgentRequest, "a_01", "broadcast");
    let a = Action::publish(sm, "");
    let i = cog.translate(&a);
    assert_eq!(i.control, Control::SPAWN_REQUEST);
    assert_eq!(i.reason, "spawn request");
    assert!(i.msg.is_some());
}

// -------------------------------------------------------------- plane sanity

#[test]
fn message_repr_helper_matches_cpython_for_plain_fields() {
    let mut m = Message::new(EventType::ArtifactPublished, "a_01", "broadcast");
    m.mid = "m-repr0000001".into();
    m.correlation_id = "c-repr0000001".into();
    let want = "Message(msg_type=<MessageType.ARTIFACT_PUBLISHED: 'ARTIFACT_PUBLISHED'>, \
                from_actor='a_01', to_actor='broadcast', body='', payload={}, \
                topic='resource.artifact.published', resource=None, task_id=None, \
                plane=<Plane.RESOURCE: 'resource'>, correlation_id='c-repr0000001', \
                caused_by=None, causal_depth=0, seq=-1, ts=0.0, mid='m-repr0000001')";
    assert_eq!(m.py_repr(), want);
    assert_eq!(Plane::Resource.as_str(), "resource");
}
