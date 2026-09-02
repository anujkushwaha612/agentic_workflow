//! Control-plane seam: human / Parent / arena-code decisions enter the
//! runtime here. Kernel does not parse `var/*.jsonl` itself.
//!
//! ```text
//!     Parent / arena-code / tests
//!              │
//!         ControlPlane
//!              │
//!            Kernel.apply_decision / inject / record_escalation
//! ```

use std::path::{Path, PathBuf};

use crate::lifecycle::AgentState;
use crate::sys::json::{parse, JMap, JValue};

/// A sanctioned override the runtime may apply at the start of a tick.
/// Spawn-approval and graph amend belong to Parent (M9) and are not
/// interpreted here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlDecision {
    ForceState { agent_id: String, state: AgentState },
    ForceTerminate { agent_id: String },
}

/// How the world above Kernel talks to it each tick.
pub trait ControlPlane {
    fn poll_decisions(&mut self) -> Vec<ControlDecision>;
    fn poll_injections(&mut self) -> Vec<JMap>;
    fn on_escalation(&mut self, note: &JMap);
}

/// File adapter used by disk-backed kernels and tests. Product may replace
/// this with a Kanban/ZeroMQ adapter without touching Kernel.
pub struct FileControlPlane {
    anchor: PathBuf,
    decision_rel: String,
    inject_rel: String,
    inbox_rel: String,
}

impl FileControlPlane {
    pub fn new(anchor: impl Into<PathBuf>) -> Self {
        FileControlPlane {
            anchor: anchor.into(),
            decision_rel: "var/decisions.jsonl".into(),
            inject_rel: "var/inject.jsonl".into(),
            inbox_rel: "var/inbox.jsonl".into(),
        }
    }

    pub fn with_inject(mut self, rel: impl Into<String>) -> Self {
        self.inject_rel = rel.into();
        self
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.anchor.join(rel)
    }

    fn read_and_clear(path: &Path) -> String {
        if !path.exists() {
            return String::new();
        }
        let text = std::fs::read_to_string(path).unwrap_or_default();
        let _ = std::fs::write(path, "");
        text
    }

    fn parse_decision(v: &JValue) -> Option<ControlDecision> {
        match v.str_or("kind", "").as_str() {
            "force_state" => {
                let aid = v.str_or("agent_id", "");
                let st = v.str_or("state", "");
                let state = AgentState::parse(&st)?;
                Some(ControlDecision::ForceState {
                    agent_id: aid,
                    state,
                })
            }
            "force_terminate" => Some(ControlDecision::ForceTerminate {
                agent_id: v.str_or("agent_id", ""),
            }),
            _ => None,
        }
    }

    fn parse_injection(v: JValue) -> Option<JMap> {
        match v.get("spawn_request") {
            Some(JValue::Obj(m)) => Some(m.clone()),
            _ => match v {
                JValue::Obj(m) => Some(m),
                _ => None,
            },
        }
    }
}

impl ControlPlane for FileControlPlane {
    fn poll_decisions(&mut self) -> Vec<ControlDecision> {
        let path = self.path(&self.decision_rel);
        let text = Self::read_and_clear(&path);
        let mut out = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = parse(line) else {
                continue;
            };
            if let Some(d) = Self::parse_decision(&v) {
                out.push(d);
            }
        }
        out
    }

    fn poll_injections(&mut self) -> Vec<JMap> {
        let path = self.path(&self.inject_rel);
        let text = Self::read_and_clear(&path);
        let mut out = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(v) = parse(line) else {
                continue;
            };
            if let Some(m) = Self::parse_injection(v) {
                out.push(m);
            }
        }
        out
    }

    fn on_escalation(&mut self, note: &JMap) {
        let path = self.path(&self.inbox_rel);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            use std::io::Write;
            let _ = writeln!(f, "{}", JValue::Obj(note.clone()).to_canon_string());
        }
    }
}
