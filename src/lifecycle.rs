//! Agent lifecycle FSM as a *closed* transition system (`arena/lifecycle.py`).
//!
//! Without an explicit table, "an agent resumes itself from COMPLETED" is a
//! silent logic bug; here it is a rejected edge, journalled as
//! ILLEGAL_TRANSITION. Illegal transitions never mutate state — the guard is at
//! the boundary, not inside policies.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AgentState {
    Created,
    Initializing,
    Idle,
    Working,
    WaitingForDependency,
    Blocked,
    Escalated,
    Paused,
    Completed,
    Terminated,
}

impl AgentState {
    pub const ALL: [AgentState; 10] = [
        AgentState::Created,
        AgentState::Initializing,
        AgentState::Idle,
        AgentState::Working,
        AgentState::WaitingForDependency,
        AgentState::Blocked,
        AgentState::Escalated,
        AgentState::Paused,
        AgentState::Completed,
        AgentState::Terminated,
    ];

    /// SCREAMING_SNAKE, exactly as Python's `StrEnum` value / journal payload.
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentState::Created => "CREATED",
            AgentState::Initializing => "INITIALIZING",
            AgentState::Idle => "IDLE",
            AgentState::Working => "WORKING",
            AgentState::WaitingForDependency => "WAITING_FOR_DEPENDENCY",
            AgentState::Blocked => "BLOCKED",
            AgentState::Escalated => "ESCALATED",
            AgentState::Paused => "PAUSED",
            AgentState::Completed => "COMPLETED",
            AgentState::Terminated => "TERMINATED",
        }
    }

    pub fn parse(s: &str) -> Option<AgentState> {
        AgentState::ALL.iter().copied().find(|v| v.as_str() == s)
    }

    /// States from which an agent holds no worker slot.
    pub fn is_sleeping(&self) -> bool {
        matches!(
            self,
            AgentState::WaitingForDependency
                | AgentState::Blocked
                | AgentState::Escalated
                | AgentState::Paused
                | AgentState::Completed
                | AgentState::Terminated
                | AgentState::Created
        )
    }

    /// States in which an agent is eligible to be given work / control-plane mail.
    pub fn is_admittable(&self) -> bool {
        matches!(
            self,
            AgentState::Idle | AgentState::Created | AgentState::Initializing
        )
    }

    /// The closed legal-edge table (`TRANSITIONS` in the reference).
    pub fn transitions(&self) -> &'static [AgentState] {
        use AgentState::*;
        match self {
            Created => &[Initializing, Terminated],
            Initializing => &[Idle, Working, Terminated],
            Idle => &[
                Working,
                WaitingForDependency,
                Blocked,
                Escalated,
                Paused,
                Terminated,
            ],
            Working => &[
                Idle,
                WaitingForDependency,
                Blocked,
                Escalated,
                Paused,
                Completed,
                Terminated,
            ],
            WaitingForDependency => &[Working, Idle, Blocked, Escalated, Terminated],
            Blocked => &[Working, Idle, Escalated, Terminated],
            Escalated => &[Working, Idle, WaitingForDependency, Blocked, Terminated],
            Paused => &[Idle, Working, WaitingForDependency, Terminated],
            // COMPLETED is a drain state: only reaping or explicit re-init.
            Completed => &[Terminated, Initializing],
            Terminated => &[],
        }
    }
}

impl fmt::Display for AgentState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn can_transition(frm: AgentState, to: AgentState) -> bool {
    frm.transitions().contains(&to)
}

#[derive(Debug)]
pub struct IllegalTransition {
    pub frm: AgentState,
    pub to: AgentState,
}

impl fmt::Display for IllegalTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let legal: Vec<&str> = self.frm.transitions().iter().map(|s| s.as_str()).collect();
        write!(
            f,
            "illegal lifecycle transition {} -> {} (legal: {})",
            self.frm,
            self.to,
            if legal.is_empty() {
                "none - terminal".to_string()
            } else {
                legal.join(", ")
            }
        )
    }
}
impl std::error::Error for IllegalTransition {}

/// Result of a requested transition — callers journal both accepted and rejected.
#[derive(Debug, Clone)]
pub struct TransitionRecord {
    pub frm: AgentState,
    pub to: AgentState,
    pub ok: bool,
    pub reason: String,
}

/// State holder with a monotonic per-state clock (needed by idle-TTL reaping).
#[derive(Debug, Clone)]
pub struct Lifecycle {
    pub state: AgentState,
    pub since: f64,
    pub history: Vec<(f64, AgentState, AgentState, String)>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Lifecycle {
            state: AgentState::Created,
            since: 0.0,
            history: Vec::new(),
        }
    }
}

impl Lifecycle {
    pub fn with_state(state: AgentState, since: f64) -> Lifecycle {
        Lifecycle {
            state,
            since,
            history: Vec::new(),
        }
    }

    pub fn request(&mut self, to: AgentState, reason: &str, now: f64) -> TransitionRecord {
        if can_transition(self.state, to) {
            let prev = self.state;
            self.state = to;
            self.since = now;
            self.history.push((now, prev, to, reason.to_string()));
            TransitionRecord {
                frm: prev,
                to,
                ok: true,
                reason: reason.to_string(),
            }
        } else {
            TransitionRecord {
                frm: self.state,
                to,
                ok: false,
                reason: format!("{} -> {} is not a legal edge", self.state, to),
            }
        }
    }

    pub fn time_in_state(&self, now: f64) -> f64 {
        now - self.since
    }
}

pub fn states_reachable_from(states: &[AgentState]) -> Vec<AgentState> {
    let mut out = std::collections::BTreeSet::new();
    for s in states {
        for t in s.transitions() {
            out.insert(*t);
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(s: &str) -> AgentState {
        AgentState::parse(s).unwrap()
    }

    #[test]
    fn table_is_closed_and_complete() {
        // every state has an entry and TERMINATED is terminal
        assert!(AgentState::Terminated.transitions().is_empty());
        // completed is a drain state
        assert!(!can_transition(state("COMPLETED"), state("WORKING")));
        assert!(can_transition(state("COMPLETED"), state("TERMINATED")));
        assert!(can_transition(state("COMPLETED"), state("INITIALIZING")));
    }

    #[test]
    fn rejected_edge_does_not_mutate() {
        let mut lc = Lifecycle::with_state(AgentState::Completed, 1.0);
        let rec = lc.request(AgentState::Working, "chaos", 2.0);
        assert!(!rec.ok);
        assert_eq!(lc.state, AgentState::Completed);
        assert_eq!(lc.since, 1.0);
        assert!(lc.history.is_empty());
    }

    #[test]
    fn parse_roundtrip_all() {
        for s in AgentState::ALL {
            assert_eq!(AgentState::parse(s.as_str()), Some(s));
        }
    }
}
