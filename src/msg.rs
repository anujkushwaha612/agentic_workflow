//! Message protocol: types, planes, causality, budgets (`arena/message.py`).
//!
//! Three planes with distinct delivery rules (control = Parent<->Agent,
//! dependency = Agent<->Agent + subscriptions, resource = never a free-for-all
//! broadcast), every message carries correlation/causality, and the bus enforces
//! MAX_CAUSAL_DEPTH so infinite ping-pong is killed structurally.

use crate::ids::{ActorName, CorrelationId, TaskId};
use crate::sys::glob::fnmatch_case;
use crate::sys::json::{JMap, JValue};
use crate::sys::rand;

pub const MAX_CAUSAL_DEPTH: i64 = 3;
pub const MSG_BUDGET_PER_TICK: i64 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Plane {
    Control,
    Dependency,
    Resource,
}

impl Plane {
    pub fn as_str(&self) -> &'static str {
        match self {
            Plane::Control => "control",
            Plane::Dependency => "dependency",
            Plane::Resource => "resource",
        }
    }
}

/// All 61 journal event types, values identical to the Python `MessageType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EventType {
    // control plane
    TaskAssigned,
    TaskStarted,
    TaskProgress,
    TaskCompleted,
    TaskFailed,
    StatusUpdate,
    AgentRegistered,
    AgentTerminated,
    AgentPaused,
    AgentResumed,
    StateTransition,
    PlanCreated,
    PlanAmended,
    Replan,
    // dependency plane
    DependencyRequest,
    DependencyReady,
    DependencyBlocked,
    WaitRegistered,
    WaitResolved,
    WaitTimeout,
    ApiContractReady,
    FeatureRequest,
    RequestAck,
    RequestDeclined,
    SpawnAgentRequest,
    SpawnApproved,
    SpawnRejected,
    // phase 2 request lifecycle
    SpawnRequestReceived,
    SpawnRequestResolved,
    RequestRerouted,
    SpawnEscalated,
    DeferredForCapacity,
    GraphAmended,
    GraphAmendRejected,
    // phase 2.5 M1/M2 real execution
    WorkspaceBound,
    ProjectCreated,
    ToolCall,
    ToolResult,
    ToolRefused,
    CognitionViolation,
    CompletionRefused,
    TaskVerified,
    CognitionBound,
    CognitionError,
    HelpRequest,
    // conflict / runtime bookkeeping (control plane)
    Blocked,
    ErrorReport,
    DuplicateClaim,
    IllegalTransition,
    DeadlockDetected,
    CycleRejected,
    BudgetExceeded,
    // resource plane
    ResourceUpdated,
    FileLocked,
    FileReleased,
    ArtifactPublished,
    // journal / runtime bookkeeping
    RunTick,
    Stats,
    Snapshot,
    CrashSimulated,
    ReplayComplete,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        use EventType::*;
        match self {
            TaskAssigned => "TASK_ASSIGNED",
            TaskStarted => "TASK_STARTED",
            TaskProgress => "TASK_PROGRESS",
            TaskCompleted => "TASK_COMPLETED",
            TaskFailed => "TASK_FAILED",
            StatusUpdate => "STATUS_UPDATE",
            AgentRegistered => "AGENT_REGISTERED",
            AgentTerminated => "AGENT_TERMINATED",
            AgentPaused => "AGENT_PAUSED",
            AgentResumed => "AGENT_RESUMED",
            StateTransition => "STATE_TRANSITION",
            PlanCreated => "PLAN_CREATED",
            PlanAmended => "PLAN_AMENDED",
            Replan => "REPLAN",
            DependencyRequest => "DEPENDENCY_REQUEST",
            DependencyReady => "DEPENDENCY_READY",
            DependencyBlocked => "DEPENDENCY_BLOCKED",
            WaitRegistered => "WAIT_REGISTERED",
            WaitResolved => "WAIT_RESOLVED",
            WaitTimeout => "WAIT_TIMEOUT",
            ApiContractReady => "API_CONTRACT_READY",
            FeatureRequest => "FEATURE_REQUEST",
            RequestAck => "REQUEST_ACK",
            RequestDeclined => "REQUEST_DECLINED",
            SpawnAgentRequest => "SPAWN_AGENT_REQUEST",
            SpawnApproved => "SPAWN_APPROVED",
            SpawnRejected => "SPAWN_REJECTED",
            SpawnRequestReceived => "SPAWN_REQUEST_RECEIVED",
            SpawnRequestResolved => "SPAWN_REQUEST_RESOLVED",
            RequestRerouted => "REQUEST_REROUTED",
            SpawnEscalated => "SPAWN_ESCALATED",
            DeferredForCapacity => "DEFERRED_FOR_CAPACITY",
            GraphAmended => "GRAPH_AMENDED",
            GraphAmendRejected => "GRAPH_AMEND_REJECTED",
            WorkspaceBound => "WORKSPACE_BOUND",
            ProjectCreated => "PROJECT_CREATED",
            ToolCall => "TOOL_CALL",
            ToolResult => "TOOL_RESULT",
            ToolRefused => "TOOL_REFUSED",
            CognitionViolation => "COGNITION_VIOLATION",
            CompletionRefused => "COMPLETION_REFUSED",
            TaskVerified => "TASK_VERIFIED",
            CognitionBound => "COGNITION_BOUND",
            CognitionError => "COGNITION_ERROR",
            HelpRequest => "HELP_REQUEST",
            Blocked => "BLOCKED",
            ErrorReport => "ERROR_REPORT",
            DuplicateClaim => "DUPLICATE_CLAIM",
            IllegalTransition => "ILLEGAL_TRANSITION",
            DeadlockDetected => "DEADLOCK_DETECTED",
            CycleRejected => "CYCLE_REJECTED",
            BudgetExceeded => "BUDGET_EXCEEDED",
            ResourceUpdated => "RESOURCE_UPDATED",
            FileLocked => "FILE_LOCKED",
            FileReleased => "FILE_RELEASED",
            ArtifactPublished => "ARTIFACT_PUBLISHED",
            RunTick => "RUN_TICK",
            Stats => "STATS",
            Snapshot => "SNAPSHOT",
            CrashSimulated => "CRASH_SIMULATED",
            ReplayComplete => "REPLAY_COMPLETE",
        }
    }

    pub fn parse(s: &str) -> Option<EventType> {
        use EventType::*;
        Some(match s {
            "TASK_ASSIGNED" => TaskAssigned,
            "TASK_STARTED" => TaskStarted,
            "TASK_PROGRESS" => TaskProgress,
            "TASK_COMPLETED" => TaskCompleted,
            "TASK_FAILED" => TaskFailed,
            "STATUS_UPDATE" => StatusUpdate,
            "AGENT_REGISTERED" => AgentRegistered,
            "AGENT_TERMINATED" => AgentTerminated,
            "AGENT_PAUSED" => AgentPaused,
            "AGENT_RESUMED" => AgentResumed,
            "STATE_TRANSITION" => StateTransition,
            "PLAN_CREATED" => PlanCreated,
            "PLAN_AMENDED" => PlanAmended,
            "REPLAN" => Replan,
            "DEPENDENCY_REQUEST" => DependencyRequest,
            "DEPENDENCY_READY" => DependencyReady,
            "DEPENDENCY_BLOCKED" => DependencyBlocked,
            "WAIT_REGISTERED" => WaitRegistered,
            "WAIT_RESOLVED" => WaitResolved,
            "WAIT_TIMEOUT" => WaitTimeout,
            "API_CONTRACT_READY" => ApiContractReady,
            "FEATURE_REQUEST" => FeatureRequest,
            "REQUEST_ACK" => RequestAck,
            "REQUEST_DECLINED" => RequestDeclined,
            "SPAWN_AGENT_REQUEST" => SpawnAgentRequest,
            "SPAWN_APPROVED" => SpawnApproved,
            "SPAWN_REJECTED" => SpawnRejected,
            "SPAWN_REQUEST_RECEIVED" => SpawnRequestReceived,
            "SPAWN_REQUEST_RESOLVED" => SpawnRequestResolved,
            "REQUEST_REROUTED" => RequestRerouted,
            "SPAWN_ESCALATED" => SpawnEscalated,
            "DEFERRED_FOR_CAPACITY" => DeferredForCapacity,
            "GRAPH_AMENDED" => GraphAmended,
            "GRAPH_AMEND_REJECTED" => GraphAmendRejected,
            "WORKSPACE_BOUND" => WorkspaceBound,
            "PROJECT_CREATED" => ProjectCreated,
            "TOOL_CALL" => ToolCall,
            "TOOL_RESULT" => ToolResult,
            "TOOL_REFUSED" => ToolRefused,
            "COGNITION_VIOLATION" => CognitionViolation,
            "COMPLETION_REFUSED" => CompletionRefused,
            "TASK_VERIFIED" => TaskVerified,
            "COGNITION_BOUND" => CognitionBound,
            "COGNITION_ERROR" => CognitionError,
            "HELP_REQUEST" => HelpRequest,
            "BLOCKED" => Blocked,
            "ERROR_REPORT" => ErrorReport,
            "DUPLICATE_CLAIM" => DuplicateClaim,
            "ILLEGAL_TRANSITION" => IllegalTransition,
            "DEADLOCK_DETECTED" => DeadlockDetected,
            "CYCLE_REJECTED" => CycleRejected,
            "BUDGET_EXCEEDED" => BudgetExceeded,
            "RESOURCE_UPDATED" => ResourceUpdated,
            "FILE_LOCKED" => FileLocked,
            "FILE_RELEASED" => FileReleased,
            "ARTIFACT_PUBLISHED" => ArtifactPublished,
            "RUN_TICK" => RunTick,
            "STATS" => Stats,
            "SNAPSHOT" => Snapshot,
            "CRASH_SIMULATED" => CrashSimulated,
            "REPLAY_COMPLETE" => ReplayComplete,
            _ => return None,
        })
    }

    pub fn plane(&self) -> Plane {
        use EventType::*;
        match self {
            ResourceUpdated | FileLocked | FileReleased | ArtifactPublished => Plane::Resource,
            DependencyRequest | DependencyReady | DependencyBlocked | WaitRegistered
            | WaitResolved | WaitTimeout | ApiContractReady | FeatureRequest | RequestAck
            | RequestDeclined | SpawnAgentRequest => Plane::Dependency,
            _ => Plane::Control,
        }
    }

    /// Second topic segment (the subscription category).
    pub fn category(&self) -> &'static str {
        use EventType::*;
        match self {
            TaskAssigned | TaskStarted | TaskProgress | TaskCompleted | TaskFailed => "task",
            AgentRegistered | AgentTerminated | AgentPaused | AgentResumed | StateTransition
            | StatusUpdate | IllegalTransition | ErrorReport => "agent",
            PlanCreated | PlanAmended | Replan | CycleRejected | GraphAmended
            | GraphAmendRejected => "plan",
            SpawnAgentRequest | SpawnApproved | SpawnRejected | SpawnRequestReceived
            | SpawnRequestResolved | SpawnEscalated | DeferredForCapacity => "spawn",
            DependencyRequest | DependencyBlocked | HelpRequest => "dependency",
            WaitRegistered | WaitResolved | WaitTimeout => "wait",
            FeatureRequest | RequestAck | RequestDeclined | RequestRerouted => "request",
            DuplicateClaim | DeadlockDetected | BudgetExceeded => "conflict",
            ApiContractReady => "contract",
            ResourceUpdated | FileLocked | FileReleased => "resource",
            ArtifactPublished => "artifact",
            ToolCall | ToolResult | ToolRefused | CognitionViolation | WorkspaceBound
            | ProjectCreated | CognitionBound | CognitionError => "exec",
            CompletionRefused | TaskVerified => "verify",
            RunTick | CrashSimulated | ReplayComplete | Stats | Snapshot => "runtime",
            DependencyReady | Blocked => "dependency",
        }
    }

    /// Topic = `plane.category.type`, the category prefix stripped once from the
    /// type segment (`topic_for` in the reference).
    pub fn topic(&self) -> String {
        let plane = self.plane().as_str();
        let cat = self.category();
        let name = self.as_str().to_lowercase();
        let name = if name.len() > cat.len() + 1 && name.starts_with(&format!("{cat}_")) {
            name[cat.len() + 1..].to_string()
        } else {
            name
        };
        format!("{plane}.{cat}.{name}")
    }

    /// Leaf types that must never trigger further messages.
    pub fn is_terminal(&self) -> bool {
        use EventType::*;
        matches!(
            self,
            StatusUpdate
                | TaskProgress
                | AgentRegistered
                | AgentTerminated
                | StateTransition
                | RunTick
                | CrashSimulated
                | ReplayComplete
                | DuplicateClaim
                | IllegalTransition
                | BudgetExceeded
                | ToolCall
                | ToolResult
                | ToolRefused
                | CognitionViolation
                | CompletionRefused
                | TaskVerified
                | WorkspaceBound
        )
    }
}

/// A bus message. Field-for-field the Python `Message` dataclass; the payload is
/// an open JSON object (the reference's `**fields`), so it stays a `JMap`.
#[derive(Debug, Clone)]
pub struct Message {
    pub msg_type: EventType,
    pub from_actor: ActorName,
    pub to_actor: ActorName,
    pub body: String,
    pub payload: JMap,
    pub topic: String,
    pub resource: Option<String>,
    pub task_id: Option<TaskId>,
    pub plane: Plane,
    pub correlation_id: CorrelationId,
    pub caused_by: Option<String>,
    pub causal_depth: i64,
    pub seq: i64,
    pub ts: f64,
    pub mid: String,
}

impl Message {
    pub fn new(
        msg_type: EventType,
        from_actor: impl Into<ActorName>,
        to_actor: impl Into<ActorName>,
    ) -> Message {
        let plane = msg_type.plane();
        let topic = msg_type.topic();
        let mid = rand::new_mid();
        let correlation_id = CorrelationId::new(rand::correlation_from_mid(&mid));
        Message {
            msg_type,
            from_actor: from_actor.into(),
            to_actor: to_actor.into(),
            body: String::new(),
            payload: JMap::new(),
            topic,
            resource: None,
            task_id: None,
            plane,
            correlation_id,
            caused_by: None,
            causal_depth: 0,
            seq: -1,
            ts: 0.0,
            mid,
        }
    }

    pub fn matches(&self, pattern: &str) -> bool {
        fnmatch_case(&self.topic, pattern) || self.topic == pattern
    }

    /// Derive a causally-linked message — the ONLY sanctioned reply path.
    #[must_use]
    pub fn child(
        &self,
        msg_type: EventType,
        from_actor: impl Into<ActorName>,
        to_actor: impl Into<ActorName>,
    ) -> Message {
        let mut m = Message::new(msg_type, from_actor, to_actor);
        m.correlation_id = self.correlation_id.clone();
        m.caused_by = Some(self.mid.clone());
        m.causal_depth = self.causal_depth + 1;
        m.task_id = self.task_id.clone();
        m
    }

    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("seq".into(), JValue::Int(self.seq));
        m.insert(
            "ts".into(),
            JValue::Float(crate::sys::json::py_round(self.ts, 4)),
        );
        m.insert("type".into(), JValue::Str(self.msg_type.as_str().into()));
        m.insert("plane".into(), JValue::Str(self.plane.as_str().into()));
        m.insert("topic".into(), JValue::Str(self.topic.clone()));
        m.insert("from".into(), JValue::Str(self.from_actor.as_str().into()));
        m.insert("to".into(), JValue::Str(self.to_actor.as_str().into()));
        m.insert(
            "correlation_id".into(),
            JValue::Str(self.correlation_id.as_str().into()),
        );
        m.insert(
            "caused_by".into(),
            self.caused_by
                .as_deref()
                .map(|s| JValue::Str(s.to_string()))
                .unwrap_or(JValue::Null),
        );
        m.insert("causal_depth".into(), JValue::Int(self.causal_depth));
        m.insert(
            "task_id".into(),
            self.task_id
                .as_ref()
                .map(|t| JValue::Str(t.as_str().to_string()))
                .unwrap_or(JValue::Null),
        );
        m.insert(
            "resource".into(),
            self.resource
                .as_deref()
                .map(|s| JValue::Str(s.to_string()))
                .unwrap_or(JValue::Null),
        );
        m.insert("body".into(), JValue::Str(self.body.clone()));
        m.insert("payload".into(), JValue::Obj(self.payload.clone()));
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_for_matches_python() {
        // cross-checked against arena.message.topic_for
        assert_eq!(EventType::TaskAssigned.topic(), "control.task.assigned");
        assert_eq!(
            EventType::DependencyReady.topic(),
            "dependency.dependency.ready"
        );
        assert_eq!(
            EventType::WaitRegistered.topic(),
            "dependency.wait.registered"
        );
        assert_eq!(
            EventType::StateTransition.topic(),
            "control.agent.state_transition"
        );
        assert_eq!(
            EventType::ResourceUpdated.topic(),
            "resource.resource.updated"
        );
        assert_eq!(EventType::ToolResult.topic(), "control.exec.tool_result");
        assert_eq!(
            EventType::SpawnRequestReceived.topic(),
            "control.spawn.request_received"
        );
        assert_eq!(EventType::Snapshot.topic(), "control.runtime.snapshot");
    }

    #[test]
    fn event_type_roundtrip_all_61() {
        let all: Vec<&'static str> = vec![
            "TASK_ASSIGNED",
            "TASK_STARTED",
            "TASK_PROGRESS",
            "TASK_COMPLETED",
            "TASK_FAILED",
            "STATUS_UPDATE",
            "AGENT_REGISTERED",
            "AGENT_TERMINATED",
            "AGENT_PAUSED",
            "AGENT_RESUMED",
            "STATE_TRANSITION",
            "PLAN_CREATED",
            "PLAN_AMENDED",
            "REPLAN",
            "DEPENDENCY_REQUEST",
            "DEPENDENCY_READY",
            "DEPENDENCY_BLOCKED",
            "WAIT_REGISTERED",
            "WAIT_RESOLVED",
            "WAIT_TIMEOUT",
            "API_CONTRACT_READY",
            "FEATURE_REQUEST",
            "REQUEST_ACK",
            "REQUEST_DECLINED",
            "SPAWN_AGENT_REQUEST",
            "SPAWN_APPROVED",
            "SPAWN_REJECTED",
            "SPAWN_REQUEST_RECEIVED",
            "SPAWN_REQUEST_RESOLVED",
            "REQUEST_REROUTED",
            "SPAWN_ESCALATED",
            "DEFERRED_FOR_CAPACITY",
            "GRAPH_AMENDED",
            "GRAPH_AMEND_REJECTED",
            "WORKSPACE_BOUND",
            "PROJECT_CREATED",
            "TOOL_CALL",
            "TOOL_RESULT",
            "TOOL_REFUSED",
            "COGNITION_VIOLATION",
            "COMPLETION_REFUSED",
            "TASK_VERIFIED",
            "COGNITION_BOUND",
            "COGNITION_ERROR",
            "HELP_REQUEST",
            "BLOCKED",
            "ERROR_REPORT",
            "DUPLICATE_CLAIM",
            "ILLEGAL_TRANSITION",
            "DEADLOCK_DETECTED",
            "CYCLE_REJECTED",
            "BUDGET_EXCEEDED",
            "RESOURCE_UPDATED",
            "FILE_LOCKED",
            "FILE_RELEASED",
            "ARTIFACT_PUBLISHED",
            "RUN_TICK",
            "STATS",
            "SNAPSHOT",
            "CRASH_SIMULATED",
            "REPLAY_COMPLETE",
        ];
        assert_eq!(all.len(), 61);
        for s in all {
            let e = EventType::parse(s).unwrap_or_else(|| panic!("{s}"));
            assert_eq!(e.as_str(), s);
        }
    }

    #[test]
    fn child_inherits_causality() {
        let a = Message::new(EventType::TaskAssigned, "parent", "backend_01");
        let b = a.child(EventType::StatusUpdate, "backend_01", "parent");
        assert_eq!(b.correlation_id, a.correlation_id);
        assert_eq!(b.caused_by.as_deref(), Some(a.mid.as_str()));
        assert_eq!(b.causal_depth, 1);
        assert_eq!(b.task_id, a.task_id);
    }
}
