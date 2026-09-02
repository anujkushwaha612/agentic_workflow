//! Strong identity types (Step 2 of the migration plan). Every id that reaches
//! the journal, the CLI or a payload is a newtype; agent/task ids additionally
//! sort deterministically (BTree ordering = Python's sorted() on strings).

use std::fmt;

macro_rules! newtype {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                $name(s.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                $name(s.to_string())
            }
        }
        impl From<String> for $name {
            fn from(s: String) -> Self {
                $name(s)
            }
        }
        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }
        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }
        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                &self.0 == other
            }
        }
    };
}

newtype!(AgentId, "Identity of a logical agent (e.g. `backend_01`).");
newtype!(
    TaskId,
    "Identity of a task in the dependency graph (e.g. `t_api_contract`)."
);
newtype!(
    ProjectId,
    "Identity of an arena-code project (`20260901-223204-slug`)."
);
newtype!(
    CorrelationId,
    "Causal correlation id (`c-<hex12>`), ties a request chain together."
);
newtype!(Rid, "Spawn-request ledger id (`rq-0001`).");
newtype!(
    ArtifactName,
    "Artifact name; doubles as the workspace-relative path contract."
);
newtype!(ClaimKey, "Claim-table key: what work, on what resources.");
newtype!(WaitId, "Durable-wait row id (`w-0001`).");
newtype!(
    AgentRole,
    "Free-text agent role (`backend`, `payment specialist`)."
);
newtype!(
    CapabilityClass,
    "Capability catalog class (`payments`, `gpu-training`, ...)."
);
newtype!(
    ActorName,
    "Mailbox owner: an agent id, `parent`, `kernel` or `dependency_manager`."
);

impl ActorName {
    pub fn is_orchestration(&self) -> bool {
        matches!(self.0.as_str(), "parent" | "kernel" | "dependency_manager")
    }
}

impl From<AgentId> for ActorName {
    fn from(a: AgentId) -> ActorName {
        ActorName(a.0)
    }
}
impl From<&AgentId> for ActorName {
    fn from(a: &AgentId) -> ActorName {
        ActorName(a.0.clone())
    }
}
impl From<AgentId> for crate::sys::json::JValue {
    fn from(a: AgentId) -> Self {
        crate::sys::json::JValue::Str(a.0)
    }
}
