//! Product / launcher layer (`arena/code/`).
//!
//! Engine modules must not import this crate path. Product *does* import
//! Kernel, Journal, Tools: it configures them, then gets out of the way.
//! CodingAgent, doctor, and the `arena-code` CLI remain deferred.

mod orchestrate;
mod project;

pub use orchestrate::{
    execution_chain, load_plan, organise, OrganiseOpts, Plan, PlanAgent, PlanTask, RunResult,
};
pub use project::{project_id, Project, ProjectError, DIRS, MANIFEST, STATE_ORDER};
