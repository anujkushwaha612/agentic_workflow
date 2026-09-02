//! Tool execution seam (M10). Kernel never implements tools; it holds an
//! optional [`ToolExecutor`]. Unbound → honest `REFUSE_NO_EXECUTOR`.

use crate::cognition::ToolResultView;
use crate::sys::json::{JMap, JValue};

/// Deferred M10 tool executor. The runtime validates Intents; this trait
/// is the only way a tool actually runs.
pub trait ToolExecutor {
    fn describe(&self, agent_id: &str) -> JMap;
    fn allowed(&self, agent_id: &str) -> Vec<String>;
    fn schemas(&self, agent_id: &str) -> Vec<JMap>;
    fn stats(&self) -> JMap;
    fn plan(
        &mut self,
        agent_id: &str,
        calls: &[JMap],
        task_id: Option<&str>,
        corr: &str,
    ) -> Vec<String>;
    fn execute(
        &mut self,
        agent_id: &str,
        tool: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
        corr: &str,
    ) -> ToolResultView;
}

/// View returned when no executor is bound. Not a fake success.
pub fn refuse_unbound(tool: &str) -> ToolResultView {
    ToolResultView {
        tool: tool.into(),
        ok: false,
        refused: "REFUSE_NO_EXECUTOR".into(),
        ..Default::default()
    }
}

/// Empty stats object for an unbound executor.
pub fn empty_stats() -> JMap {
    JMap::new()
}

/// Placeholder request ids when planning without an executor.
pub fn placeholder_rids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("r-{i:04}")).collect()
}

/// argv payload helper for verify commands.
pub fn argv_args(argv: &[String]) -> JMap {
    let mut args = JMap::new();
    args.insert(
        "argv".into(),
        JValue::Arr(argv.iter().cloned().map(JValue::Str).collect()),
    );
    args
}
