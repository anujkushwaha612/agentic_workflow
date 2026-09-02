//! Tools: jail, redaction, the 13-tool table, and the executor.
//!
//! The executor is a Kernel collaborator. It does not hold a Kernel pointer
//! (the same ownership rule as Parent / AgentActor). Journal rows for
//! TOOL_CALL / TOOL_RESULT / TOOL_REFUSED are emitted by Kernel around
//! [`ToolExecutor::plan`] / [`ToolExecutor::execute`]. `publish_artifact`
//! returns an effect in `data["_effect"]`; Kernel applies it.

mod exec;
mod jail;
mod proc;
mod redact;

pub use exec::Executor;
pub use jail::Jail;
pub use proc::{clamp_timeout, which, TIMEOUT_MAX, TIMEOUT_MIN};
pub use redact::{digest, file_digest, redact};

use std::path::PathBuf;

use crate::cognition::ToolResultView;
use crate::sys::json::{JMap, JValue};

pub const REFUSE_NO_EXECUTOR: &str = "REFUSE_NO_EXECUTOR";

pub const NEVER_RUN: &[&str] = &[
    "sudo", "su", "passwd", "shutdown", "reboot", "halt", "poweroff", "mkfs", "dd", "kill",
    "killall", "pkill",
];

pub const EGRESS_HOSTS: &[&str] = &[
    "github.com",
    "api.github.com",
    "gitlab.com",
    "bitbucket.org",
    "pypi.org",
    "npmjs.com",
    "crates.io",
    "raw.githubusercontent.com",
];

#[derive(Clone, Copy)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub args: &'static [&'static str],
}

pub const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "list_files",
        description: "List files under a jailed path",
        args: &["path", "glob"],
    },
    ToolSpec {
        name: "read_file",
        description: "Read a jailed file",
        args: &["path", "offset", "limit"],
    },
    ToolSpec {
        name: "write_file",
        description: "Write a jailed file (atomic)",
        args: &["path", "content"],
    },
    ToolSpec {
        name: "edit_file",
        description: "Replace text in a jailed file",
        args: &["path", "old", "new"],
    },
    ToolSpec {
        name: "run_command",
        description: "Run argv in the jail (no shell)",
        args: &["argv", "cwd", "timeout"],
    },
    ToolSpec {
        name: "run_tests",
        description: "Run a test argv in the jail",
        args: &["argv", "cwd", "timeout"],
    },
    ToolSpec {
        name: "inspect_git",
        description: "Read git status/log of the bound repo",
        args: &[],
    },
    ToolSpec {
        name: "commit",
        description: "Stage and commit if the tree is dirty",
        args: &["message"],
    },
    ToolSpec {
        name: "publish_artifact",
        description: "Ask the kernel to publish a named artifact",
        args: &["name", "files"],
    },
    ToolSpec {
        name: "send_message",
        description: "Reserved; kernel-side",
        args: &["to", "body"],
    },
    ToolSpec {
        name: "wait_for_event",
        description: "Reserved; kernel-side",
        args: &["condition"],
    },
    ToolSpec {
        name: "request_specialist",
        description: "Reserved; kernel-side",
        args: &["role", "reason"],
    },
    ToolSpec {
        name: "ensure_git",
        description: "Kernel-side git init (journalled)",
        args: &["argv"],
    },
];

/// How `Kernel::bind_tools` treats the agent's git root.
#[derive(Clone, Debug)]
pub enum GitBind {
    /// Deliberately no repo — `commit` refuses rather than shelling out.
    None,
    /// Same as the jail root (Python `git_root=""`).
    Jail,
    Path(PathBuf),
}

/// The only way a tool actually runs. Kernel holds one.
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

    fn bind_agent(
        &mut self,
        _agent_id: &str,
        _jail: Jail,
        _allowed: Option<Vec<String>>,
        _git_root: Option<PathBuf>,
    ) {
    }
    fn git_root(&self, _agent_id: &str) -> Option<PathBuf> {
        None
    }
    fn set_logs_dir(&mut self, _dir: Option<PathBuf>) {}
    fn bump_executions(&mut self) {}
}

/// View returned when no executor is bound. Not a fake success.
pub fn refuse_unbound(tool: &str) -> ToolResultView {
    ToolResultView {
        tool: tool.into(),
        ok: false,
        refused: REFUSE_NO_EXECUTOR.into(),
        block: "this kernel has no tool executor bound; Kernel.bind_tools() is required before tools run"
            .into(),
        ..Default::default()
    }
}

pub fn empty_stats() -> JMap {
    JMap::new()
}

pub fn placeholder_rids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("r-{i:04}")).collect()
}

/// `git init` in `root` (used by Kernel::ensure_git). Never a shell.
pub fn git_init(root: &std::path::Path) -> (i32, String, String) {
    let out = proc::run_argv(&["git".into(), "init".into()], root, &[], 20.0);
    (out.exit, out.stdout, out.stderr)
}

pub fn argv_args(argv: &[String]) -> JMap {
    let mut args = JMap::new();
    args.insert(
        "argv".into(),
        JValue::Arr(argv.iter().cloned().map(JValue::Str).collect()),
    );
    args
}
