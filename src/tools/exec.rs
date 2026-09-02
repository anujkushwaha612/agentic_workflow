//! The real executor: jail + FS + process + git. No Kernel pointer.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::cognition::ToolResultView;
use crate::sys::json::{JMap, JValue};

use super::jail::{Jail, PROTECTED_NAMES};
use super::proc::{run_argv, shlex_split, which};
use super::redact::{file_digest, redact};
use super::{ToolExecutor, ToolSpec, EGRESS_HOSTS, NEVER_RUN, TOOLS};

const MAX_OUT: usize = 24_000;
const DEFAULT_TIMEOUT: f64 = 30.0;

#[derive(Debug, Clone, Default)]
pub struct Grant {
    pub allowed: Vec<String>,
}

pub struct Executor {
    pub jails: BTreeMap<String, Jail>,
    pub grants: BTreeMap<String, Grant>,
    pub git_roots: BTreeMap<String, PathBuf>,
    pub logs_dir: Option<PathBuf>,
    pub allow_egress: bool,
    pub stats: BTreeMap<String, i64>,
    seq: u64,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    pub fn new() -> Executor {
        let mut stats = BTreeMap::new();
        for k in ["planned", "executions", "refusals", "binds"] {
            stats.insert(k.into(), 0);
        }
        Executor {
            jails: BTreeMap::new(),
            grants: BTreeMap::new(),
            git_roots: BTreeMap::new(),
            logs_dir: None,
            allow_egress: false,
            stats,
            seq: 0,
        }
    }

    fn bump(&mut self, key: &str, n: i64) {
        *self.stats.entry(key.into()).or_insert(0) += n;
    }

    fn next_rid(&mut self) -> String {
        self.seq += 1;
        format!("r-{:04}", self.seq)
    }

    fn jail(&self, agent: &str) -> Result<&Jail, String> {
        self.jails
            .get(agent)
            .ok_or_else(|| "REFUSE_NO_WORKSPACE".to_string())
    }

    fn allowed_for(&self, agent: &str) -> Vec<String> {
        self.grants
            .get(agent)
            .map(|g| g.allowed.clone())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| TOOLS.iter().map(|t| t.name.to_string()).collect())
    }

    #[allow(clippy::too_many_arguments)]
    fn result(
        &self,
        tool: &str,
        rid: &str,
        ok: bool,
        exit: Option<i64>,
        stdout: &str,
        stderr: &str,
        data: JMap,
        refused: &str,
        changed: &[String],
    ) -> ToolResultView {
        let stdout = clip(&redact(stdout, &[]), MAX_OUT);
        let stderr = clip(&redact(stderr, &[]), MAX_OUT);
        let mut data = data;
        if !data.contains_key("changed") {
            data.insert(
                "changed".into(),
                JValue::Arr(changed.iter().cloned().map(JValue::Str).collect()),
            );
        }
        data.insert("stdout".into(), JValue::Str(stdout.clone()));
        data.insert("stderr".into(), JValue::Str(stderr.clone()));
        ToolResultView {
            tool: tool.into(),
            ok,
            exit_code: exit,
            block: as_block(tool, ok, exit, &stdout, &stderr, refused),
            data,
            refused: refused.into(),
            rid: rid.into(),
        }
    }

    fn refuse(&mut self, tool: &str, rid: &str, code: &str, detail: &str) -> ToolResultView {
        self.bump("refusals", 1);
        let mut data = JMap::new();
        data.insert("detail".into(), JValue::Str(detail.into()));
        data.insert("code".into(), JValue::Str(code.into()));
        self.result(tool, rid, false, None, "", detail, data, code, &[])
    }

    fn dispatch(
        &mut self,
        agent: &str,
        tool: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
    ) -> ToolResultView {
        match tool {
            "list_files" => self.t_list_files(agent, args, rid),
            "read_file" => self.t_read_file(agent, args, rid),
            "write_file" => self.t_write_file(agent, args, rid),
            "edit_file" => self.t_edit_file(agent, args, rid),
            "run_command" => self.t_run_command(agent, args, rid),
            "run_tests" => self.t_run_tests(agent, args, rid),
            "inspect_git" => self.t_inspect_git(agent, rid),
            "commit" => self.t_commit(agent, args, rid),
            "publish_artifact" => self.t_publish_artifact(agent, args, rid, task_id),
            "send_message" | "wait_for_event" | "request_specialist" | "ensure_git" => self.refuse(
                tool,
                rid,
                "REFUSE_NOT_IMPLEMENTED",
                "handled by the kernel, not the executor",
            ),
            _ => self.refuse(tool, rid, "REFUSE_UNKNOWN_TOOL", tool),
        }
    }

    fn t_list_files(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        let jail = match self.jail(agent) {
            Ok(j) => j.clone(),
            Err(c) => return self.refuse("list_files", rid, &c, &c),
        };
        let rel = jstr(args, "path", "");
        let path = match jail.resolve(&rel, false) {
            Ok(p) => p,
            Err(c) => return self.refuse("list_files", rid, &c, &c),
        };
        let glob = jstr(args, "glob", "");
        let mut files = Vec::new();
        walk_files(&path, &jail.root, &glob, &mut files);
        files.sort();
        let listing = files.join("\n");
        let mut data = JMap::new();
        data.insert(
            "files".into(),
            JValue::Arr(files.iter().cloned().map(JValue::Str).collect()),
        );
        data.insert("n".into(), JValue::Int(files.len() as i64));
        self.bump("executions", 1);
        self.result(
            "list_files",
            rid,
            true,
            Some(0),
            &listing,
            "",
            data,
            "",
            &[],
        )
    }

    fn t_read_file(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        let jail = match self.jail(agent) {
            Ok(j) => j.clone(),
            Err(c) => return self.refuse("read_file", rid, &c, &c),
        };
        let rel = jstr(args, "path", "");
        let path = match jail.resolve(&rel, false) {
            Ok(p) => p,
            Err(c) => return self.refuse("read_file", rid, &c, &c),
        };
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let offset = args.get("offset").and_then(|v| v.as_int()).unwrap_or(1);
                let limit = args.get("limit").and_then(|v| v.as_int()).unwrap_or(0);
                let lines: Vec<&str> = text.split_inclusive('\n').collect();
                let start = (offset.max(1) as usize).saturating_sub(1);
                let slice = if limit > 0 {
                    let end = (start + limit as usize).min(lines.len());
                    &lines[start.min(lines.len())..end]
                } else {
                    &lines[start.min(lines.len())..]
                };
                let body = slice.join("");
                let mut data = JMap::new();
                data.insert("path".into(), JValue::Str(rel));
                data.insert("digest".into(), JValue::Str(file_digest(&path)));
                data.insert("bytes".into(), JValue::Int(text.len() as i64));
                self.bump("executions", 1);
                self.result("read_file", rid, true, Some(0), &body, "", data, "", &[])
            }
            Err(e) => self.refuse("read_file", rid, "REFUSE_IO", &e.to_string()),
        }
    }

    fn t_write_file(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        let jail = match self.jail(agent) {
            Ok(j) => j.clone(),
            Err(c) => return self.refuse("write_file", rid, &c, &c),
        };
        let rel = jstr(args, "path", "");
        let path = match jail.resolve(&rel, true) {
            Ok(p) => p,
            Err(c) => return self.refuse("write_file", rid, &c, &c),
        };
        let mut content = jstr(args, "content", "");
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return self.refuse("write_file", rid, "REFUSE_IO", &e.to_string());
            }
        }
        let tmp = path.with_file_name(format!(
            ".{}.arena-tmp",
            path.file_name().and_then(|s| s.to_str()).unwrap_or("f")
        ));
        match std::fs::File::create(&tmp).and_then(|mut f| {
            f.write_all(content.as_bytes())?;
            f.sync_all()
        }) {
            Ok(()) => {}
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return self.refuse("write_file", rid, "REFUSE_IO", &e.to_string());
            }
        }
        if let Err(e) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return self.refuse("write_file", rid, "REFUSE_IO", &e.to_string());
        }
        let mut data = JMap::new();
        data.insert("path".into(), JValue::Str(rel.clone()));
        data.insert("digest".into(), JValue::Str(file_digest(&path)));
        data.insert("bytes".into(), JValue::Int(content.len() as i64));
        self.bump("executions", 1);
        self.result(
            "write_file",
            rid,
            true,
            Some(0),
            &format!("wrote {rel}"),
            "",
            data,
            "",
            &[rel],
        )
    }

    fn t_edit_file(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        let jail = match self.jail(agent) {
            Ok(j) => j.clone(),
            Err(c) => return self.refuse("edit_file", rid, &c, &c),
        };
        let rel = jstr(args, "path", "");
        let path = match jail.resolve(&rel, true) {
            Ok(p) => p,
            Err(c) => return self.refuse("edit_file", rid, &c, &c),
        };
        let old = jstr(args, "old", "");
        let new = jstr(args, "new", "");
        if old.is_empty() {
            return self.refuse(
                "edit_file",
                rid,
                "REFUSE_EDIT_EMPTY_OLD",
                "old must be non-empty",
            );
        }
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) => return self.refuse("edit_file", rid, "REFUSE_IO", &e.to_string()),
        };
        let n = text.matches(&old).count();
        if n == 0 {
            return self.refuse(
                "edit_file",
                rid,
                "REFUSE_EDIT_TARGET_MISSING",
                "old string not found",
            );
        }
        let replaced = text.replace(&old, &new);
        let tmp = path.with_file_name(format!(
            ".{}.arena-tmp",
            path.file_name().and_then(|s| s.to_str()).unwrap_or("f")
        ));
        if let Err(e) = std::fs::write(&tmp, replaced.as_bytes()) {
            return self.refuse("edit_file", rid, "REFUSE_IO", &e.to_string());
        }
        if let Err(e) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return self.refuse("edit_file", rid, "REFUSE_IO", &e.to_string());
        }
        let mut data = JMap::new();
        data.insert("path".into(), JValue::Str(rel.clone()));
        data.insert("replacements".into(), JValue::Int(n as i64));
        data.insert("digest".into(), JValue::Str(file_digest(&path)));
        self.bump("executions", 1);
        self.result(
            "edit_file",
            rid,
            true,
            Some(0),
            &format!("replaced {n} occurrence(s) in {rel}"),
            "",
            data,
            "",
            &[rel],
        )
    }

    fn t_run_command(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        self.run_argv_tool("run_command", agent, args, rid)
    }

    fn t_run_tests(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        self.run_argv_tool("run_tests", agent, args, rid)
    }

    fn run_argv_tool(&mut self, tool: &str, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        let jail = match self.jail(agent) {
            Ok(j) => j.clone(),
            Err(c) => return self.refuse(tool, rid, &c, &c),
        };
        let argv = match collect_argv(args) {
            Ok(v) => v,
            Err(c) => return self.refuse(tool, rid, &c, &c),
        };
        if argv.is_empty() {
            return self.refuse(tool, rid, "REFUSE_EMPTY_ARGV", "argv is empty");
        }
        let prog = argv[0].as_str();
        let base = Path::new(prog)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(prog);
        if NEVER_RUN.contains(&base) {
            return self.refuse(
                tool,
                rid,
                "REFUSE_NEVER_RUN",
                &format!("{base} is on the never-run list"),
            );
        }
        if !self.allow_egress {
            for a in &argv {
                let low = a.to_ascii_lowercase();
                for host in EGRESS_HOSTS {
                    if low.contains(host) {
                        return self.refuse(
                            tool,
                            rid,
                            "REFUSE_EGRESS",
                            &format!("argument names egress host {host}"),
                        );
                    }
                }
            }
        }
        let cwd_rel = jstr(args, "cwd", "");
        let cwd = if cwd_rel.is_empty() {
            jail.root.clone()
        } else {
            match jail.resolve(&cwd_rel, false) {
                Ok(p) => p,
                Err(c) => return self.refuse(tool, rid, &c, &c),
            }
        };
        if !cwd.is_dir() {
            return self.refuse(tool, rid, "REFUSE_CWD", "cwd is not a directory");
        }
        let timeout = args
            .get("timeout")
            .and_then(|v| v.as_f64())
            .unwrap_or(DEFAULT_TIMEOUT);
        if which(&argv[0]).is_none() {
            self.bump("executions", 1);
            return self.result(
                tool,
                rid,
                false,
                Some(127),
                "",
                &format!("{}: not found on PATH", argv[0]),
                JMap::new(),
                "",
                &[],
            );
        }
        let out = run_argv(&argv, &cwd, &[], timeout);
        let ok = out.exit == 0;
        let mut data = JMap::new();
        data.insert(
            "argv".into(),
            JValue::Arr(argv.iter().cloned().map(JValue::Str).collect()),
        );
        data.insert("cwd".into(), JValue::Str(cwd.to_string_lossy().into()));
        self.bump("executions", 1);
        self.result(
            tool,
            rid,
            ok,
            Some(out.exit as i64),
            &out.stdout,
            &out.stderr,
            data,
            "",
            &[],
        )
    }

    fn t_inspect_git(&mut self, agent: &str, rid: &str) -> ToolResultView {
        let Some(root) = self.git_roots.get(agent).cloned() else {
            return self.refuse("inspect_git", rid, "REFUSE_NO_GIT", "no git root bound");
        };
        if !root.join(".git").exists() {
            return self.refuse("inspect_git", rid, "REFUSE_NO_GIT", "not a git repository");
        }
        let status = git(&root, &["status", "--porcelain"]);
        let log = git(&root, &["log", "-5", "--oneline"]);
        let mut data = JMap::new();
        data.insert(
            "dirty".into(),
            JValue::Bool(!status.stdout.trim().is_empty()),
        );
        data.insert("status".into(), JValue::Str(status.stdout.clone()));
        self.bump("executions", 1);
        let body = format!("status:\n{}\nlog:\n{}", status.stdout, log.stdout);
        self.result(
            "inspect_git",
            rid,
            status.exit == 0,
            Some(status.exit as i64),
            &body,
            &status.stderr,
            data,
            "",
            &[],
        )
    }

    fn t_commit(&mut self, agent: &str, args: &JMap, rid: &str) -> ToolResultView {
        let Some(root) = self.git_roots.get(agent).cloned() else {
            return self.refuse("commit", rid, "REFUSE_NO_GIT", "no git root bound");
        };
        if !root.join(".git").exists() {
            return self.refuse("commit", rid, "REFUSE_NO_GIT", "not a git repository");
        }
        let inspect = git(&root, &["status", "--porcelain"]);
        if inspect.stdout.trim().is_empty() {
            let mut data = JMap::new();
            data.insert("noop".into(), JValue::Bool(true));
            self.bump("executions", 1);
            return self.result(
                "commit",
                rid,
                false,
                Some(0),
                "working tree clean; nothing to commit",
                "",
                data,
                "",
                &[],
            );
        }
        let msg = jstr(args, "message", "agent commit");
        let add = git(&root, &["add", "-A"]);
        if add.exit != 0 {
            return self.result(
                "commit",
                rid,
                false,
                Some(add.exit as i64),
                &add.stdout,
                &add.stderr,
                JMap::new(),
                "",
                &[],
            );
        }
        let commit = git(&root, &["commit", "-m", &msg, "--allow-empty-message"]);
        self.bump("executions", 1);
        let ok = commit.exit == 0;
        self.result(
            "commit",
            rid,
            ok,
            Some(commit.exit as i64),
            &commit.stdout,
            &commit.stderr,
            JMap::new(),
            "",
            &[],
        )
    }

    fn t_publish_artifact(
        &mut self,
        agent: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
    ) -> ToolResultView {
        let name = jstr(args, "name", "");
        if name.is_empty() {
            return self.refuse(
                "publish_artifact",
                rid,
                "REFUSE_NO_ARTIFACT",
                "name is required",
            );
        }
        let jail = match self.jail(agent) {
            Ok(j) => j.clone(),
            Err(c) => return self.refuse("publish_artifact", rid, &c, &c),
        };
        let files = match args.get("files") {
            Some(JValue::Arr(a)) => a
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>(),
            _ => vec![],
        };
        let mut meta = Vec::new();
        for f in &files {
            let path = match jail.resolve(f, false) {
                Ok(p) => p,
                Err(c) => return self.refuse("publish_artifact", rid, &c, &c),
            };
            if !path.is_file() {
                return self.refuse(
                    "publish_artifact",
                    rid,
                    "REFUSE_MISSING_FILE",
                    &format!("{f} is not a file"),
                );
            }
            let mut row = JMap::new();
            row.insert("path".into(), JValue::Str(f.clone()));
            row.insert("digest".into(), JValue::Str(file_digest(&path)));
            meta.push(row);
        }
        let mut data = JMap::new();
        data.insert("artifact".into(), JValue::Str(name.clone()));
        data.insert(
            "files".into(),
            JValue::Arr(meta.iter().cloned().map(JValue::Obj).collect()),
        );
        data.insert(
            "task_id".into(),
            task_id
                .map(|s| JValue::Str(s.into()))
                .unwrap_or(JValue::Null),
        );
        data.insert("_effect".into(), JValue::Str("publish_artifact".into()));
        self.bump("executions", 1);
        self.result(
            "publish_artifact",
            rid,
            true,
            Some(0),
            &format!("publish {name}"),
            "",
            data,
            "",
            &[],
        )
    }
}

impl ToolExecutor for Executor {
    fn bind_agent(
        &mut self,
        agent_id: &str,
        jail: Jail,
        allowed: Option<Vec<String>>,
        git_root: Option<PathBuf>,
    ) {
        self.jails.insert(agent_id.into(), jail);
        self.grants.insert(
            agent_id.into(),
            Grant {
                allowed: allowed.unwrap_or_default(),
            },
        );
        match git_root {
            Some(p) => {
                self.git_roots.insert(agent_id.into(), p);
            }
            None => {
                self.git_roots.remove(agent_id);
            }
        }
        self.bump("binds", 1);
    }

    fn plan(
        &mut self,
        _agent_id: &str,
        calls: &[JMap],
        _task_id: Option<&str>,
        _correlation_id: &str,
    ) -> Vec<String> {
        self.bump("planned", calls.len() as i64);
        (0..calls.len()).map(|_| self.next_rid()).collect()
    }

    fn execute(
        &mut self,
        agent_id: &str,
        tool: &str,
        args: &JMap,
        rid: &str,
        task_id: Option<&str>,
        _correlation_id: &str,
    ) -> ToolResultView {
        if !self.jails.contains_key(agent_id) {
            return self.refuse(tool, rid, "REFUSE_NO_WORKSPACE", "agent has no jail");
        }
        if !TOOLS.iter().any(|t| t.name == tool) {
            return self.refuse(tool, rid, "REFUSE_UNKNOWN_TOOL", tool);
        }
        let grant = self
            .grants
            .get(agent_id)
            .map(|g| g.allowed.clone())
            .unwrap_or_default();
        if !grant.is_empty() && !grant.iter().any(|t| t == tool) {
            return self.refuse(tool, rid, "REFUSE_TOOL_NOT_GRANTED", tool);
        }
        self.dispatch(agent_id, tool, args, rid, task_id)
    }

    fn describe(&self, agent_id: &str) -> JMap {
        match self.jails.get(agent_id) {
            Some(j) => {
                let mut m = j.describe();
                if let Some(g) = self.git_roots.get(agent_id) {
                    m.insert("git_root".into(), JValue::Str(g.to_string_lossy().into()));
                }
                m
            }
            None => {
                let mut m = JMap::new();
                m.insert("bound".into(), JValue::Bool(false));
                m
            }
        }
    }

    fn allowed(&self, agent_id: &str) -> Vec<String> {
        self.allowed_for(agent_id)
    }

    fn schemas(&self, agent_id: &str) -> Vec<JMap> {
        let allow = self.allowed_for(agent_id);
        TOOLS
            .iter()
            .filter(|t| allow.iter().any(|n| n == t.name))
            .map(ToolSpec::schema)
            .collect()
    }

    fn stats(&self) -> JMap {
        self.stats
            .iter()
            .map(|(k, v)| (k.clone(), JValue::Int(*v)))
            .collect()
    }

    fn git_root(&self, agent_id: &str) -> Option<PathBuf> {
        self.git_roots.get(agent_id).cloned()
    }

    fn set_logs_dir(&mut self, dir: Option<PathBuf>) {
        self.logs_dir = dir;
    }

    fn bump_executions(&mut self) {
        self.bump("executions", 1);
    }
}

fn as_block(
    tool: &str,
    ok: bool,
    exit: Option<i64>,
    stdout: &str,
    stderr: &str,
    refused: &str,
) -> String {
    if !refused.is_empty() {
        return format!("{tool} refused: {refused}");
    }
    let mut s = format!(
        "{tool} ok={} exit={}",
        if ok { "true" } else { "false" },
        exit.map(|e| e.to_string()).unwrap_or_else(|| "None".into())
    );
    if !stdout.is_empty() {
        s.push('\n');
        s.push_str(stdout);
    }
    if !stderr.is_empty() {
        s.push_str("\nstderr:\n");
        s.push_str(stderr);
    }
    s
}

fn clip(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[{} bytes truncated]", &s[..end], s.len() - end)
}

fn jstr(args: &JMap, k: &str, d: &str) -> String {
    args.get(k)
        .and_then(|v| v.as_str())
        .unwrap_or(d)
        .to_string()
}

fn collect_argv(args: &JMap) -> Result<Vec<String>, String> {
    if let Some(JValue::Arr(a)) = args.get("argv") {
        let v: Vec<String> = a
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect();
        return Ok(v);
    }
    if let Some(JValue::Str(cmd)) = args.get("cmd") {
        // string form is accepted only if it has no shell metacharacters
        const META: &[char] = &['|', '&', ';', '>', '<', '`', '$', '\n', '(', ')'];
        if cmd.chars().any(|c| META.contains(&c)) {
            return Err("REFUSE_SHELL_META".into());
        }
        return shlex_split(cmd).map_err(|_| "REFUSE_SHELL_META".into());
    }
    Err("REFUSE_EMPTY_ARGV".into())
}

fn git(cwd: &Path, args: &[&str]) -> super::proc::ProcOut {
    let mut argv = vec!["git".to_string()];
    argv.extend(args.iter().map(|s| (*s).to_string()));
    let env = [
        ("GIT_AUTHOR_NAME".into(), "arena".into()),
        ("GIT_AUTHOR_EMAIL".into(), "arena@local".into()),
        ("GIT_COMMITTER_NAME".into(), "arena".into()),
        ("GIT_COMMITTER_EMAIL".into(), "arena@local".into()),
        ("GIT_TERMINAL_PROMPT".into(), "0".into()),
    ];
    run_argv(&argv, cwd, &env, 20.0)
}

fn walk_files(path: &Path, root: &Path, glob: &str, out: &mut Vec<String>) {
    if path.is_file() {
        if let Ok(rel) = path.strip_prefix(root) {
            let s = rel.to_string_lossy().replace('\\', "/");
            if glob_ok(&s, glob) {
                out.push(s);
            }
        }
        return;
    }
    let rd = match std::fs::read_dir(path) {
        Ok(r) => r,
        Err(_) => return,
    };
    for ent in rd.flatten() {
        let name = ent.file_name();
        let name_s = name.to_string_lossy();
        if PROTECTED_NAMES.contains(&name_s.as_ref()) {
            continue;
        }
        if name_s.starts_with('.') && name_s != "." && name_s != ".." {
            // list_files skips hidden except we still skip .git/.arena above
            continue;
        }
        let p = ent.path();
        if p.is_dir() {
            walk_files(&p, root, glob, out);
        } else if p.is_file() {
            if let Ok(rel) = p.strip_prefix(root) {
                let s = rel.to_string_lossy().replace('\\', "/");
                if glob_ok(&s, glob) {
                    out.push(s);
                }
            }
        }
    }
}

fn glob_ok(path: &str, glob: &str) -> bool {
    if glob.is_empty() || glob == "*" {
        return true;
    }
    // suffix match for "*.rs" and exact / simple contains
    if let Some(suf) = glob.strip_prefix('*') {
        return path.ends_with(suf);
    }
    path.contains(glob)
}

impl ToolSpec {
    pub fn schema(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("name".into(), JValue::Str(self.name.into()));
        m.insert("description".into(), JValue::Str(self.description.into()));
        m.insert(
            "args".into(),
            JValue::Arr(self.args.iter().map(|s| JValue::Str((*s).into())).collect()),
        );
        m
    }
}
