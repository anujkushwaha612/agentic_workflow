//! Project workspaces: where a run lives.
//!
//! Rules (REAL_AGENT_RUNTIME_DESIGN.md §7 / Python `arena/code/project.py`):
//! * a run only ever writes under `workspace/projects/<id>/`;
//! * a directory that already exists and is *not* a project is never touched;
//! * the journal lives inside `.arena/` so verify/replay are per-project;
//! * `.arena` is a protected jail name — agents cannot edit their own log.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ids::ProjectId;
use crate::journal::Journal;
use crate::msg::EventType;
use crate::sys::json::{parse, JMap, JValue};
use crate::RUNTIME_VERSION;

pub const MANIFEST: &str = "project.json";
pub const DIRS: &[&str] = &[".arena", "source", "artifacts", "logs"];
pub const STATE_ORDER: &[&str] = &[
    "created",
    "planned",
    "running",
    "awaiting-cortex",
    "verified",
    "completed",
    "failed",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectError {
    Exists(String),
    NotFound(String),
    Io(String),
    Invalid(String),
}

impl std::fmt::Display for ProjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectError::Exists(s)
            | ProjectError::NotFound(s)
            | ProjectError::Io(s)
            | ProjectError::Invalid(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for ProjectError {}

/// `20260901-213500-task-tracker` — sortable, readable, collision-checked by the caller.
pub fn project_id(goal: &str, when: Option<f64>) -> ProjectId {
    let ts = when.unwrap_or_else(unix_now);
    let (stamp, _) = format_utc(ts);
    let slug = slugify(goal);
    ProjectId::new(format!(
        "{stamp}-{}",
        if slug.is_empty() { "project" } else { &slug }
    ))
}

fn slugify(goal: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in goal.chars() {
        let l = c.to_ascii_lowercase();
        if l.is_ascii_alphanumeric() {
            out.push(l);
            dash = false;
        } else if !out.is_empty() && !dash {
            out.push('-');
            dash = true;
        }
        if out.len() >= 32 {
            break;
        }
    }
    out.trim_matches('-').to_string()
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// UTC civil time. CHANGE vs Python `time.localtime` (sandbox TZ is not a contract).
fn format_utc(ts: f64) -> (String, String) {
    let secs = ts.floor() as i64;
    let (y, mo, d, h, mi, s) = civil_from_unix(secs);
    (
        format!("{y:04}{mo:02}{d:02}-{h:02}{mi:02}{s:02}"),
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}+0000"),
    )
}

fn civil_from_unix(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let z = secs.div_euclid(86400);
    let tod = secs.rem_euclid(86400) as u32;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    // Howard Hinnant civil_from_days
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day, hour, min, sec)
}

#[derive(Debug, Clone)]
pub struct Project {
    pub root: PathBuf,
    pub manifest: JMap,
}

impl Project {
    pub fn arena_dir(&self) -> PathBuf {
        self.root.join(".arena")
    }
    pub fn source_dir(&self) -> PathBuf {
        self.root.join("source")
    }
    pub fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts")
    }
    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }
    pub fn journal_path(&self) -> PathBuf {
        self.arena_dir().join("j.db")
    }
    pub fn inject_path(&self) -> PathBuf {
        self.arena_dir().join("inject.jsonl")
    }
    pub fn decisions_path(&self) -> PathBuf {
        self.arena_dir().join("decisions.jsonl")
    }
    pub fn context_dir(&self) -> PathBuf {
        self.arena_dir().join("context")
    }
    pub fn is_project(&self) -> bool {
        self.arena_dir().join(MANIFEST).is_file()
    }
    pub fn project_id(&self) -> String {
        self.manifest
            .get("project_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    /// Create `workspace/projects/<id>/`. Never overwrites; never reuses a foreign directory.
    pub fn create(
        workspace: impl AsRef<Path>,
        goal: &str,
        project_id_hint: &str,
        resume: bool,
        force_new: bool,
        meta: Option<&JMap>,
        when: Option<f64>,
    ) -> Result<Project, ProjectError> {
        let ws = workspace
            .as_ref()
            .canonicalize()
            .unwrap_or_else(|_| workspace.as_ref().to_path_buf());
        let pid = if project_id_hint.is_empty() {
            project_id(goal, when).as_str().to_string()
        } else {
            project_id_hint.to_string()
        };
        let root = ws.join("projects").join(&pid);
        if root.join(MANIFEST).is_file() || root.join(".git").exists() {
            return Err(ProjectError::Exists(format!(
                "{} is an existing project or repository. Use resume to continue it, or a different id.",
                root.display()
            )));
        }
        if root.exists() {
            let occupied = std::fs::read_dir(&root)
                .map(|rd| rd.filter_map(|e| e.ok()).next().is_some())
                .unwrap_or(false);
            if occupied && !force_new {
                return Err(ProjectError::Exists(format!(
                    "{} exists and is not a project (no {MANIFEST}). Refusing to write into a directory we did not create.",
                    root.display()
                )));
            }
        }
        if resume && !root.is_dir() {
            return Err(ProjectError::NotFound(format!(
                "resume {pid}: no such project at {}",
                root.display()
            )));
        }
        for d in DIRS {
            std::fs::create_dir_all(root.join(d)).map_err(|e| ProjectError::Io(e.to_string()))?;
        }
        std::fs::create_dir_all(root.join(".arena").join("context"))
            .map_err(|e| ProjectError::Io(e.to_string()))?;
        std::fs::create_dir_all(root.join(".arena").join("tool-log"))
            .map_err(|e| ProjectError::Io(e.to_string()))?;

        let ts = when.unwrap_or_else(unix_now);
        let (_, iso) = format_utc(ts);
        let mut layout = JMap::new();
        layout.insert("journal".into(), JValue::Str(".arena/j.db".into()));
        layout.insert("source".into(), JValue::Str("source/".into()));
        layout.insert("artifacts".into(), JValue::Str("artifacts/".into()));
        layout.insert("logs".into(), JValue::Str("logs/".into()));
        layout.insert("context".into(), JValue::Str(".arena/context/".into()));
        let mut provenance = JMap::new();
        provenance.insert("launched_by".into(), JValue::Str("cli".into()));
        provenance.insert("prompt_source".into(), JValue::Str("argv".into()));
        if let Some(m) = meta {
            if let Some(v) = m.get("launched_by") {
                provenance.insert("launched_by".into(), v.clone());
            }
            if let Some(v) = m.get("prompt_source") {
                provenance.insert("prompt_source".into(), v.clone());
            }
        }
        let mut history = Vec::new();
        let mut h0 = JMap::new();
        h0.insert("state".into(), JValue::Str("created".into()));
        h0.insert("at".into(), JValue::Float(ts));
        history.push(JValue::Obj(h0));

        let mut manifest = JMap::new();
        manifest.insert("project_id".into(), JValue::Str(pid));
        manifest.insert("prompt".into(), JValue::Str(goal.into()));
        manifest.insert("created_at".into(), JValue::Float(ts));
        manifest.insert("created_at_iso".into(), JValue::Str(iso));
        manifest.insert(
            "runtime_version".into(),
            JValue::Str(RUNTIME_VERSION.into()),
        );
        manifest.insert("state".into(), JValue::Str("created".into()));
        manifest.insert("history".into(), JValue::Arr(history));
        manifest.insert("workspace".into(), JValue::Str(ws.to_string_lossy().into()));
        manifest.insert("layout".into(), JValue::Obj(layout));
        manifest.insert("provenance".into(), JValue::Obj(provenance));
        if let Some(m) = meta {
            for (k, v) in m {
                if k == "launched_by" || k == "prompt_source" {
                    continue;
                }
                manifest.insert(k.clone(), v.clone());
            }
        }
        let p = Project { root, manifest };
        p.write_manifest()?;
        p.emit_created()?;
        Ok(p)
    }

    pub fn load(
        workspace: impl AsRef<Path>,
        project_id_hint: &str,
    ) -> Result<Project, ProjectError> {
        let ws = workspace.as_ref();
        let root = ws.join("projects").join(project_id_hint);
        let mut man = root.join(".arena").join(MANIFEST);
        if !man.is_file() {
            man = root.join(MANIFEST);
        }
        if !man.is_file() {
            return Err(ProjectError::NotFound(format!(
                "no project {project_id_hint:?} under {} (looked for .arena/{MANIFEST})",
                ws.join("projects").display()
            )));
        }
        let text = std::fs::read_to_string(&man).map_err(|e| ProjectError::Io(e.to_string()))?;
        let manifest = match parse(&text) {
            Ok(JValue::Obj(m)) => m,
            Ok(_) => return Err(ProjectError::Invalid("manifest is not an object".into())),
            Err(e) => return Err(ProjectError::Invalid(e.to_string())),
        };
        Ok(Project { root, manifest })
    }

    pub fn discover(workspace: impl AsRef<Path>) -> Vec<JMap> {
        let base = workspace.as_ref().join("projects");
        let mut out = Vec::new();
        let rd = match std::fs::read_dir(&base) {
            Ok(r) => r,
            Err(_) => return out,
        };
        let mut dirs: Vec<PathBuf> = rd.filter_map(|e| e.ok().map(|e| e.path())).collect();
        dirs.sort();
        for d in dirs {
            if !d.is_dir() {
                continue;
            }
            let mut man = d.join(".arena").join(MANIFEST);
            if !man.is_file() {
                man = d.join(MANIFEST);
            }
            if !man.is_file() {
                continue;
            }
            let mut rows = match std::fs::read_to_string(&man)
                .ok()
                .and_then(|t| parse(&t).ok())
            {
                Some(JValue::Obj(m)) => m,
                _ => {
                    let mut m = JMap::new();
                    m.insert(
                        "project_id".into(),
                        JValue::Str(
                            d.file_name()
                                .map(|s| s.to_string_lossy().into())
                                .unwrap_or_default(),
                        ),
                    );
                    m.insert("state".into(), JValue::Str("unreadable-manifest".into()));
                    m
                }
            };
            if !rows.contains_key("project_id") {
                rows.insert(
                    "project_id".into(),
                    JValue::Str(
                        d.file_name()
                            .map(|s| s.to_string_lossy().into())
                            .unwrap_or_default(),
                    ),
                );
            }
            rows.insert("_path".into(), JValue::Str(d.to_string_lossy().into()));
            out.push(rows);
        }
        out
    }

    pub fn write_manifest(&self) -> Result<(), ProjectError> {
        let text = JValue::Obj(self.manifest.clone()).to_pretty_string();
        let text = if text.ends_with('\n') {
            text
        } else {
            format!("{text}\n")
        };
        for target in [self.arena_dir().join(MANIFEST), self.root.join(MANIFEST)] {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| ProjectError::Io(e.to_string()))?;
            }
            std::fs::write(&target, &text).map_err(|e| ProjectError::Io(e.to_string()))?;
        }
        Ok(())
    }

    pub fn set_state(&mut self, state: &str, extra: JMap) -> Result<(), ProjectError> {
        self.manifest
            .insert("state".into(), JValue::Str(state.into()));
        let mut row = extra;
        row.insert("state".into(), JValue::Str(state.into()));
        row.entry("at".to_string())
            .or_insert_with(|| JValue::Float(unix_now()));
        match self.manifest.get_mut("history") {
            Some(JValue::Arr(v)) => v.push(JValue::Obj(row)),
            _ => {
                self.manifest
                    .insert("history".into(), JValue::Arr(vec![JValue::Obj(row)]));
            }
        }
        self.write_manifest()
    }

    pub fn update(&mut self, extra: JMap) -> Result<(), ProjectError> {
        for (k, v) in extra {
            self.manifest.insert(k, v);
        }
        self.write_manifest()
    }

    fn emit_created(&self) -> Result<(), ProjectError> {
        let mut j = Journal::open_file(self.journal_path(), Some(unix_now()))
            .map_err(|e| ProjectError::Io(e.to_string()))?;
        let pid = self.project_id();
        let prompt = self
            .manifest
            .get("prompt")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let body = format!(
            "project {pid} created for: {}",
            prompt.chars().take(120).collect::<String>()
        );
        let mut fields = JMap::new();
        fields.insert("project_id".into(), JValue::Str(pid));
        fields.insert("prompt".into(), JValue::Str(prompt.into()));
        fields.insert(
            "runtime_version".into(),
            JValue::Str(RUNTIME_VERSION.into()),
        );
        if let Some(v) = self.manifest.get("layout") {
            fields.insert("layout".into(), v.clone());
        }
        if let Some(v) = self.manifest.get("provenance") {
            fields.insert("provenance".into(), v.clone());
        }
        j.emit(
            EventType::ProjectCreated,
            "launcher",
            "kernel",
            &body,
            fields,
            None,
            None,
            None,
            None,
            0,
        );
        j.close();
        Ok(())
    }

    pub fn status_row(&self) -> JMap {
        let src = self.source_dir();
        let mut n = 0i64;
        let mut bytes = 0i64;
        if src.is_dir() {
            fn walk(dir: &Path, n: &mut i64, bytes: &mut i64) {
                let Ok(rd) = std::fs::read_dir(dir) else {
                    return;
                };
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        if p.file_name().and_then(|s| s.to_str()) == Some(".git") {
                            continue;
                        }
                        walk(&p, n, bytes);
                    } else if p.is_file() {
                        *n += 1;
                        *bytes += std::fs::metadata(&p).map(|m| m.len() as i64).unwrap_or(0);
                    }
                }
            }
            walk(&src, &mut n, &mut bytes);
        }
        let mut m = JMap::new();
        m.insert("project_id".into(), JValue::Str(self.project_id()));
        m.insert(
            "state".into(),
            self.manifest
                .get("state")
                .cloned()
                .unwrap_or(JValue::Str("".into())),
        );
        let prompt = self
            .manifest
            .get("prompt")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        m.insert(
            "prompt".into(),
            JValue::Str(prompt.chars().take(80).collect()),
        );
        m.insert("source_files".into(), JValue::Int(n));
        m.insert("source_bytes".into(), JValue::Int(bytes));
        m.insert(
            "has_journal".into(),
            JValue::Bool(self.journal_path().exists()),
        );
        m.insert(
            "path".into(),
            JValue::Str(self.root.to_string_lossy().into()),
        );
        m
    }
}
