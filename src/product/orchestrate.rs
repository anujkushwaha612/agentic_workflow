//! Join the launcher's world (a prompt, a project directory) to the engine.
//!
//! No scheduling, lifecycle, or execution of its own — it configures Kernel,
//! binds the jail to `source/`, then gets out of the way.
//!
//! CHANGE vs Python: CodingAgent is deferred. Agents get the kernel's default
//! PolicyCognition. Seed files, if present in the plan, are written by the
//! *runtime* (not by cognition claiming it did) so verify has bytes to check.

use std::path::{Path, PathBuf};

use crate::graph::TaskSpec;
use crate::journal::EventFilter;
use crate::kernel::{Kernel, KernelOpts};
use crate::registry::SpawnBudget;
use crate::sys::json::{parse, JMap, JValue};
use crate::tools::{file_digest, GitBind};

use super::project::{Project, ProjectError};

#[derive(Debug, Clone, Default)]
pub struct PlanTask {
    pub task_id: String,
    pub title: String,
    pub role: String,
    pub skills: Vec<String>,
    pub produces: Vec<String>,
    pub consumes: Vec<String>,
    pub verify: Vec<Vec<String>>,
    pub est_work: f64,
}

#[derive(Debug, Clone, Default)]
pub struct PlanAgent {
    pub agent_id: String,
    pub role: String,
    pub skills: Vec<String>,
    pub writes: Vec<String>,
    pub reads: Vec<String>,
    pub allowed_tools: Option<Vec<String>>,
    pub task: PlanTask,
    /// Optional seed files (relpath → contents). Written by the runtime.
    pub seed: JMap,
}

#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub goal: String,
    pub agents: Vec<PlanAgent>,
    pub commit_after_verify: bool,
}

impl Plan {
    pub fn from_map(raw: &JMap) -> Result<Plan, ProjectError> {
        let goal = raw
            .get("goal")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let agents_v = match raw.get("agents") {
            Some(JValue::Arr(a)) => a,
            _ => {
                return Err(ProjectError::Invalid(
                    "plan must have an agents array".into(),
                ))
            }
        };
        let mut agents = Vec::new();
        for a in agents_v {
            let Some(obj) = (match a {
                JValue::Obj(m) => Some(m),
                _ => None,
            }) else {
                return Err(ProjectError::Invalid("agent must be an object".into()));
            };
            let agent_id = obj
                .get("agent_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let role = obj
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let task = match obj.get("task") {
                Some(JValue::Obj(t)) => t,
                _ => {
                    return Err(ProjectError::Invalid(format!(
                        "plan agent {agent_id:?} has no task object"
                    )))
                }
            };
            let task_id = task
                .get("task_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if task_id.is_empty() {
                return Err(ProjectError::Invalid(format!(
                    "plan agent {agent_id:?} has no task.task_id"
                )));
            }
            // Seed is optional (CHANGE: Python required it because CodingAgent
            // was the only writer). Empty seed is allowed; verify then checks
            // produces exist, which will fail honestly if nothing wrote them.
            let seed = match obj.get("seed") {
                Some(JValue::Obj(m)) => m.clone(),
                _ => JMap::new(),
            };
            agents.push(PlanAgent {
                agent_id,
                role: role.clone(),
                skills: str_list(obj.get("skills")),
                writes: str_list(obj.get("writes")),
                reads: str_list(obj.get("reads")),
                allowed_tools: match obj.get("allowed_tools") {
                    Some(JValue::Arr(a)) => Some(
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect(),
                    ),
                    _ => None,
                },
                task: PlanTask {
                    task_id,
                    title: task
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    role: task
                        .get("role")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&role)
                        .to_string(),
                    skills: str_list(task.get("skills")),
                    produces: str_list(task.get("produces")),
                    consumes: str_list(task.get("consumes")),
                    verify: argv_lists(task.get("verify")),
                    est_work: task.get("est_work").and_then(|v| v.as_f64()).unwrap_or(2.0),
                },
                seed,
            });
        }
        Ok(Plan {
            goal,
            agents,
            commit_after_verify: raw
                .get("commit_after_verify")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        })
    }
}

fn str_list(v: Option<&JValue>) -> Vec<String> {
    match v {
        Some(JValue::Arr(a)) => a
            .iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect(),
        _ => vec![],
    }
}

fn argv_lists(v: Option<&JValue>) -> Vec<Vec<String>> {
    match v {
        Some(JValue::Arr(rows)) => rows
            .iter()
            .filter_map(|row| match row {
                JValue::Arr(a) => Some(
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect(),
                ),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

pub fn load_plan(path: Option<&Path>) -> Result<Plan, ProjectError> {
    let Some(p) = path else {
        return Err(ProjectError::Invalid(
            "no packaged plan in this milestone; pass a plan file".into(),
        ));
    };
    let text = std::fs::read_to_string(p).map_err(|e| ProjectError::Io(e.to_string()))?;
    match parse(&text) {
        Ok(JValue::Obj(m)) => Plan::from_map(&m),
        Ok(_) => Err(ProjectError::Invalid("plan is not an object".into())),
        Err(e) => Err(ProjectError::Invalid(e.to_string())),
    }
}

#[derive(Debug, Clone, Default)]
pub struct OrganiseOpts {
    pub workspace: PathBuf,
    pub goal: String,
    pub plan: Plan,
    pub ticks: i64,
    pub project_id_hint: String,
    pub resume: bool,
    pub clock_mode: String,
    pub when: Option<f64>,
}

pub struct RunResult {
    pub project: Project,
    pub summary: JMap,
    pub verification: JMap,
    pub ok: bool,
    pub notes: Vec<String>,
}

impl RunResult {
    pub fn to_dict(&self) -> JMap {
        let mut m = JMap::new();
        m.insert("ok".into(), JValue::Bool(self.ok));
        m.insert(
            "project".into(),
            JValue::Str(self.project.root.to_string_lossy().into()),
        );
        m.insert("project_id".into(), JValue::Str(self.project.project_id()));
        m.insert(
            "state".into(),
            self.project
                .manifest
                .get("state")
                .cloned()
                .unwrap_or(JValue::Null),
        );
        m.insert("summary".into(), JValue::Obj(self.summary.clone()));
        m.insert(
            "verification".into(),
            JValue::Obj(self.verification.clone()),
        );
        m.insert(
            "notes".into(),
            JValue::Arr(self.notes.iter().cloned().map(JValue::Str).collect()),
        );
        m
    }
}

/// Create-or-resume the project, bind tools to `source/`, staff the graph, run, verify.
pub fn organise(opts: OrganiseOpts) -> Result<(RunResult, Kernel), ProjectError> {
    let mut plan = opts.plan;
    if plan.goal.is_empty() {
        plan.goal = opts.goal.clone();
    }
    let goal = if opts.goal.is_empty() {
        plan.goal.clone()
    } else {
        opts.goal.clone()
    };
    let mut project = if opts.resume && !opts.project_id_hint.is_empty() {
        let mut p = Project::load(&opts.workspace, &opts.project_id_hint)?;
        p.set_state("resuming", JMap::new())?;
        p
    } else {
        Project::create(
            &opts.workspace,
            &goal,
            &opts.project_id_hint,
            false,
            false,
            None,
            opts.when,
        )?
    };

    let n_agents = plan.agents.len() as i64;
    let budget = SpawnBudget {
        max_active_agents: 8.max(2 * n_agents + 4),
        max_concurrent_workers: 3,
        idle_ttl: 1.0e9,
        ..SpawnBudget::default()
    };

    let kopts = KernelOpts {
        journal_path: Some(project.journal_path()),
        root: Some(project.arena_dir()),
        budget,
        clock_mode: if opts.clock_mode.is_empty() {
            "virtual".into()
        } else {
            opts.clock_mode.clone()
        },
        detect_deadlocks: false,
        auto_assign: true,
        reap: false,
        transcript_dir: Some(project.context_dir().to_string_lossy().into()),
        stall_limit: 8.max(opts.ticks / 2),
        ..KernelOpts::default()
    };

    let mut k = Kernel::new(kopts).map_err(|e| ProjectError::Io(e.to_string()))?;

    let mut extra = JMap::new();
    extra.insert(
        "kernel_root".into(),
        JValue::Str(project.arena_dir().to_string_lossy().into()),
    );
    extra.insert(
        "journal".into(),
        JValue::Str(project.journal_path().to_string_lossy().into()),
    );
    project.update(extra)?;

    let mut specs: Vec<TaskSpec> = Vec::new();
    for a in &plan.agents {
        let mut t = TaskSpec::new(
            a.task.task_id.as_str(),
            &a.task.title,
            if a.task.role.is_empty() {
                &a.role
            } else {
                &a.task.role
            },
        );
        t.skills = if a.task.skills.is_empty() {
            a.skills.clone()
        } else {
            a.task.skills.clone()
        };
        t.produces = a.task.produces.clone();
        t.consumes = a.task.consumes.clone();
        t.verify = a.task.verify.clone();
        t.est_work = a.task.est_work;
        specs.push(t);
    }
    for t in specs {
        k.add_task(t).map_err(ProjectError::Invalid)?;
    }

    for a in &plan.agents {
        let skills: Vec<&str> = if a.skills.is_empty() {
            vec![a.role.as_str()]
        } else {
            a.skills.iter().map(|s| s.as_str()).collect()
        };
        k.register_agent(&a.agent_id, &a.role, &skills, 0, "parent")
            .map_err(|e| ProjectError::Invalid(e.to_string()))?;
        k.make_actor(&a.agent_id);
        let src = project.source_dir();
        let writes: Vec<&str> = if a.writes.is_empty() {
            vec![""]
        } else {
            a.writes.iter().map(|s| s.as_str()).collect()
        };
        let reads: Vec<&str> = if a.reads.is_empty() {
            vec![""]
        } else {
            a.reads.iter().map(|s| s.as_str()).collect()
        };
        let allowed: Option<Vec<&str>> = a
            .allowed_tools
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        k.bind_tools(
            &a.agent_id,
            &src,
            &writes,
            &reads,
            allowed.as_deref(),
            GitBind::Jail,
        );
        k.set_tool_logs_dir(project.logs_dir());
        let _ = k.ensure_git(&a.agent_id);
        seed_through_tools(&mut k, &a.agent_id, &a.seed)?;
        if !k.assign(&a.task.task_id, &a.agent_id) {
            return Err(ProjectError::Invalid(format!(
                "failed to assign {} to {}",
                a.task.task_id, a.agent_id
            )));
        }
    }

    project.set_state("running", JMap::new())?;
    let ticks = if opts.ticks <= 0 { 1 } else { opts.ticks };
    let summary = k.run(ticks);
    let verification = verify_all(&mut k, &plan, &project);
    let verified = verification
        .get("ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut committed = JMap::new();
    committed.insert(
        "skipped".into(),
        JValue::Str("not verified or plan opted out".into()),
    );
    if verified && plan.commit_after_verify {
        committed = commit_all(&mut k, &plan);
    }
    let all_closed = summary
        .get("done")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let state = if verified && all_closed {
        "completed"
    } else if verified {
        "verified"
    } else {
        "awaiting-cortex"
    };
    let mut extra = JMap::new();
    extra.insert("verified".into(), JValue::Bool(verified));
    extra.insert("commit".into(), JValue::Obj(committed));
    project.set_state(state, extra)?;

    let ok = verified && state == "completed";
    let res = RunResult {
        project,
        summary,
        verification,
        ok,
        notes: vec![],
    };
    Ok((res, k))
}

fn seed_through_tools(k: &mut Kernel, agent_id: &str, seed: &JMap) -> Result<(), ProjectError> {
    for (rel, contents) in seed {
        let Some(text) = contents.as_str() else {
            continue;
        };
        let mut args = JMap::new();
        args.insert("path".into(), JValue::Str(rel.clone()));
        args.insert("content".into(), JValue::Str(text.into()));
        let mut call = JMap::new();
        call.insert("tool".into(), JValue::Str("write_file".into()));
        call.insert("args".into(), JValue::Obj(args.clone()));
        let rids = k.plan_tools(agent_id, &[call], None, "seed");
        let rid = rids.first().cloned().unwrap_or_default();
        let res = k.execute_tool(agent_id, "write_file", &args, &rid, None, "seed");
        if !res.ok {
            return Err(ProjectError::Invalid(format!(
                "seed write {rel} refused: {}",
                res.refused
            )));
        }
    }
    Ok(())
}

fn verify_all(k: &mut Kernel, plan: &Plan, project: &Project) -> JMap {
    let mut results = Vec::new();
    let mut ok = true;
    for a in &plan.agents {
        let tid = a.task.task_id.as_str();
        let verdict = k.run_verify(&a.agent_id, Some(tid));
        ok = ok && verdict.ok;
        let mut files = JMap::new();
        for rel in &a.task.produces {
            let p = project.source_dir().join(rel);
            let exists = p.is_file();
            ok = ok && exists;
            let mut row = JMap::new();
            row.insert("exists".into(), JValue::Bool(exists));
            row.insert(
                "bytes".into(),
                JValue::Int(if exists {
                    std::fs::metadata(&p).map(|m| m.len() as i64).unwrap_or(0)
                } else {
                    0
                }),
            );
            row.insert(
                "sha256_16".into(),
                JValue::Str(if exists {
                    file_digest(&p)
                } else {
                    String::new()
                }),
            );
            files.insert(rel.clone(), JValue::Obj(row));
        }
        let mut one = JMap::new();
        one.insert("task_id".into(), JValue::Str(tid.into()));
        one.insert("ok".into(), JValue::Bool(verdict.ok));
        one.insert(
            "commands".into(),
            JValue::Arr(verdict.results.into_iter().map(JValue::Obj).collect()),
        );
        one.insert("files".into(), JValue::Obj(files));
        results.push(JValue::Obj(one));
    }
    if plan.agents.is_empty() {
        ok = false;
    }
    let mut out = JMap::new();
    out.insert("ok".into(), JValue::Bool(ok));
    out.insert("tasks".into(), JValue::Arr(results));
    out
}

fn commit_all(k: &mut Kernel, plan: &Plan) -> JMap {
    let Some(aid) = plan.agents.first().map(|a| a.agent_id.clone()) else {
        let mut m = JMap::new();
        m.insert("ok".into(), JValue::Bool(false));
        return m;
    };
    let mut args = JMap::new();
    args.insert(
        "message".into(),
        JValue::Str("arena-code: verified project (runtime commit)".into()),
    );
    let res = k.execute_tool(&aid, "commit", &args, "runtime", None, "");
    let mut m = JMap::new();
    m.insert("ok".into(), JValue::Bool(res.ok));
    m.insert(
        "exit".into(),
        res.exit_code.map(JValue::Int).unwrap_or(JValue::Null),
    );
    m
}

/// Evidence rows a report cares about.
pub fn execution_chain(k: &Kernel) -> Vec<JMap> {
    let want = [
        "TOOL_CALL",
        "TOOL_RESULT",
        "TOOL_REFUSED",
        "COMPLETION_REFUSED",
        "TASK_VERIFIED",
        "COGNITION_VIOLATION",
        "PROJECT_CREATED",
        "WORKSPACE_BOUND",
        "COGNITION_BOUND",
        "TASK_COMPLETED",
        "SPAWN_REQUEST_RECEIVED",
        "SPAWN_APPROVED",
        "AGENT_REGISTERED",
        "ERROR_REPORT",
        "COGNITION_ERROR",
    ];
    k.journal()
        .events(&EventFilter::new())
        .unwrap_or_default()
        .into_iter()
        .filter(|r| want.contains(&r.etype.as_str()))
        .map(|r| {
            let body = r.body();
            let mut m = JMap::new();
            m.insert("seq".into(), JValue::Int(r.seq));
            m.insert("etype".into(), JValue::Str(r.etype));
            m.insert("actor".into(), JValue::Str(r.actor));
            m.insert("body".into(), JValue::Str(body));
            m
        })
        .collect()
}
