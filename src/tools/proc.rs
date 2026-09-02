//! Subprocess runner for `run_command` / `run_tests`.
//!
//! argv-only. Timeout clamped. PATH miss → 127. Process group kill on timeout.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

pub const TIMEOUT_MIN: f64 = 0.05;
pub const TIMEOUT_MAX: f64 = 600.0;

pub fn clamp_timeout(t: f64) -> f64 {
    if !t.is_finite() {
        return 30.0;
    }
    t.clamp(TIMEOUT_MIN, TIMEOUT_MAX)
}

/// Locate `prog` on PATH. Absolute / relative-with-slash paths are used as-is.
pub fn which(prog: &str) -> Option<PathBuf> {
    if prog.contains('/') {
        let p = PathBuf::from(prog);
        return if p.exists() { Some(p) } else { None };
    }
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let cand = dir.join(prog);
        if cand.is_file() {
            return Some(cand);
        }
    }
    None
}

pub struct ProcOut {
    pub exit: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Run argv[0] with argv[1..] in `cwd`. Never a shell.
pub fn run_argv(
    argv: &[String],
    cwd: &Path,
    extra_env: &[(String, String)],
    timeout: f64,
) -> ProcOut {
    if argv.is_empty() {
        return ProcOut {
            exit: 1,
            stdout: String::new(),
            stderr: "empty argv".into(),
        };
    }
    let timeout = clamp_timeout(timeout);
    let Some(bin) = which(&argv[0]) else {
        return ProcOut {
            exit: 127,
            stdout: String::new(),
            stderr: format!("{}: not found on PATH", argv[0]),
        };
    };
    let mut cmd = Command::new(&bin);
    cmd.args(&argv[1..])
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("PYTHONUNBUFFERED", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    #[cfg(unix)]
    {
        cmd.process_group(0);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return ProcOut {
                exit: 127,
                stdout: String::new(),
                stderr: format!("{e}"),
            };
        }
    };
    #[cfg(unix)]
    let pgid = child.id();
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    let t_out = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut r) = stdout.take() {
            let _ = r.read_to_end(&mut buf);
        }
        String::from_utf8_lossy(&buf).into_owned()
    });
    let t_err = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut r) = stderr.take() {
            let _ = r.read_to_end(&mut buf);
        }
        String::from_utf8_lossy(&buf).into_owned()
    });
    let deadline = Instant::now() + Duration::from_secs_f64(timeout);
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                #[cfg(unix)]
                {
                    let _ = Command::new("kill")
                        .args(["-9", &format!("-{pgid}")])
                        .status();
                }
                let _ = child.wait();
                let stdout = t_out.join().unwrap_or_default();
                let stderr = t_err.join().unwrap_or_default();
                return ProcOut {
                    exit: 124,
                    stdout,
                    stderr: format!("{stderr}timed out after {timeout:.1}s (argv={argv:?})"),
                };
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(8)),
            Err(e) => {
                return ProcOut {
                    exit: 1,
                    stdout: t_out.join().unwrap_or_default(),
                    stderr: format!("{e}"),
                };
            }
        }
    };
    let stdout = t_out.join().unwrap_or_default();
    let stderr = t_err.join().unwrap_or_default();
    let exit = status.code().unwrap_or(1);
    ProcOut {
        exit,
        stdout,
        stderr,
    }
}

/// Minimal POSIX-ish split. Quoted strings stay together; no `$` expansion.
pub fn shlex_split(s: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    let mut in_s = false;
    let mut in_d = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_d => in_s = !in_s,
            '"' if !in_s => in_d = !in_d,
            c if c.is_whitespace() && !in_s && !in_d => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            '\\' if !in_s => {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            other => cur.push(other),
        }
    }
    if in_s || in_d {
        return Err("unclosed quote".into());
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    Ok(out)
}
