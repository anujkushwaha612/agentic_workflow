//! Workspace jail (`arena/tools.py` Jail).
//!
//! Every path an agent names is resolved (normpath + realpath) and must sit
//! inside the agent's root *and* inside a granted prefix. A refusal is a
//! result, not an exception.

use std::path::{Path, PathBuf};

use crate::sys::json::{JMap, JValue};

pub const PROTECTED_NAMES: &[&str] = &[".arena", ".git"];

#[derive(Debug, Clone)]
pub struct Jail {
    pub root: PathBuf,
    pub writes: Vec<String>,
    pub reads: Vec<String>,
    pub agent_id: String,
}

impl Jail {
    pub fn new(root: impl AsRef<Path>, writes: &[&str], reads: &[&str], agent_id: &str) -> Jail {
        Jail {
            root: PathBuf::from(root.as_ref()),
            writes: writes.iter().map(|s| (*s).to_string()).collect(),
            reads: reads.iter().map(|s| (*s).to_string()).collect(),
            agent_id: agent_id.to_string(),
        }
    }

    pub fn resolve(&self, rel: &str, write: bool) -> Result<PathBuf, String> {
        if rel.starts_with('~') || Path::new(rel).is_absolute() {
            return Err("REFUSE_ABS_PATH".into());
        }
        let candidate = normpath(&self.root.join(rel));
        let real_root = realpath_nofollow(&self.root);
        let real_cand = if candidate.exists() {
            realpath_nofollow(&candidate)
        } else {
            let parent = candidate.parent().unwrap_or(&self.root);
            let real_parent = if parent.exists() {
                realpath_nofollow(parent)
            } else {
                real_parent_or_root(parent, &real_root)
            };
            let name = candidate
                .file_name()
                .map(|s| s.to_os_string())
                .unwrap_or_default();
            real_parent.join(name)
        };
        if !is_under(&real_cand, &real_root) {
            return Err("REFUSE_OUTSIDE_ROOT".into());
        }
        let prefixes = if write { &self.writes } else { &self.reads };
        if prefixes.is_empty() {
            return Err("REFUSE_NO_GRANT".into());
        }
        let rel_inside = match real_cand.strip_prefix(&real_root) {
            Ok(p) => p.to_path_buf(),
            Err(_) => return Err("REFUSE_OUTSIDE_ROOT".into()),
        };
        let rel_s = rel_inside.to_string_lossy().replace('\\', "/");
        if write && (rel_s.is_empty() || rel_s == ".") {
            return Err("REFUSE_WRITE_ROOT".into());
        }
        for part in rel_inside.components() {
            let name = part.as_os_str().to_string_lossy();
            if PROTECTED_NAMES.contains(&name.as_ref()) {
                return Err("REFUSE_PROTECTED_PATH".into());
            }
        }
        let mut ok = false;
        for p in prefixes {
            if p.is_empty() || p == "." {
                ok = true;
                break;
            }
            let p = p.trim_start_matches("./").trim_end_matches('/');
            if rel_s == p || rel_s.starts_with(&format!("{p}/")) {
                ok = true;
                break;
            }
        }
        if !ok {
            return Err(if write {
                "REFUSE_WRITE_PREFIX".into()
            } else {
                "REFUSE_READ_PREFIX".into()
            });
        }
        Ok(real_cand)
    }

    pub fn describe(&self) -> JMap {
        let mut m = JMap::new();
        m.insert(
            "root".into(),
            JValue::Str(self.root.to_string_lossy().into()),
        );
        m.insert(
            "writes".into(),
            JValue::Arr(self.writes.iter().cloned().map(JValue::Str).collect()),
        );
        m.insert(
            "reads".into(),
            JValue::Arr(self.reads.iter().cloned().map(JValue::Str).collect()),
        );
        m.insert("bound".into(), JValue::Bool(true));
        m.insert("agent_id".into(), JValue::Str(self.agent_id.clone()));
        m
    }
}

/// Collapse `.` / `..` without touching the filesystem (Python `os.path.normpath`).
pub fn normpath(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        use std::path::Component;
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        out
    }
}

fn realpath_nofollow(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| {
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("/"))
                .join(p)
        }
    })
}

fn real_parent_or_root(parent: &Path, real_root: &Path) -> PathBuf {
    if parent.exists() {
        realpath_nofollow(parent)
    } else if let Some(gp) = parent.parent() {
        if gp.exists() {
            realpath_nofollow(gp).join(parent.file_name().unwrap_or_default())
        } else {
            real_root.to_path_buf()
        }
    } else {
        real_root.to_path_buf()
    }
}

fn is_under(cand: &Path, root: &Path) -> bool {
    cand == root || cand.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::tempdir;
    use super::*;
    use std::fs;

    fn tmp_jail() -> (tempdir::Guard, Jail) {
        let d = tempdir::new("arena-jail");
        fs::create_dir_all(d.path().join("src")).unwrap();
        let j = Jail::new(d.path(), &["src"], &["", "src"], "a");
        (d, j)
    }

    #[test]
    fn refuses_absolute_and_tilde() {
        let (_d, j) = tmp_jail();
        assert_eq!(
            j.resolve("/etc/passwd", false).unwrap_err(),
            "REFUSE_ABS_PATH"
        );
        assert_eq!(j.resolve("~/secret", false).unwrap_err(), "REFUSE_ABS_PATH");
    }

    #[test]
    fn refuses_write_at_root_and_escape() {
        let (_d, j) = tmp_jail();
        assert_eq!(j.resolve("", true).unwrap_err(), "REFUSE_WRITE_ROOT");
        assert_eq!(j.resolve(".", true).unwrap_err(), "REFUSE_WRITE_ROOT");
        assert_eq!(
            j.resolve("../outside", false).unwrap_err(),
            "REFUSE_OUTSIDE_ROOT"
        );
    }

    #[test]
    fn write_prefix_and_protected() {
        let (_d, j) = tmp_jail();
        assert_eq!(
            j.resolve("README.md", true).unwrap_err(),
            "REFUSE_WRITE_PREFIX"
        );
        assert_eq!(
            j.resolve(".git/config", false).unwrap_err(),
            "REFUSE_PROTECTED_PATH"
        );
        assert_eq!(
            j.resolve(".arena/x", true).unwrap_err(),
            "REFUSE_PROTECTED_PATH"
        );
        assert!(j.resolve("src/main.rs", true).is_ok());
    }

    #[test]
    fn empty_grants_refuse() {
        let d = tempdir::new("arena-jail-empty");
        let j = Jail::new(d.path(), &[], &[], "a");
        assert_eq!(j.resolve("src/a", true).unwrap_err(), "REFUSE_NO_GRANT");
        assert_eq!(j.resolve("src/a", false).unwrap_err(), "REFUSE_NO_GRANT");
        let _ = d;
    }
}

/// Tiny test-only temp dir that lives next to this module (no crates.io `tempfile`).
#[cfg(test)]
pub mod tempdir {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static N: AtomicU64 = AtomicU64::new(0);

    pub struct Guard {
        path: PathBuf,
    }
    impl Guard {
        pub fn path(&self) -> &Path {
            &self.path
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
    pub fn new(prefix: &str) -> Guard {
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("{prefix}-{n}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Guard { path }
    }
}
