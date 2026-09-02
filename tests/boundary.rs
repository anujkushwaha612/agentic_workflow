//! Engine must not import product. Product may import engine.
//! Enforced as a source lint so the split cannot silently rot.

use std::fs;
use std::path::Path;

const ENGINE: &[&str] = &[
    "src/actor.rs",
    "src/bus.rs",
    "src/cli.rs",
    "src/clock.rs",
    "src/cognition.rs",
    "src/control.rs",
    "src/graph.rs",
    "src/ids.rs",
    "src/journal.rs",
    "src/kernel.rs",
    "src/lifecycle.rs",
    "src/msg.rs",
    "src/parent.rs",
    "src/policy.rs",
    "src/registry.rs",
    "src/spawn.rs",
    "src/tools/mod.rs",
    "src/tools/exec.rs",
    "src/tools/jail.rs",
    "src/tools/proc.rs",
    "src/tools/redact.rs",
];

fn walk_rs(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(rd) = fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk_rs(&p, out);
        } else if p.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(p);
        }
    }
}

#[test]
fn engine_does_not_import_product() {
    let mut files: Vec<std::path::PathBuf> = ENGINE.iter().map(std::path::PathBuf::from).collect();
    walk_rs(Path::new("src/actor"), &mut files);
    walk_rs(Path::new("src/kernel"), &mut files);
    walk_rs(Path::new("src/cognition"), &mut files);
    walk_rs(Path::new("src/policy"), &mut files);
    files.sort();
    files.dedup();
    let mut offenders = Vec::new();
    for f in &files {
        if f.components().any(|c| c.as_os_str() == "product") {
            continue;
        }
        let Ok(text) = fs::read_to_string(f) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            let t = line.trim();
            if t.starts_with("//") {
                continue;
            }
            if t.contains("crate::product") || t.contains("arena::product") {
                offenders.push(format!("{}:{}: {t}", f.display(), i + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "engine imported product:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn product_exists_and_is_allowed_to_use_engine() {
    let text = fs::read_to_string("src/product/orchestrate.rs").expect("orchestrate");
    assert!(
        text.contains("crate::kernel::") && text.contains("Kernel"),
        "product must configure Kernel"
    );
}
