//! fnmatch-case semantics (Python `fnmatch.fnmatchcase`), used by topic
//! subscriptions. Supports `*`, `?`, `[seq]` / `[!seq]` like CPython's
//! translate-based matcher; all other characters are literal.

pub fn fnmatch_case(name: &str, pattern: &str) -> bool {
    match_impl(name.as_bytes(), pattern.as_bytes())
}

fn match_impl(mut name: &[u8], mut pat: &[u8]) -> bool {
    while !pat.is_empty() {
        match pat[0] {
            b'*' => {
                // collapse consecutive stars
                let mut rest = pat;
                while rest.first() == Some(&b'*') {
                    rest = &rest[1..];
                }
                if rest.is_empty() {
                    return true;
                }
                for i in 0..=name.len() {
                    if match_impl(&name[i..], rest) {
                        return true;
                    }
                }
                return false;
            }
            b'?' => {
                if name.is_empty() {
                    return false;
                }
                name = &name[1..];
                pat = &pat[1..];
            }
            b'[' => {
                if name.is_empty() {
                    return false;
                }
                let (ok, len, matched) = match_class(pat, name[0]);
                if !ok || !matched {
                    return false;
                }
                name = &name[1..];
                pat = &pat[len..];
            }
            c => {
                if name.is_empty() || name[0] != c {
                    return false;
                }
                name = &name[1..];
                pat = &pat[1..];
            }
        }
    }
    name.is_empty()
}

/// Returns (valid, pattern_len_consumed, matched_negated).
fn match_class(pat: &[u8], c: u8) -> (bool, usize, bool) {
    let mut i = 1; // past '['
    let mut negate = false;
    if i < pat.len() && (pat[i] == b'!' || pat[i] == b'^') {
        negate = true;
        i += 1;
    }
    let mut found = false;
    let mut first = true;
    while i < pat.len() {
        if pat[i] == b']' && !first {
            return (true, i + 1, found != negate);
        }
        first = false;
        let lo = pat[i];
        if i + 2 < pat.len() && pat[i + 1] == b'-' && pat[i + 2] != b']' {
            let hi = pat[i + 2];
            if lo <= c && c <= hi {
                found = true;
            }
            i += 3;
        } else {
            if lo == c {
                found = true;
            }
            i += 1;
        }
    }
    (false, pat.len(), false) // unterminated class: literal-ish fallback
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_and_globs() {
        assert!(fnmatch_case("control.task.assigned", "control.task.*"));
        assert!(fnmatch_case("dependency.wait.resolved", "dependency.*"));
        assert!(!fnmatch_case("control.agent.registered", "control.task.*"));
        assert!(fnmatch_case("anything.at.all", "*"));
        assert!(fnmatch_case("control.plan.plan_created", "control.*"));
        assert!(!fnmatch_case("resource.artifact.published", "control.*"));
        assert!(fnmatch_case("abc", "a?c"));
        assert!(fnmatch_case("aXc", "a[XYZ]c"));
        assert!(!fnmatch_case("aYc", "a[!Y]c"));
        // ']' as first char in class is literal, per fnmatch
        assert!(fnmatch_case("]x", "[]]x"));
    }
}
