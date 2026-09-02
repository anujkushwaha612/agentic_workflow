//! Secret redaction and digests (`arena/tools.py`).
//!
//! Patterns are blunt on purpose: a false positive costs `***` in a log;
//! a false negative leaks a key. No `regex` crate — std scanners only.

use crate::sys::sha256::hex_digest;
use std::path::Path;

/// Replace known secret shapes. `extra` values of length ≥ 6 are blanked first.
pub fn redact(text: &str, extra: &[&str]) -> String {
    let mut out = text.to_string();
    for secret in extra {
        if secret.len() >= 6 {
            out = out.replace(secret, "***redacted***");
        }
    }
    out = redact_sk(&out);
    out = redact_gh(&out);
    out = redact_labeled(&out);
    out = redact_bearer(&out);
    out
}

fn is_word_bound(b: u8) -> bool {
    !(b.is_ascii_alphanumeric() || b == b'_')
}

fn redact_sk(s: &str) -> String {
    let bytes = s.as_bytes();
    let lower = s.to_ascii_lowercase();
    let lb = lower.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 3 <= bytes.len()
            && &lb[i..i + 3] == b"sk-"
            && (i == 0 || is_word_bound(bytes[i - 1]))
        {
            let mut j = i + 3;
            while j < bytes.len()
                && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'-')
            {
                j += 1;
            }
            if j - (i + 3) >= 8 && (j == bytes.len() || is_word_bound(bytes[j])) {
                out.push_str("***redacted***");
                i = j;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn redact_gh(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 4 <= bytes.len()
            && bytes[i] == b'g'
            && bytes[i + 1] == b'h'
            && matches!(bytes[i + 2], b'p' | b'o' | b'u' | b's' | b'r')
            && bytes[i + 3] == b'_'
            && (i == 0 || is_word_bound(bytes[i - 1]))
        {
            let mut j = i + 4;
            while j < bytes.len() && bytes[j].is_ascii_alphanumeric() {
                j += 1;
            }
            if j - (i + 4) >= 8 && (j == bytes.len() || is_word_bound(bytes[j])) {
                out.push_str("***redacted***");
                i = j;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn starts_label(lower: &str, i: usize) -> Option<(&'static str, usize)> {
    const LABELS: &[&str] = &[
        "api_key", "api-key", "apikey", "token", "secret", "password",
    ];
    for lab in LABELS {
        if lower[i..].starts_with(lab) {
            return Some((lab, lab.len()));
        }
    }
    None
}

fn redact_labeled(s: &str) -> String {
    let lower = s.to_ascii_lowercase();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some((lab, n)) = starts_label(&lower, i) {
            let mut k = i + n;
            if k < bytes.len() && (bytes[k] == b'"' || bytes[k] == b'\'') {
                k += 1;
            }
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && (bytes[k] == b':' || bytes[k] == b'=') {
                k += 1;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k < bytes.len() && (bytes[k] == b'"' || bytes[k] == b'\'') {
                    k += 1;
                }
                let v0 = k;
                while k < bytes.len()
                    && !bytes[k].is_ascii_whitespace()
                    && bytes[k] != b'"'
                    && bytes[k] != b'\''
                    && bytes[k] != b','
                {
                    k += 1;
                }
                if k - v0 >= 6 {
                    out.push_str(lab);
                    out.push_str(":***redacted***");
                    i = k;
                    continue;
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn redact_bearer(s: &str) -> String {
    let lower = s.to_ascii_lowercase();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        if lower[i..].starts_with("authorization") {
            let mut k = i + "authorization".len();
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if k < bytes.len() && bytes[k] == b':' {
                k += 1;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if lower[k..].starts_with("bearer") {
                    let after_bearer = k + "bearer".len();
                    let mut v = after_bearer;
                    while v < bytes.len() && bytes[v].is_ascii_whitespace() {
                        v += 1;
                    }
                    let v0 = v;
                    while v < bytes.len()
                        && (bytes[v].is_ascii_alphanumeric()
                            || bytes[v] == b'.'
                            || bytes[v] == b'_'
                            || bytes[v] == b'-')
                    {
                        v += 1;
                    }
                    if v - v0 >= 8 {
                        out.push_str(&s[i..v0]);
                        out.push_str(":***redacted***");
                        i = v;
                        continue;
                    }
                }
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

pub fn digest(text: &str) -> String {
    let hex = hex_digest(text.as_bytes());
    hex[..16.min(hex.len())].to_string()
}

pub fn file_digest(path: &Path) -> String {
    match std::fs::read(path) {
        Ok(bytes) => {
            let hex = hex_digest(&bytes);
            hex[..16.min(hex.len())].to_string()
        }
        Err(_) => "unreadable".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_sk_and_github_tokens() {
        let s = redact("key sk-abcdefghijklmnop end ghp_abcdefghijklmnop", &[]);
        assert!(!s.contains("sk-abcdefghijklmnop"), "{s}");
        assert!(!s.contains("ghp_abcdefghijklmnop"), "{s}");
        assert!(s.contains("***redacted***"));
    }

    #[test]
    fn extra_secrets_are_blanked() {
        let s = redact("hello SUPERSECRET", &["SUPERSECRET"]);
        assert_eq!(s, "hello ***redacted***");
    }

    #[test]
    fn labeled_keeps_the_key_name() {
        let s = redact("api_key: hunter2plus", &[]);
        assert!(s.contains("api_key:***redacted***"), "{s}");
        assert!(!s.contains("hunter2plus"));
    }
}
