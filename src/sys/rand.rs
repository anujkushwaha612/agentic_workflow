//! Random hex identifiers (`m-<12 hex>` message ids, `c-<12 hex>` correlation
//! ids) — the moral equivalent of Python's `uuid4().hex[:12]`. Reads
//! `/dev/urandom`; on the (never observed) failure path mixes the wall clock,
//! pid and a counter so ids remain unique within a process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn random_hex(n_chars: usize) -> String {
    let bytes_needed = n_chars.div_ceil(2);
    let mut buf = vec![0u8; bytes_needed];
    let ok = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut buf))
        .is_ok();
    if !ok {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let c = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id() as u64;
        for (i, b) in buf.iter_mut().enumerate() {
            let mut h = crate::sys::sha256::Sha256::new();
            h.update(&t.to_le_bytes());
            h.update(&c.to_le_bytes());
            h.update(&pid.to_le_bytes());
            h.update(&[i as u8]);
            *b = h.finalize()[0];
        }
    }
    crate::sys::sha256::to_hex(&buf)
        .chars()
        .take(n_chars)
        .collect()
}

pub fn new_mid() -> String {
    format!("m-{}", random_hex(12))
}

pub fn correlation_from_mid(mid: &str) -> String {
    format!("c-{}", &mid[2.min(mid.len())..])
}

#[cfg(test)]
mod tests {
    #[test]
    fn ids_unique_and_shaped() {
        let a = super::new_mid();
        let b = super::new_mid();
        assert_ne!(a, b);
        assert!(a.starts_with("m-") && a.len() == 14);
        let c = super::correlation_from_mid(&a);
        assert_eq!(c, format!("c-{}", &a[2..]));
    }
}
