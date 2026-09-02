//! Platform layer: the std-only replacements for the crates the sandbox cannot
//! reach (crates.io is blocked; see RUST_MIGRATION_PLAN.md §0). Each module is
//! verified against the Python reference where parity matters.

pub mod glob;
pub mod json;
pub mod rand;
pub mod sha256;
pub mod sqlite;

pub use json::{JMap, JValue};
