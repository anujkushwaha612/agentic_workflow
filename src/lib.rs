//! Agentic OS / arena-code runtime — Rust implementation.
//!
//! Crate layout mirrors the approved architecture (RUST_MIGRATION_PLAN.md §8):
//!
//! * engine modules ([`clock`], [`msg`], [`lifecycle`], [`journal`], [`graph`],
//!   [`registry`], [`bus`], [`policy`], [`cognition`], [`actor`], [`spawn`],
//!   [`parent`], [`kernel`], [`tools`]) know nothing about projects, prompts or
//!   CLIs;
//! * product modules ([`product`]) are the launcher layer (`arena-code`,
//!   project workspaces, the coding agent, doctor);
//! * nothing in the engine imports from the product — the dependency is
//!   one-way, exactly as in the Python reference ("nothing in `arena/` imports
//!   from `arena/code/`"). `tests/boundary.rs` enforces it.
//!
//! The Python implementation under `arena/` (frozen) remains the behavioral
//! reference until the parity suite proves equivalence; see
//! `tests/parity/fixtures/` and `RUST_MIGRATION_PLAN.md`.

pub mod clock;
pub mod graph;
pub mod ids;
pub mod journal;
pub mod lifecycle;
pub mod msg;
pub mod registry;
pub mod sys;
// modules added incrementally as milestones land

/// Runtime version of the product tier (`arena-code`).
pub const RUNTIME_VERSION: &str = "0.3.0-m0m2-rust";
