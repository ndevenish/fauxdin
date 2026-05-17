//! Fauxdin: PULL→PUSH interceptor for SIMPLON detector streams with an
//! independent, swappable capture path.
//!
//! See `docs/plan.md` for the v2 architecture. Archived v1 modules live in
//! `src/old/` and are intentionally not part of the module tree.

pub mod messages;

pub mod broadcaster;
pub mod capture;
pub mod control;
pub mod lifecycle;
pub mod sink;
pub mod source;
