//! Fan-out from the source to independent subscribers (sink, lifecycle,
//! diagnostics). Each subscriber has its own bounded queue and drop policy
//! so a slow subscriber cannot stall the others.
//!
//! See `docs/plan.md` § Components / broadcaster.

#![allow(dead_code)]
