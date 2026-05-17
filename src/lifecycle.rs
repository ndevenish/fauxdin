//! SIMPLON stream state machine. Parses `dheader` / `dimage` /
//! `dseries_end` packets, tolerates missing or out-of-order packets, and
//! emits [`StreamEvent`]s annotated with sink delivery information.
//!
//! See `docs/plan.md` § Components / lifecycle.
//!
//! [`StreamEvent`]: crate::messages::StreamEvent

#![allow(dead_code)]
