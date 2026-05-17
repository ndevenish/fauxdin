//! Runtime control surface. Holds `watch` channels for every runtime knob
//! (target endpoint, mirror enable, capture backend, sink buffer size).
//! v1 binds them to CLI args; a later EPICS adapter will bind them to PVs
//! via the `epicars` crate.
//!
//! See `docs/plan.md` § Components / control.

#![allow(dead_code)]
