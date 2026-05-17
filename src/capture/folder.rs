//! Folder capture backend: one directory per series, one file per group,
//! plus an `undelivered.json` listing seqs the sink dropped.
//!
//! See `docs/plan.md` § Components / capture.

#![allow(dead_code)]
