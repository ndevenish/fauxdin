//! PULL socket reader. Reassembles multipart frames into
//! `Arc<MultipartGroup>`, attaches a monotonic [`Seq`], and feeds the
//! broadcaster.
//!
//! See `docs/plan.md` § Components / source.

#![allow(dead_code)]
