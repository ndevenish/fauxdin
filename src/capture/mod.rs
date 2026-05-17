//! Capture trait and registered backends. A capture consumes
//! [`StreamEvent`]s and writes the data out to a swappable destination
//! (folder, HDF5, S3, …).
//!
//! See `docs/plan.md` § Components / capture.
//!
//! [`StreamEvent`]: crate::messages::StreamEvent

#![allow(dead_code)]

pub mod folder;
