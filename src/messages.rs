//! Shared types passed between pipeline stages.
//!
//! No logic lives here — only the data types the source, broadcaster, sink,
//! lifecycle, and capture all need to refer to. See `docs/plan.md` for how
//! these flow through the pipeline.

use std::sync::Arc;

use tokio_util::bytes::Bytes;

/// One logical SIMPLON message: an ordered, non-empty sequence of ZMQ frames.
///
/// The source reassembles each multipart group from the PULL socket into one
/// of these and broadcasts an `Arc<MultipartGroup>` so every subscriber gets
/// a cheap reference-counted clone. The sink expands the group back into
/// individual ZMQ frames at the wire, setting `SNDMORE` on all but the last.
///
/// The first frame is the SIMPLON header JSON (`dheader-1.0`, `dimage-1.0`,
/// or `dseries_end-1.0`); subsequent frames are header appendix, payload,
/// and payload appendix as defined by the SIMPLON API.
#[derive(Debug)]
pub struct MultipartGroup {
    /// Non-empty, ordered. Empty groups are a construction-site bug.
    pub frames: Vec<Bytes>,
}

/// Monotonic identifier assigned by the source to each group.
///
/// Used as the correlation key for delivery reports from the sink. The
/// lifecycle threads this through `StreamEvent` so the capture can record
/// which groups in a series failed to make it downstream.
pub type Seq = u64;

/// Events produced by the lifecycle parser and consumed by the capture.
///
/// Variants carrying an `Arc<MultipartGroup>` borrow the same allocation
/// the sink saw; the capture never gets its own copy of the bytes.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A new acquisition series began with a `dheader` packet.
    StartSeries {
        series: u64,
        group: Arc<MultipartGroup>,
        seq: Seq,
    },
    /// An image frame within an active series.
    Frame {
        series: u64,
        frame: u64,
        group: Arc<MultipartGroup>,
        seq: Seq,
        delivery: DeliveryStatus,
    },
    /// The active series ended cleanly with a `dseries_end` packet.
    ///
    /// `undelivered_seqs` lists every group in the series whose sink
    /// delivery report was `Dropped` or `SendError` at the moment this
    /// event was emitted. A late `Delivered` after this event is logged
    /// but does not retroactively amend the record.
    EndSeries {
        series: u64,
        undelivered_seqs: Vec<Seq>,
    },
    /// The active series ended without a `dseries_end` packet.
    ///
    /// Capture backends decide how to finalize a partial series (e.g.
    /// rename the directory, set an `aborted` attribute on an HDF5).
    AbandonSeries {
        series: u64,
        reason: AbandonReason,
        undelivered_seqs: Vec<Seq>,
    },
}

/// Why a series ended without a clean `dseries_end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbandonReason {
    /// A `dheader` for a new series arrived while one was still active.
    MissingEnd,
    /// A `dimage` referenced a different series than the active one.
    SeriesSwitched,
    /// No packets received for the configured idle timeout while Active.
    Timeout,
    /// The source PULL connection was cycled mid-series.
    UpstreamReset,
}

/// Sink delivery state of a single group at the moment its `StreamEvent`
/// was emitted.
///
/// The lifecycle waits a bounded time for the sink's `DeliveryReport`
/// before emitting the event. If the report has not arrived by then the
/// event carries `Pending`; the capture should treat that as "unknown,
/// see series-end summary."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Delivered,
    Dropped,
    Pending,
}
