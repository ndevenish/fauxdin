# Plan: Fauxdin v2 architecture

A redesign of the PULL→PUSH detector-stream interceptor. Replaces the
structure described in [`../CLAUDE.md`](../CLAUDE.md) (postmortem of v1).

## Goal

A single Rust binary that sits between an Eiger detector and its downstream
SIMPLON consumer, and:

1. **Mirrors** the stream — PULL from the detector, PUSH to the existing
   consumer, with a buffer that tolerates a slow or briefly-absent
   downstream without dropping startup frames.
2. **Captures** the same stream out-of-band through a swappable backend
   (folder writer for `/dev/shm`, HDF5 container, S3, …).
3. **Stays correct** when the upstream stream is lossy or malformed:
   missing start/end packets, mid-stream series switches, orphaned image
   frames.
4. Is **eventually controllable via EPICS PVs** through the `epicars`
   crate. v1 wires the same control surface to CLI args; the PV layer is
   a later thin adapter.

## Non-goals for v2

- Delivery guarantees on the mirror path. The mirror drops on true
  backpressure; durable recording is the capture's responsibility.
- Multi-endpoint fan-out at the transport layer. One PUSH endpoint per
  sink; run multiple pipelines if needed.
- Replacing `zmq` with `rzmq`. The detector's ZeroMQ protocol version is
  not supported by `rzmq`; we stay on `zmq = "0.10"` and wrap blocking
  socket I/O in worker threads.

## What v1 got wrong (one-line recap)

- Mirror and capture shared message ownership through eager `Vec<u8>`
  copies, with asymmetric drop policies that let the two copies diverge.
- Backpressure couldn't distinguish "no peer connected yet" from "peer
  is slow", so the buffer logic was bespoke and hard to reason about.
- Lifecycle errors were swallowed; an I/O failure in `handle_start` left
  the state machine wedged.
- One ZMQ frame per buffer slot forced multipart-aware drop logic
  throughout the buffer code (the `mid_multipart_message` path).
- Per-frame S3 uploads were the wrong granularity by orders of magnitude.
- No tests for the lifecycle or writers.

Full account in [`../CLAUDE.md`](../CLAUDE.md).

## Architecture

```
PULL socket
    │
    ▼
┌─────────┐   (seq, Arc<MultipartGroup>)   ┌──────────────┐
│ Source  │ ───────────────────────────▶  │ Broadcaster  │
└─────────┘                                 └──────────────┘
                                              │       │
                  ┌───────────────────────────┘       └────────────────────┐
                  ▼                                                        ▼
            ┌──────────┐                                            ┌────────────┐
            │ PushSink │ ──── DeliveryReport stream ─────────────▶  │ Lifecycle  │
            └──────────┘                                            └────────────┘
                  │                                                        │
                  ▼                                                        ▼
            PUSH socket                                          ┌──────────────┐
                                                                  │  Capture     │ (trait)
                                                                  └──────────────┘
                                                                        │
                                                                        ▼
                                                              folder / HDF5 / S3 / …
```

Each downstream subscriber gets a cheap `Arc` clone of the group from the
broadcaster. The capture is fed `StreamEvent`s parsed by the lifecycle,
annotated with delivery information from the sink. The capture never
shares a channel with the sink — they are independent subscribers.

## Key decisions

### Unit of work is the multipart group

Each PULL'd multipart message is reassembled at the **source** into an
`Arc<MultipartGroup>` before being broadcast. All downstream stages
operate on whole groups.

Consequence for the sink: drop decisions are atomic (one group = one
buffer slot = one drop unit). There is no "in the middle of a group,
must force-enqueue" path. The sink still emits frames with correct
`SNDMORE` flags from the stored group — only the boundary moves to the
source.

Consequence for the lifecycle: it always sees a complete first frame to
parse the SIMPLON header from. No partial classifications.

### Message ownership is `Arc`, not `Vec<u8>` copy

The source allocates each `MultipartGroup` once and wraps it in `Arc`.
Subscribers hold `Arc<MultipartGroup>`. Cloning is reference-count
bumping. The only copy is the source's initial read from the zmq socket
(forced by the `zmq` crate's ownership model).

### Sequence numbers thread the pipeline

The source attaches a monotonic `seq: u64` to every group. The
broadcaster preserves it. The sink reports `Delivered(seq)` /
`Dropped(seq, reason)` via the delivery channel. The lifecycle
accumulates dropped seqs per active series and attaches them to
`EndSeries` / `AbandonSeries` events. The capture records them.

This is how "which messages were not completely delivered" gets
answered. There is no other mechanism — the seq number is the
correlation key everywhere.

### Capture cannot be blocked by sink

The broadcaster fans out to per-subscriber bounded queues. The sink's
queue is small with a `DropNewest` policy on true backpressure. The
capture's queue is large with `NeverDrop` (its own backpressure comes
from the destination — disk, S3 — and is its own problem).

A peer that stops reading the PUSH socket therefore does not cause the
capture to fall behind, and a slow capture does not stall the mirror.

### Lifecycle trusts the packet, not its own state

SIMPLON packet `series` and `frame` fields are the source of truth. Any
disagreement between the lifecycle's tracked state and the packet's
header triggers a recovery transition — the mirror keeps PUSHing the
whole time, and the capture sees an `AbandonSeries` event for the
in-progress series before the new one starts.

I/O errors from a capture do *not* mutate lifecycle state. The capture
reports the error and the lifecycle keeps going. A wedged capture is a
capture problem, not a pipeline problem.

## Components

Each component will get its own spec file in `docs/` matching the
[`sink.md`](sink.md) style (Scope / Constraints / Public API / Internal
architecture / Invariants / Test surface / Open questions / Non-goals).

### `source` — PULL socket → broadcaster input

- Owns the PULL socket on a blocking thread.
- Reassembles multipart frames into `MultipartGroup`.
- Allocates `Arc<MultipartGroup>`, attaches the next `seq`, hands off
  to the broadcaster.
- Surfaces a `SourceState` (`Disconnected` / `Connected { endpoint }`)
  and a configurable endpoint (`watch::Receiver<String>` so EPICS can
  drive it later; v1 binds it to a CLI arg).

Open: how to detect upstream silence. ZMQ PULL gives no peer-loss event
on the connect side. A "no message for N seconds while we thought we
were in a series" timer is the right place to fire a lifecycle
`AbandonSeries(Timeout)`. The source probably owns this timer because
it owns the socket.

### `broadcaster` — fan-out to independent subscribers

- N subscribers each register at startup with `(queue_capacity,
  DropPolicy)`.
- Sender attempts `try_send` per subscriber independently. A full
  subscriber gets its drop policy applied; other subscribers proceed.
- Each subscriber owns its `mpsc::Receiver<(u64, Arc<MultipartGroup>)>`.

Drop policies:

- `DropNewest` — the new group is rejected. Used by the sink.
- `DropOldest` — pop the oldest queued group, push the new. Useful for
  diagnostic taps; not used in the main pipeline.
- `NeverDrop` — back-pressure-by-bounded-buffer; sender blocks the
  fan-out task. Used by the capture. Bounded by a generous capacity so
  a transient disk/S3 hiccup doesn't stall, but a permanent stall is
  visible as a growing queue and eventually backs up to the broadcaster.

The broadcaster itself must not block on a `NeverDrop` subscriber long
enough to stall the source. Concretely: a `NeverDrop` subscriber whose
queue is full causes the broadcaster's fan-out task to await its
`send()`, which back-pressures the source. The source then queues in
its own bounded internal buffer; if that fills, it drops at the PULL
boundary with a logged warning. This is acceptable — if the capture
truly cannot keep up, the right answer is to lose messages and surface
it loudly, not to hide it.

### `sink` — buffered PUSH with true backpressure detection

Specced in full in [`sink.md`](sink.md). Summary:

- Single bounded buffer of `Arc<MultipartGroup>`.
- Peer count tracked via `zmq_socket_monitor` → state of
  `WaitingForPeer` / `Streaming`.
- "Buffer full with peer connected" = true backpressure; "buffer full
  with no peer" = startup pad overflowed. Different `DropReason`.
- Emits a `DeliveryReport` per enqueued group on a `broadcast` channel;
  this is the authoritative per-message drop signal — the state watch
  is observability only.

### `lifecycle` — stream state machine and event emitter

- Consumes `(seq, Arc<MultipartGroup>)` from the broadcaster.
- Consumes `DeliveryReport` from the sink.
- Emits `StreamEvent` (see types below).

State:

```rust
enum LifecycleState {
    Idle,
    Active { series: u64, frames_seen: usize, last_packet: Instant },
    Recovering { reason: RecoveryReason, since: Instant },
}
```

Transitions are driven by the SIMPLON header in the first frame of
each group. The rules are intentionally tolerant:

| Saw                          | While in            | Action                                                                          |
|---|---|---|
| `dheader N`                  | `Idle`              | → `Active(N)`, emit `StartSeries`                                               |
| `dheader N`                  | `Active(M)`, N ≠ M  | emit `AbandonSeries(M, MissingEnd)` then `StartSeries(N)`                       |
| `dimage` (series N)          | `Active(N)`         | emit `Frame`                                                                    |
| `dimage` (series N)          | `Active(M)`, N ≠ M  | emit `AbandonSeries(M, SeriesSwitched)`, → `Recovering(OrphanedImage)`          |
| `dimage`                     | `Idle`              | → `Recovering(OrphanedImage)`; mirror still PUSHes, capture ignores             |
| `dseries_end N`              | `Active(N)`         | emit `EndSeries(N)`, → `Idle`                                                   |
| `dseries_end`                | anything else       | log, ignore                                                                     |
| any                          | `Recovering`        | stay in `Recovering` until the next valid `dheader`                             |
| no packet for `idle_timeout` | `Active(M)`         | emit `AbandonSeries(M, Timeout)`, → `Idle` (timer-driven, not packet-driven)    |

Mirror behaviour is independent of lifecycle state — every group is
forwarded to the sink regardless. Lifecycle state only controls what
the capture sees.

The lifecycle attaches delivery information to each event:

- `Frame` includes the per-frame `delivery: DeliveryStatus` (whether
  every constituent `seq` was `Delivered`, `Dropped`, or `SendError`).
- `EndSeries` and `AbandonSeries` include `undelivered_seqs: Vec<u64>`
  for the series.

The lifecycle keeps an in-flight set of `seq` per active series and
drains it as `DeliveryReport`s arrive. On `EndSeries` /
`AbandonSeries`, any seqs still in the set are reported as undelivered
(they may yet land at the sink, but we don't wait — the capture closes
the series with the best info we have, and a late `Delivered` is logged
but cannot retroactively change the event).

### `capture` — swappable trait

```rust
pub trait Capture: Send {
    fn on_event(&mut self, event: StreamEvent) -> Result<(), CaptureError>;
    fn flush(&mut self) -> Result<(), CaptureError>;
}
```

Initial implementation: `FolderCapture` — one directory per series,
one file per group, plus an `undelivered.json` listing seqs the sink
dropped. Reproduces v1's `FolderWriter` semantics.

Future implementations: `Hdf5Capture` (aggregates frames into a
container), `S3Capture` (multipart-upload batches, not per-frame
`put_object`).

A capture failure is reported via `CaptureError` and **does not** mutate
lifecycle state. The runtime logs and, if the error is fatal for the
capture, may transition the capture into a degraded mode where it
records `EndSeries` markers but skips frame writes until recovery.

### `control` — config surface

v1: `Control` struct holds `watch::Sender` handles for every runtime
knob (target endpoint, mirror enable, capture backend selection,
sink buffer size, …). The CLI binds them to constants at startup.

v2 (later): `epicars` adapter task that owns the same `Control`,
exposes PVs via `IntercomProvider`, and writes user changes back into
the watch channels. No other component changes.

## Shared data types

Defined in a `messages` module so every component can refer to them
without circular dependencies.

```rust
pub struct MultipartGroup {
    /// Non-empty, ordered. Each frame is the bytes of one ZMQ frame.
    /// First frame is the SIMPLON header JSON; subsequent frames are
    /// header appendix, payload, payload appendix, etc.
    pub frames: Vec<Bytes>,
}

pub type Seq = u64;

pub enum StreamEvent {
    StartSeries {
        series: u64,
        group: Arc<MultipartGroup>,
        seq: Seq,
    },
    Frame {
        series: u64,
        frame: u64,
        group: Arc<MultipartGroup>,
        seq: Seq,
        delivery: DeliveryStatus,
    },
    EndSeries {
        series: u64,
        undelivered_seqs: Vec<Seq>,
    },
    AbandonSeries {
        series: u64,
        reason: AbandonReason,
        undelivered_seqs: Vec<Seq>,
    },
}

pub enum AbandonReason {
    MissingEnd,        // saw new dheader before dseries_end
    SeriesSwitched,    // dimage for a different series
    Timeout,           // no packets while Active
    UpstreamReset,     // PULL connection cycled
}

pub enum DeliveryStatus {
    Delivered,
    Dropped,
    Pending,           // sink hasn't reported yet at event-emission time
}
```

One group = one `Seq`. `StartSeries` / `Frame` carry the single `seq` of
their group; the per-series `EndSeries` / `AbandonSeries` carry the list
of seqs the sink failed to deliver.

`Bytes` is `tokio_util::bytes::Bytes`. The source builds each frame as
a `Bytes` once and never copies it again.

## Implementation order

1. **`messages`** — type definitions only. No logic.
2. **`sink`** — already specced in [`sink.md`](sink.md). Standalone,
   testable in isolation against a peer socket.
3. **`source`** — PULL → `(seq, Arc<MultipartGroup>)` channel. Testable
   against a fake PUSH from `zmq` directly.
4. **`broadcaster`** — fan-out logic. Testable with synthetic
   subscribers and synthetic drop policies.
5. **`lifecycle`** — state machine and event emission. Testable with
   synthetic message sequences (including pathological cases:
   out-of-order headers, missing ends, mid-series switches). This is
   where most of the v1 bugs lived; the test surface here is the most
   important.
6. **`capture::folder`** — port of v1 `FolderWriter` behaviour to the
   event-driven trait. Includes `undelivered.json` writing.
7. **Wiring binary** — assembles the pipeline, parses CLI args, hooks
   up `Control` watch channels with constants.
8. **`control::epics`** — later, once the rest is stable.

Each component lands with its own spec doc in `docs/` and its own test
module. The binary does not exist until step 7; everything before it
is library code.

## Test strategy

- **Per-component unit tests.** Each component's spec defines its
  required test surface (see `sink.md` for the template).
- **Lifecycle stress tests.** Synthetic message sequences exercising
  every transition in the table above, including ones not produced by
  a well-behaved detector. The state machine must be deterministic
  under every input order.
- **End-to-end integration test.** Replay a recorded SIMPLON stream
  (we have several in `dumps/`) through the full pipeline into a
  `FolderCapture`, then diff the output against the input. Should
  round-trip byte-for-byte for the captured side and frame-for-frame
  for the mirrored side (modulo intentional drops, which should match
  the recorded `undelivered.json`).

## Open questions

1. **Idle timeout default.** How long without a packet in `Active`
   before `AbandonSeries(Timeout)` fires? Eiger inter-frame gap depends
   on exposure; pick something well above the longest realistic
   exposure (60 s?) and expose as config.
2. **`NeverDrop` capture backpressure to the source.** Stated above as
   "acceptable — drop loudly at the PULL boundary." Worth confirming
   with operators that capture-falls-behind-and-drops-frames is the
   preferred failure mode vs. capture-falls-behind-and-mirror-slows.
3. **Late `DeliveryReport` after `EndSeries`.** Today: logged, ignored.
   Alternative: emit a `LateDelivery` event so the capture can append
   to the series's `undelivered.json`. Probably not worth the
   complexity; `EndSeries` is the closing record.
4. **Series ID width.** SIMPLON uses an unsigned integer; v1 used
   `usize`. `u64` here is portable. Confirm no detector firmware ever
   emits negative or string series IDs.
5. **`AbandonSeries` semantics for capture.** Does the capture
   finalize a partial series (rename to `series_N.aborted/`,
   close the HDF5 with an aborted attribute) or delete it? Probably
   capture-backend's choice — make it a method on the trait, not a
   pipeline decision.
