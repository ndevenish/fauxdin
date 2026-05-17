# Fauxdin — Postmortem

## What it was meant to be

Fauxdin (a play on "faux ODIN") was an attempt at a lightweight,
ODIN-compatible interceptor for the Eiger detector ZeroMQ stream. The goal
was a single, small Rust binary that could sit between a detector and its
downstream consumer and:

- **Mirror** the SIMPLON stream — pull from the detector's PUSH endpoint and
  re-publish it on its own PUSH endpoint so the existing downstream client
  keeps working transparently.
- **Capture** a copy of the stream out-of-band, either to a folder (matching
  ODIN's `/dev/shm` layout) or to S3, partitioned by acquisition series.
- **Be controlled at runtime** via EPICS PVs (`FAUXDIN:DETECTOR`,
  `FAUXDIN:ENABLED`, `FAUXDIN:MIRROR`) for connection target, pump
  enable/disable, and mirror enable/disable.

The end product was supposed to replace, or sit alongside, ODIN for capture
scenarios where ODIN's full machinery was overkill.

## What's here

- `src/bin/fauxdin.rs` — the binary. Spins up an EPICS server
  (`epicars::providers::IntercomProvider`), starts a `PumpHandle` on its own
  thread/runtime that bridges PULL → PUSH, and feeds a copy of every
  multipart message into an `AcquisitionLifecycle`.
- `src/zmq.rs` — `BufferedPushSocket` and `PullSocket`. The PUSH side has a
  custom pre-socket buffer with semaphore-based permits, resizeable at
  runtime, with special handling so multipart messages are never split.
- `src/writers.rs` — the `AcquisitionWriter` trait, plus `FolderWriter` (one
  file per ZMQ frame on disk) and `S3Writer` (per-frame `put_object` via
  the `minio` crate). `AcquisitionLifecycle` parses the SIMPLON
  `dheader-1.0` / `dimage-1.0` / `dseries_end-1.0` JSON envelopes and
  manages Waiting / InAcquisition / Skipping states.
- `epicars/` (submodule) — the EPICS CA server used to expose the
  control PVs.
- `gw-eiger/` (submodule) — Graeme Winter's reference Eiger tooling, kept
  for comparison / cribbing.
- `contrib/` — `thor.py` and `loki.py` (Python helpers for dumping and
  replaying detector streams), plus `send.py`/`test.py` smoke tests.
- `dumps/` — captured stream output, one directory per acquisition series.
- `scratch/` — local-only working area (rustfs binary, credentials file).

## Why it failed

Treat this as a record of what didn't work, not a guide to extend.

1. **Per-frame object writes were the wrong granularity for S3.** The
   `S3Writer` spawns one `put_object` task per ZMQ frame (`upload_file` in
   `src/writers.rs`). For an Eiger collection that's tens of thousands of
   small objects per series. Throughput, latency, and cost all suffer; the
   "Count active uploaders" commit was an attempt to surface this rather
   than fix it. A streaming/multipart aggregator (HDF5-like container, or
   at minimum batched multipart uploads) was the architecture this needed
   from day one.

2. **The "mirror everything verbatim, capture as a side-effect" split was
   leaky.** The pump (`do_pump`) and the writer (`AcquisitionLifecycle`)
   share message ownership through an unbounded mpsc with eager `Vec<u8>`
   copies (see the `Message` shim around `zmq::Message`). The forwarding
   path is allowed to drop messages on PUSH-buffer-full (`try_send` →
   `warn!("...dropping")`), but the capture path is treated as fatal. In
   practice this meant the captured copy and the downstream copy could
   diverge silently, which defeats the point of an interceptor.

3. **Three runtimes, one process.** `#[tokio::main]` for the control loop,
   a hand-rolled `current_thread` runtime inside `PumpHandle::start` for
   the pump, and `spawn_blocking` threads inside `BufferedPushSocket` /
   `PullSocket` for the synchronous `zmq` crate. The cancellation story
   across these is incomplete — `PullSocket::close` and `PullSocket::closed`
   are still `todo!()`, and shutdown relies on `Drop` cancelling tokens
   rather than awaited joins. This made shutdown and reconnect bugs hard
   to reproduce and hard to fix.

4. **Synchronous `zmq` crate, not `rzmq`.** An earlier branch tried `rzmq`
   (see commits `4ae9184` "Work using rzmq" and the commented-out
   `rzmq = "0.5.4"` in `Cargo.toml`) and was abandoned for the synchronous
   `zmq` crate wrapped in blocking threads. That choice is what forced the
   custom `BufferedPushSocket` to exist at all: ZeroMQ's own HWM doesn't
   buffer pre-connection, so frames at the start of an acquisition were
   being dropped. The buffer works, but it's a lot of bespoke
   semaphore/permit accounting (resize-with-debt, force-enqueue mid-
   multipart) that an async-native ZMQ binding would have made unnecessary.

5. **State machine couples I/O failures to acquisition state.** In
   `AcquisitionLifecycle::handle_messages`, any `Err` from a writer is
   logged but the state is preserved unchanged — so an `handle_start`
   failure leaves the lifecycle thinking it's still `Waiting` while images
   arrive and get classified as "garbage, skip series." Symptoms looked
   like dropped acquisitions when the real cause was usually S3 / disk
   errors upstream.

6. **No real test coverage.** The only test is `test_push` in `src/zmq.rs`
   exercising the buffer. There are no tests for the lifecycle state
   machine, the writers, or end-to-end pump behaviour, despite the
   lifecycle being where most of the subtle bugs lived.

## If picking this up again

Don't extend this structure. The right re-architecture is something like:

- Async-native ZMQ (`rzmq` or similar), removing the blocking-thread layer
  and `BufferedPushSocket`.
- A single owner of the message stream, with mirroring and capture as
  fan-out *subscribers* that each have their own backpressure policy
  rather than the current "best-effort to PUSH, must-succeed to writer"
  asymmetry.
- Capture writers that aggregate frames into the natural unit of the
  destination — an HDF5 (or similar) container for object stores, files
  for `/dev/shm`-style consumers — rather than one-blob-per-ZMQ-frame.
- Lifecycle state that's a proper sink-of-truth: writer errors should
  transition into a recoverable "failed series" state, not be swallowed.
- Tests for the lifecycle and writer paths, with synthetic SIMPLON
  message sequences, before adding any new transport.

## Layout reference

```
src/
  bin/fauxdin.rs    binary: EPICS PVs, pump, lifecycle wiring
  zmq.rs            BufferedPushSocket, PullSocket
  writers.rs        DetectorHeader, AcquisitionLifecycle, FolderWriter, S3Writer
  lib.rs            re-exports zmq, writers
epicars/            git submodule — EPICS CA server
gw-eiger/           git submodule — reference Eiger tooling
contrib/            thor.py, loki.py, send.py, test.py — stream helpers
dumps/              captured per-series output (gitignored content)
scratch/            local-only working area
```
