# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state — read this first

This repo is mid-rewrite. The v1 codebase has been deleted; v2 is being built
from scratch following [`docs/plan.md`](docs/plan.md). The postmortem of v1
(further down in this file) is referenced by `plan.md` as the record of why
the previous structure failed — it is *not* a description of what's in `src/`
today. When in doubt, trust `docs/plan.md` and the per-component spec files
over the postmortem.

### v2 implementation order (from `docs/plan.md`)

1. `messages` — shared types. **Done** (`src/messages.rs`).
2. `sink` — buffered PUSH with backpressure detection. Spec in
   [`docs/sink.md`](docs/sink.md). **In progress** (`src/sink.rs`).
3. `source` — PULL socket → broadcaster input. Stub (`src/source.rs`).
4. `broadcaster` — fan-out to subscribers. Stub (`src/broadcaster.rs`).
5. `lifecycle` — SIMPLON state machine. Stub (`src/lifecycle.rs`).
6. `capture::folder` — v1 `FolderWriter` semantics on the event trait.
   Stub (`src/capture/`).
7. Binary wiring (`src/bin/fauxdin.rs`) — placeholder; do not flesh out
   until 1-6 are stable.
8. `control::epics` — later thin adapter; CLI for v1.

Each component gets its own spec file in `docs/` (same shape as `sink.md`:
Scope / Constraints / Public API / Internal architecture / Invariants /
Test surface / Open questions / Non-goals) **before** the implementation.
If you're about to write a component without that spec, write the spec
first.

### Cross-cutting v2 decisions worth not re-deriving

These are settled and listed in `plan.md`; flagging the ones easy to
accidentally violate:

- **Multipart groups are the unit of work.** Reassembly happens at the
  source; every downstream stage handles whole `Arc<MultipartGroup>`s.
  No mid-multipart drop logic anywhere else in the pipeline.
- **Sequence numbers are the only correlation key.** Source attaches a
  monotonic `u64`; sink reports `Delivered(seq)` / `Dropped(seq, reason)`;
  lifecycle attaches `undelivered_seqs` to `EndSeries` / `AbandonSeries`.
  Don't invent a second identity scheme.
- **Mirror and capture are independent subscribers** with different drop
  policies (`DropNewest` for sink, `NeverDrop` for capture). Capture
  errors **never** mutate lifecycle state.
- **Async-native `rzmq` (crates.io, 0.5.x).** All ZMQ I/O is plain
  `tokio::spawn` tasks — no `spawn_blocking` in the hot path. A local
  working copy at `./rzmq/` (gitignored) carries WIP ZMTP/2.0 downgrade
  work needed for legacy Eiger detectors; switch the `rzmq` dep over to
  `path = "./rzmq/core"` when running against real v2-only peers. The
  crates.io release is fine for testing against modern libzmq (v3.1)
  peers. **Don't set `SNDTIMEO = 0` on an rzmq socket**: it propagates
  to the session's TCP `write_all` timeout (in
  `sessionx/protocol_handler/data_io.rs`), so any peer that can't drain
  at line rate becomes a fatal session error. Leave it at the default
  and rely on the front buffer + the cancel token for backpressure and
  shutdown.
- **`Bytes` for frame storage.** `tokio_util::bytes::Bytes` — one copy at
  the PULL read, `Arc`-cloned everywhere after.

## Build, test, lint

```
cargo build                            # debug build
cargo test                             # all unit tests
cargo test -p fauxdin sink::tests::    # single module's tests
cargo test sink::tests::bind_success_ephemeral_port -- --nocapture
cargo fmt
cargo clippy --all-targets -- -D warnings
```

Pre-commit (`.pre-commit-config.yaml`) runs `cargo fmt`, `cargo clippy`,
and `cargo check`. The sink tests bind ephemeral TCP ports and start
blocking worker threads — they're real integration tests of `zmq` and
take real time; expect a few seconds for the suite.

## Commit after changes

Once a change is working (build green, relevant tests pass), commit it.
Don't leave the working tree dirty between turns — each logical change
gets its own commit so the history stays bisectable. Follow the existing
`area: short imperative` subject style (`sink: …`, `source: …`).

The `epicars` and `gw-eiger` submodules are vendored dependencies; you
don't normally need to build inside them.

## Layout reference (current)

```
src/
  bin/fauxdin.rs    placeholder binary
  messages.rs       Step 1: MultipartGroup, Seq, StreamEvent, ...
  sink.rs           Step 2: PushSink (per docs/sink.md)
  source.rs         stub
  broadcaster.rs    stub
  lifecycle.rs      stub
  control.rs        stub
  capture/          stub (folder.rs, mod.rs)
  old/              v1 code, NOT in the module tree — kept for reference only
docs/
  plan.md           v2 architecture (start here)
  sink.md           sink spec
epicars/            git submodule — EPICS CA server
gw-eiger/           git submodule — reference Eiger tooling
contrib/            thor.py, loki.py — stream record/replay helpers
dumps/              captured per-series SIMPLON streams (test fixtures)
scratch/            local-only working area
```

`src/old/` is excluded from the module tree on purpose. Don't add a `pub
mod old;` to `lib.rs`; if you need to crib something from v1, copy and
adapt rather than wiring the old modules in.

---

# Fauxdin — v1 Postmortem (historical)

The rest of this file is the v1 retrospective referenced by
[`docs/plan.md`](docs/plan.md). It describes code that no longer exists
in `src/` (some still in `src/old/`) — read it for context on why v2 is
shaped the way it is, not as a guide to the current layout.

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

## v1 layout reference (no longer present)

```
src/
  bin/fauxdin.rs    binary: EPICS PVs, pump, lifecycle wiring
  zmq.rs            BufferedPushSocket, PullSocket
  writers.rs        DetectorHeader, AcquisitionLifecycle, FolderWriter, S3Writer
  lib.rs            re-exports zmq, writers
```

For the current (v2) layout see the top of this file.
