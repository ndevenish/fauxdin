# Spec: wiring — the forward-only pump

The first assembly of real components into a running process: a
**forward-only PULL→PUSH mirror**. It wires `Source → Broadcaster → PushSink`
through a small forwarding adapter, parses a CLI, and gives the whole thing
one cancellation token and a clean shutdown. This is "the basics of the
`fauxdin` executable" — the mirror half of the v2 goal
(`plan.md` §Goal item 1), with the capture half deliberately absent.

It is the structural antithesis of v1's "three runtimes, one process"
(`CLAUDE.md` postmortem §3): a single `#[tokio::main]` runtime, every
component on plain `tokio` tasks, one token that tears the lot down, and a
`shutdown` that *awaits* joins rather than relying on `Drop`.

## Scope

**In:** a library `pump` module that assembles `Source`, a `Broadcaster`
with a single `DropNewest` sink subscriber, and a `PushSink`, joined by a
forwarding-adapter task that drains the subscriber and calls
`PushSink::try_send`; a thin `bin/fauxdin.rs` that parses CLI args, initialises
tracing, builds the config, starts the pump, waits for a shutdown signal, and
shuts down; one root `CancellationToken` plus SIGINT/SIGTERM handling.

**Out (deferred):** the capture path entirely — no `lifecycle`, no `capture`
subscriber, no `StreamEvent`s, no `DeliveryReport` consumption beyond optional
logging (see [Non-goals](#non-goals)). EPICS / runtime PV control — the CLI
binds config once at startup (`plan.md` §control: "v1 binds them to CLI args").
Graceful zero-loss drain on shutdown. Multi-detector fan-out.

## Constraints

- **One runtime, one process.** `#[tokio::main]`; no `spawn_blocking` in the
  hot path; no hand-rolled secondary runtime. This is the explicit fix for
  postmortem §3.
- **Components own their own tasks; the pump owns only the glue.** `Source`
  spawns its recv+monitor tasks, `Broadcaster` its fan-out task, `PushSink`
  its worker+capacity tasks. The `pump` module adds exactly one task of its
  own — the forwarding adapter — plus the handles needed to join everything.
- **The graceful/hard distinction is a pump-wiring concern, not a component
  one.** Every component still takes exactly one `cancel` token and knows
  nothing about "graceful vs hard" — it just sees its token fire or its input
  channel close. The pump creates a root `CancellationToken`
  (`PumpConfig::cancel`, the *hard abort*) and exactly one child,
  `source_token = root.child_token()`, for the source; the broadcaster and sink
  take the root token directly. The asymmetry is entirely in what the pump does
  at teardown: graceful = cancel `source_token` alone and call
  `Broadcaster::join()` / `PushSink::drain(deadline)` (no-cancel paths), so the
  stages behind the source drain via channel-close (EOF); hard = cancel `root`,
  which cascades to the source child and aborts everything. Stopping the source
  *is* the graceful trigger — its ordinary `shutdown()` is what the pump calls.
  (The only place a second signal becomes a real token is *inside* `PushSink`,
  where `drain` uses a private `drain` token separate from its abort `cancel`;
  that is invisible to the rest of the pipeline — see `sink.md`.)
- **`Arc`-clone only after the PULL read.** The one byte copy happens at the
  source's `recv_multipart` (`source.md` §Constraints). The group flows as
  `Arc<MultipartGroup>` through the broadcaster, through the adapter, into
  `PushSink::try_send`, and is expanded back to frames only at the wire by the
  sink worker. The pump introduces no copy.
- **The sink subscriber is `DropNewest`, depth `SINK_QUEUE_DEPTH` (4).** The
  broadcaster→sink queue is a jitter slot, not the mirror buffer; the sink's
  own `buffer_capacity` is the real buffer (`broadcaster.md` §Interaction with
  the sink's own buffer). A `DropNewest` drop on this queue is a wiring-stall
  signal, not ordinary backpressure.
- **Mirror is lossy by design.** No delivery guarantee on this path
  (`plan.md` §Non-goals). The sink drops on true backpressure
  (`BackpressureFull`) or pre-peer overflow (`PrefetchOverflow`); neither
  blocks the source.

## Public API

A new library module `src/pump.rs` (added as `pub mod pump;` in `lib.rs`)
holds the assembly so it is testable without a process; `bin/fauxdin.rs`
is the thin CLI shell over it.

```rust
// src/pump.rs

pub struct PumpConfig {
    /// PULL endpoint to connect to upstream (the detector), e.g.
    /// `"tcp://127.0.0.1:9999"`. Delivered as a `watch::Receiver` to match
    /// `SourceConfig` and leave the door open for the later EPICS retarget;
    /// the forward-only build sets it once and never changes it.
    pub in_endpoint: watch::Receiver<String>,
    /// PUSH endpoint to bind for the downstream consumer, e.g.
    /// `"tcp://0.0.0.0:9999"`.
    pub out_endpoint: String,
    /// Bounded source hand-off channel depth, in groups. See
    /// `SourceConfig::channel_capacity`. Must be > 0.
    pub source_channel_capacity: usize,
    /// `ZMQ_RCVHWM` on the PULL socket. See `SourceConfig::zmq_recv_hwm`.
    pub recv_hwm: i32,
    /// In-process sink buffer depth, in groups. See
    /// `PushSinkConfig::buffer_capacity`. The real mirror buffer.
    pub sink_buffer_capacity: usize,
    /// `ZMQ_SNDHWM` on the PUSH socket. See `PushSinkConfig::zmq_send_hwm`.
    pub send_hwm: i32,
    /// Root cancellation token. Cancelling it (from anywhere, including the
    /// signal handler) tears down the whole pipeline, identically to
    /// [`Pump::shutdown`].
    pub cancel: CancellationToken,
}

pub struct Pump { /* opaque: handles + observability receivers */ }

impl Pump {
    /// Bind the sink, connect the source, build the broadcaster with its
    /// single sink subscriber, and spawn the forwarding adapter. Returns once
    /// every task is running and the PUSH socket is bound (so [`out_port`] is
    /// valid). Does not wait for an upstream peer — the source connects
    /// asynchronously. On any setup error, partially-built components are torn
    /// down before returning `Err`.
    pub async fn start(config: PumpConfig) -> Result<Pump>;

    /// Bound PUSH port (useful when `out_endpoint` requested an ephemeral
    /// port). `None` if the endpoint is not TCP.
    pub fn out_port(&self) -> Option<u16>;

    /// Live source connection state (`Disconnected` / `Connected`).
    pub fn source_state(&self) -> watch::Receiver<SourceState>;

    /// Live sink state (`WaitingForPeer` / `Streaming`).
    pub fn sink_state(&self) -> watch::Receiver<SinkState>;

    /// Groups pulled from the source and offered to the sink subscriber so
    /// far. (= `Broadcaster::forwarded`.)
    pub fn forwarded(&self) -> watch::Receiver<u64>;

    /// Graceful shutdown. Stops pulling from the PULL socket, lets every group
    /// already in the pipeline drain through to the wire, then tears down and
    /// terminates both rzmq contexts. Bounded by `drain_deadline`: if the
    /// downstream peer cannot accept the buffered groups within it, the
    /// remainder is dropped (reported `SinkShutdown` by the sink) and teardown
    /// proceeds. Cancelling the root token (`PumpConfig::cancel`) at any point
    /// — e.g. a second SIGINT — escalates to immediate abort, which this
    /// observes and stops waiting. See [Shutdown](#shutdown-graceful-by-default).
    pub async fn shutdown(self, drain_deadline: Duration);
}
```

The binary:

```rust
// src/bin/fauxdin.rs  (thin)
#[derive(clap::Parser)]
struct Args {
    /// Upstream detector PULL endpoint to connect to.
    #[arg(short = 'i', long = "in")]
    in_endpoint: String,
    /// Downstream PUSH endpoint to bind.
    #[arg(short = 'o', long = "out")]
    out_endpoint: String,
    #[arg(long, default_value_t = 256)]  source_channel_capacity: usize,
    #[arg(long, default_value_t = 500)]  sink_buffer_capacity: usize,
    #[arg(long, default_value_t = 10_000)] recv_hwm: i32,
    #[arg(long, default_value_t = 50)]   send_hwm: i32,
    /// Max time to flush the sink to the wire on a clean stop before forcing.
    #[arg(long, default_value = "5s", value_parser = humantime)] drain_timeout: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // init tracing, parse args, build PumpConfig with root = CancellationToken::new()
    let pump = Pump::start(cfg).await?;

    // First Ctrl-C → soft stop. A second Ctrl-C → hard abort, which the
    // in-progress `shutdown` observes (root cancel) and stops waiting on.
    tokio::signal::ctrl_c().await?;
    info!("interrupt: draining (press Ctrl-C again to force immediate shutdown)");
    let forcer = {
        let root = root.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("second interrupt: forcing immediate shutdown");
                root.cancel();
            }
        })
    };
    pump.shutdown(args.drain_timeout).await; // returns early if the forcer cancels root
    forcer.abort();
    Ok(())
}
```

## Internal architecture

```
                          root CancellationToken (cloned into every box below)
                                       │
  PULL  ┌────────┐  source_rx   ┌─────────────┐  Subscription.rx   ┌───────────┐         ┌──────────┐  PUSH
  ─────▶│ Source │ ───────────▶ │ Broadcaster │ ─────────────────▶ │  forward  │ ──────▶ │ PushSink │ ─────▶
        └────────┘  mpsc(cap=   └─────────────┘  mpsc(DropNewest,  │  adapter  │ try_send└──────────┘
        recv+monitor source_    fan-out task     SINK_QUEUE_DEPTH) │  (1 task) │         worker+capacity
        tasks        channel_                    one subscriber     └───────────┘          tasks
                     capacity)                   ("sink")
```

The forwarding adapter is the only new code:

```text
loop {
  select! biased {
    _ = cancel.cancelled()      => break,
    item = subscription.rx.recv() => match item {
      Some((seq, group)) => match sink.try_send(seq, group) {
        Enqueued                 => trace,
        Dropped(BackpressureFull
              | PrefetchOverflow)=> debug/count   // ordinary mirror loss
        Dropped(SinkShutdown)
              | ShuttingDown     => break,         // sink is going down
      },
      None => break,   // broadcaster closed the sink queue → shut down
    }
  }
}
```

`try_send` is non-blocking, so the adapter drains the broadcaster→sink queue
as fast as it fills; that queue should sit near-empty and the
`DropNewest`/4 policy effectively never trips. All real buffering and
backpressure live inside the sink's own buffer.

### Why the broadcaster, with one subscriber?

The pump could forward `source_rx` straight into the sink. It goes through the
broadcaster anyway so that (a) the architecture is uniform with the eventual
capture build — adding the capture is then *one more `subscribe()` call plus a
lifecycle task*, no reshaping of the data plane — and (b) the
`forwarded`/`dropped` accounting and per-subscriber policy already exist. With
a single `DropNewest` subscriber the broadcaster runs phase 1 only and never
parks, so it costs an `Arc` clone and a `try_send` per group.

### Backpressure in the forward-only build

There is **no `NeverDrop` subscriber** in this build (capture is absent), so
the broadcaster fan-out task never `await`s a send and never stalls. The
source's hand-off channel and the broadcaster→sink queue therefore both stay
near-empty in steady state, and the source's door-drop path
(`source.md` §"Drop policy") should never fire. A slow or absent downstream
consumer surfaces *only* as the sink filling its own buffer and reporting
`BackpressureFull` / `PrefetchOverflow` — the mirror loses frames, loudly and
locally, and the source keeps draining the wire. This is the intended lossy
mirror (`plan.md` §Non-goals).

### Shutdown (graceful by default)

The mirror is lossy under *backpressure*, but a clean operator-initiated stop
should not throw away frames the downstream peer would happily accept. So
`Pump::shutdown` does not fire the root token; it **stops the front and lets
the pipeline drain**, escalating to the hard token only if a deadline is
exceeded. The whole point is to stop *consuming from the PULL socket* while
flushing everything already pulled.

The mechanism is channel-close (EOF) propagation — the components already exit
this way when their input closes, draining what is buffered first:

1. **Stop ingest.** `Source::shutdown` cancels the source's child token: the
   recv loop stops calling `recv_multipart` (no more frames pulled — frames
   still sitting unread in the socket/OS buffer are abandoned, which is the
   intended "stop consuming"), joins recv+monitor, closes the PULL socket,
   terminates the source's rzmq context. Its `tx` drops, so `source_rx` closes.
2. **Drain the broadcaster.** `Broadcaster::join` awaits the fan-out task's
   *natural* completion (no cancel): it drains the remaining buffered groups
   from `source_rx`, delivers each to every subscriber, then exits on EOF,
   closing every subscriber `rx`. (When a `NeverDrop` capture subscriber later
   exists, this step is exactly where the broadcaster *waits* for a slow
   capture to accept the tail of the stream — no special-casing.)
3. **Drain the adapter.** The forwarding-adapter task drains its now-closing
   subscriber `rx`, `try_send`-ing each remaining group into the sink buffer,
   then exits on EOF. Awaiting this handle proves the broadcaster delivered
   everything to the sink subscriber.
4. **Flush the sink.** `PushSink::drain(deadline)` lets the worker push every
   buffered group to the wire, reporting real `Sent` / `SendError` — **not**
   `SinkShutdown` — then stops and terminates the sink's rzmq context. If the
   peer cannot drain within `deadline`, drain escalates: the root token is
   cancelled, the worker aborts its current send, and the still-buffered
   groups report `Dropped(SinkShutdown)`.

Steps 1–3 are bounded by fast operations (`try_send`, `Arc` clones); the only
step that can genuinely block is the sink flush in step 4, which carries its
own `deadline`.

#### Signal policy

The **binary** owns signals; `Pump` installs no handler (so it stays testable
and a library embedding it keeps control of process signals). The policy:

- **First Ctrl-C (SIGINT)** — soft stop: call `Pump::shutdown(drain_timeout)`,
  the graceful drain above.
- **Second Ctrl-C** — hard abort: cancel the root token. Because the
  in-progress `shutdown` is awaiting at each step on tasks that watch the root
  token (and `drain` races it), the cancel propagates immediately and
  `shutdown` stops waiting — **regardless of how much `drain_timeout`
  remained**. Still-buffered groups then report `Dropped(SinkShutdown)`.

SIGTERM (service-manager stop) is treated like the first Ctrl-C: a soft stop.
There is no "wait forever" state — either the drain finishes, the deadline
fires, or a second interrupt forces it.

This generalises to the capture build unchanged: the same EOF cascade drains
the capture subscriber, and each terminal sink (`PushSink`, and later a
capture backend) gets its own bounded `drain`/`flush` before teardown.

**Required component support** (specced alongside this doc):
`PushSink::drain(deadline)` — flush-to-wire-then-stop, distinct from the
abort-semantics `shutdown()`/`Drop` (see `sink.md`); and `Broadcaster::join()`
— await natural completion without cancelling (see `broadcaster.md`). The
source needs no addition — its existing `shutdown()` *is* the stop-ingest
action.

### Control surface (minimal)

Per `plan.md` §control, the runtime knobs are `watch` channels so an EPICS
adapter can later own them. This build wires only one through the type system:
the source endpoint, as `PumpConfig::in_endpoint: watch::Receiver<String>`.
The binary creates `watch::channel(args.in_endpoint)`, keeps the sender (so
the channel never closes), and passes the receiver in. A full `control::Control`
struct is deferred; everything else (capacities, HWMs) is a plain value fixed
at startup.

## Invariants

1. **Single copy.** Exactly one byte copy per group, at the source PULL read;
   the pump and broadcaster move `Arc`s only; the sink expands to frames at
   the wire. (Asserted indirectly: an end-to-end byte-for-byte round trip.)
2. **Single runtime.** No `spawn_blocking` in the data path; one
   `#[tokio::main]`. (Structural — reviewed, not unit-tested.)
3. **One cancel, full teardown.** After root `cancel()` (or a second signal),
   every task exits and both rzmq contexts terminate; `shutdown` resolves and
   the process can exit. No reliance on `Drop` to make progress.
4. **No needless drops on clean stop.** A graceful `shutdown` drops nothing the
   downstream peer would have accepted within `drain_timeout`: every group
   already pulled from the PULL socket is delivered to the sink subscriber and
   flushed to the wire. Loss on a clean stop happens only past the deadline (a
   peer that won't drain), reported as `SinkShutdown`. Frames not yet pulled
   from the socket when ingest stops are abandoned — that is the boundary of
   "stop consuming", not a drop of in-flight work.
5. **Order preserved, drops are the sink's.** The single subscriber receives
   groups in strictly increasing `Seq` order; the adapter offers each to the
   sink exactly once; the only expected losses are the sink's
   `BackpressureFull` / `PrefetchOverflow`. A broadcaster→sink `DropNewest`
   drop is a defect signal, not normal operation.
6. **Mirror never blocks ingest.** With no `NeverDrop` subscriber, a stalled
   downstream consumer cannot back-pressure the source into door-drops; it can
   only fill the sink buffer. The source keeps reading the wire throughout.
7. **Exactly one subscriber.** The broadcaster is built with one `subscribe`
   call ("sink"); adding capture later is additive.

## Errors

- `Pump::start` returns `Result`: a sink bind failure, a source connect
  failure (bad endpoint), or an invalid config (zero capacities, non-positive
  HWMs — validated by the component configs) all surface as `Err`, after
  tearing down anything already built. The binary prints the error and exits
  non-zero.
- Runtime conditions (peer comes and goes, buffer fills, wire errors) are
  handled inside the components per their specs; none propagate out of the
  running pump. The adapter treats `ShuttingDown` / closed `rx` as "exit",
  not as an error.

## Test surface

Testable without a process: `Pump` binds an ephemeral PUSH port and connects
to a libzmq PUSH peer, exactly as the sink/source tests already do.

- **Forwarding adapter in isolation:** a synthetic `mpsc` subscriber channel +
  a real `PushSink` (ephemeral port, libzmq PULL peer). Push N groups into the
  channel → peer receives all N intact, in order. Close the channel → adapter
  exits. Fire `cancel` → adapter exits promptly.
- **End-to-end round trip (the headline test):** libzmq PUSH peer →
  `Pump { in = peer, out = ephemeral }` → libzmq PULL consumer. Send N
  multipart groups with a drained consumer → consumer receives all N,
  frame-for-frame, in order. (Forward-only analogue of `plan.md` §Test
  strategy's replay test; a recorded `dumps/` stream can be substituted later.)
- **States reach steady values:** after a peer attaches each side,
  `source_state` reaches `Connected` and `sink_state` reaches `Streaming`;
  `forwarded` advances to N.
- **Lossy mirror under a stalled consumer:** consumer connects but never
  drains, flood the peer → the sink eventually reports drops (observed via
  `sink_state` staying `Streaming` while `forwarded` keeps climbing past the
  sink buffer depth) and the source keeps draining (no wedge); `cancel`
  still tears down promptly.
- **Graceful drain (the shutdown headline test):** drained consumer attached,
  push N groups through, then `Pump::shutdown(deadline)` *without* cancelling
  root → the consumer receives **all N** intact (nothing thrown away on a clean
  stop), and `shutdown` resolves. Variant: stop ingest with groups still
  buffered in the sink and assert they flush to the wire rather than reporting
  `SinkShutdown`.
- **Drain deadline escalation:** stalled (non-draining) consumer, buffered
  groups, `shutdown(short_deadline)` → resolves within ~`deadline` (does not
  hang on the dead peer); the undrained groups are dropped, not flushed.
- **Clean shutdown:** `Pump::start` then `shutdown` resolves within a bound;
  dropping the `Pump` without `shutdown` also terminates cleanly; firing the
  root `cancel` externally then `shutdown` resolves promptly (hard-abort path).
- **Start errors:** bad `out_endpoint` (unbindable) and bad `in_endpoint`
  both return `Err` from `start` with no leaked tasks.

The CLI parsing in `bin/fauxdin.rs` stays thin enough to need no test beyond
clap's own; the logic under test lives in `pump`.

## Open questions

1. **~~Graceful drain on shutdown.~~** *Resolved — built in.* `shutdown`
   stops ingest and drains the pipeline to the wire with a bounded
   `drain_deadline`, escalating to the hard token on timeout (see
   [Shutdown](#shutdown-graceful-by-default)). Chosen now rather than deferred
   because it is the same EOF-cascade mechanism the capture build needs, and it
   keeps a clean operator stop from throwing away frames the peer would accept.
   Settled: default `drain_timeout` is **5s**, exposed as `--drain-timeout`.
   First Ctrl-C / SIGTERM triggers the soft drain; a second Ctrl-C forces the
   hard abort regardless of remaining timeout (see
   [Shutdown §Signal policy](#signal-policy)).
2. **Surfacing sink drops.** The adapter can count `Dropped(_)` outcomes into a
   `watch<u64>` for observability now, but the *authoritative* per-seq drop
   record is the sink's `DeliveryReport`, which nothing consumes until the
   lifecycle exists. For the forward-only build a coarse counter + `debug!`
   log is enough; full reconciliation waits for `lifecycle`.
3. **Source retarget without EPICS.** The `in_endpoint` watch is plumbed but
   the binary never writes it. A `--in` change at runtime needs either the
   EPICS adapter or a small CLI/admin surface; out of scope here.

## Non-goals (worth stating)

- **No capture, no lifecycle.** This is the mirror only. The broadcaster has
  one subscriber; `StreamEvent` / `DeliveryReport` are not consumed. Adding
  capture is a later, additive step (one `subscribe(NeverDrop)` + a lifecycle
  task + a capture backend).
- **No delivery guarantee under backpressure.** Frames are dropped when the
  peer can't keep up *while running* (`plan.md` §Non-goals). Durability is the
  capture's job, which isn't here. Note this is distinct from *shutdown*, which
  is graceful and drains (above) — the no-guarantee applies to steady-state
  backpressure, not to a clean stop.
- **No EPICS / runtime control.** CLI binds config once. The endpoint watch is
  the only runtime-shaped knob, and even it is set once.
- **No unbounded drain wait.** Graceful shutdown flushes to the wire only
  within `drain_deadline`; it will not block forever on a dead peer. A dead
  peer past the deadline loses its still-buffered tail (reported
  `SinkShutdown`).
- **Not multi-detector / multi-endpoint.** One PULL in, one PUSH out; run
  multiple processes for multiple detectors (`plan.md` §Non-goals).
