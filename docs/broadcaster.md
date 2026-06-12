# Spec: `Broadcaster`

The fan-out stage. Takes the source's single stream of
`(Seq, Arc<MultipartGroup>)` and delivers each group to N independent
subscribers, each with its own bounded queue and its own drop policy, so a
slow subscriber cannot stall the others.

It is the structural fix for the v1 mistake where the mirror and the capture
shared message ownership with asymmetric "best-effort to PUSH, must-succeed
to writer" semantics that let the two copies diverge silently (`CLAUDE.md`
postmortem §2). Here the asymmetry is explicit and per-subscriber: the sink
subscribes lossy, the capture subscribes lossless, and neither can interfere
with the other.

## Scope

**In:** fan-out of whole `Arc<MultipartGroup>`s from one source channel to
N bounded per-subscriber queues; per-subscriber drop policy; backpressure
propagation to the source for lossless subscribers; per-subscriber drop
accounting; clean shutdown.

**Out (deferred):** parsing frame contents — the broadcaster is as
byte-blind as the source (see [Non-goals](#non-goals)). Multi-source
merging. Reordering, dedup, or retry. Latest-wins diagnostic taps (a
`DropOldest`-style policy) — no v1 consumer needs one, so it is not in the
`DropPolicy` enum (see [Non-goals](#non-goals)).

## Constraints

- Pure tokio: one `tokio::spawn` fan-out task, no `spawn_blocking`, no ZMQ.
  The broadcaster touches no socket — it is plumbing between the source and
  the downstream stages.
- `Arc`-clone only. A delivery is a reference-count bump on the
  `Arc<MultipartGroup>`; the frame bytes are never copied here. The one byte
  copy in the pipeline already happened at the source's PULL read
  (`source.md` §Constraints).
- Groups are atomic and opaque. The unit of fan-out is one whole
  `Arc<MultipartGroup>` + its `Seq`; the broadcaster never inspects or splits
  a group. (Multipart reassembly is the source's job; the boundary already
  moved there — `plan.md` §"Unit of work is the multipart group".)
- The input is the exact `mpsc::Receiver<(Seq, Arc<MultipartGroup>)>` that
  `Source::connect` returns. The broadcaster takes ownership of it.

## Public API

```rust
pub struct Broadcaster { /* opaque */ }

pub struct BroadcasterBuilder { /* opaque */ }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropPolicy {
    /// Queue full → reject the just-arrived group, keep what's queued.
    /// Used by the sink: the mirror is allowed to lose frames under true
    /// backpressure, and the freshest-but-rejected group is the one to lose
    /// last, so older queued groups still drain in order.
    DropNewest,
    /// Queue full → never drop here; the fan-out task `await`s the send,
    /// which back-pressures the source. Used by the capture: durable
    /// recording. Its own backpressure (disk/S3) surfaces as a growing queue
    /// and ultimately as source-side door drops, never as a silent loss here.
    NeverDrop,
}

pub struct Subscription {
    /// The receiving end this subscriber drains. Yields groups in strictly
    /// increasing `Seq` order (with gaps wherever this subscriber's policy
    /// dropped). Closes when the source channel closes or the broadcaster is
    /// cancelled.
    pub rx: mpsc::Receiver<(Seq, Arc<MultipartGroup>)>,
    /// Monotonic count of groups this subscriber's policy has dropped.
    /// `watch` so diagnostics / EPICS can observe without polling the queue.
    /// Always 0 for a `NeverDrop` subscriber (by invariant).
    pub dropped: watch::Receiver<u64>,
}

impl BroadcasterBuilder {
    pub fn new() -> Self;

    /// Register one subscriber before the fan-out starts. `label` appears in
    /// logs and the drop metric. `capacity` is the bounded queue depth in
    /// whole groups and must be `> 0`. Returns the receiving end plus its
    /// drop counter.
    pub fn subscribe(
        &mut self,
        label: &str,
        capacity: usize,
        policy: DropPolicy,
    ) -> Subscription;

    /// Consume the source's hand-off receiver and spawn the fan-out task.
    /// Cancelling `cancel` ends the task and closes every subscriber `rx`.
    /// Pass a child of the pipeline token so one `cancel()` tears the whole
    /// pipeline down. Returns immediately; the task runs until the source
    /// channel closes or `cancel` fires. Logs a `warn` at startup if more
    /// than one `NeverDrop` subscriber was registered (see Internal
    /// architecture — they are sent to sequentially, so their park times add
    /// up).
    pub fn spawn(
        self,
        source_rx: mpsc::Receiver<(Seq, Arc<MultipartGroup>)>,
        cancel: CancellationToken,
    ) -> Broadcaster;
}

impl Broadcaster {
    /// Total groups pulled from the source so far (= the number offered to
    /// each subscriber). Observability; pairs with each subscriber's
    /// `dropped` to reconstruct delivered counts.
    pub fn forwarded(&self) -> watch::Receiver<u64>;

    /// Await the fan-out task's completion. Resolves once the task has
    /// exited (source channel closed or `cancel` fired). After this every
    /// subscriber `rx` observes a closed channel.
    pub async fn shutdown(self);
}
```

`MultipartGroup` and `Seq` are defined in `messages.rs`. The item type
`(Seq, Arc<MultipartGroup>)` is shared verbatim with the source's hand-off
channel (`source.md`) and each subscriber's queue — no transformation
happens in the broadcaster.

## Internal architecture

```
source hand-off ── mpsc::Receiver<(Seq, Arc<MultipartGroup>)> ─┐
                                                               ▼
        ┌──────────────────── fan-out task ────────────────────────┐
        │  tokio::spawn, selects over:                              │
        │    - source_rx.recv()      (next group, or close)         │
        │    - cancel.cancelled()    (shutdown)                     │
        │                                                           │
        │  owns: Vec<Sub> in registration order, each:              │
        │    { label, tx: mpsc::Sender<(Seq, Arc<Group>)>,          │
        │      policy: DropPolicy, dropped: watch::Sender<u64> }    │
        │                                                           │
        │  per group: deliver to every live Sub (see below),       │
        │             bump `forwarded`                              │
        └─────────────────────────────────────────────────────────┘
                  │                              │
                  ▼                              ▼
            sink rx (small,                capture rx (large,
            DropNewest)                    NeverDrop)
```

One `tokio::spawn` task. The builder accumulates the subscriber table before
`spawn`; subscribers cannot be added after the task starts (registration is
startup-only, matching `plan.md` §broadcaster: "N subscribers each register
at startup").

### Delivery order within one group

For each `(seq, group)` the task delivers to subscribers in **two phases**,
so the lossy/fast subscribers are never made to wait on a lossless/slow one
*for the same group*:

1. **Non-blocking phase** — every `DropNewest` subscriber gets a `try_send`.
   - `Ok` → delivered (an `Arc` clone).
   - `Err(Full)` → apply `DropNewest`: drop the group for this subscriber,
     `dropped += 1`, log at `warn` (rate-limited). Other subscribers are
     unaffected. The only v1 `DropNewest` subscriber is the sink, whose queue
     is sized so it should never fill (see Interaction with the sink's own
     buffer), so a drop here is a real signal and warrants `warn`, not
     `debug`. If a genuinely-lossy `DropNewest` subscriber is added later,
     revisit the level per-subscriber.
   - `Err(Closed)` → the subscriber's `rx` was dropped; remove the
     subscriber from the table, log at `info`, continue.
2. **Blocking phase** — every `NeverDrop` subscriber gets a `send().await`,
   **sequentially**, each raced against `cancel.cancelled()`.
   - `Ok` → delivered.
   - cancelled mid-send → abort the fan-out and exit (the group is simply
     not delivered to that subscriber; shutdown is in progress).
   - `Err(Closed)` → remove the subscriber, log, continue.

Because phase 1 finishes before phase 2 begins, a full capture queue delays
only the *next* group's fan-out, not the sink's copy of the *current* one.

The main pipeline has exactly one `NeverDrop` subscriber (the capture), so
sequential phase-2 sends cost nothing. The design keeps them sequential for
simplicity rather than `join`-ing them concurrently. If more than one
`NeverDrop` subscriber is ever registered their park times add up (a stalled
first one delays delivery to the second), so `spawn` logs a `warn` at startup
when it counts `> 1`. That warning is the trigger to revisit concurrent
phase-2 sends — don't pre-build it.

### Backpressure propagation

A `NeverDrop` subscriber whose queue is full parks the fan-out task on its
`send().await` (phase 2). While parked, the task does not call
`source_rx.recv()`, so the source's bounded hand-off channel fills, and the
source then **drops at the PULL door** with a logged warning and a burned
`Seq` (`source.md` §"Drop policy", `plan.md` §"Capture cannot be blocked by
sink"). This is the intended and only load-shedding path for a hopelessly
behind capture: visible (seq gap + log), never a silent drop inside the
broadcaster.

The broadcaster therefore performs **no drop of its own for `NeverDrop`
subscribers** — its `dropped` counter for them stays 0, and the real signal
lives on the source's door-drop counter.

### Interaction with the sink's own buffer

The sink (`sink.md`) has its *own* internal buffer, peer-awareness, and the
authoritative `DeliveryReport` stream. The wiring drains the sink's
broadcaster queue and forwards each group into `PushSink::try_send`, which is
non-blocking — so in normal operation the broadcaster→sink queue drains as
fast as it fills and effectively never reaches `DropNewest`. It is therefore
fixed **small** — **4 groups** — as a jitter-absorbing hand-off slot, not the
mirror's real buffer (the sink's own `buffer_capacity` is that). The exact
value barely matters because the queue shouldn't fill; 4 is enough to ride
out a scheduling hiccup in the forwarding adapter. A `DropNewest` drop here
therefore means the adapter genuinely stalled, which is why it logs at `warn`
(above). Consequently:

- The **authoritative** record of which groups the mirror dropped is the
  sink's `DeliveryReport` (`Dropped(BackpressureFull | PrefetchOverflow)`),
  not the broadcaster's per-subscriber `dropped` counter.
- The broadcaster's sink-side `dropped` counter only ticks in the pathology
  where the forwarding adapter itself stalls (it shouldn't) — treat a
  non-zero value there as a wiring bug worth alarming on, distinct from
  ordinary mirror backpressure.

The capture path has no such second buffer: the broadcaster's `NeverDrop`
queue *is* the capture's inbound buffer, drained by the lifecycle.

### Drop accounting

Each subscriber owns a `watch::Sender<u64>`; the fan-out task increments and
publishes on every policy drop. `forwarded` is incremented once per group
pulled from the source. `forwarded - dropped(sub)` is the count delivered to
a given subscriber (subject to whatever that subscriber then does with it).

## Invariants

1. **Fan-out fidelity.** Every group received from the source is offered to
   every live subscriber exactly once, before the next group is pulled. No
   subscriber is skipped except when its `rx` is already closed.
2. **Per-subscriber order.** A subscriber observes groups in strictly
   increasing `Seq` order. Drops leave gaps; they never reorder or duplicate.
3. **`Arc`-only.** Delivery is an `Arc` clone — the same allocation the
   source built reaches every subscriber. No frame bytes are copied in the
   broadcaster. (Tests assert pointer equality of the delivered `Arc`s.)
4. **Independence of lossy subscribers.** A full `DropNewest` queue never
   blocks the fan-out task; its policy is applied synchronously and the other
   subscribers proceed.
5. **`NeverDrop` is truly lossless here.** The broadcaster never drops a
   group destined for a `NeverDrop` subscriber. Its `dropped` counter is
   always 0. Backpressure is propagated to the source instead.
6. **No panic on a dead subscriber.** A subscriber that drops its `rx` is
   removed from the table on the next send; the fan-out continues for the
   rest. A broadcaster with zero live subscribers keeps draining the source
   (so the source never wedges) and exits when the source channel closes.
7. **Bounded blocking.** The fan-out task only ever `await`s on `NeverDrop`
   sends, and always races them against `cancel`. Shutdown is therefore
   prompt even with a permanently stalled `NeverDrop` subscriber.

## Errors

- `BroadcasterBuilder::subscribe` validates `capacity > 0` (panics or
  `debug_assert` on 0 — startup wiring bug).
- `spawn` does not fail — it starts the task unconditionally.
- All runtime conditions (full queues, closed receivers, source close,
  cancel) are handled in-task per the architecture above; none propagate as
  a `Result` to the caller.

## Test surface

Testable in isolation with synthetic `mpsc` source channels and synthetic
subscribers — no ZMQ, no real sink/capture.

- **Single subscriber, all delivered:** send N groups → subscriber yields
  `(0..N)` in order, frames intact, `forwarded == N`, `dropped == 0`.
- **Multi-subscriber fan-out:** two subscribers, both `NeverDrop`, send N →
  each yields all N, and the delivered `Arc`s are pointer-equal across
  subscribers (no copy).
- **`DropNewest` under stall:** capacity `K`, stall this subscriber, send
  `> K` groups → it receives the first `K` (oldest), the excess are dropped,
  `dropped` counts the excess, a `warn` is emitted (rate-limited); a second
  `NeverDrop` subscriber still gets all groups (independence).
- **`NeverDrop` does not drop:** stall then resume a `NeverDrop` subscriber →
  it eventually receives every group in order, `dropped == 0`.
- **Multiple `NeverDrop` warns:** register two `NeverDrop` subscribers →
  `spawn` emits the startup `warn`; both still receive every group in order.
- **Backpressure to source:** small `source_rx` + small `NeverDrop` queue,
  stall the subscriber → the source-side sender blocks (or, with the real
  source, door-drops); assert the fan-out task is parked and not spinning.
- **Mixed policies:** `DropNewest` sink + `NeverDrop` capture. Stall the sink
  only → sink drops, capture gets all. Stall the capture only → capture
  backpressures, sink keeps receiving until the source channel fills.
- **Phase ordering:** with a full `NeverDrop` queue, the current group still
  reaches a healthy `DropNewest` subscriber before the task parks on the
  `NeverDrop` send.
- **Dead subscriber:** drop one subscriber's `rx` mid-stream → fan-out logs,
  removes it, and continues delivering to the rest; no panic.
- **Zero live subscribers:** drop all `rx`s → broadcaster keeps draining the
  source to completion (source never wedges), then exits.
- **Cancel:** fire `cancel` → task exits, every `rx` observes close,
  `shutdown()` resolves.
- **Cancel mid-`NeverDrop`-send:** stall a `NeverDrop` subscriber, then
  `cancel` → the awaited send is abandoned and the task exits promptly
  (bounded shutdown, no hang).
- **Source close:** drop the source sender → task exits, every `rx` closes,
  `shutdown()` resolves.

## Open questions

None outstanding. Earlier questions are resolved in the spec above:
sequential `NeverDrop` sends with a startup `warn` on more than one
(Internal architecture); fixed broadcaster→sink queue depth of 4 with a
`warn` on full (Interaction with the sink's own buffer); `DropOldest` omitted
from `DropPolicy` (Non-goals).

## Non-goals (worth stating)

- **Not a parser, not series-aware.** Like the source, the broadcaster ships
  opaque `Arc<MultipartGroup>`s. Series/abandon logic is the lifecycle's job.
- **Not multi-source.** One source hand-off channel in. Run multiple
  pipelines for multiple detectors.
- **Not reordering / dedup / retry.** Order is preserved per subscriber;
  drops are real drops, surfaced via the `dropped` counter (and, for the
  capture's source-side shedding, via the source's door-drop counter).
- **No latest-wins / `DropOldest` tap.** Evicting the oldest queued group
  can't be done on a producer-side `mpsc::Sender` anyway (no producer-side
  pop) and no v1 consumer needs it, so the policy is omitted. If a diagnostic
  tap ever wants "only the freshest group," add it then as a separate
  ring-buffer- or `watch`-backed subscription rather than overloading
  `DropPolicy`.
- **Not the authoritative mirror-drop record.** That is the sink's
  `DeliveryReport`. The broadcaster's sink-side counter is a coarse,
  should-stay-zero health signal.
