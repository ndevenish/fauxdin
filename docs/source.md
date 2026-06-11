# Spec: `Source`

The inbound side of the pump. Owns a ZeroMQ PULL socket connected to the
detector's PUSH endpoint, reassembles each multipart message into an
`Arc<MultipartGroup>`, stamps it with a monotonic [`Seq`], and hands it to
the broadcaster. Surfaces a live connection state for diagnostics.

## Scope

**In:** ZMQ PULL transport (connect side), multipart reassembly, `Seq`
assignment, hand-off to the broadcaster over a bounded channel,
monitor-driven connection state, runtime endpoint retargeting, clean
shutdown.

**Out (deferred):** parsing the SIMPLON header — the source is byte-blind,
it never looks inside a frame. Series state, idle-timeout, and
abandon decisions live in the **lifecycle**, not here (see
[Non-goals](#non-goals)). EPICS PV control surface (the endpoint watch and
state watch are shaped so an EPICS adapter can drive them later without
changing the API).

## Constraints

- Async-native `rzmq` from crates.io (0.5.x). Same dep story as the sink:
  a local working copy at `./rzmq/` (gitignored) carries WIP ZMTP/2.0
  downgrade work for legacy Eiger detectors — switch the dep to
  `path = "./rzmq/core"` when running against v2-only peers.
- The PULL socket and its monitor live in `tokio::spawn` tasks; **no
  `spawn_blocking`**. (This supersedes the "blocking thread" wording in
  `plan.md` §source, which predates the async-native `rzmq` decision; the
  cross-cutting decision in `CLAUDE.md` — all ZMQ I/O on plain tokio tasks
  — governs.)
- One byte copy total, at the PULL read: each `rzmq::Msg` is copied once
  into a `Bytes` when the group is built. Everything after the source is
  `Arc`-clone only.
- Multipart groups are atomic: the source emits a group only once
  `recv_multipart` has returned every frame. A group is never partially
  observed downstream.

## Public API

```rust
pub struct Source { /* opaque */ }

pub struct SourceConfig {
    /// Endpoint to PULL-connect to, e.g. `"tcp://127.0.0.1:9999"`. Driven
    /// by a watch so EPICS/CLI can retarget at runtime; writing a new value
    /// cycles the socket (disconnect old, connect new). v1 binds this to a
    /// CLI arg via a `watch` that never changes after startup.
    pub endpoint: watch::Receiver<String>,
    /// Bounded capacity of the hand-off channel to the broadcaster, counted
    /// in groups. When full, the source drops at the PULL boundary with a
    /// logged warning rather than blocking the recv loop (see Drop policy).
    pub channel_capacity: usize,
    /// ZMQ_RCVHWM applied to the PULL socket.
    pub zmq_recv_hwm: i32,
    /// Cancellation token. Cancelling it (from anywhere) stops the recv
    /// loop, closes the socket, and ends the group stream. Pass a child of
    /// the pipeline token so one `cancel()` tears the whole pipeline down.
    pub cancel: CancellationToken,
}

#[derive(Clone, Debug)]
pub enum SourceState {
    /// No live peer: initial state, between connect retries, or after a
    /// disconnect / retarget. The PULL socket may still be attempting to
    /// (re)connect in the background — rzmq retries automatically.
    Disconnected { endpoint: String },
    /// ZMTP handshake completed with the upstream PUSH peer. This — not a
    /// raw TCP `Connected` — is the "we have a live peer" signal.
    Connected { endpoint: String, peer_addr: String },
}

impl Source {
    /// Create the socket, connect to the current `endpoint`, start the recv
    /// and monitor tasks. Returns the handle plus the receiving end of the
    /// hand-off channel — the wiring passes that receiver to the
    /// broadcaster. `connect` does not wait for a peer (ZMQ connect is
    /// asynchronous and retries); it returns once the socket exists and the
    /// tasks are spawned.
    pub async fn connect(
        config: SourceConfig,
    ) -> Result<(Self, mpsc::Receiver<(Seq, Arc<MultipartGroup>)>)>;

    /// Live connection state. Updates on every variant transition, driven by
    /// rzmq monitor events. Diagnostics surface; not a correctness signal.
    pub fn state(&self) -> watch::Receiver<SourceState>;

    /// The next `Seq` the source will assign. Monotonic, process-global,
    /// never reset across reconnects or retargets. For observability/tests.
    pub fn next_seq(&self) -> Seq;

    /// Stop the recv loop, close the socket, drain rzmq actors. Resolves
    /// once both tasks have joined. The hand-off receiver then observes a
    /// closed channel.
    pub async fn shutdown(self);
}
```

`MultipartGroup`, `Seq`, and the downstream event types are defined in
`messages.rs`. The hand-off item type `(Seq, Arc<MultipartGroup>)` is the
exact type the broadcaster consumes (`plan.md` §broadcaster).

## Internal architecture

```
                  ┌──────────────── recv task ─────────────────┐
upstream PUSH ───▶│  loop, select over:                        │
                  │    - socket.recv_multipart()  (a group)    │
                  │    - endpoint.changed()        (retarget)  │
                  │    - cancel.cancelled()        (shutdown)  │
                  │                                            │
                  │  owns:                                     │
                  │    - rzmq::Socket (PULL)                   │
                  │    - next_seq: u64 (monotonic)             │
                  │  on a group: copy frames → Bytes, build    │
                  │    Arc<MultipartGroup>, stamp seq,         │
                  │    try_send to broadcaster channel         │
                  └─────────────────────────────────────────────┘

                  ┌──────────────── monitor task ──────────────┐
                  │  loop, recv rzmq MonitorReceiver:           │
                  │    HandshakeSucceeded → Connected           │
                  │    Disconnected / Closed → Disconnected     │
                  │  writes SourceState watch                   │
                  └─────────────────────────────────────────────┘
```

Two `tokio::spawn` tasks, no `spawn_blocking`:

1. **Recv task** owns the PULL socket. Calls `recv_multipart().await`, which
   returns a complete `Vec<Msg>` (one whole multipart group) or an error.
   Builds the `MultipartGroup`, wraps in `Arc`, assigns the next `Seq`, and
   `try_send`s `(seq, group)` to the broadcaster channel. Also watches the
   `endpoint` for retarget and `cancel` for shutdown.
2. **Monitor task** drains the `MonitorReceiver` returned by
   `socket.monitor(...)` and maps connecter-side `SocketEvent`s onto
   `SourceState`. It owns no socket and makes no series decisions.

### Seq assignment

`next_seq` starts at 0 and increments by one per group emitted, in recv
order. It is **process-global and monotonic** — a reconnect or retarget does
*not* reset it. This is what makes `Seq` a stable correlation key across the
sink's `DeliveryReport`s and the lifecycle's `undelivered_seqs` even when the
upstream connection cycles mid-acquisition.

A group dropped at the PULL boundary (channel full) still consumes its `Seq`
— the number is burned, leaving a gap. A gap in delivered seqs is therefore
"the source dropped it before the broadcaster," distinct from the sink's
`Dropped` (dropped after broadcast). Both are visible; neither is silent.

### State derivation

`SourceState` is computed solely from monitor events on the connect side:

| `SocketEvent`             | Resulting state                          |
|---|---|
| `HandshakeSucceeded`      | `Connected { endpoint, peer_addr }`      |
| `Disconnected` / `Closed` | `Disconnected { endpoint }`              |
| `ConnectRetried` etc.     | logged only; stays `Disconnected`        |

Raw TCP `Connected` is *not* treated as a live peer — only
`HandshakeSucceeded` is, per the comment in rzmq `socket/events.rs`. The
state watch is observability; correctness never depends on it.

### Retarget

A new value on the `endpoint` watch cycles the connection: disconnect the
old endpoint, connect the new one. State goes `Disconnected` then (on the
next handshake) `Connected`. `Seq` continues unbroken. Whether an in-flight
series should be abandoned across a retarget is the **lifecycle's** call,
made from observing the `SourceState` watch — not the source's.

### Drop policy

The hand-off channel to the broadcaster is bounded (`channel_capacity`). On
a full channel the recv task does **not** block — it drops the just-read
group at the door, increments a dropped counter, and logs a warning. This is
the "drop loudly at the PULL boundary" failure mode from `plan.md`
§broadcaster: if a `NeverDrop` capture subscriber back-pressures the
broadcaster hard enough to fill the source channel, the source sheds load
visibly (seq gap + log) rather than stalling the mirror.

Crucially, the recv loop must never block, because blocking it would back up
into the PULL socket's RCVHWM queue and from there to the detector. ZMQ
PUSH/PULL is a *backpressure* pattern, not a lossy one: rzmq's PULL incoming
queue is a bounded channel sized to RCVHWM (`pull_socket.rs`,
`fair_queue.rs`), and when it is full the session blocks pushing into it,
propagating backpressure through TCP to the detector's PUSH socket. A
detector mid-exposure cannot pause, so it drops frames *internally* where we
cannot see or report them. Shedding must therefore happen at our channel
(visible), never via socket backpressure (invisible). See the RCVHWM open
question below.

## Invariants

1. **Byte-blind.** The source never parses frame contents. It does not know
   what a `dheader` / `dimage` / `dseries_end` is. No series logic here.
2. **Multipart atomicity.** A group is emitted only after `recv_multipart`
   returns every frame. Downstream never sees a partial group, and frame
   order within a group is preserved exactly as received.
3. **Seq monotone and global.** `next_seq` strictly increases by one per
   group ever read, never resets across reconnect/retarget. No two groups
   share a `Seq`; a burned `Seq` (door-dropped group) leaves a permanent
   gap.
4. **Never blocks the recv loop on a downstream stall.** A full hand-off
   channel drops at the door; it does not back up into the socket.
5. **No panic on recv error.** A `recv_multipart` error transitions toward
   `Disconnected` (via the monitor) and the loop continues/retries; it does
   not tear the task down except on `cancel`.
6. **State is diagnostics-only.** Nothing in the source's correctness
   depends on `SourceState`; it is derived from the monitor for observers.

## Errors

- `connect()` returns `Err` for: invalid endpoint string, libzmq/rzmq init
  failure, monitor setup failure. No tasks are spawned on failure.
- Post-connect transport errors do not propagate as `Result`s — they show
  up as `SourceState::Disconnected` and recv-loop retries. The source stays
  alive until `cancel` / `shutdown`.

## Test surface

Required coverage before this is considered done. Testable against a plain
`rzmq` PUSH socket as the fake detector.

- **Connect + receive:** bind a fake PUSH, connect the source, send one
  group → broadcaster channel yields `(0, group)` with frames intact and in
  order.
- **Seq monotonicity:** send N groups → seqs `0..N` in order, no gaps when
  the channel is never full.
- **Multipart reassembly:** send a 4-frame multipart message → exactly one
  `MultipartGroup` with 4 frames in original order; no splitting, no
  merging of adjacent groups.
- **State on handshake:** connect a peer → `SourceState` transitions to
  `Connected` after the handshake (not before).
- **State on disconnect:** drop the fake PUSH → state transitions to
  `Disconnected`.
- **Retarget:** write a new endpoint to the watch → source connects to the
  new fake PUSH, state cycles `Connected → Disconnected → Connected`, seq
  continues unbroken across the cycle.
- **Door drop on full channel:** use `channel_capacity = K`, stall the
  consumer, send > K groups → excess groups are dropped at the door (seqs
  burned, gap visible), recv loop keeps draining the socket, warning logged.
- **No partial group on cancel mid-recv:** cancel while a multipart is
  arriving → no partial group is emitted; channel closes cleanly.
- **Shutdown:** `shutdown()` → recv and monitor tasks join, socket closes,
  broadcaster channel observes close.
- **Recv error resilience:** force a transport error → task does not panic
  or exit; recovers to `Disconnected` and retries.

## Open questions

1. **Door-drop counter surface.** The dropped-at-PULL count is currently
   internal + logged. Should it be exposed (a `watch<u64>` or a field on
   `SourceState`) so EPICS can alarm on it? Leaning yes — it's the
   capture-can't-keep-up signal operators most want to see. (`plan.md`
   open question 2.)
2. **`UpstreamReset` plumbing.** The lifecycle needs to map a mid-series
   disconnect to `AbandonSeries(UpstreamReset)`. Does it observe the
   `SourceState` watch directly, or does the source push an explicit
   reset signal onto a side channel? Leaning toward the lifecycle observing
   the existing `SourceState` watch — no new channel, source stays
   series-ignorant. Pin this down in `lifecycle.md`.
3. **RCVHWM vs. channel_capacity interaction.** The two are bounded buffers
   *in series* on the drain path: detector → PULL RCVHWM queue → recv task →
   hand-off channel (`channel_capacity`) → broadcaster. Because the recv
   loop unconditionally pulls from the socket and drops at the channel door,
   it continuously empties the RCVHWM queue, so the **channel is the binding
   constraint by construction** — it fills when the broadcaster (i.e. the
   `NeverDrop` capture) stalls. RCVHWM is upstream of the shed point and
   only fills if the *recv task itself* stalls; the two therefore guard
   different stalls, not the same one.

   PUSH/PULL is a backpressure pattern, **not lossy** (confirmed in rzmq:
   PULL's incoming queue is a bounded channel sized to RCVHWM and blocks
   when full). So a too-small RCVHWM is the dangerous direction: a brief
   recv-task scheduling gap backs pressure up to the detector, which then
   drops frames internally where we cannot observe them. RCVHWM is thus a
   *jitter-absorption buffer, never a load-shedding knob*; all intentional
   shedding happens at `channel_capacity`.

   Sizing: RCVHWM counts frames (multipart HWM accounting is per-part and
   libzmq-version-dependent), `channel_capacity` counts whole groups — so
   they are not directly comparable; convert via frames-per-group. Treat
   them as two independent knobs, not a derived ratio. Socket-buffer memory
   is bounded by `RCVHWM × max_group_bytes`, and Eiger frames can be
   multi-MB, so RCVHWM cannot simply be set huge. Proposed defaults:
   `channel_capacity` in the tens-to-low-hundreds of groups (the deliberate
   "capture hopelessly behind" threshold); `RCVHWM` a few hundred frames
   (256–1024) as pure jitter headroom, with the memory implication called
   out in config docs. State the invariant outright: *the recv loop never
   blocks, so RCVHWM never propagates backpressure to the detector under
   normal operation.*
4. **Idle detection ownership confirmed elsewhere.** `AbandonSeries(Timeout)`
   is **not** here — the lifecycle owns the idle timer off its own
   `last_packet` (it has the series state; the source does not). Recorded
   here only to close the `plan.md` §source open question, which had
   tentatively placed the timer in the source.

## Non-goals (worth stating)

- **Not a parser.** Header inspection is the lifecycle's job; the source
  ships opaque bytes.
- **Not the owner of series/abandon state.** Connection state ≠ series
  state. A live-but-silent peer (`Connected`, no messages) cannot tell you a
  series stalled — only the lifecycle, which tracks `Active(M)` and
  `last_packet`, can. The source surfaces transport facts (`Connected` /
  `Disconnected`); the lifecycle decides what they mean for a series.
- **Not multi-endpoint.** One PULL connection per source. Run multiple
  pipelines for multiple detectors.
- **Not a delivery-guarantee buffer.** A door drop is a real drop, surfaced
  via a seq gap and a log; the source does not retry or hold groups.
