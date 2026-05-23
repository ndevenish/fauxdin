# Spec: `PushSink`

The outbound side of the pump. Takes a stream of multipart message groups
and pushes them out a ZeroMQ PUSH socket, absorbing slow consumers with a
bounded in-process buffer.

## Scope

**In:** ZMQ PUSH transport, peer-count-aware buffering, true-backpressure
detection, per-message delivery reporting, atomic multipart preservation,
clean shutdown.

**Out (deferred to a later revision):** multi-endpoint fan-out, transport
other than ZMQ PUSH, EPICS PV control surface (the state watcher and config
fields are designed so EPICS bindings can be added later without changing
the API).

## Constraints

- Async-native `rzmq` from crates.io (0.5.x). Modern libzmq (v3.1) peers
  work against the published release. A local working copy at `./rzmq/`
  (gitignored) carries WIP ZMTP/2.0 downgrade work for legacy Eiger
  detectors — switch the dep to `path = "./rzmq/core"` when running
  against v2-only peers.
- The PUSH socket and its monitor live in tokio tasks; no `spawn_blocking`
  is used in the hot path.
- Multipart groups are atomic — the sink either sends all frames of a group
  with correct `SNDMORE` flags or drops the whole group.

## Public API

```rust
pub struct PushSink { /* opaque */ }

pub struct PushSinkConfig {
    /// Number of multipart groups the in-process buffer can hold. Hard cap.
    pub buffer_capacity: usize,
    /// ZMQ_SNDHWM applied to the PUSH socket. Must be > 0.
    pub zmq_send_hwm: i32,
    /// Cancellation token. Cancelling it (from anywhere) drains the buffer
    /// and stops the worker, identical to calling `shutdown()`. Default: a
    /// fresh token. Pass a child of a parent pipeline token when wiring
    /// into a larger process so one `cancel()` tears the whole pipeline
    /// down.
    pub cancel: CancellationToken,
}

#[derive(Clone, Debug)]
pub enum SinkState {
    /// Socket bound; no peer has connected. Buffer fills freely up to
    /// `buffer_capacity` — startup pad, not backpressure.
    WaitingForPeer { buffered: usize },
    /// At least one peer connected. Buffer-fill is observable via `buffered`;
    /// the per-message `DeliveryReport` stream is the authoritative record
    /// of which groups were actually dropped.
    Streaming { peers: usize, buffered: usize },
}

#[derive(Clone, Debug)]
pub struct DeliveryReport {
    pub seq: u64,
    pub outcome: DeliveryOutcome,
}

#[derive(Clone, Debug)]
pub enum DeliveryOutcome {
    /// All frames of the group accepted by the PUSH socket.
    Delivered,
    /// Group never reached the socket. See DropReason.
    Dropped(DropReason),
    /// Send was attempted but the socket returned a non-EAGAIN error.
    /// String is the libzmq error for diagnostics.
    SendError(String),
}

#[derive(Clone, Debug)]
pub enum DropReason {
    /// Buffer full with a peer connected — true backpressure.
    BackpressureFull,
    /// Buffer full with no peer connected — startup pad overflowed.
    PrefetchOverflow,
    /// Sink was shutting down when the message was processed.
    SinkShutdown,
}

#[derive(Debug)]
pub enum EnqueueOutcome {
    /// Accepted into the buffer. A DeliveryReport for this seq will follow.
    Enqueued,
    /// Rejected at the door. No DeliveryReport will be emitted.
    Dropped(DropReason),
    /// Sink is shutting down. No DeliveryReport will be emitted.
    ShuttingDown,
}

impl PushSink {
    /// Bind the socket, start the worker thread, and wait for both to come
    /// up. Returns once the socket is bound (so `port()` is valid) or with
    /// an error if bind fails. `endpoint` is e.g. `"tcp://0.0.0.0:9998"`
    /// or `"tcp://127.0.0.1:0"` for an ephemeral port.
    pub async fn bind(endpoint: &str, config: PushSinkConfig) -> Result<Self>;

    /// Non-blocking enqueue. Multipart groups are atomic. Never blocks the
    /// caller, never waits on the socket.
    pub fn try_send(&self, seq: u64, group: Arc<MultipartGroup>) -> EnqueueOutcome;

    /// Subscribe to delivery reports. Each `Enqueued` produces exactly one
    /// report on every active subscriber. Late subscribers do not see
    /// reports emitted before subscription; lagging subscribers see
    /// `broadcast::error::RecvError::Lagged` and must reconcile.
    pub fn delivery_reports(&self) -> broadcast::Receiver<DeliveryReport>;

    /// Live state. Updates on every variant transition and every peer-count
    /// change; buffered-count differences alone do not trigger emission.
    pub fn state(&self) -> watch::Receiver<SinkState>;

    /// Bound TCP port if `endpoint` was `tcp://`; None otherwise.
    pub fn port(&self) -> Option<u16>;

    /// Currently-effective buffer capacity. Equal to
    /// `PushSinkConfig::buffer_capacity` immediately after `bind`; reflects
    /// the last `set_buffer_capacity` call once the capacity task has
    /// applied it.
    pub fn buffer_capacity(&self) -> usize;

    /// Resize the in-process buffer at runtime. Hands the new capacity to a
    /// background task that adjusts the existing semaphore — the send and
    /// receive paths are untouched. Growing is immediate; shrinking forgets
    /// as many available permits as it can and records the remainder as
    /// debt to be paid down as in-flight items complete. Returns `Err` for
    /// `0` or if the sink is shutting down.
    pub fn set_buffer_capacity(&self, new_cap: usize) -> Result<()>;

    /// Initiate shutdown. Any messages still in the buffer that have not
    /// been handed to the PUSH socket emit `Dropped(SinkShutdown)`. The
    /// future resolves once the worker thread has joined and the socket is
    /// closed.
    pub async fn shutdown(self);
}
```

`MultipartGroup` is defined elsewhere (in `source.rs` / `messages.rs`):
ordered, non-empty sequence of frames with cheap `Arc` cloning. The sink
treats it as `&[impl AsRef<[u8]>]` and sets `SNDMORE` on all but the last
frame.

## Internal architecture

```
async caller ──try_send──→ outbox (mpsc::UnboundedSender + Semaphore)
                                    │
                                    ▼
        ┌───────────────────── worker task ───────────────────────┐
        │  tokio::spawn selects over:                             │
        │    - outbox.recv()         (new groups to send)         │
        │    - monitor_rx.recv()     (peer connect/disconnect)    │
        │    - cancel.cancelled()    (shutdown)                   │
        │                                                         │
        │  owns:                                                  │
        │    - rzmq::Socket (PUSH; cloneable handle)              │
        │    - rzmq MonitorReceiver                               │
        │    - peer_count: usize (local; reflected to state)      │
        │    - buffered: derived from semaphore                   │
        └─────────────────────────────────────────────────────────┘
```

Two concurrent tasks in total — both `tokio::spawn`, no `spawn_blocking`:

1. **Worker** owns the PUSH socket and its monitor receiver. Drives the
   send loop. Updates `SinkState`. The receiver is held as
   `Option<MonitorReceiver>` so that, once the channel closes during
   socket teardown, the worker drops the monitor arm from subsequent
   selects instead of busy-looping on a terminal `Err`.
2. **Capacity task** listens on a `watch::Receiver<usize>` and reconciles
   `current_cap` / `debt` with the shared semaphore. Does not touch the
   socket or `state`.

### Buffer / permit accounting

- `Arc<Semaphore>` with `current_cap` initial permits.
- `try_send` takes a permit synchronously (`try_acquire_owned`). If
  available, the permit is bundled into the outbox message and dropped on
  dequeue inside the worker. If unavailable, the message is rejected.
- No "force enqueue mid-multipart" path — groups are atomic, so the buffer
  unit equals the permit unit equals the drop unit.

#### Runtime resize

A `watch::Sender<usize>` (`cap_tx`) lives on the sink and is the only way
to change capacity. `set_buffer_capacity(n)` validates `n > 0` and writes
to that watch. The capacity task is the sole reader and the sole writer of
the shared `current_cap` / `debt` atomics:

- **Grow** (`new > old`): if there's outstanding shrink debt, pay it down
  first by decrementing `debt`. Any remainder is handed to
  `Semaphore::add_permits`.
- **Shrink** (`new < old`): call `Semaphore::forget_permits(old - new)`,
  which removes only the *available* permits and returns the count
  actually forgotten. Any shortfall is added to `debt`. While `debt > 0`,
  the capacity task also waits on `Semaphore::acquire_owned`; each permit
  returned by a completed send is forgotten and decrements `debt` by one.
- `current_cap` is written last on every change so observers see a
  consistent denominator for `buffered` (see below).

The send path (`try_send` and the worker's send loop) never sees any of
this — it just observes the semaphore the capacity task has shaped.

### State derivation

State is computed from two inputs:
- `peer_count`: incremented on `ZMQ_EVENT_ACCEPTED`, decremented on
  `ZMQ_EVENT_DISCONNECTED` (server side of `tcp://`). Clamped at zero.
- `buffered`: `(current_cap + debt) - semaphore.available_permits()`. The
  `+ debt` accounts for slots that have been forgotten from the semaphore
  to honour an in-progress shrink but still hold a real in-flight item.

Transitions:

| current               | event              | next               |
|---|---|---|
| `WaitingForPeer`      | peer_count → ≥ 1   | `Streaming`        |
| `Streaming`           | peer_count → 0     | `WaitingForPeer`   |

The state watcher emits on variant transitions and on peer-count changes
within `Streaming`. It does NOT emit on every buffered-level change — the
authoritative per-message signal is `DeliveryReport`, not the state watch.
Subscribers that want to react to buffer fill should read `buffered`
directly when they receive a state update and apply their own threshold.

### Drop policy

On `try_send` with no permit available:

| peer_count | Returned                          |
|---|---|
| ≥ 1        | `Dropped(BackpressureFull)`       |
| 0          | `Dropped(PrefetchOverflow)`       |

Drop is always at the door, never silent. The producer always learns
immediately. No DeliveryReport is emitted for door drops — the
`EnqueueOutcome` is the report.

Drops *after* a permit is granted only happen on shutdown
(`Dropped(SinkShutdown)`) or transport error (`SendError`). Both produce
DeliveryReports.

### Send loop

For each `(seq, group, permit)` pulled from the outbox:

1. Reassemble the stored `Bytes` frames into a `Vec<rzmq::Msg>` (cheap —
   `Bytes::clone` is a refcount bump).
2. Call `socket.send_multipart(frames).await`, racing it against
   `cancel.cancelled()`. The send future is cancel-safe per rzmq's
   contract: dropping it does not corrupt the actor's state.
3. `Ok(())`: emit `Delivered(seq)`, drop permit.
4. `Err(_)`: emit `SendError(e)`, drop permit. **Never panic.**
5. Cancellation: emit `Dropped(SinkShutdown)`, drop permit.

`SNDTIMEO` is left at its default (`None`) so `send_multipart` blocks the
worker until the peer is connected and has accepted every frame. The
worker holds its permit for the duration, so a slow or absent peer
naturally fills the front buffer and surfaces drop reasons to `try_send`
without ever queueing a partial multipart on the wire. **Do not set
`SNDTIMEO = 0`**: rzmq propagates the same value to the session's TCP
`write_all` timeout, and a zero-duration write timeout converts every
slow peer into a fatal session error and tears the connection down.

### Shutdown

`shutdown()`:

1. Cancel the worker's cancellation token.
2. Worker stops calling `outbox.recv()`, drains remaining messages with
   `try_recv`, emits `Dropped(SinkShutdown)` for each, exits.
3. `shutdown()` joins the worker and capacity tasks, calls
   `socket.close().await`, then `Context::term().await` to drain any
   remaining rzmq actors.

`Drop` on `PushSink` cancels the token but does not await join or call
`ctx.term()`. Use `shutdown()` for clean exit; use `Drop` for panics.

## Invariants

1. **No panic on send error.** Any libzmq error other than `EAGAIN`
   produces a `SendError` report and continues. The worker thread does not
   exit on transport errors; only on shutdown or fatal channel closure.
2. **Multipart atomicity.** A group is either fully sent (all frames with
   correct `SNDMORE` flags, in order) or not sent at all. The sink will
   not interleave frames from different groups.
3. **Permit conservation.** Every `Enqueued` consumes exactly one permit.
   Every dequeue, drop, or shutdown returns exactly one permit. There is
   no "force enqueue" path. `set_buffer_capacity` may forget returned
   permits (recording shortfall as `debt`) but never invents new ones —
   `try_send` cannot observe a phantom slot.
4. **Exactly-one report per Enqueued.** For each `try_send` that returns
   `Enqueued`, exactly one `DeliveryReport` is emitted to each subscriber
   that is alive at emission time. (`broadcast::Receiver::Lagged` is the
   subscriber's problem.)
5. **State is monotone wrt peer events.** `peer_count` cannot go negative.
   A `DISCONNECTED` without a matching `ACCEPTED` is logged and ignored.

## Errors

- `bind()` returns `Err` for: invalid endpoint, port in use, libzmq init
  failure, monitor setup failure. Workers are not started on bind failure.
- All post-bind errors are reported via `DeliveryReport::SendError` or
  state-channel updates. The sink stays alive until explicit shutdown.

## Test surface

Required test coverage before this is considered done:

- **Bind error path:** invalid endpoint → `bind()` returns `Err`, no
  threads leaked.
- **Bind success:** ephemeral port → `port()` returns Some.
- **Pre-peer buffering:** push `buffer_capacity` groups with no peer →
  all return `Enqueued`, state remains `WaitingForPeer { buffered: N }`,
  one more push returns `Dropped(PrefetchOverflow)`.
- **Connect transitions state:** push some groups, connect a peer →
  state transitions to `Streaming`, buffered drains.
- **Multipart preservation:** push a 4-frame group → peer receives 4
  frames with correct `SNDMORE` flags and original order.
- **True backpressure:** connect peer that doesn't drain, fill buffer →
  subsequent pushes return `Dropped(BackpressureFull)` while state stays
  `Streaming`.
- **Delivery reports:** every `Enqueued` produces exactly one report;
  drops at the door produce zero reports.
- **Peer disconnect mid-stream:** disconnect peer mid-send → state goes
  `WaitingForPeer`, in-flight group is reported (TBD outcome).
- **Shutdown drains:** push N groups, immediately `shutdown()` → all N
  unsent groups emit `Dropped(SinkShutdown)`, threads join.
- **Concurrent senders:** N tasks each call `try_send` → permit count
  never exceeds `buffer_capacity`, no double-emit, no lost report.
- **Runtime resize — validation:** `set_buffer_capacity(0)` → `Err`,
  cap unchanged.
- **Runtime resize — grow:** fill to original cap, observe drop; grow
  past it; subsequent `try_send`s up to the new cap succeed.
- **Runtime resize — shrink within occupancy:** with `M < N` items in
  flight, shrink to a value above `M`; remaining headroom matches
  `new_cap - M`; no debt persists.
- **Runtime resize — shrink below occupancy:** shrink to less than
  current occupancy; new sends rejected until a peer drains the buffer
  and debt is paid down; then exactly `new_cap` slots reopen.

## Open questions

1. **Peer-lost mid-group.** Should a group that was being retried when the
   last peer disconnected emit `Dropped(BackpressureFull)`,
   `Dropped(PeerLost)`, or `SendError("peer disconnected")`? Leaning
   toward a dedicated `PeerLost` variant — it's a different operational
   signal from "buffer full."
2. **EAGAIN retry cap.** Currently unbounded. Realistic worst case is a
   peer that connects but never reads. Either bound the retries with a
   timeout (group is then dropped with `PeerLost`) or rely on operator to
   shut down. Leaning toward a config field `send_retry_max: Duration`.
3. **Monitor reader resilience.** What if the monitor PAIR errors? Today
   we'd lose peer events and state would freeze. Either reopen the
   monitor or treat as fatal and shut the worker down.
4. **`broadcast` channel size for delivery reports.** Too small → slow
   subscribers see `Lagged`. Too large → memory. Default 4096; expose as
   config?

## Non-goals (worth stating)

- **Not a retry buffer for delivery guarantees.** Dropped messages are
  dropped. The capture path is responsible for durable recording.
- **Not multi-endpoint.** One PUSH endpoint per sink. Run multiple sinks
  for fan-out.
- **Not order-preserving across drops.** Within a stream of successful
  sends, order is preserved. Drops are not "held" for later delivery.
