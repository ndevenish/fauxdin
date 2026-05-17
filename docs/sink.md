# Spec: `PushSink`

The outbound side of the pump. Takes a stream of multipart message groups
and pushes them out a ZeroMQ PUSH socket, absorbing slow consumers with a
bounded in-process buffer.

## Scope

**In:** ZMQ PUSH transport, peer-count-aware buffering, true-backpressure
detection, per-message delivery reporting, atomic multipart preservation,
clean shutdown.

**Out (deferred to a later revision):** runtime buffer resize, multi-endpoint
fan-out, transport other than ZMQ PUSH, EPICS PV control surface (the state
watcher and config fields are designed so EPICS bindings can be added later
without changing the API).

## Constraints

- Synchronous `zmq = "0.10"` crate (libzmq binding). `rzmq` is not viable —
  it doesn't support the ZeroMQ protocol versions the detector speaks.
- The PUSH socket and its monitor must run on a dedicated OS thread; the
  rest of the type is async (tokio).
- Multipart groups are atomic — the sink either sends all frames of a group
  with correct `SNDMORE` flags or drops the whole group.

## Public API

```rust
pub struct PushSink { /* opaque */ }

pub struct PushSinkConfig {
    /// e.g. "tcp://0.0.0.0:9998" or "tcp://*:*" for ephemeral port.
    pub endpoint: String,
    /// Number of multipart groups the in-process buffer can hold. Hard cap.
    pub buffer_capacity: usize,
    /// ZMQ_SNDHWM applied to the PUSH socket. Must be > 0.
    pub zmq_send_hwm: i32,
    /// Backoff between retries when libzmq returns `EAGAIN` on `send`.
    /// The worker sends with `DONTWAIT` so it stays reactive to cancel
    /// and peer-monitor events; on `EAGAIN` it sleeps this interval
    /// before retrying, also waking on cancel or peer disconnect during
    /// the sleep. The sync `zmq` crate has no POLLOUT-style hook that
    /// composes with tokio, so a timed retry is the pragmatic
    /// alternative — adds at most one interval of latency per retry.
    pub send_retry_interval: Duration,  // e.g. 50ms
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
    /// an error if bind fails.
    pub async fn bind(config: PushSinkConfig) -> Result<Self>;

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
        ┌───────────────────── worker thread ─────────────────────┐
        │  current-thread tokio runtime selects over:             │
        │    - outbox.recv()         (new groups to send)         │
        │    - monitor_rx.recv()     (peer connect/disconnect)    │
        │    - cancel.cancelled()    (shutdown)                   │
        │                                                         │
        │  owns:                                                  │
        │    - zmq::Socket (PUSH)                                 │
        │    - zmq::Socket (PAIR, reading socket_monitor events)  │
        │    - peer_count: usize (local; reflected to state)      │
        │    - buffered: derived from semaphore                   │
        └─────────────────────────────────────────────────────────┘
```

Two threads in total:

1. **Worker thread** (`spawn_blocking` running a current-thread runtime).
   Owns the PUSH socket. Drives the send loop. Updates `SinkState`.
2. **Monitor reader** (`spawn_blocking` with a plain loop). Opens the
   inproc PAIR for `zmq_socket_monitor` and forwards parsed events into a
   tokio `mpsc` consumed by the worker.

The monitor reader is its own thread because libzmq's monitor PAIR is read
synchronously, and we don't want to interleave it with the send loop. It's
small and dies on socket close.

### Buffer / permit accounting

- `Arc<Semaphore>` with `buffer_capacity` permits.
- `try_send` takes a permit synchronously (`try_acquire_owned`). If
  available, the permit is bundled into the outbox message and dropped on
  dequeue inside the worker. If unavailable, the message is rejected.
- No "force enqueue mid-multipart" path — groups are atomic, so the buffer
  unit equals the permit unit equals the drop unit.
- No runtime resize in v1. When EPICS control is added, the only addition
  is a `watch::Receiver<usize>` that a separate task uses to call
  `add_permits` / `forget_permits` on the same semaphore. The send and
  receive paths do not change.

### State derivation

State is computed from two inputs:
- `peer_count`: incremented on `ZMQ_EVENT_ACCEPTED`, decremented on
  `ZMQ_EVENT_DISCONNECTED` (server side of `tcp://`). Clamped at zero.
- `buffered`: `buffer_capacity - semaphore.available_permits()`.

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

1. For each frame `f` in the group except the last:
   `socket.send(f, DONTWAIT | SNDMORE)`. On `EAGAIN`, sleep
   `send_retry_interval` and retry. On other error, abort this group, emit
   `SendError`, drop permit. **Never panic.**
2. For the last frame: same, without `SNDMORE`.
3. If all frames sent: emit `Delivered(seq)`, drop permit.
4. If cancelled mid-group: emit `Dropped(SinkShutdown)`, drop permit.
   (Note: this may leave a partial multipart on the wire from libzmq's
   perspective. ZMQ documents that closing a PUSH socket mid-multipart
   discards the partial; we rely on that.)

`EAGAIN` retry has no hard cap in v1. A peer that disconnects mid-group
will produce DISCONNECTED on the monitor; the send loop checks
`peer_count == 0` between retries and, if so, drops the in-flight group
with `Dropped(BackpressureFull)` (or a dedicated `PeerLost` variant — TBD,
see open questions).

### Shutdown

`shutdown()`:

1. Cancel the worker's cancellation token.
2. Worker stops calling `outbox.recv()`, drains remaining messages with
   `try_recv`, emits `Dropped(SinkShutdown)` for each, closes both
   sockets, exits.
3. Monitor reader thread exits when its PAIR socket closes.
4. `shutdown()` future joins both threads and resolves.

`Drop` on `PushSink` cancels the token but does not await join. Use
`shutdown()` for clean exit; use `Drop` for panics.

## Invariants

1. **No panic on send error.** Any libzmq error other than `EAGAIN`
   produces a `SendError` report and continues. The worker thread does not
   exit on transport errors; only on shutdown or fatal channel closure.
2. **Multipart atomicity.** A group is either fully sent (all frames with
   correct `SNDMORE` flags, in order) or not sent at all. The sink will
   not interleave frames from different groups.
3. **Permit conservation.** Every `Enqueued` consumes exactly one permit.
   Every dequeue, drop, or shutdown returns exactly one permit. There is
   no "debt" or "force enqueue" path.
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
