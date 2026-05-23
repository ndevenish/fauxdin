//! Buffered PUSH socket with peer-count-aware true-backpressure detection
//! and per-group delivery reporting.
//!
//! See `docs/sink.md` for the full component spec.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use rzmq::socket::options as zmq_opts;
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Context, Msg, Socket, SocketType};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use url::Url;

use crate::messages::{MultipartGroup, Seq};

const DELIVERY_BROADCAST_CAPACITY: usize = 4096;
const MONITOR_CAPACITY: usize = 256;

// ============================================================================
// Public configuration and reporting types
// ============================================================================

#[derive(Debug, Clone)]
pub struct PushSinkConfig {
    /// The total allowed enqueued messages. The actual number of
    /// currently buffered messages may be higher than this, if it was
    /// resized while above the new capacity.
    pub buffer_capacity: usize,
    /// In addition to this buffer, the underlying PUSH socket is given a
    /// send-side high water mark of `zmq_send_hwm`, so the maximum number of
    /// messages that can be queued downstream of `try_send` is roughly
    /// `buffer_capacity + zmq_send_hwm`, depending on whether the socket is
    /// internally buffering (it does not do this before a peer is connected,
    /// for instance). A value of zero or less is rejected — if you truly
    /// want an unlimited buffer, this socket wrapper is unnecessary.
    pub zmq_send_hwm: i32,
    /// Reserved for future use. The current implementation does not poll
    /// or retry — the worker awaits `send_multipart` (which blocks until
    /// the peer is connected and accepts the message) and races it only
    /// against [`Self::cancel`]. The field is kept on the public config so
    /// callers that have it set don't need to change, and so a future
    /// timeout-based variant can wire it back in without an API break.
    pub send_retry_interval: Duration,
    /// Cancellation token. The sink clones this and stores it; cancelling it
    /// (from anywhere) drains the buffer and stops the worker, identically to
    /// calling [`PushSink::shutdown`]. Default: a fresh token owned only by
    /// this sink. Pass a child of a parent pipeline token when wiring into a
    /// larger process so one `cancel()` tears the whole pipeline down.
    pub cancel: CancellationToken,
}

impl Default for PushSinkConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 500,
            zmq_send_hwm: 50,
            send_retry_interval: Duration::from_millis(50),
            cancel: CancellationToken::new(),
        }
    }
}

impl PushSinkConfig {
    fn validate(&self) -> Result<()> {
        if self.buffer_capacity == 0 {
            return Err(anyhow!("buffer_capacity must be > 0"));
        }
        if self.zmq_send_hwm <= 0 {
            return Err(anyhow!("zmq_send_hwm must be > 0"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkState {
    WaitingForPeer { buffered: usize },
    Streaming { peers: usize, buffered: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryReport {
    pub seq: Seq,
    pub outcome: DeliveryOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered,
    Dropped(DropReason),
    SendError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    BackpressureFull,
    PrefetchOverflow,
    SinkShutdown,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Enqueued,
    Dropped(DropReason),
    ShuttingDown,
}

// ============================================================================
// PushSink
// ============================================================================

/// Outbound side of the pump: a ZeroMQ PUSH socket fed by a bounded
/// in-process buffer of multipart groups.
///
/// # What it does
///
/// [`try_send`](Self::try_send) hands a [`MultipartGroup`] to an async
/// worker task that owns the underlying rzmq PUSH socket and writes every
/// frame out as one logical multipart message. Groups are atomic: either
/// every frame reaches the socket or the whole group is dropped — never a
/// half-sent group. Each accepted group eventually produces exactly one
/// [`DeliveryReport`] on the [`delivery_reports`](Self::delivery_reports)
/// broadcast channel, tagged with the caller's [`Seq`] so upstream can
/// reconcile what made it through.
///
/// # Why it exists (and isn't just a raw PUSH socket)
///
/// Raw ZMQ PUSH has two properties that don't fit this pipeline:
///
/// 1. **No pre-connection buffering.** ZeroMQ's send-side HWM only
///    applies once a peer has connected; frames sent before that are
///    dropped (in libzmq) or block the sender (in rzmq). The detector can
///    start streaming at any time, so we need a startup pad that absorbs
///    frames until a downstream consumer actually attaches.
/// 2. **HWM-full and no-peer look identical to the sender.** A blocked
///    send doesn't tell you *why* — full peer buffer or no peer at all.
///    That distinction matters here: pre-peer overflow is a startup-pad
///    problem, peer-connected overflow is real backpressure, and they
///    should be reported differently and may drive different policies
///    upstream.
///
/// `PushSink` wraps the socket with a semaphore-bounded buffer in front
/// and consumes the socket's monitor stream so peer count is always
/// known. Drops are classified as
/// [`PrefetchOverflow`](DropReason::PrefetchOverflow) (no peer yet) or
/// [`BackpressureFull`](DropReason::BackpressureFull) (peer connected,
/// can't keep up), and [`state`](Self::state) exposes the
/// `WaitingForPeer` / `Streaming` distinction as a `watch` channel.
///
/// # Threading
///
/// The sink is fully tokio-native — two async tasks back it:
///
/// 1. A worker (`tokio::spawn`) owning the rzmq PUSH socket and its
///    monitor receiver; runs the send loop and updates `SinkState`.
/// 2. A capacity task (`tokio::spawn`) that reconciles `current_cap` /
///    `debt` with the shared semaphore on `set_buffer_capacity`.
///
/// # Shutdown
///
/// Drop or [`shutdown`](Self::shutdown) cancels the worker. Any groups
/// still in the buffer report
/// [`Dropped(SinkShutdown)`](DropReason::SinkShutdown). `shutdown` awaits
/// every task and terminates the rzmq context; `Drop` just signals and
/// lets the tasks exit.
pub struct PushSink {
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<WorkItem>,
    permits: Arc<Semaphore>,
    peers: Arc<AtomicUsize>,
    state_rx: watch::Receiver<SinkState>,
    reports_tx: broadcast::Sender<DeliveryReport>,
    worker: Option<JoinHandle<()>>,
    capacity: Option<JoinHandle<()>>,
    port: Option<u16>,
    /// Currently-effective buffer capacity. Driven by the capacity task in
    /// response to `set_buffer_capacity` writes on `cap_tx`. The semaphore
    /// is the source of truth for whether a send is admitted; this is for
    /// reporting and for `buffered()` computation.
    current_cap: Arc<AtomicUsize>,
    /// Outstanding shrink debt: permits that still need to be forgotten to
    /// realise a capacity reduction that overlapped in-flight items. Drained
    /// by the capacity task as permits are returned.
    debt: Arc<AtomicUsize>,
    cap_tx: watch::Sender<usize>,
    shutting_down: Arc<AtomicBool>,
    socket: Socket,
    ctx: Context,
}

impl PushSink {
    /// Bind an rzmq PUSH socket, given buffering configuration.
    ///
    /// Regardless of ZeroMQ HWM settings or whether the socket is being
    /// actively drained by an external client, a minimum `buffer_capacity`
    /// items can be buffered inside the [`PushSink`]. This quantity can be
    /// adjusted at run-time without reopening the socket, upon which the
    /// number of queued messages can exceed the buffer length.
    ///
    /// In addition to this buffer, the rzmq PUSH socket is configured with
    /// a send-side high water mark `zmq_send_hwm`, so the maximum number of
    /// messages that can be buffered downstream of `try_send` is roughly
    /// `buffer_capacity + zmq_send_hwm`, depending on whether the socket
    /// is internally buffering (it does not do this before a peer is
    /// connected, for instance).
    ///
    /// Returns once the socket is bound (so [`port`](Self::port) is valid)
    /// and all backing tasks have been started.
    pub async fn bind(endpoint: &str, config: PushSinkConfig) -> Result<Self> {
        config.validate()?;

        let ctx = Context::new().map_err(|e| anyhow!("rzmq context creation failed: {e}"))?;
        match Self::bind_inner(endpoint, config, ctx.clone()).await {
            Ok(sink) => Ok(sink),
            Err(e) => {
                let _ = ctx.term().await;
                Err(e)
            }
        }
    }

    async fn bind_inner(endpoint: &str, config: PushSinkConfig, ctx: Context) -> Result<Self> {
        let socket = ctx
            .socket(SocketType::Push)
            .map_err(|e| anyhow!("socket creation failed: {e}"))?;
        socket
            .set_option(zmq_opts::SNDHWM, config.zmq_send_hwm)
            .await
            .map_err(|e| anyhow!("set_sndhwm failed: {e}"))?;
        // Note on SNDTIMEO: rzmq uses the same option for both "wait for
        // peer / pipe slot" (the user-facing semantics) and the underlying
        // TCP `write_all` timeout in `data_io.rs`. Setting it to zero would
        // make TCP writes time out immediately on any peer that can't drain
        // at line rate, which rzmq treats as a fatal session error and
        // tears the connection down. Leaving it at the default (`None`) means
        // the worker awaits `send_multipart` until the peer connects and
        // accepts the bytes; the cancellation token is the only thing that
        // unblocks it. Backpressure is observed at the door instead: the
        // worker holds its permit while it's awaiting `send_multipart`, so
        // a slow peer naturally fills the front buffer and surfaces
        // `BackpressureFull` / `PrefetchOverflow` to `try_send`.
        // LINGER=0: on shutdown, drop unsent frames instead of blocking the
        // context teardown.
        socket
            .set_option(zmq_opts::LINGER, 0i32)
            .await
            .map_err(|e| anyhow!("set_linger failed: {e}"))?;

        let rzmq_monitor = socket
            .monitor(MONITOR_CAPACITY)
            .await
            .map_err(|e| anyhow!("monitor setup failed: {e}"))?;

        socket
            .bind(endpoint)
            .await
            .map_err(|e| anyhow!("bind to {endpoint} failed: {e}"))?;
        let port = read_port(&socket).await;

        let cancel = config.cancel.clone();
        let permits = Arc::new(Semaphore::new(config.buffer_capacity));
        let peers = Arc::new(AtomicUsize::new(0));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let current_cap = Arc::new(AtomicUsize::new(config.buffer_capacity));
        let debt = Arc::new(AtomicUsize::new(0));
        let (tx, outbox_rx) = mpsc::unbounded_channel::<WorkItem>();
        let (reports_tx, _) = broadcast::channel(DELIVERY_BROADCAST_CAPACITY);
        let (state_tx, state_rx) = watch::channel(SinkState::WaitingForPeer { buffered: 0 });
        let (cap_tx, cap_rx) = watch::channel(config.buffer_capacity);

        let worker = {
            let socket = socket.clone();
            let cancel = cancel.clone();
            let permits = permits.clone();
            let peers = peers.clone();
            let reports = reports_tx.clone();
            let shutting_down = shutting_down.clone();
            let current_cap = current_cap.clone();
            let debt = debt.clone();
            let config = config.clone();
            tokio::spawn(async move {
                let worker = Worker {
                    config,
                    socket,
                    cancel,
                    permits,
                    reports,
                    state_tx,
                    monitor_rx: Some(rzmq_monitor),
                    outbox: outbox_rx,
                    peers_atomic: peers,
                    shutting_down,
                    peers: 0,
                    current_cap,
                    debt,
                };
                worker.run().await;
            })
        };

        let capacity = {
            let permits = permits.clone();
            let cancel = cancel.clone();
            let current_cap = current_cap.clone();
            let debt = debt.clone();
            tokio::spawn(async move {
                capacity_task(cap_rx, permits, current_cap, debt, cancel).await;
            })
        };

        Ok(Self {
            cancel,
            tx,
            permits,
            peers,
            state_rx,
            reports_tx,
            worker: Some(worker),
            capacity: Some(capacity),
            port,
            current_cap,
            debt,
            cap_tx,
            shutting_down,
            socket,
            ctx,
        })
    }

    /// Non-blocking enqueue. Multipart groups are atomic. Never blocks.
    pub fn try_send(&self, seq: Seq, group: Arc<MultipartGroup>) -> EnqueueOutcome {
        if self.shutting_down.load(Ordering::Acquire) {
            return EnqueueOutcome::ShuttingDown;
        }
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let reason = if self.peers.load(Ordering::Acquire) > 0 {
                    DropReason::BackpressureFull
                } else {
                    DropReason::PrefetchOverflow
                };
                return EnqueueOutcome::Dropped(reason);
            }
        };
        let work = WorkItem { seq, group, permit };
        if self.tx.send(work).is_err() {
            return EnqueueOutcome::ShuttingDown;
        }
        EnqueueOutcome::Enqueued
    }

    /// Subscribe to delivery reports. Each `Enqueued` produces exactly one
    /// report per active subscriber.
    pub fn delivery_reports(&self) -> broadcast::Receiver<DeliveryReport> {
        self.reports_tx.subscribe()
    }

    /// Live state. Updates on every variant transition and every peer-count
    /// change; not on every buffer-level change.
    pub fn state(&self) -> watch::Receiver<SinkState> {
        self.state_rx.clone()
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Currently-effective buffer capacity. May differ from
    /// `PushSinkConfig::buffer_capacity` if `set_buffer_capacity` has been
    /// called since `bind`.
    pub fn buffer_capacity(&self) -> usize {
        self.current_cap.load(Ordering::Acquire)
    }

    /// Resize the in-process buffer. Implemented as a hand-off to the
    /// background capacity task — the send path is untouched.
    ///
    /// Growing is immediate (extra permits are added to the semaphore).
    /// Shrinking forgets as many available permits as it can; any
    /// outstanding overshoot becomes `debt` that is paid off opportunistically
    /// as in-flight items complete and release their permits.
    pub fn set_buffer_capacity(&self, new_cap: usize) -> Result<()> {
        if new_cap == 0 {
            return Err(anyhow!("buffer_capacity must be > 0"));
        }
        // Channel can only "fail" if all receivers are gone — i.e. capacity
        // task has exited. Treat that as "sink is shutting down".
        if self.cap_tx.send(new_cap).is_err() {
            return Err(anyhow!("sink is shutting down"));
        }
        Ok(())
    }

    /// Initiate shutdown. Drains the buffer, emitting `Dropped(SinkShutdown)`
    /// for each undelivered message. Resolves once every background task has
    /// joined and the rzmq context has terminated.
    pub async fn shutdown(mut self) {
        self.shutting_down.store(true, Ordering::Release);
        self.cancel.cancel();
        if let Some(h) = self.worker.take() {
            let _ = h.await;
        }
        if let Some(h) = self.capacity.take() {
            let _ = h.await;
        }
        let _ = self.socket.close().await;
        let _ = self.ctx.term().await;
    }
}

impl Drop for PushSink {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Release);
        self.cancel.cancel();
    }
}

// ============================================================================
// Internals
// ============================================================================

struct WorkItem {
    seq: Seq,
    group: Arc<MultipartGroup>,
    /// RAII handle on a buffer slot. Dropped after the send completes or is
    /// abandoned, returning the slot to the semaphore.
    permit: OwnedSemaphorePermit,
}

async fn read_port(socket: &Socket) -> Option<u16> {
    let bytes = socket.get_option(zmq_opts::LAST_ENDPOINT).await.ok()?;
    let endpoint = String::from_utf8(bytes).ok()?;
    if endpoint.is_empty() {
        return None;
    }
    Url::parse(&endpoint).ok()?.port()
}

fn compute_state(peers: usize, buffered: usize) -> SinkState {
    if peers == 0 {
        SinkState::WaitingForPeer { buffered }
    } else {
        SinkState::Streaming { peers, buffered }
    }
}

/// Returns true if the two states differ in a way that should trigger a
/// state-watch emission: variant change, or peer-count change within the
/// same variant. Buffered-count differences alone do NOT trigger emission.
fn state_significantly_differs(a: &SinkState, b: &SinkState) -> bool {
    use SinkState::*;
    match (a, b) {
        (WaitingForPeer { .. }, WaitingForPeer { .. }) => false,
        (Streaming { peers: p1, .. }, Streaming { peers: p2, .. }) => p1 != p2,
        _ => true,
    }
}

struct Worker {
    config: PushSinkConfig,
    socket: Socket,
    cancel: CancellationToken,
    permits: Arc<Semaphore>,
    reports: broadcast::Sender<DeliveryReport>,
    state_tx: watch::Sender<SinkState>,
    /// rzmq's monitor channel. `recv(&self)` returns a cancel-safe future,
    /// so we `select!` on it directly. Set to `None` once the channel
    /// closes (which happens when the socket actor is torn down) so the
    /// select loop drops the monitor arm and doesn't busy-loop on a
    /// terminal `Err`.
    monitor_rx: Option<MonitorReceiver>,
    outbox: mpsc::UnboundedReceiver<WorkItem>,
    peers_atomic: Arc<AtomicUsize>,
    shutting_down: Arc<AtomicBool>,
    peers: usize,
    current_cap: Arc<AtomicUsize>,
    debt: Arc<AtomicUsize>,
}

enum WorkerAction {
    Break,
    Monitor(SocketEvent),
    MonitorClosed,
    Work(WorkItem),
}

impl Worker {
    async fn run(mut self) {
        loop {
            let cancel = self.cancel.clone();
            let action = match self.monitor_rx.as_ref() {
                Some(mon_rx) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => WorkerAction::Break,
                        ev = mon_rx.recv() => match ev {
                            Ok(e) => WorkerAction::Monitor(e),
                            Err(_) => WorkerAction::MonitorClosed,
                        },
                        work = self.outbox.recv() => match work {
                            Some(w) => WorkerAction::Work(w),
                            None => WorkerAction::Break,
                        },
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => WorkerAction::Break,
                        work = self.outbox.recv() => match work {
                            Some(w) => WorkerAction::Work(w),
                            None => WorkerAction::Break,
                        },
                    }
                }
            };

            match action {
                WorkerAction::Break => break,
                WorkerAction::MonitorClosed => self.monitor_rx = None,
                WorkerAction::Monitor(ev) => {
                    self.handle_monitor(ev);
                    self.emit_state();
                }
                WorkerAction::Work(WorkItem { seq, group, permit }) => {
                    let outcome = self.send_group(&group).await;
                    drop(permit);
                    let _ = self.reports.send(DeliveryReport { seq, outcome });
                }
            }
        }
        // Drain outstanding work as SinkShutdown
        while let Ok(item) = self.outbox.try_recv() {
            let WorkItem { seq, permit, .. } = item;
            drop(permit);
            let _ = self.reports.send(DeliveryReport {
                seq,
                outcome: DeliveryOutcome::Dropped(DropReason::SinkShutdown),
            });
        }
        self.shutting_down.store(true, Ordering::Release);
        debug!("sink worker exiting");
    }

    fn handle_monitor(&mut self, ev: SocketEvent) {
        match ev {
            SocketEvent::HandshakeSucceeded { .. } => self.peers += 1,
            SocketEvent::Disconnected { .. } => self.peers = self.peers.saturating_sub(1),
            _ => {}
        }
        self.peers_atomic.store(self.peers, Ordering::Release);
    }

    fn buffered(&self) -> usize {
        // After a shrink, some "consumed" slots are accounted as `debt` rather
        // than as outstanding permits, so the effective denominator is
        // `current_cap + debt`.
        let cap = self.current_cap.load(Ordering::Acquire);
        let debt = self.debt.load(Ordering::Acquire);
        (cap + debt).saturating_sub(self.permits.available_permits())
    }

    fn emit_state(&self) {
        let new = compute_state(self.peers, self.buffered());
        self.state_tx.send_if_modified(|cur| {
            if state_significantly_differs(cur, &new) {
                *cur = new;
                true
            } else {
                false
            }
        });
    }

    async fn send_group(&mut self, group: &MultipartGroup) -> DeliveryOutcome {
        let n = group.frames.len();
        if n == 0 {
            return DeliveryOutcome::SendError("empty MultipartGroup".to_string());
        }
        let frames: Vec<Msg> = group
            .frames
            .iter()
            .map(|b| Msg::from_bytes(b.clone()))
            .collect();

        // With SNDTIMEO unset, `send_multipart` blocks until the peer is
        // connected and has accepted every frame, or returns
        // `HostUnreachable` if the chosen peer was lost mid-send. The send
        // future is cancel-safe (single mailbox + oneshot), so racing it
        // against the cancellation token is sound.
        let cancel = self.cancel.clone();
        let res = tokio::select! {
            biased;
            _ = cancel.cancelled() => return DeliveryOutcome::Dropped(DropReason::SinkShutdown),
            res = self.socket.send_multipart(frames) => res,
        };

        match res {
            Ok(()) => {
                trace!("sink delivered {} frames", n);
                DeliveryOutcome::Delivered
            }
            Err(e) => {
                warn!("sink send error: {e}");
                DeliveryOutcome::SendError(format!("{e}"))
            }
        }
    }
}

/// Owns the live `current_cap` and `debt` atomics; reconciles them with the
/// underlying semaphore when `cap_rx` changes, and drains shrink-debt as
/// permits come back. The send and receive paths never touch any of this —
/// they only see the semaphore state that this task arranges.
async fn capacity_task(
    mut cap_rx: watch::Receiver<usize>,
    permits: Arc<Semaphore>,
    current_cap: Arc<AtomicUsize>,
    debt: Arc<AtomicUsize>,
    cancel: CancellationToken,
) {
    // Receiver starts at version 0, same as the sender's initial version, so
    // `changed()` only fires on real sends. Crucially, we do NOT call
    // `mark_unchanged()` — a `set_buffer_capacity` call that lands between
    // `bind` returning and this task's first poll must still wake us, and
    // `mark_unchanged` would silently consume that wake-up.
    loop {
        let outstanding = debt.load(Ordering::Acquire);
        if outstanding > 0 {
            // Race the next cap change against a permit becoming available so
            // we can forget it and pay down debt one slot at a time.
            let acquire = permits.clone().acquire_owned();
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                changed = cap_rx.changed() => {
                    if changed.is_err() { break; }
                    let new_cap = *cap_rx.borrow_and_update();
                    apply_cap_change(&permits, &current_cap, &debt, new_cap);
                }
                permit = acquire => {
                    match permit {
                        Ok(p) => {
                            p.forget();
                            debt.fetch_sub(1, Ordering::AcqRel);
                        }
                        Err(_) => break,
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                changed = cap_rx.changed() => {
                    if changed.is_err() { break; }
                    let new_cap = *cap_rx.borrow_and_update();
                    apply_cap_change(&permits, &current_cap, &debt, new_cap);
                }
            }
        }
    }
    debug!("sink capacity task exiting");
}

/// Apply a single capacity change. Grows hand `add_permits` to the semaphore
/// (after first paying down any existing debt). Shrinks attempt
/// `forget_permits` for the whole delta and record any unmet portion as new
/// debt for the task loop to drain later.
fn apply_cap_change(
    permits: &Arc<Semaphore>,
    current_cap: &Arc<AtomicUsize>,
    debt: &Arc<AtomicUsize>,
    new_cap: usize,
) {
    if new_cap == 0 {
        // Validation in `set_buffer_capacity` prevents this; defend anyway.
        return;
    }
    let old_cap = current_cap.load(Ordering::Acquire);
    if new_cap == old_cap {
        return;
    }
    if new_cap > old_cap {
        let mut to_add = new_cap - old_cap;
        let cur_debt = debt.load(Ordering::Acquire);
        if cur_debt > 0 {
            let pay = cur_debt.min(to_add);
            debt.fetch_sub(pay, Ordering::AcqRel);
            to_add -= pay;
        }
        if to_add > 0 {
            permits.add_permits(to_add);
        }
    } else {
        let want = old_cap - new_cap;
        let forgotten = permits.forget_permits(want);
        let unmet = want - forgotten;
        if unmet > 0 {
            debt.fetch_add(unmet, Ordering::AcqRel);
        }
    }
    current_cap.store(new_cap, Ordering::Release);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::time::Instant;
    use tokio_util::bytes::Bytes;

    fn group(frames: &[&[u8]]) -> Arc<MultipartGroup> {
        Arc::new(MultipartGroup {
            frames: frames.iter().map(|f| Bytes::copy_from_slice(f)).collect(),
        })
    }

    const TEST_ENDPOINT: &str = "tcp://127.0.0.1:0";

    fn test_config() -> PushSinkConfig {
        PushSinkConfig {
            buffer_capacity: 10,
            zmq_send_hwm: 2,
            send_retry_interval: Duration::from_millis(10),
            cancel: CancellationToken::new(),
        }
    }

    /// libzmq PULL peer; exercises rzmq PUSH ↔ libzmq PULL wire interop.
    fn pull_peer(port: u16) -> (zmq::Context, zmq::Socket) {
        let ctx = zmq::Context::new();
        let sock = ctx.socket(zmq::SocketType::PULL).unwrap();
        sock.set_rcvtimeo(2000).unwrap();
        sock.connect(&format!("tcp://127.0.0.1:{port}")).unwrap();
        (ctx, sock)
    }

    /// Poll the state watcher until `pred` returns true. Panics with a useful
    /// diagnostic on timeout. Returns the matching state.
    async fn wait_for<F>(
        state_rx: &mut watch::Receiver<SinkState>,
        mut pred: F,
        timeout: Duration,
    ) -> SinkState
    where
        F: FnMut(&SinkState) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let v = state_rx.borrow();
                if pred(&v) {
                    return v.clone();
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!(
                    "wait_for timed out after {timeout:?}; current state = {:?}",
                    *state_rx.borrow()
                );
            }
            // Either the state changes, or the deadline ticks down.
            let _ = tokio::time::timeout(remaining, state_rx.changed()).await;
        }
    }

    // ------- bind and config validation -------

    #[tokio::test]
    async fn bind_success_ephemeral_port() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let port = sink.port().expect("ephemeral bind must report a port");
        assert!(port > 0);
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn bind_error_invalid_endpoint() {
        assert!(
            PushSink::bind("not-a-real-endpoint", test_config())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn bind_error_zero_capacity() {
        let cfg = PushSinkConfig {
            buffer_capacity: 0,
            ..test_config()
        };
        assert!(PushSink::bind(TEST_ENDPOINT, cfg).await.is_err());
    }

    #[tokio::test]
    async fn bind_error_negative_hwm() {
        let cfg = PushSinkConfig {
            zmq_send_hwm: 0,
            ..test_config()
        };
        assert!(PushSink::bind(TEST_ENDPOINT, cfg).await.is_err());
    }

    // ------- buffer & door-drop behaviour -------

    #[tokio::test]
    async fn pre_peer_buffering_fills_then_overflows() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        for i in 0..10u64 {
            assert_eq!(
                sink.try_send(i, group(&[b"x"])),
                EnqueueOutcome::Enqueued,
                "expected slot {i} to be available"
            );
        }
        // Buffer full, no peer: PrefetchOverflow.
        assert_eq!(
            sink.try_send(99, group(&[b"x"])),
            EnqueueOutcome::Dropped(DropReason::PrefetchOverflow),
        );
        // State must still be WaitingForPeer.
        assert!(matches!(
            *sink.state().borrow(),
            SinkState::WaitingForPeer { .. }
        ));
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn door_drops_emit_no_delivery_report() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let mut reports = sink.delivery_reports();
        // Fill the buffer with no peer.
        for i in 0..10u64 {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        // Door-dropped message — must produce no DeliveryReport.
        let dropped_seq = 99u64;
        assert_eq!(
            sink.try_send(dropped_seq, group(&[b"x"])),
            EnqueueOutcome::Dropped(DropReason::PrefetchOverflow),
        );
        // Wait a moment for any reports to surface; collect what we get.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut seen_seqs = HashSet::new();
        while let Ok(r) = reports.try_recv() {
            seen_seqs.insert(r.seq);
        }
        assert!(
            !seen_seqs.contains(&dropped_seq),
            "door-dropped seq must not produce a report; saw {seen_seqs:?}"
        );
        sink.shutdown().await;
    }

    // ------- peer connection state -------
    //
    // Tests that share a tokio runtime with a libzmq peer must use the
    // multi-thread flavor: the rzmq sink and its internal actors all run as
    // tokio tasks, so blocking recv_* / recv_multipart calls on libzmq
    // sockets would stall the whole runtime under the default
    // current-thread test executor.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_transitions_state_to_streaming() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let port = sink.port().unwrap();
        let mut state = sink.state();
        assert!(matches!(*state.borrow(), SinkState::WaitingForPeer { .. }));
        let (_ctx, peer) = pull_peer(port);
        // Push a couple to validate they drain after connect.
        for i in 0..3u64 {
            sink.try_send(i, group(&[b"hi"]));
        }
        let s = wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;
        match s {
            SinkState::Streaming { peers, .. } => assert_eq!(peers, 1),
            _ => unreachable!(),
        }
        // Drain peer so shutdown doesn't have anything to report-drop.
        for _ in 0..3 {
            let _ = peer.recv_bytes(0).unwrap();
        }
        sink.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_disconnect_returns_to_waiting_for_peer() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let port = sink.port().unwrap();
        let mut state = sink.state();
        let (ctx, peer) = pull_peer(port);
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;
        // Drop the peer; rzmq will fire Disconnected on the monitor.
        drop(peer);
        drop(ctx);
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::WaitingForPeer { .. }),
            Duration::from_secs(5),
        )
        .await;
        sink.shutdown().await;
    }

    // ------- multipart correctness -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_group_arrives_intact() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let port = sink.port().unwrap();
        let (_ctx, peer) = pull_peer(port);
        let mut state = sink.state();
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;
        sink.try_send(0, group(&[b"frame0", b"frame1", b"frame2", b"frame3"]));
        let received = peer.recv_multipart(0).unwrap();
        assert_eq!(received.len(), 4, "expected 4 frames, got {received:?}");
        assert_eq!(received[0], b"frame0");
        assert_eq!(received[1], b"frame1");
        assert_eq!(received[2], b"frame2");
        assert_eq!(received[3], b"frame3");
        sink.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_frame_group_arrives_intact() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let port = sink.port().unwrap();
        let (_ctx, peer) = pull_peer(port);
        let mut state = sink.state();
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;
        sink.try_send(7, group(&[b"only"]));
        let received = peer.recv_multipart(0).unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], b"only");
        sink.shutdown().await;
    }

    // ------- backpressure -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn true_backpressure_drops_with_backpressure_full() {
        let cfg = PushSinkConfig {
            buffer_capacity: 5,
            zmq_send_hwm: 1,
            send_retry_interval: Duration::from_millis(5),
            ..test_config()
        };
        let sink = PushSink::bind(TEST_ENDPOINT, cfg).await.unwrap();
        let port = sink.port().unwrap();
        let (_ctx, peer) = pull_peer(port);
        peer.set_rcvhwm(1).unwrap();
        let mut state = sink.state();
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;

        // Flood with payloads large enough that the OS TCP send buffer can
        // hold only a few before the kernel refuses more writes. Tiny
        // payloads disappear into the (typically multi-MB) TCP buffer
        // faster than `try_send` can produce them, so we'd never see the
        // buffer saturate — bumping payload size is what forces the worker
        // to stall on `ResourceLimitReached` long enough for our front
        // buffer to fill.
        let payload = vec![0u8; 64 * 1024];
        let payload_slice: &[u8] = &payload;
        let mut got_bp_drop = false;
        for i in 0..200u64 {
            match sink.try_send(i, group(&[payload_slice])) {
                EnqueueOutcome::Enqueued => {}
                EnqueueOutcome::Dropped(DropReason::BackpressureFull) => {
                    got_bp_drop = true;
                    break;
                }
                other => {
                    panic!("unexpected outcome on flood iteration {i}: {other:?}; peers must be >0")
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(
            got_bp_drop,
            "expected to see BackpressureFull drop while peer connected"
        );
        // Hand peer back so shutdown is clean.
        drop(peer);
        sink.shutdown().await;
    }

    // ------- delivery reports -------

    #[tokio::test]
    async fn every_enqueued_produces_a_report() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let port = sink.port().unwrap();
        let mut reports = sink.delivery_reports();
        let (_ctx, peer) = pull_peer(port);
        let mut state = sink.state();
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;

        let n = 8u64;
        for i in 0..n {
            assert_eq!(sink.try_send(i, group(&[b"d"])), EnqueueOutcome::Enqueued);
        }
        // Drain so messages actually complete sending.
        let drain_task = tokio::task::spawn_blocking(move || {
            for _ in 0..n {
                let _ = peer.recv_bytes(0).unwrap();
            }
            peer
        });

        let mut delivered = HashSet::new();
        while delivered.len() < n as usize {
            let r = tokio::time::timeout(Duration::from_secs(5), reports.recv())
                .await
                .expect("timed out waiting for delivery report")
                .expect("reports channel closed");
            if r.outcome == DeliveryOutcome::Delivered {
                delivered.insert(r.seq);
            } else {
                panic!("unexpected outcome: {:?}", r);
            }
        }
        let _peer = drain_task.await.unwrap();
        assert_eq!(delivered, (0u64..n).collect::<HashSet<_>>());
        sink.shutdown().await;
    }

    // ------- shutdown -------

    #[tokio::test]
    async fn shutdown_drains_buffer_with_sink_shutdown_reports() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        let mut reports = sink.delivery_reports();
        // No peer — every message sits in our buffer or in the worker's
        // in-progress send.
        let n = 10u64;
        for i in 0..n {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        sink.shutdown().await;
        // After shutdown completes, all reports have been broadcast.
        let mut shutdown_drops = HashSet::new();
        // Channel may report `Closed` once exhausted; just loop until empty.
        loop {
            match reports.try_recv() {
                Ok(r) => {
                    if let DeliveryOutcome::Dropped(DropReason::SinkShutdown) = r.outcome {
                        shutdown_drops.insert(r.seq);
                    } else {
                        panic!("unexpected outcome during shutdown drain: {:?}", r);
                    }
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    if let Err(broadcast::error::TryRecvError::Empty)
                    | Err(broadcast::error::TryRecvError::Closed) = reports.try_recv().map(|r| {
                        if let DeliveryOutcome::Dropped(DropReason::SinkShutdown) = r.outcome {
                            shutdown_drops.insert(r.seq);
                        }
                    }) {
                        break;
                    }
                }
                Err(broadcast::error::TryRecvError::Closed) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            }
        }
        assert_eq!(
            shutdown_drops,
            (0u64..n).collect::<HashSet<_>>(),
            "every buffered message must report SinkShutdown on shutdown"
        );
    }

    #[tokio::test]
    async fn try_send_after_shutdown_returns_shutting_down() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        // We can't call shutdown(self) and then try_send. Instead, set the
        // flag manually via Drop semantics: keep a clone of the cancel token
        // by holding a state receiver alive, drop the sink, see that
        // queueing fails. We instead exercise the path by spawning shutdown
        // concurrently with try_send.
        let cancel = sink.cancel.clone();
        let shutting_down = sink.shutting_down.clone();
        let tx = sink.tx.clone();
        let permits = sink.permits.clone();
        let peers = sink.peers.clone();
        // Cancel the worker; mark shutting_down. Then make a fake PushSink
        // pointing at the same internals — the public API is what we test.
        cancel.cancel();
        shutting_down.store(true, Ordering::Release);
        // We can't reconstruct a PushSink from the outside, but we don't
        // need to: try_send only consults shutting_down + permits + tx.
        // Build a synthetic sink for the API call.
        let fake = PushSinkForTest {
            tx,
            permits,
            peers,
            shutting_down,
        };
        let r = fake.try_send(0, group(&[b"x"]));
        assert_eq!(r, EnqueueOutcome::ShuttingDown);
        sink.shutdown().await;
    }

    /// Minimal mirror of PushSink::try_send for testing the shutting-down
    /// path without needing to reconstruct a full PushSink.
    struct PushSinkForTest {
        tx: mpsc::UnboundedSender<WorkItem>,
        permits: Arc<Semaphore>,
        peers: Arc<AtomicUsize>,
        shutting_down: Arc<AtomicBool>,
    }
    impl PushSinkForTest {
        fn try_send(&self, seq: Seq, group: Arc<MultipartGroup>) -> EnqueueOutcome {
            if self.shutting_down.load(Ordering::Acquire) {
                return EnqueueOutcome::ShuttingDown;
            }
            let permit = match self.permits.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    let reason = if self.peers.load(Ordering::Acquire) > 0 {
                        DropReason::BackpressureFull
                    } else {
                        DropReason::PrefetchOverflow
                    };
                    return EnqueueOutcome::Dropped(reason);
                }
            };
            let work = WorkItem { seq, group, permit };
            if self.tx.send(work).is_err() {
                return EnqueueOutcome::ShuttingDown;
            }
            EnqueueOutcome::Enqueued
        }
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_via_drop() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        // Drop without calling shutdown — Drop impl must cancel cleanly.
        drop(sink);
        // If Drop hangs or panics this test never returns; that itself is
        // the assertion.
    }

    // ------- runtime resize -------

    #[tokio::test]
    async fn set_buffer_capacity_zero_is_rejected() {
        let sink = PushSink::bind(TEST_ENDPOINT, test_config()).await.unwrap();
        assert!(sink.set_buffer_capacity(0).is_err());
        assert_eq!(sink.buffer_capacity(), 10);
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn grow_capacity_admits_previously_rejected_sends() {
        let cfg = PushSinkConfig {
            buffer_capacity: 3,
            ..test_config()
        };
        let sink = PushSink::bind(TEST_ENDPOINT, cfg).await.unwrap();
        // Fill the original cap.
        for i in 0..3u64 {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        assert_eq!(
            sink.try_send(99, group(&[b"x"])),
            EnqueueOutcome::Dropped(DropReason::PrefetchOverflow)
        );

        // Grow and let the capacity task observe the watch.
        sink.set_buffer_capacity(6).unwrap();
        for _ in 0..50 {
            if sink.buffer_capacity() == 6 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(sink.buffer_capacity(), 6);

        // Now three more sends should be accepted.
        for i in 10..13u64 {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        assert_eq!(
            sink.try_send(199, group(&[b"x"])),
            EnqueueOutcome::Dropped(DropReason::PrefetchOverflow)
        );
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn shrink_capacity_within_occupancy_forgets_available_permits() {
        let cfg = PushSinkConfig {
            buffer_capacity: 10,
            ..test_config()
        };
        let sink = PushSink::bind(TEST_ENDPOINT, cfg).await.unwrap();
        // Use 3 of 10 (no peer, so they sit in the buffer).
        for i in 0..3u64 {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        // Shrink to 5. Occupancy is 3, so 5 free permits get forgotten and 2
        // free permits remain. No debt expected.
        sink.set_buffer_capacity(5).unwrap();
        for _ in 0..50 {
            if sink.buffer_capacity() == 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Two more enqueues must succeed, a third must fail.
        for i in 100..102u64 {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        assert_eq!(
            sink.try_send(199, group(&[b"x"])),
            EnqueueOutcome::Dropped(DropReason::PrefetchOverflow)
        );
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn shrink_capacity_below_occupancy_accumulates_debt_and_drains() {
        let cfg = PushSinkConfig {
            buffer_capacity: 10,
            zmq_send_hwm: 1,
            send_retry_interval: Duration::from_millis(5),
            ..test_config()
        };
        let sink = PushSink::bind(TEST_ENDPOINT, cfg).await.unwrap();
        // Fill to 8 with no peer.
        for i in 0..8u64 {
            assert_eq!(sink.try_send(i, group(&[b"x"])), EnqueueOutcome::Enqueued);
        }
        // Shrink to 3 — only 2 permits are immediately forgettable (the 2 still
        // free); the remaining 5 become debt.
        sink.set_buffer_capacity(3).unwrap();
        for _ in 0..50 {
            if sink.buffer_capacity() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // No new sends should be admitted right now.
        assert!(matches!(
            sink.try_send(99, group(&[b"x"])),
            EnqueueOutcome::Dropped(_),
        ));

        // Attach a peer and drain so the worker releases permits; the capacity
        // task will eat them to pay down the debt.
        let port = sink.port().unwrap();
        let (_ctx, peer) = pull_peer(port);
        let drain = tokio::task::spawn_blocking(move || {
            peer.set_rcvtimeo(2000).unwrap();
            for _ in 0..8 {
                let _ = peer.recv_bytes(0).unwrap();
            }
            peer
        });
        let _peer = drain.await.unwrap();

        // Once everything has flushed and debt is paid, exactly 3 fresh sends
        // should be admitted.
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut admitted = 0;
        while Instant::now() < deadline && admitted < 3 {
            match sink.try_send(200 + admitted as u64, group(&[b"y"])) {
                EnqueueOutcome::Enqueued => admitted += 1,
                EnqueueOutcome::Dropped(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                other => panic!("unexpected outcome during drain wait: {other:?}"),
            }
        }
        assert_eq!(
            admitted, 3,
            "expected new capacity of 3 to be available after debt drains"
        );
        assert_eq!(
            sink.try_send(999, group(&[b"y"])),
            EnqueueOutcome::Dropped(DropReason::BackpressureFull),
            "fourth send must fail at the shrunken cap"
        );
        sink.shutdown().await;
    }

    // ------- concurrency -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_preserve_permit_conservation() {
        let cfg = PushSinkConfig {
            buffer_capacity: 20,
            zmq_send_hwm: 5,
            send_retry_interval: Duration::from_millis(5),
            ..test_config()
        };
        let sink = Arc::new(PushSink::bind(TEST_ENDPOINT, cfg).await.unwrap());
        let port = sink.port().unwrap();
        let (_ctx, peer) = pull_peer(port);
        let mut state = sink.state();
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;

        let total: u64 = 200;
        let senders = 8u64;
        let per_sender = total / senders;
        let mut handles = vec![];
        for t in 0..senders {
            let sink = sink.clone();
            handles.push(tokio::spawn(async move {
                let mut counts = (0u32, 0u32, 0u32); // (enqueued, dropped_bp, dropped_pre)
                for i in 0..per_sender {
                    let seq = t * per_sender + i;
                    match sink.try_send(seq, group(&[b"x"])) {
                        EnqueueOutcome::Enqueued => counts.0 += 1,
                        EnqueueOutcome::Dropped(DropReason::BackpressureFull) => counts.1 += 1,
                        EnqueueOutcome::Dropped(DropReason::PrefetchOverflow) => counts.2 += 1,
                        EnqueueOutcome::Dropped(DropReason::SinkShutdown)
                        | EnqueueOutcome::ShuttingDown => {
                            panic!("unexpected shutdown during concurrent send")
                        }
                    }
                    if i % 5 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                counts
            }));
        }

        // Drain on a blocking task so the peer keeps consuming.
        let drain = tokio::task::spawn_blocking(move || {
            let mut received = 0u64;
            peer.set_rcvtimeo(500).unwrap();
            loop {
                match peer.recv_bytes(0) {
                    Ok(_) => received += 1,
                    Err(zmq::Error::EAGAIN) => break,
                    Err(_) => break,
                }
            }
            (received, peer)
        });

        let mut totals = (0u32, 0u32, 0u32);
        for h in handles {
            let c = h.await.unwrap();
            totals.0 += c.0;
            totals.1 += c.1;
            totals.2 += c.2;
        }
        let (received, _peer) = drain.await.unwrap();

        // Invariants:
        //  - Every send accounted for as either Enqueued or Dropped.
        assert_eq!(
            (totals.0 + totals.1 + totals.2) as u64,
            total,
            "every try_send must return a definite outcome"
        );
        //  - We saw at least one Enqueued (otherwise the test is degenerate).
        assert!(totals.0 > 0, "expected some Enqueued outcomes");
        //  - Received count never exceeds Enqueued count.
        assert!(
            received <= totals.0 as u64,
            "received ({received}) cannot exceed enqueued ({})",
            totals.0
        );

        // Cleanly shut down (need to extract from Arc; if other refs remain
        // we can't, so this best-effort).
        match Arc::try_unwrap(sink) {
            Ok(sink) => sink.shutdown().await,
            Err(_) => { /* Drop will clean up */ }
        }
    }
}
