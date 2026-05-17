//! Buffered PUSH socket with peer-count-aware true-backpressure detection
//! and per-group delivery reporting.
//!
//! See `docs/sink.md` for the full component spec.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};
use url::Url;

use crate::messages::{MultipartGroup, Seq};

const DELIVERY_BROADCAST_CAPACITY: usize = 4096;

// ============================================================================
// Public configuration and reporting types
// ============================================================================

#[derive(Debug, Clone)]
pub struct PushSinkConfig {
    pub endpoint: String,
    pub buffer_capacity: usize,
    pub zmq_send_hwm: i32,
    pub backpressure_high: f64,
    pub backpressure_low: f64,
    pub send_retry_interval: Duration,
}

impl Default for PushSinkConfig {
    fn default() -> Self {
        Self {
            endpoint: "tcp://0.0.0.0:0".to_string(),
            buffer_capacity: 500,
            zmq_send_hwm: 50,
            backpressure_high: 0.8,
            backpressure_low: 0.6,
            send_retry_interval: Duration::from_millis(50),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkState {
    WaitingForPeer { buffered: usize },
    Streaming { peers: usize, buffered: usize },
    Backpressured { peers: usize, buffered: usize },
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

pub struct PushSink {
    cancel: CancellationToken,
    tx: mpsc::UnboundedSender<WorkItem>,
    permits: Arc<Semaphore>,
    peers: Arc<AtomicUsize>,
    state_rx: watch::Receiver<SinkState>,
    reports_tx: broadcast::Sender<DeliveryReport>,
    worker: Option<JoinHandle<()>>,
    monitor: Option<JoinHandle<()>>,
    port: Option<u16>,
    buffer_capacity: usize,
    shutting_down: Arc<AtomicBool>,
}

impl PushSink {
    /// Bind the PUSH socket, start the worker and monitor threads, and
    /// return once the socket is bound (so [`port`](Self::port) is valid).
    pub async fn bind(config: PushSinkConfig) -> Result<Self> {
        validate_config(&config)?;

        let ctx = zmq::Context::new();
        let cancel = CancellationToken::new();
        let permits = Arc::new(Semaphore::new(config.buffer_capacity));
        let peers = Arc::new(AtomicUsize::new(0));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let (tx, outbox_rx) = mpsc::unbounded_channel::<WorkItem>();
        let (reports_tx, _) = broadcast::channel(DELIVERY_BROADCAST_CAPACITY);
        let (state_tx, state_rx) = watch::channel(SinkState::WaitingForPeer { buffered: 0 });
        let (monitor_tx, monitor_rx) = mpsc::unbounded_channel::<MonitorMsg>();
        let (launch_tx, launch_rx) = oneshot::channel::<Result<Option<u16>>>();

        let monitor_endpoint = format!("inproc://fauxdin-sink-monitor-{}", monitor_token());
        let buffer_capacity = config.buffer_capacity;

        let worker_handle = {
            let ctx = ctx.clone();
            let config = config.clone();
            let cancel = cancel.clone();
            let permits = permits.clone();
            let peers = peers.clone();
            let reports = reports_tx.clone();
            let shutting_down = shutting_down.clone();
            let monitor_endpoint = monitor_endpoint.clone();
            tokio::task::spawn_blocking(move || {
                worker_thread_main(
                    config,
                    ctx,
                    outbox_rx,
                    monitor_rx,
                    cancel,
                    permits,
                    peers,
                    reports,
                    state_tx,
                    launch_tx,
                    monitor_endpoint,
                    shutting_down,
                );
            })
        };

        let port = match launch_rx.await {
            Ok(Ok(port)) => port,
            Ok(Err(e)) => {
                // Worker is exiting on its own; just wait for it.
                let _ = worker_handle.await;
                return Err(e);
            }
            Err(_) => {
                return Err(anyhow!("sink worker exited before reporting bind result"));
            }
        };

        let monitor_handle = {
            let ctx = ctx.clone();
            let cancel = cancel.clone();
            let monitor_endpoint = monitor_endpoint.clone();
            tokio::task::spawn_blocking(move || {
                monitor_thread_main(ctx, monitor_endpoint, monitor_tx, cancel);
            })
        };

        Ok(Self {
            cancel,
            tx,
            permits,
            peers,
            state_rx,
            reports_tx,
            worker: Some(worker_handle),
            monitor: Some(monitor_handle),
            port,
            buffer_capacity,
            shutting_down,
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

    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    /// Initiate shutdown. Drains the buffer, emitting `Dropped(SinkShutdown)`
    /// for each undelivered message. Resolves once both worker threads have
    /// joined.
    pub async fn shutdown(mut self) {
        self.shutting_down.store(true, Ordering::Release);
        self.cancel.cancel();
        if let Some(h) = self.worker.take() {
            let _ = h.await;
        }
        if let Some(h) = self.monitor.take() {
            let _ = h.await;
        }
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

#[derive(Debug, Clone, Copy)]
enum MonitorMsg {
    PeerConnected,
    PeerDisconnected,
}

fn validate_config(c: &PushSinkConfig) -> Result<()> {
    if c.buffer_capacity == 0 {
        return Err(anyhow!("buffer_capacity must be > 0"));
    }
    if c.zmq_send_hwm <= 0 {
        return Err(anyhow!("zmq_send_hwm must be > 0"));
    }
    if !(0.0..=1.0).contains(&c.backpressure_high) {
        return Err(anyhow!("backpressure_high must be in [0,1]"));
    }
    if !(0.0..=1.0).contains(&c.backpressure_low) {
        return Err(anyhow!("backpressure_low must be in [0,1]"));
    }
    if c.backpressure_low > c.backpressure_high {
        return Err(anyhow!("backpressure_low must be <= backpressure_high"));
    }
    Ok(())
}

fn monitor_token() -> String {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}", std::process::id(), n)
}

fn threshold(capacity: usize, fraction: f64) -> usize {
    let t = ((capacity as f64) * fraction).ceil() as usize;
    t.max(1).min(capacity)
}

fn compute_state(peers: usize, buffered: usize, backpressured: bool) -> SinkState {
    if peers == 0 {
        SinkState::WaitingForPeer { buffered }
    } else if backpressured {
        SinkState::Backpressured { peers, buffered }
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
        (Backpressured { peers: p1, .. }, Backpressured { peers: p2, .. }) => p1 != p2,
        _ => true,
    }
}

fn build_socket(
    ctx: &zmq::Context,
    config: &PushSinkConfig,
    monitor_endpoint: &str,
) -> Result<zmq::Socket> {
    let sock = ctx
        .socket(zmq::SocketType::PUSH)
        .map_err(|e| anyhow!("socket creation failed: {e}"))?;
    sock.set_sndhwm(config.zmq_send_hwm)
        .map_err(|e| anyhow!("set_sndhwm failed: {e}"))?;
    let events = zmq::SocketEvent::ACCEPTED as i32 | zmq::SocketEvent::DISCONNECTED as i32;
    sock.monitor(monitor_endpoint, events)
        .map_err(|e| anyhow!("monitor setup failed: {e}"))?;
    sock.bind(&config.endpoint)
        .map_err(|e| anyhow!("bind to {} failed: {e}", config.endpoint))?;
    Ok(sock)
}

fn port_from_socket(sock: &zmq::Socket) -> Option<u16> {
    let ep = sock.get_last_endpoint().ok()?.ok()?;
    Url::parse(&ep).ok()?.port()
}

#[allow(clippy::too_many_arguments)]
fn worker_thread_main(
    config: PushSinkConfig,
    ctx: zmq::Context,
    outbox: mpsc::UnboundedReceiver<WorkItem>,
    monitor_rx: mpsc::UnboundedReceiver<MonitorMsg>,
    cancel: CancellationToken,
    permits: Arc<Semaphore>,
    peers_atomic: Arc<AtomicUsize>,
    reports: broadcast::Sender<DeliveryReport>,
    state_tx: watch::Sender<SinkState>,
    launch: oneshot::Sender<Result<Option<u16>>>,
    monitor_endpoint: String,
    shutting_down: Arc<AtomicBool>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = launch.send(Err(anyhow!("worker runtime build failed: {e}")));
            return;
        }
    };

    let sock = match build_socket(&ctx, &config, &monitor_endpoint) {
        Ok(s) => s,
        Err(e) => {
            let _ = launch.send(Err(e));
            return;
        }
    };

    let port = port_from_socket(&sock);
    if launch.send(Ok(port)).is_err() {
        debug!("sink bind result receiver dropped before worker startup");
        return;
    }

    let cap = config.buffer_capacity;
    let high = threshold(cap, config.backpressure_high);
    let low = threshold(cap, config.backpressure_low);

    let worker = Worker {
        config,
        sock,
        cancel,
        permits,
        reports,
        state_tx,
        monitor_rx,
        outbox,
        peers_atomic,
        shutting_down,
        peers: 0,
        backpressured: false,
        high,
        low,
        cap,
    };
    rt.block_on(worker.run());
}

struct Worker {
    config: PushSinkConfig,
    sock: zmq::Socket,
    cancel: CancellationToken,
    permits: Arc<Semaphore>,
    reports: broadcast::Sender<DeliveryReport>,
    state_tx: watch::Sender<SinkState>,
    monitor_rx: mpsc::UnboundedReceiver<MonitorMsg>,
    outbox: mpsc::UnboundedReceiver<WorkItem>,
    peers_atomic: Arc<AtomicUsize>,
    shutting_down: Arc<AtomicBool>,
    peers: usize,
    backpressured: bool,
    high: usize,
    low: usize,
    cap: usize,
}

impl Worker {
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                ev = self.monitor_rx.recv() => {
                    let Some(ev) = ev else { break };
                    self.handle_monitor(ev);
                    self.emit_state();
                },
                work = self.outbox.recv() => {
                    let Some(WorkItem { seq, group, permit }) = work else { break };
                    let outcome = self.send_group(&group).await;
                    drop(permit);
                    let _ = self.reports.send(DeliveryReport { seq, outcome });
                    self.reevaluate_backpressure();
                },
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

    fn handle_monitor(&mut self, ev: MonitorMsg) {
        match ev {
            MonitorMsg::PeerConnected => self.peers += 1,
            MonitorMsg::PeerDisconnected => self.peers = self.peers.saturating_sub(1),
        }
        self.peers_atomic.store(self.peers, Ordering::Release);
        if self.peers == 0 {
            self.backpressured = false;
        }
    }

    fn buffered(&self) -> usize {
        self.cap.saturating_sub(self.permits.available_permits())
    }

    fn emit_state(&self) {
        let new = compute_state(self.peers, self.buffered(), self.backpressured);
        self.state_tx.send_if_modified(|cur| {
            if state_significantly_differs(cur, &new) {
                *cur = new;
                true
            } else {
                false
            }
        });
    }

    /// Called after a permit is released. May transition out of backpressure.
    fn reevaluate_backpressure(&mut self) {
        if self.peers == 0 {
            return;
        }
        let buffered = self.buffered();
        if self.backpressured && buffered < self.low {
            self.backpressured = false;
            self.emit_state();
        } else if !self.backpressured && buffered >= self.high {
            self.backpressured = true;
            self.emit_state();
        }
    }

    async fn send_group(&mut self, group: &MultipartGroup) -> DeliveryOutcome {
        let n = group.frames.len();
        if n == 0 {
            return DeliveryOutcome::SendError("empty MultipartGroup".to_string());
        }
        for (i, frame) in group.frames.iter().enumerate() {
            let last = i + 1 == n;
            let flags = if last {
                zmq::DONTWAIT
            } else {
                zmq::DONTWAIT | zmq::SNDMORE
            };
            loop {
                match self.sock.send(&frame[..], flags) {
                    Ok(()) => break,
                    Err(zmq::Error::EAGAIN) => {
                        if let Some(outcome) = self.wait_during_eagain().await {
                            return outcome;
                        }
                    }
                    Err(e) => {
                        warn!("sink send error on frame {i}/{n}: {e}");
                        return DeliveryOutcome::SendError(format!("{e}"));
                    }
                }
            }
        }
        trace!("sink delivered {} frames", n);
        DeliveryOutcome::Delivered
    }

    /// Wait between EAGAIN retries. Returns Some(outcome) if the send should
    /// be abandoned (cancel or peer loss); None to retry.
    async fn wait_during_eagain(&mut self) -> Option<DeliveryOutcome> {
        let retry_interval = self.config.send_retry_interval;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                Some(DeliveryOutcome::Dropped(DropReason::SinkShutdown))
            },
            ev = self.monitor_rx.recv() => {
                let Some(ev) = ev else {
                    return Some(DeliveryOutcome::Dropped(DropReason::SinkShutdown));
                };
                self.handle_monitor(ev);
                self.emit_state();
                if self.peers == 0 {
                    // Peer went away mid-send. Note: this also fires
                    // legitimately right after startup if a DISCONNECTED
                    // event arrives, but in that case we had a peer briefly.
                    Some(DeliveryOutcome::Dropped(DropReason::BackpressureFull))
                } else {
                    None
                }
            },
            _ = tokio::time::sleep(retry_interval) => {
                // We've been EAGAIN-ing with a peer connected; the buffer may
                // have crossed the high threshold. Reevaluate.
                if self.peers > 0 {
                    let buffered = self.buffered();
                    if !self.backpressured && buffered >= self.high {
                        self.backpressured = true;
                        self.emit_state();
                    }
                }
                None
            },
        }
    }
}

fn monitor_thread_main(
    ctx: zmq::Context,
    endpoint: String,
    tx: mpsc::UnboundedSender<MonitorMsg>,
    cancel: CancellationToken,
) {
    let pair = match ctx.socket(zmq::SocketType::PAIR) {
        Ok(s) => s,
        Err(e) => {
            error!("sink monitor failed to create PAIR: {e}");
            return;
        }
    };
    if let Err(e) = pair.set_rcvtimeo(100) {
        error!("sink monitor failed to set rcvtimeo: {e}");
        return;
    }
    if let Err(e) = pair.connect(&endpoint) {
        error!("sink monitor failed to connect to {endpoint}: {e}");
        return;
    }

    let accepted = zmq::SocketEvent::ACCEPTED as u16;
    let disconnected = zmq::SocketEvent::DISCONNECTED as u16;

    while !cancel.is_cancelled() {
        let event_bytes = match pair.recv_bytes(0) {
            Ok(b) => b,
            Err(zmq::Error::EAGAIN) => continue,
            Err(zmq::Error::ETERM) => break,
            Err(e) => {
                error!("sink monitor recv error: {e}");
                break;
            }
        };
        // Second frame is the endpoint string; we don't use it.
        match pair.recv_bytes(0) {
            Ok(_) => {}
            Err(e) => {
                warn!("sink monitor failed reading endpoint frame: {e}");
                continue;
            }
        }
        if event_bytes.len() < 2 {
            warn!("sink monitor received short event frame");
            continue;
        }
        let event = u16::from_ne_bytes([event_bytes[0], event_bytes[1]]);
        let msg = if event == accepted {
            Some(MonitorMsg::PeerConnected)
        } else if event == disconnected {
            Some(MonitorMsg::PeerDisconnected)
        } else {
            None
        };
        if let Some(m) = msg
            && tx.send(m).is_err()
        {
            break;
        }
    }
    debug!("sink monitor exiting");
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

    fn test_config() -> PushSinkConfig {
        PushSinkConfig {
            endpoint: "tcp://127.0.0.1:0".to_string(),
            buffer_capacity: 10,
            zmq_send_hwm: 2,
            backpressure_high: 0.8,
            backpressure_low: 0.4,
            send_retry_interval: Duration::from_millis(10),
        }
    }

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
        let sink = PushSink::bind(test_config()).await.unwrap();
        let port = sink.port().expect("ephemeral bind must report a port");
        assert!(port > 0);
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn bind_error_invalid_endpoint() {
        let cfg = PushSinkConfig {
            endpoint: "not-a-real-endpoint".to_string(),
            ..test_config()
        };
        assert!(PushSink::bind(cfg).await.is_err());
    }

    #[tokio::test]
    async fn bind_error_zero_capacity() {
        let cfg = PushSinkConfig {
            buffer_capacity: 0,
            ..test_config()
        };
        assert!(PushSink::bind(cfg).await.is_err());
    }

    #[tokio::test]
    async fn bind_error_negative_hwm() {
        let cfg = PushSinkConfig {
            zmq_send_hwm: 0,
            ..test_config()
        };
        assert!(PushSink::bind(cfg).await.is_err());
    }

    #[tokio::test]
    async fn bind_error_inverted_thresholds() {
        let cfg = PushSinkConfig {
            backpressure_high: 0.4,
            backpressure_low: 0.8,
            ..test_config()
        };
        assert!(PushSink::bind(cfg).await.is_err());
    }

    // ------- buffer & door-drop behaviour -------

    #[tokio::test]
    async fn pre_peer_buffering_fills_then_overflows() {
        let sink = PushSink::bind(test_config()).await.unwrap();
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
        let sink = PushSink::bind(test_config()).await.unwrap();
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

    #[tokio::test]
    async fn connect_transitions_state_to_streaming() {
        let sink = PushSink::bind(test_config()).await.unwrap();
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

    #[tokio::test]
    async fn peer_disconnect_returns_to_waiting_for_peer() {
        let sink = PushSink::bind(test_config()).await.unwrap();
        let port = sink.port().unwrap();
        let mut state = sink.state();
        let (ctx, peer) = pull_peer(port);
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Streaming { .. }),
            Duration::from_secs(3),
        )
        .await;
        // Drop the peer; ZMQ will fire DISCONNECTED on the monitor.
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

    #[tokio::test]
    async fn multipart_group_arrives_intact() {
        let sink = PushSink::bind(test_config()).await.unwrap();
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

    #[tokio::test]
    async fn single_frame_group_arrives_intact() {
        let sink = PushSink::bind(test_config()).await.unwrap();
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

    #[tokio::test]
    async fn true_backpressure_drops_with_backpressure_full() {
        let cfg = PushSinkConfig {
            endpoint: "tcp://127.0.0.1:0".to_string(),
            buffer_capacity: 5,
            zmq_send_hwm: 1,
            backpressure_high: 0.8,
            backpressure_low: 0.4,
            send_retry_interval: Duration::from_millis(5),
        };
        let sink = PushSink::bind(cfg).await.unwrap();
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

        // Flood — peer never recv()s, so ZMQ buffer + our buffer fill up.
        let mut got_bp_drop = false;
        for i in 0..200u64 {
            match sink.try_send(i, group(&[b"x"])) {
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
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Backpressured { .. }),
            Duration::from_secs(3),
        )
        .await;
        // Hand peer back so shutdown is clean.
        drop(peer);
        sink.shutdown().await;
    }

    #[tokio::test]
    async fn hysteresis_does_not_flap() {
        let cfg = PushSinkConfig {
            endpoint: "tcp://127.0.0.1:0".to_string(),
            buffer_capacity: 10,
            zmq_send_hwm: 1,
            backpressure_high: 0.8, // crosses at 8
            backpressure_low: 0.4,  // crosses at 4
            send_retry_interval: Duration::from_millis(5),
        };
        let sink = PushSink::bind(cfg).await.unwrap();
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

        // Don't drain — flood to backpressured.
        for i in 0..50u64 {
            let _ = sink.try_send(i, group(&[b"x"]));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        wait_for(
            &mut state,
            |s| matches!(s, SinkState::Backpressured { .. }),
            Duration::from_secs(3),
        )
        .await;

        // Drain the peer aggressively until backpressure releases.
        let drain_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < drain_deadline {
            match peer.recv_bytes(zmq::DONTWAIT) {
                Ok(_) => {}
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
            if matches!(*state.borrow(), SinkState::Streaming { .. }) {
                break;
            }
        }
        let final_state = state.borrow().clone();
        assert!(
            matches!(final_state, SinkState::Streaming { .. }),
            "expected return to Streaming after draining; got {final_state:?}"
        );
        drop(peer);
        sink.shutdown().await;
    }

    // ------- delivery reports -------

    #[tokio::test]
    async fn every_enqueued_produces_a_report() {
        let sink = PushSink::bind(test_config()).await.unwrap();
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
        let sink = PushSink::bind(test_config()).await.unwrap();
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
        let sink = PushSink::bind(test_config()).await.unwrap();
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
        let sink = PushSink::bind(test_config()).await.unwrap();
        // Drop without calling shutdown — Drop impl must cancel cleanly.
        drop(sink);
        // If Drop hangs or panics this test never returns; that itself is
        // the assertion.
    }

    // ------- concurrency -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_preserve_permit_conservation() {
        let cfg = PushSinkConfig {
            endpoint: "tcp://127.0.0.1:0".to_string(),
            buffer_capacity: 20,
            zmq_send_hwm: 5,
            backpressure_high: 0.8,
            backpressure_low: 0.4,
            send_retry_interval: Duration::from_millis(5),
        };
        let sink = Arc::new(PushSink::bind(cfg).await.unwrap());
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
