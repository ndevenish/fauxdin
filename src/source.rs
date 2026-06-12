//! PULL socket reader. Reassembles multipart frames into
//! `Arc<MultipartGroup>`, attaches a monotonic [`Seq`], and feeds the
//! broadcaster.
//!
//! See `docs/source.md` for the full component spec.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, anyhow};
use rzmq::socket::options as zmq_opts;
use rzmq::socket::{MonitorReceiver, SocketEvent};
use rzmq::{Context, Msg, Socket, SocketType};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::messages::{MultipartGroup, Seq};

const MONITOR_CAPACITY: usize = 256;

/// One group handed from the source to the broadcaster: its [`Seq`] and the
/// reassembled multipart message. This is the exact item type the
/// broadcaster consumes.
pub type Group = (Seq, Arc<MultipartGroup>);

// ============================================================================
// Public configuration and state types
// ============================================================================

#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Endpoint to PULL-connect to, e.g. `"tcp://127.0.0.1:9999"`. Driven by
    /// a watch so EPICS/CLI can retarget at runtime; writing a new value
    /// cycles the socket (disconnect old, connect new). v1 binds this to a
    /// CLI arg via a watch that never changes after startup.
    pub endpoint: watch::Receiver<String>,
    /// Bounded capacity of the hand-off channel to the broadcaster, counted
    /// in groups. When full, the source drops at the PULL boundary with a
    /// logged warning rather than blocking the recv loop — blocking would
    /// back pressure up into the PULL socket and from there to the detector,
    /// which cannot pause and would drop frames invisibly. Must be > 0.
    pub channel_capacity: usize,
    /// `ZMQ_RCVHWM` applied to the PULL socket. A jitter-absorption buffer,
    /// not a load-shedding knob: because the recv loop never blocks, this is
    /// never the binding constraint under normal operation (the hand-off
    /// channel is). Memory is bounded by `zmq_recv_hwm × max_group_bytes`,
    /// so it cannot simply be set huge. Must be > 0.
    pub zmq_recv_hwm: i32,
    /// Cancellation token. Cancelling it (from anywhere) stops the recv loop,
    /// closes the socket, and ends the group stream, identically to calling
    /// [`Source::shutdown`]. Pass a child of a parent pipeline token when
    /// wiring into a larger process so one `cancel()` tears the whole
    /// pipeline down.
    pub cancel: CancellationToken,
}

impl SourceConfig {
    fn validate(&self) -> Result<()> {
        if self.channel_capacity == 0 {
            return Err(anyhow!("channel_capacity must be > 0"));
        }
        if self.zmq_recv_hwm <= 0 {
            return Err(anyhow!("zmq_recv_hwm must be > 0"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceState {
    /// No live peer: initial state, between connect retries, or after a
    /// disconnect / retarget. The PULL socket may still be attempting to
    /// (re)connect in the background — rzmq retries automatically.
    Disconnected { endpoint: String },
    /// ZMTP handshake completed with the upstream PUSH peer. This — not a raw
    /// TCP `Connected` — is the "we have a live peer" signal.
    Connected { endpoint: String, peer_addr: String },
}

// ============================================================================
// Source
// ============================================================================

/// Inbound side of the pump: a ZeroMQ PULL socket connected to the
/// detector's PUSH endpoint.
///
/// # What it does
///
/// An async worker task owns the rzmq PULL socket, calls `recv_multipart`,
/// reassembles each multipart message into a [`MultipartGroup`], wraps it in
/// an `Arc`, stamps it with the next monotonic [`Seq`], and hands
/// `(seq, group)` to the broadcaster over a bounded channel (the receiving
/// end of which [`connect`](Self::connect) returns).
///
/// # What it deliberately does not do
///
/// The source is **byte-blind**: it never parses frame contents and knows
/// nothing about SIMPLON headers, series, or acquisition state. Series and
/// abandon decisions belong to the lifecycle, which has the state the source
/// does not. Connection state (`Connected` / `Disconnected`) is a transport
/// fact surfaced for diagnostics and for the lifecycle to interpret; the
/// source never decides what a disconnect means for a series.
///
/// # Backpressure
///
/// The recv loop never blocks on the downstream. ZMQ PUSH/PULL is a
/// backpressure (blocking) pattern, not a lossy one, so blocking the loop
/// would back pressure up into the socket and on to the detector — which
/// cannot pause and would then drop frames internally where we cannot
/// observe them. Instead, when the hand-off channel is full the source drops
/// the just-read group at the door, burning its [`Seq`] (leaving a visible
/// gap) and logging a warning. A burned seq is therefore "the source dropped
/// it before the broadcaster," distinct from the sink's later `Dropped`.
///
/// # Threading
///
/// Two tokio tasks back the source — no `spawn_blocking`:
///
/// 1. A recv task owning the rzmq PULL socket; runs the receive loop and
///    handles runtime endpoint retargeting.
/// 2. A monitor task draining the socket's monitor stream and mapping
///    connecter-side events onto [`SourceState`].
///
/// # Shutdown
///
/// Drop or [`shutdown`](Self::shutdown) cancels both tasks. `shutdown` awaits
/// them, closes the socket, and terminates the rzmq context; `Drop` just
/// signals. Once the recv task exits, the hand-off receiver observes a closed
/// channel.
pub struct Source {
    cancel: CancellationToken,
    state_rx: watch::Receiver<SourceState>,
    next_seq: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    recv: Option<JoinHandle<()>>,
    monitor: Option<JoinHandle<()>>,
    socket: Socket,
    ctx: Context,
}

impl Source {
    /// Create the PULL socket, connect to the current `endpoint`, and start
    /// the recv and monitor tasks.
    ///
    /// Does not wait for a peer: ZMQ connect is asynchronous and retries in
    /// the background, so this returns once the socket exists and the tasks
    /// are spawned. Returns the handle plus the receiving end of the hand-off
    /// channel — the wiring passes that receiver to the broadcaster.
    pub async fn connect(config: SourceConfig) -> Result<(Self, mpsc::Receiver<Group>)> {
        config.validate()?;
        let ctx = Context::new().map_err(|e| anyhow!("rzmq context creation failed: {e}"))?;
        match Self::connect_inner(config, ctx.clone()).await {
            Ok(pair) => Ok(pair),
            Err(e) => {
                let _ = ctx.term().await;
                Err(e)
            }
        }
    }

    async fn connect_inner(
        config: SourceConfig,
        ctx: Context,
    ) -> Result<(Self, mpsc::Receiver<Group>)> {
        let socket = ctx
            .socket(SocketType::Pull)
            .map_err(|e| anyhow!("socket creation failed: {e}"))?;
        socket
            .set_option(zmq_opts::RCVHWM, config.zmq_recv_hwm)
            .await
            .map_err(|e| anyhow!("set_rcvhwm failed: {e}"))?;

        // Set up the monitor before connecting so we don't miss the first
        // handshake/connect events.
        let monitor_rx = socket
            .monitor(MONITOR_CAPACITY)
            .await
            .map_err(|e| anyhow!("monitor setup failed: {e}"))?;

        let initial_endpoint = config.endpoint.borrow().clone();
        socket
            .connect(&initial_endpoint)
            .await
            .map_err(|e| anyhow!("connect to {initial_endpoint} failed: {e}"))?;

        let cancel = config.cancel.clone();
        let next_seq = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel::<Group>(config.channel_capacity);
        let (state_tx, state_rx) = watch::channel(SourceState::Disconnected {
            endpoint: initial_endpoint.clone(),
        });

        let monitor = {
            let cancel = cancel.clone();
            tokio::spawn(async move { monitor_task(monitor_rx, state_tx, cancel).await })
        };

        let recv = {
            let task = RecvTask {
                socket: socket.clone(),
                endpoint_rx: config.endpoint,
                current_endpoint: initial_endpoint,
                tx,
                next_seq: next_seq.clone(),
                dropped: dropped.clone(),
                cancel: cancel.clone(),
            };
            tokio::spawn(async move { task.run().await })
        };

        Ok((
            Self {
                cancel,
                state_rx,
                next_seq,
                dropped,
                recv: Some(recv),
                monitor: Some(monitor),
                socket,
                ctx,
            },
            rx,
        ))
    }

    /// Live connection state. Updates on every variant transition, driven by
    /// rzmq monitor events. Diagnostics surface; correctness never depends on
    /// it.
    pub fn state(&self) -> watch::Receiver<SourceState> {
        self.state_rx.clone()
    }

    /// The next [`Seq`] the source will assign. Monotonic, process-global,
    /// never reset across reconnects or retargets.
    pub fn next_seq(&self) -> Seq {
        self.next_seq.load(Ordering::Acquire)
    }

    /// Number of groups dropped at the PULL boundary because the hand-off
    /// channel was full. Each such drop burns a [`Seq`], leaving a gap.
    pub fn dropped_at_door(&self) -> u64 {
        self.dropped.load(Ordering::Acquire)
    }

    /// Stop the recv and monitor tasks, close the socket, and drain rzmq
    /// actors. Resolves once both tasks have joined. The hand-off receiver
    /// then observes a closed channel.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(h) = self.recv.take() {
            let _ = h.await;
        }
        if let Some(h) = self.monitor.take() {
            let _ = h.await;
        }
        let _ = self.socket.close().await;
        let _ = self.ctx.term().await;
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ============================================================================
// Internals
// ============================================================================

/// Owns the PULL socket and runs the receive loop. Also watches the endpoint
/// for runtime retargets, since it owns the socket.
struct RecvTask {
    socket: Socket,
    endpoint_rx: watch::Receiver<String>,
    current_endpoint: String,
    tx: mpsc::Sender<Group>,
    next_seq: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    cancel: CancellationToken,
}

impl RecvTask {
    async fn run(mut self) {
        // Sync the watch version to the current value so a no-op `changed()`
        // doesn't fire a spurious retarget on the first iteration.
        self.endpoint_rx.mark_unchanged();
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                changed = self.endpoint_rx.changed() => {
                    if changed.is_err() {
                        // All senders dropped: endpoint can never change
                        // again, but we keep receiving on the current one.
                        // Fall through to a recv-only loop.
                        self.recv_only_loop().await;
                        break;
                    }
                    let new_ep = self.endpoint_rx.borrow_and_update().clone();
                    self.retarget(new_ep).await;
                }
                res = self.socket.recv_multipart() => {
                    match res {
                        Ok(frames) => self.handle_group(frames),
                        Err(e) => {
                            // rzmq auto-reconnects across peer loss, so a recv
                            // error here is a terminal socket condition (close
                            // / context term), not a transient disconnect.
                            warn!("source recv error, ending recv loop: {e}");
                            break;
                        }
                    }
                }
            }
        }
        debug!("source recv task exiting");
    }

    /// Once the endpoint watch can no longer change, drop the retarget arm
    /// and just receive until cancelled or the socket dies.
    async fn recv_only_loop(&mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                res = self.socket.recv_multipart() => {
                    match res {
                        Ok(frames) => self.handle_group(frames),
                        Err(e) => {
                            warn!("source recv error, ending recv loop: {e}");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Cycle the connection to a new endpoint. State follows via the monitor.
    async fn retarget(&mut self, new_ep: String) {
        if new_ep == self.current_endpoint {
            return;
        }
        debug!(from = %self.current_endpoint, to = %new_ep, "source retargeting");
        if let Err(e) = self.socket.disconnect(&self.current_endpoint).await {
            warn!(endpoint = %self.current_endpoint, "source disconnect failed: {e}");
        }
        match self.socket.connect(&new_ep).await {
            Ok(()) => self.current_endpoint = new_ep,
            Err(e) => {
                // Keep the old endpoint as our notion of "current" so a later
                // change still produces a clean disconnect/connect pair.
                warn!(endpoint = %new_ep, "source connect to new endpoint failed: {e}");
            }
        }
    }

    /// Build a group from received frames, stamp the next seq, and hand it to
    /// the broadcaster — dropping at the door (and burning the seq) if the
    /// channel is full.
    fn handle_group(&self, frames: Vec<Msg>) {
        if frames.is_empty() {
            // rzmq should never yield an empty multipart, but never emit one.
            trace!("source received empty multipart, ignoring");
            return;
        }
        let group = Arc::new(MultipartGroup {
            // One copy at the PULL read: `data_bytes` is a cheap refcount
            // clone of rzmq's internal `Bytes`; everything downstream is
            // Arc-clone only.
            frames: frames
                .iter()
                .map(|m| m.data_bytes().unwrap_or_default())
                .collect(),
        });
        // fetch_add returns the value to assign and advances the counter, so
        // the seq is consumed whether or not the group is delivered.
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        match self.tx.try_send((seq, group)) {
            Ok(()) => trace!(seq, "source emitted group"),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let total = self.dropped.fetch_add(1, Ordering::AcqRel) + 1;
                warn!(
                    seq,
                    total_dropped = total,
                    "source hand-off channel full; dropping group at PULL boundary"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Broadcaster gone — shutdown in progress. Nothing to do; the
                // recv loop will exit on its next cancel poll.
                trace!(seq, "source hand-off channel closed");
            }
        }
    }
}

/// Drain the socket's monitor stream and map connecter-side events onto
/// [`SourceState`]. Owns no socket and makes no series decisions.
async fn monitor_task(
    monitor_rx: MonitorReceiver,
    state_tx: watch::Sender<SourceState>,
    cancel: CancellationToken,
) {
    // `HandshakeSucceeded` is the live-peer signal but carries only the
    // endpoint, so remember the peer address from the preceding `Connected`
    // (transport) event to populate `SourceState::Connected`.
    let mut last_peer_addr: Option<String> = None;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            ev = monitor_rx.recv() => {
                match ev {
                    Ok(SocketEvent::Connected { peer_addr, .. }) => {
                        last_peer_addr = Some(peer_addr);
                    }
                    Ok(SocketEvent::HandshakeSucceeded { endpoint }) => {
                        let peer_addr = last_peer_addr.clone().unwrap_or_default();
                        set_state(&state_tx, SourceState::Connected { endpoint, peer_addr });
                    }
                    Ok(SocketEvent::Disconnected { endpoint })
                    | Ok(SocketEvent::Closed { endpoint }) => {
                        last_peer_addr = None;
                        set_state(&state_tx, SourceState::Disconnected { endpoint });
                    }
                    Ok(other) => trace!("source monitor event: {other:?}"),
                    Err(_) => break, // channel closed -> socket torn down
                }
            }
        }
    }
    debug!("source monitor task exiting");
}

fn set_state(state_tx: &watch::Sender<SourceState>, new: SourceState) {
    state_tx.send_if_modified(|cur| {
        if *cur != new {
            *cur = new;
            true
        } else {
            false
        }
    });
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    const LOCALHOST: &str = "tcp://127.0.0.1:*";

    /// A libzmq PUSH peer that binds an ephemeral port; the source connects to
    /// it. Exercises rzmq PULL ↔ libzmq PUSH wire interop. Returns the context
    /// (kept alive), the socket, and the bound endpoint string.
    fn push_peer() -> (zmq::Context, zmq::Socket, String) {
        let ctx = zmq::Context::new();
        let sock = ctx.socket(zmq::SocketType::PUSH).unwrap();
        sock.set_sndtimeo(2000).unwrap();
        sock.set_sndhwm(10_000).unwrap();
        sock.bind(LOCALHOST).unwrap();
        let endpoint = sock.get_last_endpoint().unwrap().unwrap();
        (ctx, sock, endpoint)
    }

    fn config(endpoint: &str) -> (SourceConfig, watch::Sender<String>) {
        let (ep_tx, ep_rx) = watch::channel(endpoint.to_string());
        let cfg = SourceConfig {
            endpoint: ep_rx,
            channel_capacity: 16,
            zmq_recv_hwm: 10_000,
            cancel: CancellationToken::new(),
        };
        (cfg, ep_tx)
    }

    async fn wait_for_state<F>(
        state_rx: &mut watch::Receiver<SourceState>,
        mut pred: F,
        timeout: Duration,
    ) -> SourceState
    where
        F: FnMut(&SourceState) -> bool,
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
                    "wait_for_state timed out after {timeout:?}; current = {:?}",
                    *state_rx.borrow()
                );
            }
            let _ = tokio::time::timeout(remaining, state_rx.changed()).await;
        }
    }

    async fn recv_group(rx: &mut mpsc::Receiver<Group>, timeout: Duration) -> Group {
        tokio::time::timeout(timeout, rx.recv())
            .await
            .expect("timed out waiting for group")
            .expect("hand-off channel closed")
    }

    // ------- config validation -------

    #[tokio::test]
    async fn connect_rejects_zero_channel_capacity() {
        let (mut cfg, _tx) = config("tcp://127.0.0.1:9999");
        cfg.channel_capacity = 0;
        assert!(Source::connect(cfg).await.is_err());
    }

    #[tokio::test]
    async fn connect_rejects_nonpositive_recv_hwm() {
        let (mut cfg, _tx) = config("tcp://127.0.0.1:9999");
        cfg.zmq_recv_hwm = 0;
        assert!(Source::connect(cfg).await.is_err());
    }

    #[tokio::test]
    async fn connect_rejects_invalid_endpoint() {
        let (cfg, _tx) = config("not-a-real-endpoint");
        assert!(Source::connect(cfg).await.is_err());
    }

    // ------- receive + reassembly -------
    //
    // Tests sharing a runtime with a libzmq peer use the multi-thread flavor:
    // the source's actors are tokio tasks, so blocking libzmq send/recv on the
    // test task would stall a current-thread executor.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_and_receive_one_group() {
        let (_ctx, peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;

        peer.send("hello", 0).unwrap();
        let (seq, group) = recv_group(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(seq, 0);
        assert_eq!(group.frames.len(), 1);
        assert_eq!(&group.frames[0][..], b"hello");
        source.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seq_is_monotonic() {
        let (_ctx, peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;

        for i in 0..5u64 {
            peer.send(format!("msg{i}").as_bytes(), 0).unwrap();
        }
        for i in 0..5u64 {
            let (seq, group) = recv_group(&mut rx, Duration::from_secs(3)).await;
            assert_eq!(seq, i, "seqs must arrive in order with no gaps");
            assert_eq!(&group.frames[0][..], format!("msg{i}").as_bytes());
        }
        source.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_group_reassembled_intact() {
        let (_ctx, peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;

        let parts: Vec<&[u8]> = vec![b"header", b"appendix", b"payload", b"trailer"];
        peer.send_multipart(&parts, 0).unwrap();

        let (seq, group) = recv_group(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(seq, 0);
        assert_eq!(group.frames.len(), 4, "expected 4 frames, got {group:?}");
        assert_eq!(&group.frames[0][..], b"header");
        assert_eq!(&group.frames[1][..], b"appendix");
        assert_eq!(&group.frames[2][..], b"payload");
        assert_eq!(&group.frames[3][..], b"trailer");
        source.shutdown().await;
    }

    // ------- connection state -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn state_reaches_connected_on_handshake() {
        let (_ctx, _peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, _rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();
        assert!(matches!(*state.borrow(), SourceState::Disconnected { .. }));
        let s = wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;
        match s {
            SourceState::Connected {
                endpoint: ep,
                peer_addr,
            } => {
                assert_eq!(ep, endpoint);
                // The monitor populates this from the transport `Connected`
                // event preceding the handshake; a regression in that
                // correlation surfaces as an empty addr.
                assert!(!peer_addr.is_empty(), "peer_addr should be populated");
            }
            _ => unreachable!(),
        }
        source.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn state_returns_to_disconnected_when_peer_drops() {
        let (ctx, peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, _rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;
        drop(peer);
        drop(ctx);
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Disconnected { .. }),
            Duration::from_secs(5),
        )
        .await;
        source.shutdown().await;
    }

    // ------- retarget -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retarget_switches_peer_and_continues_seq() {
        let (_ctx1, peer1, endpoint1) = push_peer();
        let (_ctx2, peer2, endpoint2) = push_peer();
        let (cfg, ep_tx) = config(&endpoint1);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();

        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { endpoint, .. } if *endpoint == endpoint1),
            Duration::from_secs(3),
        )
        .await;
        peer1.send("from1", 0).unwrap();
        let (seq0, group0) = recv_group(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(seq0, 0);
        assert_eq!(&group0.frames[0][..], b"from1");

        // Retarget to peer2.
        ep_tx.send(endpoint2.clone()).unwrap();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { endpoint, .. } if *endpoint == endpoint2),
            Duration::from_secs(5),
        )
        .await;
        peer2.send("from2", 0).unwrap();
        let (seq1, group1) = recv_group(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(seq1, 1, "seq must continue unbroken across a retarget");
        assert_eq!(&group1.frames[0][..], b"from2");
        source.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receives_after_endpoint_senders_dropped() {
        let (_ctx, peer, endpoint) = push_peer();
        // Drop the watch sender so the endpoint can never change again; this
        // drives the recv task into its sender-closed `recv_only_loop` branch.
        let (cfg, ep_tx) = config(&endpoint);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        drop(ep_tx);
        let mut state = source.state();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;

        // Frames must still flow on the original endpoint via recv_only_loop.
        peer.send("after-drop", 0).unwrap();
        let (seq, group) = recv_group(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(seq, 0);
        assert_eq!(&group.frames[0][..], b"after-drop");
        source.shutdown().await;
    }

    // ------- door drop on full channel -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_channel_drops_at_door_and_burns_seq() {
        let (_ctx, peer, endpoint) = push_peer();
        let (mut cfg, _tx) = config(&endpoint);
        cfg.channel_capacity = 3;
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        let mut state = source.state();
        wait_for_state(
            &mut state,
            |s| matches!(s, SourceState::Connected { .. }),
            Duration::from_secs(3),
        )
        .await;

        // Send 10 with a never-drained channel of capacity 3. The recv loop
        // keeps draining the socket, so all 10 are read (seqs burned) but only
        // the first 3 land in the channel; the other 7 drop at the door.
        let n = 10u64;
        for i in 0..n {
            peer.send(format!("m{i}").as_bytes(), 0).unwrap();
        }

        // Wait until all 10 have been pulled off the socket.
        let deadline = Instant::now() + Duration::from_secs(5);
        while source.next_seq() < n {
            if Instant::now() >= deadline {
                panic!("only {} of {n} groups pulled", source.next_seq());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(source.next_seq(), n, "every group consumes a seq");
        assert_eq!(
            source.dropped_at_door(),
            n - 3,
            "7 groups must drop at the door"
        );

        // The channel holds exactly the first 3 seqs; the rest are a gap.
        let mut got = Vec::new();
        while let Ok((seq, _)) = rx.try_recv() {
            got.push(seq);
        }
        assert_eq!(got, vec![0, 1, 2], "only the pre-overflow seqs survive");
        source.shutdown().await;
    }

    // ------- shutdown -------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_closes_handoff_channel() {
        let (_ctx, _peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        source.shutdown().await;
        // Recv task gone -> sender dropped -> channel closed.
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_without_shutdown_closes_handoff_channel() {
        let (_ctx, _peer, endpoint) = push_peer();
        let (cfg, _tx) = config(&endpoint);
        let (source, mut rx) = Source::connect(cfg).await.unwrap();
        drop(source);
        // Drop cancels the token; the recv task must observe that, exit, and
        // drop its sender, which the receiver sees as a closed channel. If Drop
        // stopped cancelling or the recv task ignored the token, this hangs to
        // the timeout instead.
        let closed = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("recv task did not exit after Drop");
        assert!(closed.is_none(), "channel should be closed after Drop");
    }
}
