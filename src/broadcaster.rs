//! Fan-out from the source to independent subscribers (sink, lifecycle,
//! diagnostics). Each subscriber has its own bounded queue and drop policy
//! so a slow subscriber cannot stall the others.
//!
//! See `docs/broadcaster.md` for the full component spec.

#![allow(dead_code)]

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::messages::{MultipartGroup, Seq};

/// Fixed depth of the broadcaster→sink hand-off queue. The sink has its own
/// internal buffer; this queue is only a jitter-absorbing slot for the
/// forwarding adapter and should never fill in normal operation. See
/// `docs/broadcaster.md` § Interaction with the sink's own buffer.
pub const SINK_QUEUE_DEPTH: usize = 4;

/// One group flowing through the broadcaster: its [`Seq`] and the reassembled
/// multipart message. Identical to the source's hand-off item type — nothing
/// is transformed here.
type Group = (Seq, Arc<MultipartGroup>);

// ============================================================================
// Public types
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropPolicy {
    /// Queue full → reject the just-arrived group, keep what's queued. Used by
    /// the sink: the mirror may lose frames under true backpressure, and the
    /// freshest-but-rejected group is the one to lose last, so older queued
    /// groups still drain in order.
    DropNewest,
    /// Queue full → never drop here; the fan-out task `await`s the send, which
    /// back-pressures the source. Used by the capture: durable recording. Its
    /// own backpressure surfaces as source-side door drops, never as a silent
    /// loss here.
    NeverDrop,
}

/// One registered subscriber's receiving end plus its drop counter.
pub struct Subscription {
    /// The receiving end this subscriber drains. Yields groups in strictly
    /// increasing [`Seq`] order (with gaps wherever this subscriber's policy
    /// dropped). Closes when the source channel closes or the broadcaster is
    /// cancelled.
    pub rx: mpsc::Receiver<Group>,
    /// Monotonic count of groups this subscriber's policy has dropped. Always
    /// 0 for a [`DropPolicy::NeverDrop`] subscriber (by invariant).
    pub dropped: watch::Receiver<u64>,
}

// ============================================================================
// BroadcasterBuilder
// ============================================================================

/// Accumulates the subscriber table before the fan-out task starts.
/// Subscribers register at startup only; none can be added after
/// [`spawn`](Self::spawn).
pub struct BroadcasterBuilder {
    subs: Vec<SubReg>,
}

struct SubReg {
    label: String,
    tx: mpsc::Sender<Group>,
    policy: DropPolicy,
    dropped_tx: watch::Sender<u64>,
}

impl Default for BroadcasterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BroadcasterBuilder {
    pub fn new() -> Self {
        Self { subs: Vec::new() }
    }

    /// Register one subscriber before the fan-out starts. `label` appears in
    /// logs and the drop metric. `capacity` is the bounded queue depth in
    /// whole groups and must be `> 0`. Returns the receiving end plus its drop
    /// counter.
    pub fn subscribe(&mut self, label: &str, capacity: usize, policy: DropPolicy) -> Subscription {
        assert!(capacity > 0, "broadcaster subscriber capacity must be > 0");
        let (tx, rx) = mpsc::channel(capacity);
        let (dropped_tx, dropped_rx) = watch::channel(0u64);
        self.subs.push(SubReg {
            label: label.to_string(),
            tx,
            policy,
            dropped_tx,
        });
        Subscription {
            rx,
            dropped: dropped_rx,
        }
    }

    /// Consume the source's hand-off receiver and spawn the fan-out task.
    /// Cancelling `cancel` ends the task and closes every subscriber `rx`.
    /// Pass a child of the pipeline token so one `cancel()` tears the whole
    /// pipeline down. Returns immediately.
    pub fn spawn(self, source_rx: mpsc::Receiver<Group>, cancel: CancellationToken) -> Broadcaster {
        let never_drop = self
            .subs
            .iter()
            .filter(|s| s.policy == DropPolicy::NeverDrop)
            .count();
        if never_drop > 1 {
            // Phase-2 sends are sequential, so a stalled first NeverDrop
            // subscriber delays delivery to the second. This is the trigger to
            // revisit concurrent phase-2 sends, not a fatal condition.
            warn!(
                count = never_drop,
                "broadcaster has >1 NeverDrop subscriber; their phase-2 park times add up"
            );
        }

        let (forwarded_tx, forwarded_rx) = watch::channel(0u64);
        let subs = self
            .subs
            .into_iter()
            .map(|r| Sub {
                label: r.label,
                tx: r.tx,
                policy: r.policy,
                dropped_tx: r.dropped_tx,
                dropped: 0,
            })
            .collect();

        let task = {
            let fanout = FanOut {
                source_rx,
                subs,
                forwarded: forwarded_tx,
                forwarded_count: 0,
                cancel: cancel.clone(),
            };
            tokio::spawn(async move { fanout.run().await })
        };

        Broadcaster {
            cancel,
            forwarded: forwarded_rx,
            task: Some(task),
        }
    }
}

// ============================================================================
// Broadcaster
// ============================================================================

/// Handle to the running fan-out task. See `docs/broadcaster.md`.
pub struct Broadcaster {
    cancel: CancellationToken,
    forwarded: watch::Receiver<u64>,
    task: Option<JoinHandle<()>>,
}

impl Broadcaster {
    /// Total groups pulled from the source so far (= the number offered to
    /// each subscriber). Pairs with each subscriber's `dropped` to reconstruct
    /// delivered counts.
    pub fn forwarded(&self) -> watch::Receiver<u64> {
        self.forwarded.clone()
    }

    /// Cancel the fan-out task and await its completion. After this resolves,
    /// every subscriber `rx` observes a closed channel.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(h) = self.task.take() {
            let _ = h.await;
        }
    }
}

impl Drop for Broadcaster {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ============================================================================
// Internals
// ============================================================================

/// One subscriber as seen by the fan-out task.
struct Sub {
    label: String,
    tx: mpsc::Sender<Group>,
    policy: DropPolicy,
    dropped_tx: watch::Sender<u64>,
    /// Local mirror of the published drop count.
    dropped: u64,
}

/// The single fan-out task: pulls groups from the source and offers each to
/// every live subscriber per its policy.
struct FanOut {
    source_rx: mpsc::Receiver<Group>,
    subs: Vec<Sub>,
    forwarded: watch::Sender<u64>,
    forwarded_count: u64,
    cancel: CancellationToken,
}

impl FanOut {
    async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => break,
                item = self.source_rx.recv() => match item {
                    Some((seq, group)) => {
                        // Counted as pulled the moment it leaves the source,
                        // before delivery; `forwarded` is "pulled so far".
                        self.forwarded_count += 1;
                        let _ = self.forwarded.send(self.forwarded_count);
                        if !self.deliver(seq, group).await {
                            // Cancelled mid-NeverDrop-send: shutdown in
                            // progress, abandon the fan-out.
                            break;
                        }
                    }
                    None => break, // source channel closed
                }
            }
        }
        debug!("broadcaster fan-out task exiting");
    }

    /// Offer one group to every live subscriber in two phases — non-blocking
    /// `DropNewest` first, then sequential `NeverDrop` sends — so a slow
    /// `NeverDrop` subscriber never delays the lossy ones *for this group*.
    /// Returns `false` if cancelled mid-send (the caller should stop).
    async fn deliver(&mut self, seq: Seq, group: Arc<MultipartGroup>) -> bool {
        let cancel = self.cancel.clone();
        // Indices of subscribers whose `rx` closed; removed after both phases
        // so phase-1 indices stay valid through phase 2.
        let mut dead: Vec<usize> = Vec::new();

        // Phase 1 — DropNewest, non-blocking.
        for (i, sub) in self.subs.iter_mut().enumerate() {
            if sub.policy != DropPolicy::DropNewest {
                continue;
            }
            match sub.tx.try_send((seq, group.clone())) {
                Ok(()) => trace!(label = %sub.label, seq, "broadcaster delivered (DropNewest)"),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.dropped += 1;
                    let _ = sub.dropped_tx.send(sub.dropped);
                    // This queue is sized so it should never fill (the sink's
                    // own buffer is the real mirror buffer), so a drop here is
                    // a real signal — warn, rate-limited to powers of two.
                    if sub.dropped.is_power_of_two() {
                        warn!(
                            label = %sub.label,
                            seq,
                            dropped = sub.dropped,
                            "broadcaster subscriber queue full; dropping group (DropNewest)"
                        );
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    info!(label = %sub.label, "broadcaster subscriber rx closed; removing");
                    dead.push(i);
                }
            }
        }

        // Phase 2 — NeverDrop, blocking, sequential, each raced against cancel.
        for (i, sub) in self.subs.iter_mut().enumerate() {
            if sub.policy != DropPolicy::NeverDrop {
                continue;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return false,
                res = sub.tx.send((seq, group.clone())) => {
                    match res {
                        Ok(()) => {
                            trace!(label = %sub.label, seq, "broadcaster delivered (NeverDrop)")
                        }
                        Err(_) => {
                            info!(label = %sub.label, "broadcaster subscriber rx closed; removing");
                            dead.push(i);
                        }
                    }
                }
            }
        }

        for i in dead.into_iter().rev() {
            self.subs.remove(i);
        }
        true
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_util::bytes::Bytes;

    fn group(tag: &[u8]) -> Arc<MultipartGroup> {
        Arc::new(MultipartGroup {
            frames: vec![Bytes::copy_from_slice(tag)],
        })
    }

    /// Receive one group from a subscriber, failing the test on timeout/close.
    async fn recv(rx: &mut mpsc::Receiver<Group>) -> Group {
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for group")
            .expect("subscriber channel closed unexpectedly")
    }

    /// Wait for a `watch::Receiver<u64>` to reach `target`, or panic. Panics
    /// distinctly on timeout vs. the sender being dropped before `target` —
    /// the latter would otherwise let a stalled fan-out pass silently.
    async fn wait_count(rx: &mut watch::Receiver<u64>, target: u64) {
        let reached = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if *rx.borrow() >= target {
                    return true;
                }
                if rx.changed().await.is_err() {
                    // Sender gone: no further updates can arrive. The last
                    // value is whatever `borrow()` now reports.
                    return *rx.borrow() >= target;
                }
            }
        })
        .await;
        match reached {
            Ok(true) => {}
            Ok(false) => panic!(
                "watch sender dropped before reaching count {target}; last = {}",
                *rx.borrow()
            ),
            Err(_) => panic!(
                "timed out waiting for count {target}; last = {}",
                *rx.borrow()
            ),
        }
    }

    // ------- validation -------

    #[tokio::test]
    #[should_panic(expected = "capacity must be > 0")]
    async fn subscribe_zero_capacity_panics() {
        let mut b = BroadcasterBuilder::new();
        let _ = b.subscribe("bad", 0, DropPolicy::DropNewest);
    }

    // ------- happy path -------

    #[tokio::test]
    async fn single_subscriber_all_delivered() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let sub = b.subscribe("s", 64, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());
        let mut rx = sub.rx;

        let n = 20u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        for i in 0..n {
            let (seq, g) = recv(&mut rx).await;
            assert_eq!(seq, i, "groups must arrive in order");
            assert_eq!(&g.frames[0][..], b"x");
        }
        let mut fwd = bc.forwarded();
        wait_count(&mut fwd, n).await;
        assert_eq!(*fwd.borrow(), n);
        assert_eq!(*sub.dropped.borrow(), 0);
        bc.shutdown().await;
    }

    #[tokio::test]
    async fn multi_subscriber_fanout_is_arc_shared() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let a = b.subscribe("a", 64, DropPolicy::NeverDrop);
        let c = b.subscribe("c", 64, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());
        let mut rxa = a.rx;
        let mut rxc = c.rx;

        let n = 10u64;
        let mut sent = Vec::new();
        for i in 0..n {
            let g = group(format!("g{i}").as_bytes());
            sent.push(g.clone());
            src_tx.send((i, g)).await.unwrap();
        }
        for i in 0..n {
            let (sa, ga) = recv(&mut rxa).await;
            let (sc, gc) = recv(&mut rxc).await;
            assert_eq!(sa, i);
            assert_eq!(sc, i);
            // Same allocation reached both subscribers — no byte copy.
            assert!(Arc::ptr_eq(&ga, &gc), "subscribers must share the Arc");
            assert!(
                Arc::ptr_eq(&ga, &sent[i as usize]),
                "must be the source Arc"
            );
        }
        bc.shutdown().await;
    }

    // ------- DropNewest -------

    #[tokio::test]
    async fn drop_newest_under_stall_keeps_oldest_and_counts_excess() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let k = 3usize;
        let lossy = b.subscribe("lossy", k, DropPolicy::DropNewest);
        // A NeverDrop subscriber proves independence: it must get everything.
        let durable = b.subscribe("durable", 64, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());

        // Drain the durable subscriber so the fan-out task never parks.
        let mut rx_durable = durable.rx;
        let drain = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some((seq, _)) = rx_durable.recv().await {
                got.push(seq);
            }
            got
        });

        // The lossy subscriber is never drained, so its queue fills at k.
        let n = 10u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        drop(src_tx);

        let mut fwd = bc.forwarded();
        wait_count(&mut fwd, n).await;

        // Lossy: only the first k seqs survive; the rest are counted dropped.
        let mut dropped = lossy.dropped;
        wait_count(&mut dropped, n - k as u64).await;
        assert_eq!(*dropped.borrow(), n - k as u64);

        let mut rx_lossy = lossy.rx;
        let mut got_lossy = Vec::new();
        while let Ok((seq, _)) = rx_lossy.try_recv() {
            got_lossy.push(seq);
        }
        assert_eq!(got_lossy, vec![0, 1, 2], "oldest k groups must survive");

        // Durable: got everything in order (independence).
        let got_durable = drain.await.unwrap();
        assert_eq!(got_durable, (0..n).collect::<Vec<_>>());
        bc.shutdown().await;
    }

    // ------- NeverDrop -------

    #[tokio::test]
    async fn never_drop_is_lossless_under_stall() {
        // Small queue, producer outruns consumer initially, then we drain.
        let (src_tx, src_rx) = mpsc::channel(4);
        let mut b = BroadcasterBuilder::new();
        let sub = b.subscribe("durable", 2, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());

        let n = 30u64;
        let producer = tokio::spawn(async move {
            for i in 0..n {
                // `send().await` back-pressures naturally when the chain stalls.
                src_tx.send((i, group(b"x"))).await.unwrap();
            }
        });

        // Start draining after a beat so the queues genuinely back up first.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut rx = sub.rx;
        for i in 0..n {
            let (seq, _) = recv(&mut rx).await;
            assert_eq!(seq, i, "NeverDrop must preserve every group in order");
        }
        producer.await.unwrap();
        assert_eq!(*sub.dropped.borrow(), 0, "NeverDrop never drops");
        bc.shutdown().await;
    }

    #[tokio::test]
    async fn multiple_never_drop_all_receive_in_order() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let a = b.subscribe("a", 64, DropPolicy::NeverDrop);
        let c = b.subscribe("c", 64, DropPolicy::NeverDrop);
        // spawn emits the >1-NeverDrop warn here.
        let bc = b.spawn(src_rx, CancellationToken::new());
        let mut rxa = a.rx;
        let mut rxc = c.rx;

        let n = 10u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        for i in 0..n {
            assert_eq!(recv(&mut rxa).await.0, i);
            assert_eq!(recv(&mut rxc).await.0, i);
        }
        bc.shutdown().await;
    }

    // ------- backpressure -------

    #[tokio::test]
    async fn never_drop_backpressures_source() {
        // Small source channel + small NeverDrop queue + stalled subscriber:
        // the fan-out task parks on send and stops draining the source, so the
        // source-side sender eventually sees a full channel.
        let (src_tx, src_rx) = mpsc::channel(2);
        let mut b = BroadcasterBuilder::new();
        let sub = b.subscribe("durable", 1, DropPolicy::NeverDrop);
        let cancel = CancellationToken::new();
        let bc = b.spawn(src_rx, cancel.clone());
        // Hold the rx so it never drains.
        let _rx = sub.rx;

        // Produce via try_send until the source channel reports full, proving
        // the fan-out task is parked rather than spinning.
        let mut saw_full = false;
        for i in 0..50u64 {
            match src_tx.try_send((i, group(b"x"))) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    saw_full = true;
                    break;
                }
                Err(e) => panic!("unexpected source send error: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            saw_full,
            "source channel must fill while subscriber is stalled"
        );

        // Cancel must unblock the parked send promptly.
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), bc.shutdown())
            .await
            .expect("shutdown did not resolve after cancel");
    }

    // ------- mixed policies -------

    #[tokio::test]
    async fn mixed_stall_sink_capture_unaffected() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let sink = b.subscribe("sink", SINK_QUEUE_DEPTH, DropPolicy::DropNewest);
        let capture = b.subscribe("capture", 64, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());

        // Drain capture; leave sink stalled.
        let mut rx_capture = capture.rx;
        let drain = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some((seq, _)) = rx_capture.recv().await {
                got.push(seq);
            }
            got
        });

        let n = 20u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        drop(src_tx);

        let mut fwd = bc.forwarded();
        wait_count(&mut fwd, n).await;

        // Sink kept only its queue depth; the rest dropped.
        let mut sink_dropped = sink.dropped;
        wait_count(&mut sink_dropped, n - SINK_QUEUE_DEPTH as u64).await;
        assert_eq!(*sink_dropped.borrow(), n - SINK_QUEUE_DEPTH as u64);

        // Capture got everything.
        let got_capture = drain.await.unwrap();
        assert_eq!(got_capture, (0..n).collect::<Vec<_>>());
        bc.shutdown().await;
    }

    // ------- phase ordering -------

    #[tokio::test]
    async fn current_group_reaches_dropnewest_before_parking_on_neverdrop() {
        let (src_tx, src_rx) = mpsc::channel(8);
        let mut b = BroadcasterBuilder::new();
        // Register the NeverDrop subscriber FIRST. Phase ordering is by policy,
        // not registration order, so a correct broadcaster still serves the
        // DropNewest `fast` in phase 1 before parking on `slow` in phase 2. A
        // naive single-pass-in-registration-order implementation would park on
        // `slow` first and `fast` would never receive group 1 — this test
        // fails for that implementation, which is the point.
        let slow = b.subscribe("slow", 1, DropPolicy::NeverDrop);
        let fast = b.subscribe("fast", 8, DropPolicy::DropNewest);
        let cancel = CancellationToken::new();
        let bc = b.spawn(src_rx, cancel.clone());
        let mut rx_fast = fast.rx;
        let _rx_slow = slow.rx; // never drained → fills after one group

        // Group 0: fast gets it (phase 1), slow queue takes its one slot.
        // Group 1: fast gets it (phase 1) BEFORE the task parks on slow (phase 2).
        src_tx.try_send((0, group(b"a"))).unwrap();
        src_tx.try_send((1, group(b"b"))).unwrap();

        assert_eq!(recv(&mut rx_fast).await.0, 0);
        assert_eq!(
            recv(&mut rx_fast).await.0,
            1,
            "fast subscriber must get the current group before the task parks"
        );

        cancel.cancel();
        bc.shutdown().await;
    }

    // ------- dead / zero subscribers -------

    #[tokio::test]
    async fn dead_subscriber_removed_others_continue() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let dead = b.subscribe("dead", 8, DropPolicy::DropNewest);
        let live = b.subscribe("live", 64, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());

        // Kill one subscriber before any traffic.
        drop(dead.rx);

        let mut rx_live = live.rx;
        let n = 15u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        for i in 0..n {
            assert_eq!(
                recv(&mut rx_live).await.0,
                i,
                "live subscriber keeps flowing"
            );
        }
        bc.shutdown().await;
    }

    #[tokio::test]
    async fn zero_live_subscribers_still_drains_source() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let only = b.subscribe("only", 8, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());
        drop(only.rx); // no live subscribers at all

        let n = 12u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        drop(src_tx);

        // The source must drain to completion (never wedge), then the task
        // exits on source close.
        let mut fwd = bc.forwarded();
        wait_count(&mut fwd, n).await;
        assert_eq!(*fwd.borrow(), n);
        tokio::time::timeout(Duration::from_secs(3), bc.shutdown())
            .await
            .expect("broadcaster did not exit after source close");
    }

    // ------- shutdown -------

    #[tokio::test]
    async fn cancel_closes_all_subscribers() {
        let (_src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let a = b.subscribe("a", 8, DropPolicy::NeverDrop);
        let c = b.subscribe("c", 8, DropPolicy::DropNewest);
        let cancel = CancellationToken::new();
        let bc = b.spawn(src_rx, cancel.clone());
        let mut rxa = a.rx;
        let mut rxc = c.rx;

        cancel.cancel();
        assert!(
            recv_closed(&mut rxa).await,
            "subscriber a must observe close"
        );
        assert!(
            recv_closed(&mut rxc).await,
            "subscriber c must observe close"
        );
        bc.shutdown().await;
    }

    #[tokio::test]
    async fn cancel_mid_never_drop_send_exits_promptly() {
        let (src_tx, src_rx) = mpsc::channel(8);
        let mut b = BroadcasterBuilder::new();
        let sub = b.subscribe("durable", 1, DropPolicy::NeverDrop);
        let cancel = CancellationToken::new();
        let bc = b.spawn(src_rx, cancel.clone());
        let _rx = sub.rx; // stalled

        // First group fills the queue; second parks the task on send().await.
        src_tx.try_send((0, group(b"x"))).unwrap();
        src_tx.try_send((1, group(b"x"))).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), bc.shutdown())
            .await
            .expect("task did not exit promptly when cancelled mid-send");
    }

    #[tokio::test]
    async fn source_close_exits_and_closes_subscribers() {
        let (src_tx, src_rx) = mpsc::channel(64);
        let mut b = BroadcasterBuilder::new();
        let sub = b.subscribe("s", 64, DropPolicy::NeverDrop);
        let bc = b.spawn(src_rx, CancellationToken::new());
        let mut rx = sub.rx;

        let n = 5u64;
        for i in 0..n {
            src_tx.send((i, group(b"x"))).await.unwrap();
        }
        drop(src_tx); // source closes

        for i in 0..n {
            assert_eq!(recv(&mut rx).await.0, i);
        }
        // After the drained source closes, the subscriber sees the channel close.
        assert!(
            recv_closed(&mut rx).await,
            "subscriber must close on source close"
        );
        tokio::time::timeout(Duration::from_secs(3), bc.shutdown())
            .await
            .expect("shutdown did not resolve after source close");
    }

    /// Await a `None` (closed) from a subscriber within a timeout.
    async fn recv_closed(rx: &mut mpsc::Receiver<Group>) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_secs(3), rx.recv()).await,
            Ok(None)
        )
    }
}
