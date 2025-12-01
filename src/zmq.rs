use std::{
    panic,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::{
    runtime::Builder,
    select,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

use url::Url;
use zmq::Message;

/// Wrapper for zmq PUSH socket sending data out of the program
///
/// PushSocket contains it's own (bounded) message buffer, alongside any ZMQ
/// internal high water mark buffer. This means that you can definitively tell
/// that the buffer is full (e.g. not being drained at all, or being drained
/// slowly). This allows us to guarantee a set amount of message buffering, and
/// avoids issues where the initial images might get dropped because the PUSH
/// socket doesn't start buffering until it's seen a client.
pub struct PushSocket {
    cancel_token: CancellationToken,
    /// Queue for messages.
    tx: mpsc::UnboundedSender<(zmq::Message, OwnedSemaphorePermit)>,
    tx_permits: Arc<Semaphore>,
    /// The total allowed enqueued messages. The actual number may be higher
    /// than this if it was resized while above the new capacity.
    buffer_size: usize,
    /// How many permits we need to drop before we can send again.
    buffer_debt: AtomicUsize,
    /// The inner loop join handle
    inner_loop: Option<JoinHandle<()>>,
    /// The bound port, if known. Will be None for non-tcp protocols.
    port: Option<u16>,
}
impl PushSocket {
    /// Bind a zmq PUSH socket, given context and initial buffer length
    ///
    /// Regardless of ZeroMQ HWM settings or whether the socket is being
    /// actively drained by an external client, `presocket_queue_length` items
    /// can be buffered inside the [`PushSocket`]. This quantity can be
    /// adjusted at run-time without reopening the socket.
    ///
    /// In addition to this buffer, the ZeroMQ socket itself will be set up
    /// with a high water mark capacity of `zmq_send_hmw`, so the maximum
    /// number of messages that can be buffered is `presocket_queue_length +
    /// zmq_send_hwm`, depending on whether the ZeroMQ socket is internally
    /// buffering (it does not do this before being connected to, for
    /// instance). Whereas normally in ZMQ a HWM of zero means "no limit", here
    /// it will cause an error, because if you want an unlimited buffer then
    /// this socket wrapper is unnecessary.
    ///
    /// Awaiting will wait for the socket to be bound and the worker thread to
    /// be started.
    pub async fn bind(
        endpoint: &str,
        context: zmq::Context,
        cancel_token: CancellationToken,
        presocket_queue_length: usize,
        zmq_send_hwm: usize,
    ) -> Result<PushSocket> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let (launch_tx, launch_rx) = oneshot::channel();
        let cancel_token = cancel_token.child_token();
        let inner_cancel_token = cancel_token.clone();
        let inner_endpoint = endpoint.to_string();
        let inner_loop = tokio::task::spawn_blocking(move || {
            let rt = Builder::new_current_thread().enable_time().build().unwrap();

            let sock_out = match context.socket(zmq::SocketType::PUSH) {
                Ok(sock) => sock,
                Err(e) => {
                    let _ = launch_tx.send(Err(e.into()));
                    return;
                }
            };
            let Ok(snd_hwm): Result<i32, _> = zmq_send_hwm.try_into() else {
                let _ = launch_tx.send(Err(anyhow!(
                    "zmq_send_hwm {zmq_send_hwm} is too large (must be i32)"
                )));
                return;
            };
            if let Err(e) = sock_out.set_sndhwm(snd_hwm) {
                let _ = launch_tx.send(Err(e.into()));
                return;
            };
            if let Err(e) = sock_out.bind(&inner_endpoint) {
                let _ = launch_tx.send(Err(e.into()));
                return;
            }
            let real_port = match sock_out.get_last_endpoint() {
                Ok(Ok(endpoint)) => match Url::parse(&endpoint) {
                    Ok(url) => url.port(),
                    _ => None,
                },
                _ => None,
            };

            rt.block_on(async move {
                let _ = launch_tx.send(Ok(real_port));
                loop {
                    let msg: Message = select! {
                        _ = inner_cancel_token.cancelled() => break,
                        maybe_msg = rx.recv() => match maybe_msg {
                            Some((msg, _)) => msg,
                            None => break,
                        }
                    };

                    // Currently, rust zmq always swallows the message,
                    // even on failure. So, pull the data out here and copy
                    // it every time we try to send.
                    // See: https://github.com/erickt/rust-zmq/issues/211
                    let more_flag = if msg.get_more() { zmq::SNDMORE } else { 0 };
                    let message_data = msg.to_vec();

                    // Now, we want to send this message into the output
                    // socket. But, we need to be careful to do this in
                    // a way that we can cancel if need be. So, try to send,
                    // and wait a short backoff if we fail.
                    loop {
                        match sock_out.send(&message_data, zmq::DONTWAIT | more_flag) {
                            Ok(()) => break,
                            Err(zmq::Error::EAGAIN) => {
                                select! {
                                    _ = inner_cancel_token.cancelled() => break,
                                    _ = tokio::time::sleep(Duration::from_millis(50)) => (),
                                }
                            }
                            Err(e) => panic!("Got unexpected error trying to send: {e}"),
                        }
                    }
                }
                debug!("Closing PushSocket thread");
            });
        });
        // Wait for the actual binding
        let port = launch_rx.await??;
        Ok(PushSocket {
            cancel_token: cancel_token,
            tx,
            tx_permits: Arc::new(Semaphore::new(presocket_queue_length)),
            buffer_size: presocket_queue_length,
            buffer_debt: 0.into(),
            inner_loop: Some(inner_loop),
            port,
        })
    }

    /// Attempt to send a message to the PUSH socket
    ///
    /// This will never block, but if the queue is full (or closed) will return
    /// an associated Err.
    pub fn try_send(
        &self,
        message: zmq::Message,
    ) -> Result<(), mpsc::error::TrySendError<zmq::Message>> {
        // First, if we have any leftover permits to drop because of a resize
        let debt = self.buffer_debt.load(Ordering::Relaxed);
        if debt > 0 {
            self.buffer_debt
                .fetch_sub(self.tx_permits.forget_permits(debt), Ordering::Relaxed);
        }
        let Ok(permit) = self.tx_permits.clone().try_acquire_owned() else {
            return Err(mpsc::error::TrySendError::Full(message));
        };
        // We have a permit to send
        if let Err(mpsc::error::SendError((message, _))) = self.tx.send((message, permit)) {
            // We failed to send, which means the channel was closed
            return Err(mpsc::error::TrySendError::Closed(message));
        }
        Ok(())
    }
    /// Alter the internal buffer capacity.
    ///
    /// If lowering below the current queue length, this will prevent
    /// messages from being sent until it drops below the new value.
    pub fn set_buffer_length(&mut self, new_length: usize) {
        if new_length == self.buffer_size {
        } else if new_length > self.buffer_size {
            // Work out any interactions with any previous outstanding debt
            let mut debt = self.buffer_debt.load(Ordering::SeqCst);
            let mut to_add = new_length - self.buffer_size;
            if to_add >= debt {
                to_add -= debt;
                debt = 0;
            } else {
                to_add = 0;
                debt -= to_add;
            }
            if to_add > 0 {
                self.tx_permits.add_permits(to_add);
            }
            self.buffer_debt.store(debt, Ordering::SeqCst);
        } else {
            // Reducing buffer size
            let to_forget = self.buffer_size - new_length;
            let debt = to_forget - self.tx_permits.forget_permits(to_forget);
            self.buffer_debt.store(debt, Ordering::Relaxed);
            self.buffer_size = new_length;
        }
        self.buffer_size = new_length;
    }

    /// Get the instantaneous count of buffered messages
    pub fn buffered_count(&self) -> usize {
        let out = (self.buffer_size + self.buffer_debt.load(Ordering::Relaxed))
            .saturating_sub(self.tx_permits.available_permits());
        println!(
            "Size: {} + debt {} - avail {} = {}",
            self.buffer_size,
            self.buffer_debt.load(Ordering::Relaxed),
            self.tx_permits.available_permits(),
            out
        );
        out
    }

    /// Close the socket and wait for it finish closing
    pub fn close(mut self) -> JoinHandle<()> {
        self.cancel_token.cancel();
        self.inner_loop.take().unwrap()
    }
    pub fn port(&self) -> Option<u16> {
        self.port
    }
}

impl Drop for PushSocket {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

pub struct PullSocket {
    cancel: CancellationToken,
    recv: mpsc::UnboundedReceiver<zmq::Message>,
    multipart_pending: Vec<zmq::Message>,
}

impl PullSocket {
    pub fn connect(endpoint: &str, context: zmq::Context, tasks: &mut JoinSet<()>) -> Result<Self> {
        let token = CancellationToken::new();
        let inner_token = token.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        // let (closed_tx, closed_rx) = oneshot::channel();
        // let fin_token = CancellationToken::new();
        // let inner_fin_token = fin_token.clone();
        let inner_endpoint = endpoint.to_string();
        tasks.spawn_blocking(|| {
            thread::spawn(move || {
                let socket = context.socket(zmq::PULL).unwrap();
                socket.set_rcvtimeo(100).unwrap();
                socket.connect(&inner_endpoint).unwrap();
                inner_recv(socket, inner_token, tx);
                println!("Closing thread");
            })
            .join()
            .unwrap();
        });

        Ok(PullSocket {
            cancel: token,
            recv: rx,
            multipart_pending: Vec::new(),
        })
    }
    pub async fn recv(&mut self) -> Option<zmq::Message> {
        if !self.multipart_pending.is_empty() {
            // We are partway through receiving a multipart message (e.g.
            // the awaiting future was cancelled before finishing). Just
            // return the first message.
            Some(self.multipart_pending.remove(0))
        } else {
            self.recv.recv().await
        }
    }
    pub async fn recv_multipart(&mut self) -> Option<Vec<zmq::Message>> {
        loop {
            match self.recv.recv().await {
                Some(msg) => {
                    let is_more = msg.get_more();
                    self.multipart_pending.push(msg);
                    // Keep looping here until we have all the messages
                    if is_more {
                        continue;
                    } else {
                        break;
                    }
                }
                None => {
                    if self.multipart_pending.is_empty() {
                        return None;
                    } else {
                        break;
                    }
                }
            }
        }
        Some(std::mem::take(&mut self.multipart_pending))
    }

    pub async fn close(self) {
        self.cancel.cancel();
        todo!();
        // self.recv_task.take().map(|v| v.join());
    }
    /// Wait (Cancel-safe) for the socket to be closed.
    pub async fn closed(&self) {
        // self.recv_task.
        todo!();
    }
}

impl Drop for PullSocket {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Inner actor to handle recv messages
fn inner_recv(
    socket: zmq::Socket,
    token: CancellationToken,
    sender: mpsc::UnboundedSender<zmq::Message>,
) {
    let mut num = 0usize;
    let mut num_fin = 0usize;
    while !token.is_cancelled() {
        // println!("Inner receive: Loop start");
        // print!("  inner_recv {}\r", spinner.next().unwrap());
        // let _ = std::io::stdout().flush();

        let mut msg = zmq::Message::new();
        match socket.recv(&mut msg, 0) {
            Ok(_) => {
                let is_more = msg.get_more();
                if sender.send(msg).is_err() {
                    break;
                }
                if !is_more {
                    num_fin += 1;
                }
                num += 1;
                print!(" Inner receive: Got message {} ({} final)\r", num, num_fin);
            }
            Err(zmq::Error::EAGAIN) => {
                // println!("Inner receive: Waiting for ZMQ, got EAGAIN");
                continue;
            }
            Err(e) => {
                error!("Unexpected return from zmq_recv: {e}");
                break;
            }
        }
    }
    println!("Inner receive: Ending");
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing_subscriber::fmt::format;

    use crate::zmq::PushSocket;

    use anyhow::Result;

    #[tokio::test]
    async fn test_push() -> Result<()> {
        let context = zmq::Context::new();
        let cancel = CancellationToken::new();
        let mut sock_out =
            PushSocket::bind("tcp://127.0.0.1:*", context, cancel.clone(), 10, 1).await?;
        println!("Bound to port: {:?}", sock_out.port());

        // Try to send enough messages to fill the buffer
        for n in 0u8..10 {
            sock_out.try_send(format!("{n}").as_bytes().into()).unwrap();
        }
        // One message will be picked up to be sent, so wait for this to happen
        tokio::time::sleep(Duration::from_millis(10)).await;
        sock_out.try_send("11".as_bytes().into()).unwrap();
        // We should now have a full queue, ensure that sending fails
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sock_out.buffered_count(), 10);
        assert!(matches!(
            sock_out.try_send("message".as_bytes().into()),
            Err(mpsc::error::TrySendError::Full(_)),
        ));
        // Change the buffer size, make sure we can now send
        sock_out.set_buffer_length(11);
        assert_eq!(sock_out.buffered_count(), 10);
        sock_out.try_send("message".as_bytes().into()).unwrap();
        assert_eq!(sock_out.buffered_count(), 11);

        // Connect a zeromq socket to this
        let inctx = zmq::Context::new();
        let sock_in = inctx.socket(zmq::SocketType::PULL).unwrap();
        sock_in.set_rcvhwm(1).unwrap();
        sock_in
            .connect(&format!("tcp://127.0.0.1:{}", sock_out.port().unwrap()))
            .unwrap();
        // Get the first message
        assert_eq!(
            sock_in.recv_bytes(0).unwrap(),
            "0".as_bytes(),
            "Got test message out of connected socket"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sock_out.buffered_count(), 10);
        assert_eq!(sock_in.recv_bytes(0).unwrap(), "1".as_bytes(),);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sock_out.buffered_count(), 9);
        // drop(sock_in);
        // Now, check how our sender is doing
        println!("Output buffer: {}", sock_out.buffered_count());
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Cleanup
        sock_out.close().await.unwrap();
        Ok(())
    }
}
