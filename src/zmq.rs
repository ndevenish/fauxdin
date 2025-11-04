use std::{
    io::Write,
    panic,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit, broadcast, mpsc, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

pub use safesocket::Socket;

#[allow(clippy::disallowed_types)]
mod safesocket {
    use std::{marker::PhantomData, ops::Deref, rc::Rc};

    /// Basic newtype wrapper of zmq::Socket that correctly marks the socket as !Sync and !Send
    pub struct Socket {
        inner: zmq::Socket,
        _nosend: PhantomData<Rc<()>>,
    }

    impl Deref for Socket {
        type Target = zmq::Socket;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }
    impl Socket {
        pub fn new(socket: zmq::Socket) -> Self {
            Self {
                inner: socket,
                _nosend: PhantomData,
            }
        }
        pub fn into_inner(self) -> zmq::Socket {
            self.inner
        }
        pub fn as_inner(&self) -> &zmq::Socket {
            &self.inner
        }
        pub fn as_inner_mut(&mut self) -> &zmq::Socket {
            &self.inner
        }
    }
}

pub struct PushSocket {
    cancel_token: CancellationToken,
    /// Queue for messages.
    tx: mpsc::UnboundedSender<(zmq::Message, OwnedSemaphorePermit)>,
    tx_permits: Arc<Semaphore>,
    /// The total allowed enqueued messages. The actual number may be higher
    /// than this if it was resized while above the new capacity.
    buffer_size: usize,
    // How many permits we need to drop before we can send again.
    buffer_debt: AtomicUsize,
}
impl PushSocket {
    pub fn bind(
        endpoint: &str,
        context: zmq::Context,
        presocket_queue_length: usize,
    ) -> PushSocket {
        let (tx, rx) = mpsc::unbounded_channel();
        PushSocket {
            cancel_token: CancellationToken::new(),
            tx,
            tx_permits: Arc::new(Semaphore::new(presocket_queue_length)),
            buffer_size: presocket_queue_length,
            buffer_debt: 0.into(),
        }
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
                let socket = safesocket::Socket::new(context.socket(zmq::PULL).unwrap());
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

    pub async fn close(mut self) {
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
pub fn ball_spinner() -> impl Iterator<Item = &'static str> {
    [
        "( ●    )",
        "(  ●   )",
        "(   ●  )",
        "(    ● )",
        "(     ●)",
        "(    ● )",
        "(   ●  )",
        "(  ●   )",
        "( ●    )",
        "(●     )",
    ]
    .iter()
    .copied()
    .cycle()
}

/// Inner actor to handle recv messages
fn inner_recv(
    socket: safesocket::Socket,
    token: CancellationToken,
    sender: mpsc::UnboundedSender<zmq::Message>,
) {
    let mut spinner = ball_spinner();
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

// struct PushSocket {
//     send_task: thread::JoinHandle<()>,
//     cancel: CancellationToken,
//     send: mpsc::UnboundedSender<zmq::Message>,
// }

// impl PushSocket {
//     fn bind(endpoint: &str) -> Result<Self> {
//         let context = zmq::Context::new();
//         let socket = context.socket(zmq::PUSH)?;
//         socket.bind(endpoint)?;

//         let token = CancellationToken::new();
//         let inner_token = token.clone();
//         let (tx, rx) = mpsc::unbounded_channel();
//         Ok(Self {
//             recv_task: thread::spawn(|| {
//                 inner_send(socket, inner_token, rx);
//             }),
//             cancel: token,
//             recv: rx,
//         })
//     }
// }

// /// Inner actor to handle send messages
// fn inner_send_multipart(
//     socket: zmq::Socket,
//     token: CancellationToken,
//     sender: mpsc::UnboundedReceiver<Vec<zmq::Message>>,
// ) {
//     while !token.is_cancelled() {
//         // socket.send_multipart(iter, flags)
//         // let mut msg = zmq::Message::new();
//         // match socket.recv(&mut msg, 0) {
//         //     Ok(_) => {
//         //         if sender.send(msg).is_err() {
//         //             break;
//         //         }
//         //     }
//         //     Err(zmq::Error::EAGAIN) => continue,
//         //     Err(e) => {
//         //         warn!("Unexpected return from zmq_recv: {e}");
//         //         break;
//         //     }
//         // }
//     }
// }
