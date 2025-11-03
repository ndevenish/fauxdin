use std::{io::Write, panic, thread, time::Duration};

use anyhow::Result;
use tokio::sync::mpsc;
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

pub struct PullSocket {
    pub recv_task: Option<thread::JoinHandle<()>>,
    /// Token cancelled when the recv loop has finished
    fin_token: CancellationToken,
    cancel: CancellationToken,
    recv: mpsc::UnboundedReceiver<zmq::Message>,
    multipart_pending: Vec<zmq::Message>,
}

impl PullSocket {
    pub fn connect(endpoint: &str) -> Result<Self> {
        let token = CancellationToken::new();
        let inner_token = token.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        // let (closed_tx, closed_rx) = oneshot::channel();
        let fin_token = CancellationToken::new();
        let inner_fin_token = fin_token.clone();
        let inner_endpoint = endpoint.to_string();
        Ok(PullSocket {
            recv_task: Some(thread::spawn(move || {
                let result = panic::catch_unwind(move || {
                    let context = zmq::Context::new();
                    let socket = safesocket::Socket::new(context.socket(zmq::PULL).unwrap());
                    socket.set_rcvtimeo(100).unwrap();
                    socket.connect(&inner_endpoint).unwrap();
                    inner_recv(socket, inner_token, tx);
                });
                inner_fin_token.cancel();
                println!("Closing thread");
                if let Err(payload) = result {
                    panic::resume_unwind(payload);
                }
            })),
            fin_token,
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

    pub fn close(mut self) {
        self.cancel.cancel();
        self.recv_task.take().map(|v| v.join());
    }
    /// Wait (Cancel-safe) for the socket to be closed.
    pub async fn closed(&self) {
        self.fin_token.cancelled().await;
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
    while !token.is_cancelled() {
        // println!("Inner receive: Loop start");
        // print!("  inner_recv {}\r", spinner.next().unwrap());
        // let _ = std::io::stdout().flush();

        let mut msg = zmq::Message::new();
        match socket.recv(&mut msg, 0) {
            Ok(_) => {
                print!(" Inner receive: Got message {}\r", num);
                num += 1;
                if sender.send(msg).is_err() {
                    break;
                }
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
