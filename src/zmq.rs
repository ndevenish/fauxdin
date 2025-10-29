use std::{panic, thread};

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub struct PullSocket {
    recv_task: Option<thread::JoinHandle<()>>,
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
                    // Spin these up on the same thread, as Socket should be !Send
                    let context = zmq::Context::new();
                    let socket = context.socket(zmq::PULL).unwrap();
                    socket
                        .monitor("inproc://monitor.req", zmq::SocketEvent::ALL as i32)
                        .unwrap();
                    socket.connect(&inner_endpoint).unwrap();
                    socket.set_rcvtimeo(100).unwrap();
                    inner_recv(socket, inner_token, tx);
                });
                inner_fin_token.cancel();
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
            // We were asked for a single message before the multipart ended... return it
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

/// Inner actor to handle recv messages
fn inner_recv(
    socket: zmq::Socket,
    token: CancellationToken,
    sender: mpsc::UnboundedSender<zmq::Message>,
) {
    while !token.is_cancelled() {
        let mut msg = zmq::Message::new();
        match socket.recv(&mut msg, 0) {
            Ok(_) => {
                if sender.send(msg).is_err() {
                    println!("Error: Failed to send");
                    break;
                }
            }
            Err(zmq::Error::EAGAIN) => {
                continue;
            }
            Err(e) => {
                warn!("Unexpected return from zmq_recv: {e}");
                break;
            }
        }
    }
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
