use std::{cell::RefCell, sync::Arc, thread};

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

// struct Context {
//     context: zmq::Context,
// }
// impl Context {
//     pub fn new() -> Self {
//         Self {
//             context: zmq::Context::new(),
//         }
//     }
//     pub fn push(&self) -> Result<Socket> {
//         Ok(Socket {
//             socket: Some(self.context.socket(zmq::PUSH)?),
//             recv_task: Default::default(),
//             cancel: CancellationToken::new(),
//         })
//     }
//     pub fn pull(&self) -> Result<Socket> {
//         Ok(Socket {
//             socket: Some(self.context.socket(zmq::PULL)?),
//             recv_task: Default::default(),
//             cancel: CancellationToken::new(),
//         })
//     }
// }

// struct InnerSocket {

// }

pub struct PullSocket {
    recv_task: Option<thread::JoinHandle<()>>,
    cancel: CancellationToken,
    recv: mpsc::UnboundedReceiver<zmq::Message>,
}

impl PullSocket {
    pub fn connect(endpoint: &str) -> Result<Self> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::PULL)?;
        socket.monitor("inproc://monitor", zmq::SocketEvent::ALL as i32)?;
        socket.connect(endpoint)?;
        socket.set_rcvtimeo(100)?;
        let token = CancellationToken::new();
        let inner_token = token.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        Ok(PullSocket {
            recv_task: Some(thread::spawn(|| {
                inner_recv(socket, inner_token, tx);
            })),
            cancel: token,
            recv: rx,
        })
    }
    pub async fn recv(&mut self) -> Option<zmq::Message> {
        self.recv.recv().await
    }
    pub fn close(mut self) {
        self.cancel.cancel();
        self.recv_task.take().map(|v| v.join());
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
                println!("Sending {msg:?}");
                if sender.send(msg).is_err() {
                    println!("Error: Failed to send");
                    break;
                }
            }
            Err(zmq::Error::EAGAIN) => {
                println!("EAGAIN");
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
