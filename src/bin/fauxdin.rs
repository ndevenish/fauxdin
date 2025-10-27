use std::{thread::JoinHandle, time::Duration};

use anyhow::Result;
use epicars::{ServerBuilder, client::Watcher};
use fauxdin::zmq::PullSocket;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
// use rzmq::{Msg, ZmqError};
use tracing::info;
use tracing_subscriber::EnvFilter;

// async fn maybe_recv_multipart(
//     t: &mut Option<CancelSafeSocket>,
// ) -> Option<Result<Vec<rzmq::Msg>, rzmq::ZmqError>> {
//     match t {
//         Some(socket) => Some(socket.recv_multipart().await),
//         None => None,
//     }
// }

async fn maybe_recv_multipart(t: &mut Option<PullSocket>) -> Option<Option<Vec<zmq::Message>>> {
    match t {
        Some(socket) => Some(socket.recv_multipart().await),
        None => None,
    }
}
/// Wrap an [`rzmq::Socket`] in a cancel-safe way
// struct CancelSafeSocket {
//     socket: rzmq::Socket,
//     messages: Vec<Msg>,
// }

// impl CancelSafeSocket {
//     pub fn new(socket: rzmq::Socket) -> Self {
//         Self {
//             socket,
//             messages: Vec::new(),
//         }
//     }
//     /// Receive a multi-part message. Cancel-safe.
//     pub async fn recv_multipart(&mut self) -> Result<Vec<Msg>, ZmqError> {
//         loop {
//             let msg = match self.socket.recv().await {
//                 Ok(m) => m,
//                 Err(e) => return Err(e),
//             };
//             let is_last_message = !msg.is_more();
//             self.messages.push(msg);
//             if is_last_message {
//                 break;
//             }
//         }
//         Ok(std::mem::take(&mut self.messages))
//     }
//     pub async fn close(&mut self) -> Result<(), ZmqError> {
//         self.socket.close().await
//     }
// }

// struct ZMQPump {
//     sock_in: Option<PullSocket>,
//     sock_out: PushSocket,
// }

// impl ZMQPump {
//     fn new(initial_target_endpoint: &str, push_endpoint: &str) -> Self {}
// }

fn do_pump(target_endpoint: Watcher<String>, push_endpoint: &str, stop: CancellationToken) {
    while !stop.cancelled() {}
}
struct PumpHandle {
    handle: JoinHandle<()>,
    stop: CancellationToken,
    messages: mpsc::UnboundedReceiver<zmq::Message>,
}

impl PumpHandle {
    fn new() -> Self {}
    pub fn stop(&mut self) {
        self.stop.cancel();
    }
    pub async fn recv(&mut self) -> Option<zmq::Message> {
        self.messages.recv().await
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::default()
            .add_directive("warn".parse().unwrap())
            .add_directive(
                format!("{}=debug", env!("CARGO_CRATE_NAME"))
                    .parse()
                    .unwrap(),
            )
    });
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let mut library = epicars::providers::IntercomProvider::new();
    let mut target: Watcher<String> = library
        .build_pv("FAUXDIN:DETECTOR", "tcp://127.0.0.1:9999".into())
        .minimum_length(100)
        .build()
        .unwrap()
        .watch();
    let mut enabled = library.add_pv("FAUXDIN:ENABLED", true).unwrap().watch();

    let current_target = target.borrow_and_update().unwrap();
    let mut sock_in = Some(PullSocket::connect(&current_target)?);
    info!("Connecting PULL socket to {current_target}");

    let _server = ServerBuilder::new(library).start().await.unwrap();

    loop {
        tokio::select! {
            Ok(_) = target.changed() => {
                // The user changed the connection endpoint via our PV server.. connect to the new one
                let endpoint = target.borrow_and_update().unwrap();
                if let Some(sock_to_close) = sock_in.take() {
                    sock_to_close.close();
                }
                if endpoint.is_empty() {
                    info!("Connection endpoint cleared, suspending connection");
                } else {
                    info!("Connection target changed to {endpoint}, making new connection");
                    sock_in = Some(PullSocket::connect(&endpoint)?);
                }
            },
            Ok(_) = enabled.changed() => {
                if enabled.borrow_and_update().unwrap {

                } else {

                }
            },
            Some(Some(messages)) = maybe_recv_multipart(&mut sock_in) => {
                // let sizes =;
                println!("Received: {} messages, size: {}", messages.len(),  messages.iter().map(|m| m.len().to_string()).collect::<Vec<_>>().join(", "));
            }
        }
    }
}
