use std::time::Duration;

use anyhow::Result;
use epicars::{ServerBuilder, client::Watcher};
use fauxdin::zmq::PullSocket;
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

async fn maybe_recv(t: &mut Option<PullSocket>) -> Option<Option<zmq::Message>> {
    match t {
        Some(socket) => Some(socket.recv().await),
        None => todo!(),
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

    // let ctx_in = rzmq::Context::new().unwrap();
    // let ctx_out = rzmq::Context::new().unwrap();

    // Create the pipe out, for others to connect to
    // let push_endpoint = "tcp://0.0.0.0:9999";
    // info!("Creating output PUSH socket {push_endpoint}");
    // let sock_out = ctx_out.socket(rzmq::SocketType::Push)?;

    let current_target = target.borrow_and_update().unwrap();
    let mut sock_in = Some(PullSocket::connect(&current_target)?);
    info!("Connecting PULL socket to {current_target}");

    let _server = ServerBuilder::new(library).start().await.unwrap();

    let ct = zmq::Context::new();
    let mon = ct.socket(zmq::PAIR)?;
    mon.connect("inproc://monitor")?;
    loop {
        let event_msg = match mon.recv_msg(zmq::DONTWAIT) {
            Ok(msg) => msg,
            Err(zmq::Error::EAGAIN) => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => Err(e)?,
        };
        println!("Message: {event_msg:?}");
    }
    // println!("{:?}", &sock_in.as_mut().unwrap().recv().await);

    // let ctx = zmq::Context::new();

    // loop {
    //     tokio::select! {
    //         Ok(_) = target.changed() => {
    //             // The user changed the connection endpoint via our PV server
    //             let endpoint = target.borrow_and_update().unwrap();
    //             if let Some(sock_to_close) = sock_in.take() {
    //                 sock_to_close.close();
    //             }
    //             if endpoint.is_empty() {
    //                 info!("Connection endpoint cleared, suspending connection");
    //                 continue;
    //             }

    //             info!("Connection target changed to {endpoint}, making new connection");
    //             sock_in = Some(PullSocket::connect(&endpoint)?);
    //             continue;
    //         },
    //         Some(message) = maybe_recv(&mut sock_in) => {
    //             println!("Received: {message:?}");
    //         }
    //     }
    // }
}
