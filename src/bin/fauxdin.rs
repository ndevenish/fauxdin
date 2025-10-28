use std::thread::JoinHandle;

use anyhow::Result;
use epicars::{ServerBuilder, client::Watcher};
use fauxdin::zmq::PullSocket;
use tokio::{
    runtime::{self, Runtime},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, trace};
use tracing_subscriber::EnvFilter;

/// Asynchronously call recv, if the argument is Some()
async fn maybe_recv(t: &mut Option<PullSocket>) -> Option<Option<zmq::Message>> {
    match t {
        Some(socket) => Some(socket.recv().await),
        None => None,
    }
}
/// Asynchronously call recv_multipart, if the argument is Some()
async fn maybe_recv_multipart(t: &mut Option<PullSocket>) -> Option<Option<Vec<zmq::Message>>> {
    match t {
        Some(socket) => Some(socket.recv_multipart().await),
        None => None,
    }
}

/// Basic wrapper of message data (as [`zmq::Message`] is not Clone)
struct Message {
    pub data: Vec<u8>,
    pub is_more: bool,
}
impl From<&zmq::Message> for Message {
    fn from(value: &zmq::Message) -> Self {
        Message {
            data: value.to_vec(),
            is_more: value.get_more(),
        }
    }
}

/// Handle movement of messages between input and output ZMQ streams
async fn do_pump(
    mut target_endpoint: Watcher<String>,
    push_endpoint: &str,
    stop: CancellationToken,
    copy_to: mpsc::UnboundedSender<Message>,
) -> Result<()> {
    // Make the sockets
    let mut sock_in = None;
    let endpoint = target_endpoint.borrow_and_update()?;
    if !endpoint.is_empty() {
        sock_in = Some(PullSocket::connect(&endpoint)?);
    }

    let ctx = zmq::Context::new();
    let sock_out = ctx.socket(zmq::SocketType::PUSH)?;
    sock_out.bind(push_endpoint)?;

    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            Ok(_) = target_endpoint.changed() => {
                // We have updated the target
                let endpoint = target_endpoint.borrow_and_update().unwrap();
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
            Some(Some(msg)) = maybe_recv(&mut sock_in) => {
                // Send the message internally. This is a fatal error if
                // it fails, because otherwise we are just a message
                // pump with no way to resume mirroring - meaning that
                // we will eventually have to terminate anyway.
                copy_to.send((&msg).into()).expect("Failed to mirror messages: Was it dropped without clean shutdown?");
                let get_more = msg.get_more();
                // Equally, failing to pass on the message is also a fatal error
                trace!("Forwarded {} byte message to output.", msg.len());
                sock_out.send(msg, if get_more { zmq::SNDMORE } else { 0 }).unwrap();
            }

        }
    }
    Ok(())
}

struct PumpHandle {
    handle: Option<JoinHandle<()>>,
    stop: CancellationToken,
    messages: mpsc::UnboundedReceiver<Message>,
    multipart_pending: Vec<Vec<u8>>,
}

impl PumpHandle {
    fn start(target_endpoint: Watcher<String>, out_push_endpoint: &str) -> Self {
        let stop = CancellationToken::new();
        let inner_stop = stop.clone();
        let inner_out_endpoint = out_push_endpoint.to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = Some(std::thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async move {
                do_pump(target_endpoint, &inner_out_endpoint, inner_stop, tx)
                    .await
                    .unwrap();
            })
        }));
        Self {
            handle,
            stop,
            messages: rx,
            multipart_pending: Vec::new(),
        }
    }
    pub fn stop(&mut self) {
        self.stop.cancel();
        self.handle.take().map(|h| h.join());
    }
    /// Receive a single message. Cancel-safe.
    pub async fn recv(&mut self) -> Option<Message> {
        self.messages.recv().await
    }
    /// Receive a multipart message. Cancel-safe.
    ///
    /// Returns `None` when the sending channel has been closed and there
    /// are no more individual messages to deliver. If the sending channel
    /// is closed and there are incomplete multipart messages, the partial
    /// message will be returned.
    pub async fn recv_multipart(&mut self) -> Option<Vec<Vec<u8>>> {
        loop {
            match self.messages.recv().await {
                Some(msg) => {
                    let is_more = msg.is_more;
                    self.multipart_pending.push(msg.data);
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
    let target: Watcher<String> = library
        .build_pv("FAUXDIN:DETECTOR", "tcp://127.0.0.1:9999".into())
        .minimum_length(100)
        .build()
        .unwrap()
        .watch();
    let mut enabled = library.add_pv("FAUXDIN:ENABLED", true).unwrap().watch();

    let _server = ServerBuilder::new(library).start().await.unwrap();
    let mut pump = PumpHandle::start(target, "tcp://0.0.0.0:9999");
    loop {
        tokio::select! {
            Ok(_) = enabled.changed() => {
                if enabled.borrow_and_update().unwrap() {

                } else {

                }
            },
            Some(messages) = pump.recv_multipart() => {
                println!("Received: {} messages, size: {}", messages.len(),  messages.iter().map(|m| m.len().to_string()).collect::<Vec<_>>().join(", "));
            }
        }
    }
}
