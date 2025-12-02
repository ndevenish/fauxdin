use core::panic;
use std::{
    path::{Path, PathBuf},
    thread::JoinHandle,
};

use anyhow::Result;
use clap::Parser;
use epicars::{ServerBuilder, client::Watcher};
use fauxdin::{
    writers::FolderWriter,
    zmq::{BufferedPushSocket, PullSocket},
};
use tokio::{runtime, sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::EnvFilter;

/// Asynchronously call recv, if the argument is Some()
async fn maybe_recv(t: &mut Option<PullSocket>) -> Option<Option<zmq::Message>> {
    match t {
        Some(socket) => Some(socket.recv().await),
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
    mut enabled: Watcher<bool>,
    mut target_endpoint: Watcher<String>,
    push_endpoint: &str,
    stop: CancellationToken,
    copy_to: mpsc::UnboundedSender<Message>,
) -> Result<()> {
    let ctx = zmq::Context::new();
    let mut subtasks = JoinSet::new();
    // Make the sockets
    let mut sock_in = None;
    {
        let endpoint = target_endpoint.borrow_and_update()?;
        if !endpoint.is_empty() && enabled.borrow_and_update()? {
            sock_in = Some(PullSocket::connect(&endpoint, ctx.clone(), &mut subtasks)?);
        }
    }

    let socket_out = BufferedPushSocket::bind(push_endpoint, ctx.clone(), stop.clone(), 500, 50)
        .await
        .unwrap();

    info!(
        "Starting message pump from {} to {}",
        target_endpoint
            .borrow_and_update()
            .unwrap_or("<ERROR>".to_string()),
        push_endpoint
    );
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            Ok(_) = enabled.changed() => {
                let endpoint = target_endpoint.borrow_and_update()?;
                let enabled = enabled.borrow_and_update()?;
                if enabled && sock_in.is_none() {
                    // Turning on. Connect to target again
                    info!("Message pump enabled via PV. Connecting to {endpoint}");
                    sock_in = Some(PullSocket::connect(&endpoint, ctx.clone(), &mut subtasks)?);
                } else if !enabled && let Some(socket) = sock_in.take() {
                    // Turning off. Close down the input port.
                    info!("Message pump disabled. Closing down incoming ZMQ connection.");
                    socket.close().await;
                }
            },
            Ok(_) = target_endpoint.changed() => {
                // We have updated the target
                let endpoint = target_endpoint.borrow_and_update().unwrap();
                if let Some(sock_to_close) = sock_in.take() {
                    sock_to_close.close().await;
                }
                if endpoint.is_empty() {
                    info!("Connection endpoint cleared, suspending connection");
                } else {
                    info!("Connection target changed to {endpoint}, making new connection");
                    sock_in = Some(PullSocket::connect(&endpoint, ctx.clone(), &mut subtasks)?);
                }
            },
            Some(Some(msg)) = maybe_recv(&mut sock_in) => {
                // Send the message internally. This is a fatal error if
                // it fails, because otherwise we are just a message
                // pump with no way to resume mirroring - meaning that
                // we will eventually have to terminate anyway.
                copy_to.send((&msg).into()).expect("Failed to mirror messages: Was it dropped without clean shutdown?");
                trace!("Forwarded {} byte message", msg.len());

                // Try to send this message to the output socket. If this fails,
                // then we may want to carry on anyway
                match socket_out.try_send(msg) {
                    Ok(()) => (),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Unfortunate, but what we are here to deal with
                        warn!("Failed to forward message to PUSH, dropping");
                    },
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        panic!("Internal output PUSH socket closed; did we fail to cleanly shutdown?");
                    }
                }
            },
        }
    }
    info!("Closed message pump");
    Ok(())
}

struct PumpHandle {
    handle: Option<JoinHandle<()>>,
    stop: CancellationToken,
    messages: mpsc::UnboundedReceiver<Message>,
    multipart_pending: Vec<Vec<u8>>,
}

impl PumpHandle {
    fn start(
        enabled: Watcher<bool>,
        target_endpoint: Watcher<String>,
        out_push_endpoint: &str,
    ) -> Self {
        let stop = CancellationToken::new();
        let inner_stop = stop.clone();
        let inner_out_endpoint = out_push_endpoint.to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        // Start the pump in it's own current thread
        let handle = Some(std::thread::spawn(move || {
            let rt = runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async move {
                do_pump(
                    enabled,
                    target_endpoint,
                    &inner_out_endpoint,
                    inner_stop,
                    tx,
                )
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
            match self.recv().await {
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

/// Act as a passthrough for streamed detector data, optionally
/// intercepting and mirroring the data stream.
#[derive(Parser, Debug)]
struct Args {
    /// Where to stream intercepted values to. Currently, only accepts a folder target.
    #[arg(default_value = "./dump")]
    output: String,
    /// Show debug output by default
    #[arg(short, action = clap::ArgAction::Count)]
    verbose: u8,
}
impl Args {
    /// Return the user verbosity setting as a string
    fn verbosity(&self) -> &str {
        match self.verbose {
            0 => "info",
            1 => "debug",
            2.. => "trace",
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Handle setting up default logging:
    // - This crate should be whatever is specified by the verbosity argument (default "info")
    // - All other crates should be "warn"
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::default()
            .add_directive("warn".parse().unwrap())
            .add_directive(
                format!("{}={}", env!("CARGO_CRATE_NAME"), args.verbosity())
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
    let mut mirror = library.add_pv("FAUXDIN:MIRROR", true).unwrap().watch();

    let _server = ServerBuilder::new(library).start().await.unwrap();
    let mut pump = PumpHandle::start(enabled.clone(), target, "tcp://0.0.0.0:9998");
    info!("Writing data stream out to: {}", args.output);
    let writer = FolderWriter::new(Path::new(&args.output));
    loop {
        tokio::select! {
            _ = enabled.changed() => {
                debug!("Enable signal changed");
            },
            _ = mirror.changed() => {
                debug!("Mirroring toggled");
                writer.toggle(mirror.borrow().unwrap_or_default());
            }
            m = pump.recv_multipart() => match m {
                Some(messages) => if mirror.borrow().unwrap_or_default() {
                    writer.write(messages);
                },
                None => {
                    error!("Internal receiver terminated prematurely");
                    break;
                }
            }
        }
    }
    pump.stop();
    Ok(())
}
