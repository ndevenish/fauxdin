use anyhow::Result;
use epicars::client::Watcher;
use tracing::info;
use tracing_subscriber::EnvFilter;
use zeromq::Socket;

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
        .build_pv("FAUXDIN:DETECTOR", "tcp://localhost:9999".into())
        .minimum_length(100)
        .build()
        .unwrap()
        .watch();

    // Create the pipe out, for others to connect to
    let mut out = zeromq::PushSocket::new();
    let push_endpoint = "tcp://0.0.0.0:9999";
    info!("Creating output PUSH socket {push_endpoint}");
    out.bind(push_endpoint).await?;

    // Create the input pipe, that connects to the detector
    let mut zmq_in = zeromq::PullSocket::new();
    info!(
        "Connecting PULL socket to {}",
        target.borrow_and_update().unwrap()
    );
    zmq_in.connect(&target.borrow_and_update().unwrap()).await?;
    loop {
        tokio::select! {
            _ = target.changed() => {
                let endpoint = target.borrow_and_update().unwrap();
                info!("Connection target changed to {endpoint}");
                zmq_in.connect(&endpoint).await?;
                continue;
            },

        }
    }
}
