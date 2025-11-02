use std::{thread, time::Duration};

use fauxdin::zmq::PullSocket;

#[tokio::main]
async fn main() {
    // console_subscriber::init();

    thread::sleep(Duration::from_secs(1));
    let mut sock_in = PullSocket::connect("tcp://127.0.0.1:9999").unwrap();
    println!("Message: {:?}", sock_in.recv().await);
    println!("Got message in main thread; Stopping.");
}
