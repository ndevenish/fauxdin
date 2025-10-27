use fauxdin::zmq::PullSocket;

#[tokio::main]
async fn main() {
    // let c = zmq::Context::new();
    // let s = c.socket(zmq::PULL).unwrap();
    // s.connect("tcp://127.0.0.1:9999").unwrap();
    // let mut msg = zmq::Message::new();
    // s.recv(&mut msg, 0).unwrap();
    // println!("{:?}", msg.as_str());

    let mut p = PullSocket::connect("tcp://127.0.0.1:9999").unwrap();
    println!("{:?}", p.recv().await);
}
