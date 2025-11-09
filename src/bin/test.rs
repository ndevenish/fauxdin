

#[tokio::main]
async fn main() {
    // console_subscriber::init();

    let context = zmq::Context::new();
    let socket = context.socket(zmq::PULL).unwrap();
    socket.connect("tcp://127.0.0.1:9999").unwrap();
    let mut msg = zmq::Message::new();
    println!("Message: {:?}: {:?}", socket.recv(&mut msg, 0), msg);
}
