use super::*;
use crate::config::config_tests::committee;
use crate::crypto::crypto_tests::keys;
use crate::messages::messages_tests::{block, vote};
use futures::future::try_join_all;
use tokio::task::JoinHandle;

// Fixture.
pub fn listener(address: SocketAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (read, write) = socket.into_split();
        let _transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
        let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
        match transport_read.next().await {
            Some(Ok(_)) => assert!(true),
            _ => assert!(false),
        }
    })
}

#[tokio::test]
async fn send() {
    // Make the network sender.
    let mut committee = committee();
    committee.increment_base_port(5000);
    let (myself, _) = keys().pop().unwrap();
    let sender = NetSender::make(myself, committee.clone()).await;

    // Run a TCP server.
    let (recipient, _) = keys().pop().unwrap();
    let recipient_address = committee.address(&recipient).unwrap();
    let handle = listener(recipient_address);

    // Send a vote.
    let message = NetMessage::Vote(vote(), recipient);
    let result = sender.send(message).await;
    assert!(result.is_ok());

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
    // Make the network sender.
    let mut committee = committee();
    committee.increment_base_port(5100);
    let mut keys = keys();
    let (myself, _) = keys.pop().unwrap();
    let sender = NetSender::make(myself, committee.clone()).await;

    // Run 3 TCP servers.
    let handles: Vec<_> = (0..3)
        .map(|_| {
            let (recipient, _) = keys.pop().unwrap();
            let recipient_address = committee.address(&recipient).unwrap();
            listener(recipient_address)
        })
        .collect();

    // Send a vote.
    let message = NetMessage::Block(block());
    let result = sender.send(message).await;
    assert!(result.is_ok());

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn receive() {
    // Make the network receiver.
    let mut committee = committee();
    committee.increment_base_port(5200);
    let (myself, _) = keys().pop().unwrap();
    let address = committee.address(&myself).unwrap();
    let (tx_core, mut rx_core) = channel(10);
    NetReceiver::make(&address, tx_core).await;

    // Make the address and message to send.
    let message = CoreMessage::Propose(block());
    let bytes = Bytes::from(bincode::serialize(&message).unwrap());

    // Send a value and ensure we get an ACK.
    let stream = TcpStream::connect(address.clone()).await.unwrap();
    let (read, write) = stream.into_split();
    let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
    transport_write.send(bytes.clone()).await.unwrap();
    if let None | Some(Err(_)) = transport_read.next().await {
        assert!(false);
    }

    // Ensure the message gets passed to the core.
    match rx_core.recv().await {
        Some(CoreMessage::Propose(b)) => assert_eq!(b, block()),
        _ => assert!(false),
    }
}
