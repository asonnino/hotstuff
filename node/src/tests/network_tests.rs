use super::*;
use crate::config::config_tests::committee;
use crate::crypto::crypto_tests::keys;
use crate::messages::messages_tests::{block, vote};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;

// Fixture.
pub async fn listener(address: SocketAddr) -> JoinHandle<()> {
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
    committee.set_base_port(5000);
    let (myself, _) = keys().pop().unwrap();
    let sender = NetSender::make(myself, committee.clone()).await;

    // Run a TCP server and wait a little to allow it to boot.
    let (recipient, _) = keys().pop().unwrap();
    let recipient_address = committee.address(&recipient).unwrap();
    let handle = listener(recipient_address).await;
    sleep(Duration::from_millis(100)).await;

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
    committee.set_base_port(5100);
    let mut keys = keys();
    let (myself, _) = keys.pop().unwrap();
    let sender = NetSender::make(myself, committee.clone()).await;

    // Run 3 TCP servers.
    let (recipient, _) = keys.pop().unwrap();
    let recipient_address = committee.address(&recipient).unwrap();
    let handle_1 = listener(recipient_address).await;

    let (recipient, _) = keys.pop().unwrap();
    let recipient_address = committee.address(&recipient).unwrap();
    let handle_2 = listener(recipient_address).await;

    let (recipient, _) = keys.pop().unwrap();
    let recipient_address = committee.address(&recipient).unwrap();
    let handle_3 = listener(recipient_address).await;

    // wWit a little to allow the servers to boot
    sleep(Duration::from_millis(100)).await;

    // Send a vote.
    let message = NetMessage::Block(block());
    let result = sender.send(message).await;
    assert!(result.is_ok());

    // Ensure all servers received the broadcast.
    assert!(handle_1.await.is_ok());
    assert!(handle_2.await.is_ok());
    assert!(handle_3.await.is_ok());
}

#[tokio::test]
async fn receive() {
    // Make the network receiver.
    let mut committee = committee();
    committee.set_base_port(5200);
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
