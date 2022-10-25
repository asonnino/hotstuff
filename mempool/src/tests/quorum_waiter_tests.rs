use super::*;
use crate::common::{batch, committee_with_base_port, keys, listener};
use crate::mempool::MempoolMessage;
use bytes::Bytes;
use futures::future::try_join_all;
use network::ReliableSender;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn wait_for_quorum() {
    let (tx_message, rx_message) = channel(1);
    let (tx_batch, mut rx_batch) = channel(1);
    let (_, rx_ack) = channel(1);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_000);

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(
        committee.clone(),
        /* stake */ 1,
        rx_message,
        tx_batch,
        rx_ack,
    );

    // Make a batch.
    let message = MempoolMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();
    let expected = Bytes::from(serialized.clone());

    // Spawn enough listeners to acknowledge our batches.
    let mut names = Vec::new();
    let mut addresses = Vec::new();
    let mut listener_handles = Vec::new();
    for (name, address) in committee.broadcast_addresses(&myself) {
        let handle = listener(address, Some(expected.clone()));
        names.push(name);
        addresses.push(address);
        listener_handles.push(handle);
    }

    // Broadcast the batch through the network.
    let bytes = Bytes::from(serialized.clone());
    let handlers = ReliableSender::new().broadcast(addresses, bytes).await;

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let message = QuorumWaiterMessage {
        batch: serialized.clone(),
        handlers: handlers,
    };
    tx_message.send(message).await.unwrap();

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let (batch, _, _) = rx_batch.recv().await.unwrap();
    assert_eq!(batch, serialized);

    // Ensure the other listeners correctly received the batch.
    assert!(try_join_all(listener_handles).await.is_ok());
}

#[tokio::test]
async fn wait_for_quorum_ack() {
    let (tx_message, rx_message) = channel(1);
    let (tx_batch, mut rx_batch) = channel(1);
    let (tx_ack, rx_ack) = channel(1);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_000);

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(
        committee.clone(),
        /* stake */ 1,
        rx_message,
        tx_batch,
        rx_ack,
    );

    // Make a batch.
    let message = MempoolMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();

    // Digest of the batch.
    let digest = Digest::hash(&serialized);

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let message = QuorumWaiterMessage {
        batch: serialized.clone(),
        handlers: Vec::new(),
    };
    tx_message.send(message).await.unwrap();

    for (peer, _) in committee.broadcast_addresses(&myself) {
        tx_ack.send((peer, digest.clone())).await.unwrap();
    }

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let (batch, _, _) = rx_batch.recv().await.unwrap();
    assert_eq!(batch, serialized);
}
