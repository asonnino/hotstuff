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
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_000);

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(committee.clone(), /* stake */ 1, rx_message, tx_batch);

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
        handlers: names.into_iter().zip(handlers.into_iter()).collect(),
    };
    tx_message.send(message).await.unwrap();

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let output = rx_batch.recv().await.unwrap();
    assert_eq!(output, serialized);

    // Ensure the other listeners correctly received the batch.
    assert!(try_join_all(listener_handles).await.is_ok());
}

#[tokio::test]
async fn dissemination_deadline_with_stalled_quorum() {
    let (tx_message, rx_message) = channel(1);
    let (tx_batch, mut rx_batch) = channel(1);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_100);
    let peers: Vec<_> = committee
        .broadcast_addresses(&myself)
        .into_iter()
        .map(|(name, _)| name)
        .collect();

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(committee.clone(), /* stake */ 1, rx_message, tx_batch);

    // Send a first batch: two peers acknowledge it (enough for a quorum), the third does not.
    let mut senders = Vec::new();
    let mut handlers = Vec::new();
    for name in &peers {
        let (tx, rx) = tokio::sync::oneshot::channel();
        senders.push(tx);
        handlers.push((*name, rx));
    }
    let mut slow = senders.pop().unwrap();
    for sender in senders {
        sender.send(Bytes::new()).unwrap();
    }
    let message = QuorumWaiterMessage {
        batch: vec![1],
        handlers,
    };
    tx_message.send(message).await.unwrap();
    assert_eq!(rx_batch.recv().await.unwrap(), vec![1]);

    // Send a second batch that no peer ever acknowledges, so it never reaches a quorum.
    let mut stalled_senders = Vec::new();
    let mut handlers = Vec::new();
    for name in &peers {
        let (tx, rx) = tokio::sync::oneshot::channel::<Bytes>();
        stalled_senders.push(tx);
        handlers.push((*name, rx));
    }
    let message = QuorumWaiterMessage {
        batch: vec![2],
        handlers,
    };
    tx_message.send(message).await.unwrap();

    // The slow peer's handler of the first batch must still be dropped once the dissemination
    // deadline expires, even though the `QuorumWaiter` is stuck waiting for the second batch.
    let deadline = Duration::from_millis(DISSEMINATION_DEADLINE * 4);
    assert!(
        tokio::time::timeout(deadline, slow.closed()).await.is_ok(),
        "dissemination handles outlived the deadline while waiting for a stalled quorum"
    );
}
