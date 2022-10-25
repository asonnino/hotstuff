use super::*;
use crate::common::{batch, committee_with_base_port, keys};
use crate::mempool::MempoolMessage;
use tokio::sync::mpsc::channel;

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

    tx_message.send(serialized.clone()).await.unwrap();

    for (peer_key, _) in committee.broadcast_addresses(&myself) {
        tx_ack.send((peer_key, digest.clone())).await.unwrap();
    }

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let (batch, _, _) = rx_batch.recv().await.unwrap();
    assert_eq!(batch, serialized);
}
