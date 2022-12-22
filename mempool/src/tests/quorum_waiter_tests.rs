use std::collections::HashSet;

use super::*;
use crate::common::{batch, committee_with_base_port, keys};
use crate::mempool::MempoolMessage;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn wait_for_quorum_ack() {
    let (tx_batch, mut rx_batch) = channel(1);
    let (tx_ack, rx_ack) = channel(1);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_000);
    let stake_map = Arc::new(DashMap::new());

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(
        myself,
        committee.clone(),
        tx_batch,
        rx_ack,
        stake_map.clone(),
    );

    // Make a batch.
    let message = MempoolMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();

    let digest = Digest::hash(&serialized);

    // Add it to the stake_map
    stake_map.insert(
        digest,
        BlockInProcess {
            stake: 1,
            block: serialized.clone(),
            acks: HashSet::new(),
        },
    );

    // Digest of the batch.
    let digest = Digest::hash(&serialized);

    for (peer, _) in committee.broadcast_addresses(&myself) {
        tx_ack.send((peer, digest.clone())).await.unwrap();
    }

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let (batch, _, _) = rx_batch.recv().await.unwrap();
    assert_eq!(batch, serialized);
}
