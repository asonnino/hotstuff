use crate::batch_maker::BlockInProcess;
use crate::config::Committee;
use crate::processor::SerializedBatchMessage;
use crypto::{Digest, PublicKey};
use dashmap::DashMap;
use log::{debug, warn};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_processor: Sender<(SerializedBatchMessage, Digest, PublicKey)>,
    /// Channel to receive acknowledgements from the network.
    rx_ack: Receiver<(PublicKey, Digest)>,
    /// Map of batches that we have received and are waiting for acknowledgements.
    stake_map: Arc<DashMap<Digest, BlockInProcess>>,
}
impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        tx_processor: Sender<(SerializedBatchMessage, Digest, PublicKey)>,
        rx_ack: Receiver<(PublicKey, Digest)>,
        stake_map: Arc<DashMap<Digest, BlockInProcess>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                tx_processor,
                rx_ack,
                stake_map,
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        // Handle acknowledgements.
        while let Some((peer, digest)) = self.rx_ack.recv().await {
            self.handle_ack(peer, digest).await
        }
    }

    async fn handle_ack(&mut self, peer: PublicKey, digest: Digest) {
        // Check if an ack was not already received from this peer
        if let Some(mut block_in_process) = self.stake_map.get_mut(&digest) {
            if block_in_process.acks.insert(peer) {
                debug!(
                    "Received ack from {:?} for {:?}",
                    self.committee.mempool_address(&peer),
                    digest
                );
                // Update the stake and read it
                block_in_process.stake += self.committee.stake(&peer);
                if block_in_process.stake >= self.committee.quorum_threshold() {
                    // Deliver the batch
                    let (digest, block_in_process) = self
                        .stake_map
                        .remove(&digest)
                        .expect("The block should be in the map");
                    if self.tx_processor.capacity() < 10 {
                        warn!(
                            "tx_processor_ack capacity: {:?}",
                            self.tx_processor.capacity()
                        );
                    }
                    let _ = self
                        .tx_processor
                        .send((block_in_process.block, digest, self.name))
                        .await;
                }
            }
        }
    }
}
