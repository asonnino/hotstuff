use crate::config::{Committee, Stake};
use crate::processor::SerializedBatchMessage;
use crypto::{Digest, PublicKey};
use log::info;
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `MempoolMessage::Batch` message.
    pub batch: SerializedBatchMessage,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<SerializedBatchMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<(SerializedBatchMessage, Digest, Option<PublicKey>)>,
    /// Channel to receive acknowledgements from the network.
    rx_ack: Receiver<(PublicKey, Digest)>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<SerializedBatchMessage>,
        tx_batch: Sender<(SerializedBatchMessage, Digest, Option<PublicKey>)>,
        rx_ack: Receiver<(PublicKey, Digest)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
                rx_ack,
            }
            .run()
            .await;
        });
    }

    // /// Helper function. It waits for a future to complete and then delivers a value.
    // async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
    //     let _ = wait_for.await;
    //     deliver
    // }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(batch) = self.rx_message.recv().await {
            let digest = Digest::hash(&batch);

            // Set of publickeys which already sent an acknowledgement.
            let mut acks = HashSet::<PublicKey>::new();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the consensus (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = self.stake;
            while total_stake < self.committee.quorum_threshold() {
                if let Some((peer, ackd_digest)) = self.rx_ack.recv().await {
                    if ackd_digest == digest && !acks.contains(&peer) {
                        let stake = self.committee.stake(&peer);
                        total_stake += stake;
                        acks.insert(peer);
                    }
                }
            }
            info!("Quorum reached for batch {:?}", &digest);
            self.tx_batch
                .send((batch, digest, None))
                .await
                .expect("Failed to deliver batch");
        }
    }
}
