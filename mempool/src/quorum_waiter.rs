use crate::config::{Committee, Stake};
use crate::processor::SerializedBatchMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::future::join_all;
use log::info;
use network::ReliableSender;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

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
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<SocketAddr>,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<SerializedBatchMessage>,
        tx_batch: Sender<(SerializedBatchMessage, Digest, Option<PublicKey>)>,
        rx_ack: Receiver<(PublicKey, Digest)>,
        mempool_addresses: Vec<SocketAddr>,
    ) {
        info!("Broadcasting batches to {:?}", &mempool_addresses);
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
                rx_ack,
                mempool_addresses,
                network: ReliableSender::new(),
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
        // TODO add an ack[digest][publickey] map to keep track of the acks we have received.

        let mut acks = HashMap::new();
        let mut blocks = HashMap::new();
        let mut stakes = HashMap::new();

        loop {
            tokio::select! {
                // Broadcast the batch to the network.
                Some(batch) = self.rx_message.recv() => {
                    let digest = Digest::hash(&batch);
                    acks.insert(digest.clone(), HashSet::new());
                    blocks.insert(digest.clone(), batch.clone());
                    stakes.insert(digest.clone(), self.stake);
                    let handlers = self
                        .network
                        .broadcast(self.mempool_addresses.clone(), Bytes::from(batch))
                        .await;
                    // Spawn a new tasks to wait for the handlers
                    tokio::spawn(async move { join_all(handlers).await });
                },
                // Handle acknowledgements.
                Some((peer, digest)) = self.rx_ack.recv() => {
                    // Check if an ack from this peer was not already received
                    if let Some(ack_map) = acks.get_mut(&digest) {
                        if ack_map.insert(peer){
                            // Update the stake and read it
                            let stake = stakes.get_mut(&digest).expect("Stakes not found");
                            *stake += self.committee.stake(&peer);

                            if stake >= &mut self.committee.quorum_threshold() {
                                // Delete the maps
                                acks.remove(&digest);
                                stakes.remove(&digest);

                                // Deliver the batch
                                let _ = self.tx_batch.send((blocks.remove(&digest).expect("Block not found"), digest, None)).await;
                            }
                        }
                    }
                },
            }
        }
    }
}
