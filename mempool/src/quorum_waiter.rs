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
struct BlockInProcess {
    stake: u32,
    block: Vec<u8>,
    acks: HashSet<PublicKey>,
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

    /// Main loop.
    async fn run(&mut self) {
        let mut stake_map = HashMap::new();
        loop {
            tokio::select! {
                // Broadcast the batch to the network.
                Some(batch) = self.rx_message.recv() => {
                    let digest = Digest::hash(&batch);
                    stake_map.insert(digest.clone(), BlockInProcess {
                        stake : self.stake,
                        block : batch.clone(),
                        acks : HashSet::new(),
                    });

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
                    if let Some(block_in_process) = stake_map.get_mut(&digest) {
                        if block_in_process.acks.insert(peer){
                            // Update the stake and read it
                            block_in_process.stake += self.committee.stake(&peer);

                            if block_in_process.stake >= self.committee.quorum_threshold() {
                                // Deliver the batch
                                let block_in_process = stake_map.remove(&digest).expect("The block should be in the map");

                                let _ = self.tx_batch.send((block_in_process.block, digest, None)).await;
                            }
                        }
                    }
                },
            }
        }
    }
}
