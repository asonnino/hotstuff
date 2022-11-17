use crate::config::{Committee, Stake};
use crate::processor::SerializedBatchMessage;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::ReliableSender;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<SerializedBatchMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<(SerializedBatchMessage, Digest, PublicKey)>,
    /// Channel to receive acknowledgements from the network.
    rx_ack: Receiver<(PublicKey, Digest)>,
    /// The network addresses of the other mempools.
    mempool_addresses: Vec<SocketAddr>,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
    /// Peers that are not directly connected to us.
    indirect_peers: Vec<(PublicKey, SocketAddr)>,
}
struct BlockInProcess {
    stake: u32,
    block: Vec<u8>,
    acks: HashSet<PublicKey>,
}

async fn wait_block<T>(duration: Duration, value: T) -> T {
    sleep(duration).await;
    value
}

// Duration before sending batches to the indirect peers if no ack was received
const INDIRECT_PEERS_TIMEOUT: Duration = Duration::from_millis(500);

// Duration before sending a batch to a second indirect peer when we already sent it to one
const SLOW_PEERS_TIMEOUT: Duration = Duration::from_millis(50);

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<SerializedBatchMessage>,
        tx_batch: Sender<(SerializedBatchMessage, Digest, PublicKey)>,
        rx_ack: Receiver<(PublicKey, Digest)>,
        mempool_addresses: Vec<SocketAddr>,
        indirect_peers: Vec<(PublicKey, SocketAddr)>,
    ) {
        info!("Broadcasting batches to {:?}", &mempool_addresses);
        info!("Indirect_peers {:?}", indirect_peers);
        tokio::spawn(async move {
            Self {
                name,
                committee,
                stake,
                rx_message,
                tx_batch,
                rx_ack,
                mempool_addresses,
                network: ReliableSender::new(),
                indirect_peers,
            }
            .run()
            .await;
        });
    }

    /// Main loop.
    async fn run(&mut self) {
        let mut stake_map = HashMap::new();
        let mut my_block_queue = FuturesUnordered::new();
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
                    debug!("Broadcasting batch {:?} to {:?}", digest, self.mempool_addresses);
                    let handlers = self
                        .network
                        .broadcast(self.mempool_addresses.clone(), Bytes::from(batch))
                        .await;
                    my_block_queue.push(wait_block(INDIRECT_PEERS_TIMEOUT, (digest, 0)));

                    // Spawn a new task to wait for the handlers
                    tokio::spawn(async move { join_all(handlers).await });
                },
                // Handle acknowledgements.
                Some((peer, digest)) = self.rx_ack.recv() => {
                    debug!("Received ack from {:?} for {:?}", self.committee.mempool_address(&peer), digest);
                    // Check if an ack was not already received from this peer
                    if let Some(block_in_process) = stake_map.get_mut(&digest) {
                        if block_in_process.acks.insert(peer){
                            // Update the stake and read it
                            block_in_process.stake += self.committee.stake(&peer);
                            if block_in_process.stake >= self.committee.quorum_threshold() {
                                // Deliver the batch
                                let block_in_process = stake_map.remove(&digest).expect("The block should be in the map");
                                debug!("tx_batch capacity: {:?}", self.tx_batch.capacity());
                                let _ = self.tx_batch.send((block_in_process.block, digest, self.name)).await;
                            }
                        }
                    }
                },
                Some((digest, index)) = my_block_queue.next() => {
                    // For each (digest, index) in the queue, send the batch to the indirect peer for which no ack was received
                    // starting at index
                    if let Some(block_in_process) = stake_map.get_mut(&digest) {
                        // Find the last indirect peer from which no ack was received, starting from index
                        let mut i = index;
                        while i < self.indirect_peers.len() {
                            let (peer, address) = self.indirect_peers[i];
                            if !block_in_process.acks.contains(&peer) {
                                // Send the batch to the indirect peer
                                let handler = self
                                    .network
                                    .send(address, Bytes::from(block_in_process.block.clone()))
                                    .await;
                                // Spawn a new task to wait for the handlers
                                tokio::spawn(async move { handler.await });
                                // Add the block to the queue
                                my_block_queue.push(wait_block(SLOW_PEERS_TIMEOUT, (digest, i+1)));
                                break;
                            }
                            i += 1;
                        }
                    }
                },
            }
        }
    }
}
