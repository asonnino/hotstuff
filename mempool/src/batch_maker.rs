use crate::config::Stake;
use crate::mempool::MempoolMessage;
use crate::Topology;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use dashmap::DashMap;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use log::{debug, info};
use network::ReliableSender;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::task::yield_now;
use tokio::time::{sleep, timeout, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// BatchWithSender stores the batch and the sender of the batch.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchWithSender {
    pub batch: Batch,
    pub sender: PublicKey,
}

/// Represents a block for which we are waiting for enough acknowledgements.
pub struct BlockInProcess {
    pub stake: u32,
    pub block: Vec<u8>,
    pub acks: HashSet<PublicKey>,
}

/// Assemble clients transactions into batches.
pub struct BatchMaker<T>
where
    T: Topology,
{
    /// Self name.
    name: PublicKey,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// Stores acknowledged batches.
    stake_map: Arc<DashMap<Digest, BlockInProcess>>,
    /// Topology of the network
    topology: T,
    /// Stake of current authority.
    stake: Stake,
    /// Network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
    /// block_queue to program sending batches to indirect_peers.
    block_queue: FuturesUnordered<IndirectPeerFuture>,
}

type IndirectPeerFuture =
    Pin<Box<dyn Future<Output = (Digest, Vec<(PublicKey, SocketAddr, usize)>)> + Send>>;

/// Duration before sending batches to the indirect peers if no ack was received
pub const INDIRECT_PEERS_TIMEOUT: Duration = Duration::from_millis(10000);

/// Duration before sending a batch to a second indirect peer when we already sent it to one
pub const SLOW_PEERS_TIMEOUT: Duration = Duration::from_millis(400);

/// Duration before dropping a handler
pub const HANDLER_TIMEOUT: Duration = Duration::from_secs(60);

/// Sleeps for `duration` and then returns the value.
async fn wait_block<T>(duration: Duration, value: T) -> T {
    sleep(duration).await;
    value
}

impl<T> BatchMaker<T>
where
    T: Topology,
{
    pub fn spawn(
        name: PublicKey,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        stake_map: Arc<DashMap<Digest, BlockInProcess>>,
        topology: T,
        stake: Stake,
    ) {
        info!(
            "Broadcasting batches to {:?}",
            &topology.broadcast_peers(name)
        );
        debug!("Indirectly connected to {:?}", topology.indirect_peers());
        tokio::spawn(async move {
            Self {
                name,
                batch_size,
                max_batch_delay,
                rx_transaction,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                stake_map,
                topology,
                stake,
                network: ReliableSender::new(),
                block_queue: FuturesUnordered::new(),
            }
            .run()
            .await;
        });
    }

    /// Handles a single transaction by adding it to a batch and sealing the batch if it's full.
    async fn single_transaction(&mut self, transaction: Transaction) {
        // If the transaction is too big to fit in a batch, we drop it.
        self.current_batch_size += transaction.len();
        self.current_batch.push(transaction);
        if self.current_batch_size >= self.batch_size {
            self.seal().await;
        }
    }

    /// Sends the block to the next indirect_peer for which no ack was received starting from index.
    async fn send_to_indirect_peers(
        &mut self,
        mut indirect_peers: Vec<(PublicKey, SocketAddr, usize)>,
        digest: Digest,
        index: usize,
    ) {
        let block_in_process = {
            match self.stake_map.get(&digest) {
                Some(block_in_process) => block_in_process,
                None => return,
            }
        };
        let mut i = index;
        let current_number_of_hop = indirect_peers[i].2;
        let mut late_peer = false;

        while i < indirect_peers.len() && indirect_peers[i].2 == current_number_of_hop {
            let (peer, address, _) = indirect_peers[i];
            if !block_in_process.acks.contains(&peer) {
                // Send the batch to the indirect peer
                debug!("Sending batch {:?} to indirect peer {:?}", digest, &peer);
                let handler = self
                    .network
                    .send(address, Bytes::from(block_in_process.block.clone()))
                    .await;
                // Spawn a new task to wait for the handlers
                tokio::spawn(async move {
                    let _ = timeout(HANDLER_TIMEOUT, handler).await;
                });
                late_peer = true;
            }
            i += 1;
        }

        if i < indirect_peers.len() {
            let mut wait_duration = INDIRECT_PEERS_TIMEOUT
                .saturating_mul((indirect_peers[i].2 - current_number_of_hop) as u32);

            if late_peer {
                // If a peer is late
                wait_duration += INDIRECT_PEERS_TIMEOUT;
            }
            // Schedule sending the batch to the next indirect peers
            self.block_queue
                .push(Box::pin(wait_block(SLOW_PEERS_TIMEOUT, (digest, i))));
        }
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.single_transaction(transaction).await;
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                },
                Some((digest, index)) = self.block_queue.next() => {
                    self.send_to_indirect_peers(digest, index).await;
                },
                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let now = Instant::now();
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;

        let batch = BatchWithSender {
            batch: self.current_batch.drain(..).collect(),
            sender: self.name,
        };
        let message = MempoolMessage::Batch(batch); // Costly operation.
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        let digest = Digest::hash(&serialized);
        self.stake_map.insert(
            digest.clone(),
            BlockInProcess {
                stake: self.stake,
                block: serialized.clone(),
                acks: HashSet::new(),
            },
        );
        debug!("Broadcasting batch {:?}", digest);

        let handlers = self
            .network
            .broadcast(
                self.topology
                    .broadcast_peers(self.name)
                    .into_iter()
                    .map(|(_, addr)| addr)
                    .collect(),
                Bytes::from(serialized),
            )
            .await;

        // Schedule sending the batch to the indirect peers
        let indirect_peers = self.topology.indirect_peers();
        if !indirect_peers.is_empty() {
            self.block_queue.push(Box::pin(wait_block(
                3 * INDIRECT_PEERS_TIMEOUT,
                (digest.clone(), indirect_peers),
            )));
        }
        // Spawn a new task to wait for the handlers
        tokio::spawn(async move {
            // Join all with timeout
            let _ = timeout(HANDLER_TIMEOUT, join_all(handlers)).await;
        });
        let duration = now.elapsed();
        debug!("Batch sealed in {:?}", duration);

        #[cfg(feature = "benchmark")]
        {
            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }
        yield_now().await;
    }
}
