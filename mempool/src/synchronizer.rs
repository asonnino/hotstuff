use crate::{config::Committee, mempool::MempoolMessage};
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use log::debug;
use network::SimpleSender;
#[cfg(not(test))]
use rand::Rng as _;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use store::Store;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::{sleep, Duration, Instant},
};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

// The `Synchronizer` is responsible to keep the mempool in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when requesting coded batches.
    sync_nodes: usize,
    /// The bias of the coin used to select the sync algorithm.
    sync_bias: usize,
    /// Input channel to receive the digests of certificates from the consensus. We need to sync
    /// the batches of behind these digests.
    rx_digest: Receiver<Vec<(Digest, PublicKey)>>,
    /// Inform the `Reconstructor` of the missing batches.
    tx_missing: Sender<Digest>,
    /// Inform the `BatchMaker` that one of our batches made it to a block.
    tx_control: Sender<Vec<Digest>>,
    /// A network sender to send requests to the other mempools.
    network: SimpleSender,
    /// Keeps the root (of batches) that are waiting to be processed by the consensus. Their
    /// processing will resume when we get the missing batches in the store. It also keeps the
    /// round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, u128>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        sync_retry_delay: u64,
        sync_nodes: usize,
        sync_bias: usize,
        rx_digest: Receiver<Vec<(Digest, PublicKey)>>,
        tx_missing: Sender<Digest>,
        tx_control: Sender<Vec<Digest>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                sync_retry_delay,
                sync_nodes,
                sync_bias,
                rx_digest,
                tx_missing,
                tx_control,
                network: SimpleSender::new(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(missing: Digest, mut store: Store) -> Digest {
        store
            .notify_read(missing.to_vec())
            .await
            .expect("Failed to read store");
        missing
    }

    async fn sync(&mut self, missing: Digest) {
        #[cfg(not(test))]
        let coin = rand::thread_rng().gen_range(0, self.committee.size());
        #[cfg(test)]
        let coin = match missing.to_vec()[0] % 2 == 0 {
            true => 0,
            false => self.committee.size(),
        };

        let (message, nodes) = match coin < self.sync_bias {
            true => (
                MempoolMessage::ShardRequest(missing, self.name),
                self.committee.size(),
            ),
            false => (
                MempoolMessage::BatchRequest(missing, self.name),
                self.sync_nodes,
            ),
        };

        let addresses = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .map(|(_, address)| *address)
            .collect();
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
        self.network
            .lucky_broadcast(addresses, Bytes::from(serialized), nodes)
            .await;
    }

    /// Main loop listening to the consensus' messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle consensus' messages.
                Some(digests) = self.rx_digest.recv() => {
                    let mut feedback = Vec::new();

                    for (digest, author) in digests {
                        // Notify the batch maker that this batch made it to a block.
                        if author == self.name {
                            feedback.push(digest.clone());
                        }
                        
                        // Ensure we do not send twice the same sync request.
                        if self.pending.contains_key(&digest) {
                            continue;
                        }

                        // Ensure we don't already have the coded batch.
                        if self
                            .store
                            .read(digest.to_vec())
                            .await
                            .expect("Failed to read store")
                            .is_some()
                        {
                            continue;
                        }

                        // Register the missing root.
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();
                        let fut = Self::waiter(digest.clone(), self.store.clone());
                        waiting.push(fut);
                        self.pending.insert(digest.clone(), now);

                        // Notify the reconstructor task about this missing batch.
                        self.tx_missing.send(digest.clone()).await.expect("Failed to send root");

                        // Try to sync with other nodes
                        self.sync(digest).await;
                    }

                    if !feedback.is_empty() {
                        self.tx_control.send(feedback).await.expect("Failed to send root");
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(digest) = waiting.next() =>{
                    // We got the batch, remove it from the pending list.
                    debug!("Finished to sync batch {}", digest);
                    self.pending.remove(&digest);
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, timestamp) in &mut self.pending {
                        if *timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                            *timestamp = now;
                        }
                    }
                    for digest in retry {
                        self.sync(digest).await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}
