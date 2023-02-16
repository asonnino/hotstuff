use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::future::join_all;
use log::{debug, warn};
use network::ReliableSender;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;

use crate::topologies::traits::Topology;
use crate::{mempool::MempoolMessage, Committee};

const MAX_BEFORE_CLEANING: usize = 1000;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Duration before dropping a handler
pub const HANDLER_TIMEOUT: Duration = Duration::from_secs(30);

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor<T: Topology> {
    /// The public key of this authority.
    name: PublicKey,
    /// Store
    store: Store,
    /// The committee information.
    committee: Committee,
    /// Reliable sender
    network: ReliableSender,
    /// Network topology
    topology: T,
    /// Input channel to receive batches
    rx_batch: Receiver<(SerializedBatchMessage, Digest, PublicKey)>,
    /// Output channel to send the batches' digest to consensus
    tx_digest: Sender<Digest>,
}

impl<T: Topology + Send + Sync + 'static> Processor<T> {
    /// Spawn a processor
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        // The persistent storage.
        store: Store,
        // Input channel to receive batches.
        rx_batch: Receiver<(SerializedBatchMessage, Digest, PublicKey)>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
        // Network topology
        topology: T,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                store,
                committee,
                network: ReliableSender::new(),
                topology,
                rx_batch,
                tx_digest,
            }
            .run()
            .await;
        });
    }

    async fn send_ack(&mut self, digest: Digest, source: &PublicKey) {
        let source_addr = self
            .committee
            .mempool_address(&source)
            .expect("Did not find source");
        let payload = Bytes::from(
            bincode::serialize(&MempoolMessage::Ack((self.name, digest.clone()))).unwrap(),
        );
        let handler = self.network.send(source_addr, payload).await;
        debug!("Sent Ack to {} for batch {}", source_addr, &digest);
        tokio::spawn(handler);
    }

    async fn relay_batch(
        &mut self,
        batch: SerializedBatchMessage,
        digest: Digest,
        source: PublicKey,
    ) {
        let peers = {
            match self.topology.broadcast_peers(source) {
                Some(peer) => peer
                    .read()
                    .unwrap()
                    .get_children()
                    .iter()
                    .map(|t| t.read().unwrap().addr)
                    .collect(),
                None => vec![],
            }
        };

        if peers.is_empty() {
            return;
        }

        debug!("Relaying batch {} to {:?}", &digest, &peers);
        let handlers = self.network.broadcast(peers, batch.into()).await;
        // Await the handlers
        tokio::spawn(async move {
            // Join all with timeout
            let _ = timeout(HANDLER_TIMEOUT, join_all(handlers)).await;
        });
    }

    async fn run(&mut self) {
        let mut seen = HashSet::new();
        let mut count = 0;
        while let Some((batch, digest, source)) = self.rx_batch.recv().await {
            if !seen.insert(digest.clone()) {
                continue;
            }
            self.store.write(digest.to_vec(), batch.clone()).await;
            if source != self.name {
                // Send an ack to the source.
                self.send_ack(digest.clone(), &source).await;

                // Send the batch to other peers.
                self.relay_batch(batch, digest.clone(), source).await;
            }
            if self.tx_digest.capacity() < 10 {
                warn!("tx_digest capacity: {:?}", self.tx_digest.capacity());
            }
            debug!("Sending digest {} to consensus", &digest);
            self.tx_digest
                .send(digest)
                .await
                .expect("Failed to send batch digest");

            count += 1;
            // If count is greater than MAX_BEFORE_CLEANING, clean the seen set and reset count.
            if count > MAX_BEFORE_CLEANING {
                seen.clear();
                count = 0;
            }
        }
    }
}
