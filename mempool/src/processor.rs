use std::collections::HashSet;

use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::future::join_all;
use log::{debug, warn};
use network::ReliableSender;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::topologies::traits::Topology;
use crate::{mempool::MempoolMessage, Committee};

const MAX_BEFORE_CLEANING: usize = 1000;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

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

    async fn run(&mut self) {
        let mut seen = HashSet::new();
        let mut count = 0;
        while let Some((batch, digest, source)) = self.rx_batch.recv().await {
            if !seen.insert(digest.clone()) {
                continue;
            }
            self.store.write(digest.to_vec(), batch.clone()).await;
            if source != self.name {
                // If peer is not the current node then the message was received by another peer and should be ack'd then relayed.
                let source_addr = self
                    .committee
                    .mempool_address(&source)
                    .expect("Did not find source");
                let payload = Bytes::from(
                    bincode::serialize(&MempoolMessage::Ack((self.name, digest.clone()))).unwrap(),
                );
                let handler = self.network.send(source_addr, payload).await;
                debug!("Sent Ack to {} for batch {}", source_addr, &digest);
                // Await the handlers
                tokio::spawn(async { handler.await });

                // Send the batch to other peers.
                let peers = self
                    .topology
                    .broadcast_peers(source)
                    .iter()
                    .map(|e| e.read().unwrap().addr)
                    .collect();
                debug!("Relaying batch {} to {:?}", &digest, &peers);
                let handlers = self.network.broadcast(peers, batch.into()).await;
                // Await the handlers
                tokio::spawn(async move {
                    join_all(handlers).await;
                });
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
