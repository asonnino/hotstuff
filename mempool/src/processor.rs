use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::future::join_all;
use log::info;
use network::ReliableSender;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{mempool::MempoolMessage, Committee, Topology};

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
    rx_batch: Receiver<(SerializedBatchMessage, Digest, Option<PublicKey>)>,
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
        rx_batch: Receiver<(SerializedBatchMessage, Digest, Option<PublicKey>)>,
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
        while let Some((batch, digest, peer)) = self.rx_batch.recv().await {
            self.store.write(digest.to_vec(), batch.clone()).await;
            if let Some(source) = peer {
                // If peer is not None then the message was received by another peer.
                let source_addr = self
                    .committee
                    .mempool_address(&source)
                    .expect("Did not find source");
                let payload = Bytes::from(
                    bincode::serialize(&MempoolMessage::Ack((self.name, digest.clone()))).unwrap(),
                );
                let handler = self.network.send(source_addr, payload).await;
                info!("Sent Ack to {} for batch {}", source_addr, &digest);
                // Await the handlers
                tokio::spawn(async { handler.await });

                // Send the batch to other peers.
                let peers = self
                    .topology
                    .broadcast_peers(source)
                    .iter()
                    .map(|e| e.1)
                    .collect();
                info!("Relaying batch {} to {:?}", &digest, &peers);
                let handlers = self.network.broadcast(peers, batch.into()).await;
                // Await the handlers
                tokio::spawn(async move {
                    join_all(handlers).await;
                });
            } else {
                // If peer is None then the message is ours and must be sent to consensus
                self.tx_digest
                    .send(digest)
                    .await
                    .expect("Failed to send batch digest");
            }
        }
    }
}
