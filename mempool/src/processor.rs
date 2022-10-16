use std::net::SocketAddr;

use bytes::Bytes;
use crypto::{Digest, PublicKey};
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
    /// Store
    store: Store,
    /// The committee information.
    committee: Committee,
    /// Reliable sender
    network: ReliableSender,
    /// Network topology
    topology: T,
    /// Input channel to receive batches
    rx_batch: Receiver<(
        SerializedBatchMessage,
        Digest,
        Option<(SocketAddr, PublicKey)>,
    )>,
    /// Output channel to send the batches' digest to consensus
    tx_digest: Sender<Digest>,
}

impl<T: Topology + Send + Sync + 'static> Processor<T> {
    /// Spawn a processor
    pub fn spawn(
        committee: Committee,
        // The persistent storage.
        store: Store,
        // Input channel to receive batches.
        rx_batch: Receiver<(
            SerializedBatchMessage,
            Digest,
            Option<(SocketAddr, PublicKey)>,
        )>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
        // Network topology
        topology: T,
    ) {
        tokio::spawn(async move {
            Self {
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

            // Send an ack to the peer that sent the batch if it isn't the sender.
            if let Some((addr, sender)) = peer {
                if let Some(source) = self.committee.get_public_key(&addr) {
                    if source != &sender {
                        let payload = Bytes::from(
                            bincode::serialize(&MempoolMessage::Ack(digest.clone())).unwrap(),
                        );
                        self.network.send(addr, payload).await;
                    }
                }

                // Send the batch to other peers.
                self.network
                    .broadcast(
                        self.topology
                            .broadcast_peers(sender)
                            .iter()
                            .map(|e| e.1)
                            .collect(),
                        batch.into(),
                    )
                    .await;
            }

            // Send the batch to consensus.
            self.tx_digest
                .send(digest.clone())
                .await
                .expect("Failed to send batch digest");
        }
    }
}
