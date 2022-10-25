use std::net::SocketAddr;

use bytes::Bytes;
use crypto::{Digest, PublicKey};
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
            if let Some((addr, sender)) = peer {
                // If peer is not None then the message was received by another peer.
                if let Some(source) = self.committee.get_public_key(&addr) {
                    // Send an ACK to the sender of the batch if we did not receive the message from him.
                    if source != &sender {
                        let payload = Bytes::from(
                            bincode::serialize(&MempoolMessage::Ack(digest.clone())).unwrap(),
                        );
                        self.network.send(addr, payload).await;
                    }
                }

                // Send the batch to other peers.
                let peers = self
                    .topology
                    .broadcast_peers(sender)
                    .iter()
                    .map(|e| e.1)
                    .collect();
                info!("Relaying batch {} to {:?}", &digest, peers);
                self.network.broadcast(peers, batch.into()).await;
            } else {
                // If peer is None then the message is ours and must be sent to consensus
                info!("Sending batch digest {digest} to consensus");
                self.tx_digest
                    .send(digest)
                    .await
                    .expect("Failed to send batch digest");
            }
        }
    }
}
