use std::net::SocketAddr;

use bytes::Bytes;
use crypto::{Digest, PublicKey};
use network::ReliableSender;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{mempool::MempoolMessage, Committee};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor {
    /// The committee information.
    committee: Committee,
    /// Reliable sender
    network: ReliableSender,
}

impl Processor {
    /// Spawn a processor
    pub fn spawn(
        committee: Committee,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<(
            SerializedBatchMessage,
            Digest,
            Option<(SocketAddr, PublicKey)>,
        )>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            let mut processor = Self {
                committee,
                network: ReliableSender::new(),
            };

            while let Some((batch, digest, peer)) = rx_batch.recv().await {
                store.write(digest.to_vec(), batch).await;
                tx_digest
                    .send(digest.clone())
                    .await
                    .expect("Failed to send batch digest");
                if let Some((addr, sender)) = peer {
                    if let Some(source) = processor.committee.get_public_key(&addr) {
                        if source != &sender {
                            let payload = Bytes::from(
                                bincode::serialize(&MempoolMessage::Ack(digest)).unwrap(),
                            );
                            processor.network.send(addr, payload).await;
                        }
                    }
                }
            }
        });
    }
}
