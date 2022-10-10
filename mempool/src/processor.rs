use async_trait::async_trait;
use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `MempoolMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

#[async_trait]
pub trait Processor {
    /// Process a batch.
    async fn process(
        batch: SerializedBatchMessage,
        digest: Digest,
        mut store: Store,
        tx_digest: Sender<Digest>,
    ) -> ();

    /// Spawn a processor
    fn spawn(
        // The persistent storage.
        store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<(SerializedBatchMessage, Digest)>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            while let Some((batch, digest)) = rx_batch.recv().await {
                Self::process(batch, digest, store.clone(), tx_digest.clone()).await
            }
        });
    }
}

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct DigestProcessor;

#[async_trait]
impl Processor for DigestProcessor {
    async fn process(
        batch: SerializedBatchMessage,
        digest: Digest,
        mut store: Store,
        tx_digest: Sender<Digest>,
    ) -> () {
        store.write(digest.to_vec(), batch).await;
        tx_digest
            .send(digest)
            .await
            .expect("Failed to send batch digest");
    }
}
