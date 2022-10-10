use async_trait::async_trait;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use std::marker::PhantomData;
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
        mut store: Store,
        tx_digest: Sender<Digest>,
    ) -> ();

    /// Spawn a processor
    fn spawn(
        // The persistent storage.
        store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            while let Some(batch) = rx_batch.recv().await {
                Self::process(batch, store.clone(), tx_digest.clone()).await
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
        mut store: Store,
        tx_digest: Sender<Digest>,
    ) -> () {
        let batch_digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());
        store.write(batch_digest.to_vec(), batch).await;
        tx_digest
            .send(batch_digest)
            .await
            .expect("Failed to send batch digest");
    }
}

pub struct AckProcessor<T: Processor> {
    pub phantom: PhantomData<T>,
}

#[async_trait]
impl<T: Processor> Processor for AckProcessor<T> {
    async fn process(batch: SerializedBatchMessage, store: Store, tx_digest: Sender<Digest>) -> () {
        // TODO
        T::process(batch, store, tx_digest).await;
    }
}
