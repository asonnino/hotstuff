use super::*;
use crate::common::batch;
use crate::mempool::MempoolMessage;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn hash_and_store() {
    let (tx_batch, rx_batch) = channel(1);
    let (tx_digest, mut rx_digest) = channel(1);

    // Create a new test store.
    let path = ".db_test_hash_and_store";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Spawn a new `Processor` instance.
    DigestProcessor::spawn(store.clone(), rx_batch, tx_digest);

    // Send a batch to the `Processor`.
    let message = MempoolMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();
    let digest = Digest::hash(&serialized);

    tx_batch
        .send((serialized.clone(), digest.clone()))
        .await
        .unwrap();

    // Ensure the `Processor` outputs the batch's digest.
    let received = rx_digest.recv().await.unwrap();
    assert_eq!(digest.clone(), received);

    // Ensure the `Processor` correctly stored the batch.
    let stored_batch = store.read(digest.to_vec()).await.unwrap();
    assert!(stored_batch.is_some(), "The batch is not in the store");
    assert_eq!(stored_batch.unwrap(), serialized);
}
