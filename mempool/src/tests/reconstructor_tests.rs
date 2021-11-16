use super::*;
use crate::common::{batch, committee, keys};
use crypto::SignatureService;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn reconstruct() {
    let (tx_missing, rx_missing) = channel(1);
    let (tx_shard, rx_shard) = channel(1);
    let (_tx_batch, rx_batch) = channel(1);
    let (name, secret) = keys().pop().unwrap();
    let mut signature_service = SignatureService::new(secret);

    // Create a new test store.
    let path = ".db_test_reconstruct";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    Reconstructor::spawn(committee(), store.clone(), rx_missing, rx_shard, rx_batch);

    // Make a coded batch.
    let (batch, size) = batch();
    let coded_batch = CodedBatch::new(batch, size, &committee());
    let tree = coded_batch.commit();

    // Register a missing batch.
    let serialized_root = tree.get_root().serialize();
    let root = Digest(serialized_root[0..32].try_into().unwrap());
    tx_missing.send(root).await.unwrap();

    // Make the coded shards and send them to the constructor.
    for (i, shard) in coded_batch.shards.into_iter().enumerate() {
        let authenticated_shard = AuthenticatedShard::new(
            shard,
            /* destination */ i,
            &tree,
            name,
            &mut signature_service,
        )
        .await;

        tx_shard.send(authenticated_shard).await.unwrap();
    }

    // Ensure the batch is property reconstructed and stored.
    let stored = store.read(serialized_root).await.unwrap();
    assert!(stored.is_some());
    match bincode::deserialize(&stored.unwrap()).unwrap() {
        MempoolMessage::CodedBatch(mut batch) => assert!(batch.expand(&committee()).is_ok()),
        _ => panic!("Wrong protocol message"),
    }
}
