use super::*;
use crate::common::{committee_with_base_port, keys, listener};
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn sync_shards() {
    let (tx_certificate, rx_certificate) = channel(1);
    let (tx_missing, mut rx_missing) = channel(1);

    let mut keys = keys();
    let (name, _) = keys.pop().unwrap();
    let committee = committee_with_base_port(7_000);

    // Create a new test store.
    let path = ".db_test_sync_shards";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Synchronizer` instance.
    Synchronizer::spawn(
        name,
        committee.clone(),
        store.clone(),
        /* sync_retry_delay */ 1_000_000, // Ensure it is not triggered.
        /* sync_nodes */ 3,
        rx_certificate,
        tx_missing,
    );

    // Spawn the listeners to receive our batch requests.
    let handles: Vec<_> = committee
        .broadcast_addresses(&name)
        .iter()
        .map(|(_, address)| listener(*address))
        .collect();
    tokio::task::yield_now().await;

    // Send a sync request.
    let missing = Digest([0; 32]); // Ensure the synchronizer requests shards.
    tx_certificate.send(vec![missing.clone()]).await.unwrap();

    // Ensure the synchronizer register the missing root.
    let digest = rx_missing.recv().await.unwrap();
    assert_eq!(digest, missing);

    // Ensure the other nodes receive the sync request.
    let ok = try_join_all(handles).await.unwrap().iter().all(|x| {
        match bincode::deserialize(x).unwrap() {
            MempoolMessage::ShardRequest(digest, origin) => digest == missing && origin == name,
            _ => false,
        }
    });
    assert!(ok);
}

#[tokio::test]
async fn sync_batch() {
    let (tx_certificate, rx_certificate) = channel(1);
    let (tx_missing, mut rx_missing) = channel(1);

    let mut keys = keys();
    let (name, _) = keys.pop().unwrap();
    let committee = committee_with_base_port(7_100);

    // Create a new test store.
    let path = ".db_test_sync_batch";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Synchronizer` instance.
    Synchronizer::spawn(
        name,
        committee.clone(),
        store.clone(),
        /* sync_retry_delay */ 1_000_000, // Ensure it is not triggered.
        /* sync_nodes */ committee.size(),
        rx_certificate,
        tx_missing,
    );

    // Spawn the listeners to receive our batch requests.
    let handles: Vec<_> = committee
        .broadcast_addresses(&name)
        .iter()
        .map(|(_, address)| listener(*address))
        .collect();
    tokio::task::yield_now().await;

    // Send a sync request.
    let missing = Digest([1; 32]); // Ensure the synchronizer requests the batch.
    tx_certificate.send(vec![missing.clone()]).await.unwrap();

    // Ensure the synchronizer register the missing root.
    let digest = rx_missing.recv().await.unwrap();
    assert_eq!(digest, missing);

    // Ensure the other nodes receive the sync request.
    let ok = try_join_all(handles).await.unwrap().iter().all(|x| {
        match bincode::deserialize(x).unwrap() {
            MempoolMessage::BatchRequest(digest, origin) => digest == missing && origin == name,
            _ => false,
        }
    });
    assert!(ok);
}
