use super::*;
use crate::common::{committee, committee_with_base_port, keys, listener, transaction};
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_authenticated_shard, mut rx_authenticated_shard) = channel(1);
    let (tx_root, mut rx_root) = channel(1);

    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(6_000);

    let path = ".db_test_make_batch";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        name,
        committee.clone(),
        signature_service,
        store.clone(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        tx_authenticated_shard,
        tx_root,
    );

    // Spawn the receiver that will receive coded shards.
    let handles: Vec<_> = committee
        .broadcast_addresses(&name)
        .into_iter()
        .map(|(name, x)| (name, listener(x)))
        .collect();
    tokio::task::yield_now().await;

    // Send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure we receive the coded batch's root.
    let root = rx_root.recv().await.unwrap();

    // Ensure we receive our own authenticated shard to vote on.
    let (shard, _) = rx_authenticated_shard.recv().await.unwrap();
    assert_eq!(root, shard.root);

    // Ensure everybody else received their authenticated shard.
    let mut ok = true;
    for (name, handle) in handles {
        let bytes = handle.await.unwrap();
        match bincode::deserialize(&bytes).unwrap() {
            MempoolMessage::AuthenticatedShard(shard) => {
                ok &= shard.verify(&name, &committee).is_ok()
            }
            _ => panic!("Unexpected protocol message"),
        }
    }
    assert!(ok);

    // Ensure we correctly stored the coded batch.
    let stored = store.read(root.to_vec()).await.unwrap();
    assert!(stored.is_some());
    let message: MempoolMessage = bincode::deserialize(&stored.unwrap()).unwrap();
    match message {
        MempoolMessage::CodedBatch(mut x) => {
            x.expand(&committee).unwrap();
            let serialized_root = x.commit().get_root().serialize();
            assert_eq!(root.to_vec(), serialized_root)
        }
        _ => panic!("Unexpected message type"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_authenticated_shard, _rx_authenticated_shard) = channel(1);
    let (tx_root, mut rx_root) = channel(1);

    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let path = ".db_test_batch_timeout";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        name,
        committee(),
        signature_service,
        store,
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
        tx_authenticated_shard,
        tx_root,
    );

    // Do not send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure we receive the coded batch's root.
    let _ = rx_root.recv().await.unwrap();
}
