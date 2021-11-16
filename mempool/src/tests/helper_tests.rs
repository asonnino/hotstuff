use super::*;
use crate::{
    coded_batch::{AuthenticatedShard, CodedBatch},
    common::{batch, committee_with_base_port, keys, listener},
};
use crypto::SignatureService;
use smtree::traits::Serializable as _;
use std::{convert::TryInto as _, fs};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn shard_reply() {
    let (tx_request, rx_request) = channel(1);
    let mut keys = keys();
    let (name, secret) = keys.pop().unwrap();
    let committee = committee_with_base_port(8_000);
    let mut signature_service = SignatureService::new(secret);

    // Create a new test store.
    let path = ".db_test_shard_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make the expected shard.
    let (batch, size) = batch();
    let coded_batch = CodedBatch::new(batch, size, &committee);
    let tree = coded_batch.commit();

    let index = committee.index(&name).unwrap();
    let shard = AuthenticatedShard::new(
        coded_batch.shards[index].clone(),
        /* destination */ index,
        &tree,
        name,
        &mut signature_service,
    )
    .await;

    // Add the shard to the store.
    let message = MempoolMessage::AuthenticatedShard(shard.clone());
    let serialized = bincode::serialize(&message).unwrap();
    let mut key = shard.root.to_vec();
    key.extend(name.to_vec());
    store.write(key, serialized).await;

    // Spawn an `Helper` instance.
    Helper::spawn(name, committee.clone(), store, rx_request);

    // Spawn a listener to receive the batch reply.
    let (requestor, _) = keys.pop().unwrap();
    let address = committee.mempool_address(&requestor).unwrap();
    let handle = listener(address);
    tokio::task::yield_now().await;

    // Send a batch request.
    let want_shard = true; // Request the shard (not a batch).
    tx_request
        .send((shard.root.clone(), requestor, want_shard))
        .await
        .unwrap();

    // Ensure the requestor received the batch (ie. it did not panic).
    let received = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        MempoolMessage::ShardReply(x) => assert_eq!(x, shard),
        _ => panic!("Unexpected protocol message"),
    }
}

#[tokio::test]
async fn batch_reply() {
    let (tx_request, rx_request) = channel(1);
    let mut keys = keys();
    let (name, _) = keys.pop().unwrap();
    let committee = committee_with_base_port(8_100);

    // Create a new test store.
    let path = ".db_test_batch_reply";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make the expected batch.
    let (batch, size) = batch();
    let mut coded_batch = CodedBatch::new(batch, size, &committee);
    let tree = coded_batch.commit();
    let serialized_root = tree.get_root().serialize();
    let root = Digest(serialized_root[0..32].try_into().unwrap());

    // Add the batch to the store.
    coded_batch.compress(&committee);
    let message = MempoolMessage::CodedBatch(coded_batch.clone());
    let serialized = bincode::serialize(&message).unwrap();
    store.write(serialized_root, serialized).await;

    // Spawn an `Helper` instance.
    Helper::spawn(name, committee.clone(), store, rx_request);

    // Spawn a listener to receive the batch reply.
    let (requestor, _) = keys.pop().unwrap();
    let address = committee.mempool_address(&requestor).unwrap();
    let handle = listener(address);
    tokio::task::yield_now().await;

    // Send a batch request.
    let want_shard = false; // Request the batch (not a shard).
    tx_request
        .send((root, requestor, want_shard))
        .await
        .unwrap();

    // Ensure the requestor received the batch (ie. it did not panic).
    let received = handle.await.unwrap();
    match bincode::deserialize(&received).unwrap() {
        MempoolMessage::CodedBatch(x) => assert_eq!(x, coded_batch),
        _ => panic!("Unexpected protocol message"),
    }
}
