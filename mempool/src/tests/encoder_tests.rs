use super::*;
use crate::common::{committee, committee_with_base_port, keys, listener};
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn encode_and_reconstruct() {
    let (name, secret) = keys().pop().unwrap();
    let mut signature_service = SignatureService::new(secret);

    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];
    let (data_shards, parity_shards) = committee().shards();
    let symbols_length = batch_size / data_shards;
    let parity = vec![0u8; symbols_length * parity_shards];

    let mut shards: Vec<Vec<u8>> = batch
        .clone()
        .into_iter()
        .flatten()
        .chain(parity.into_iter())
        .chunks(symbols_length)
        .into_iter()
        .map(|x| x.collect::<Vec<_>>())
        .collect();

    ReedSolomon::new(data_shards, parity_shards)
        .expect("Failed to initialize RS encoder")
        .encode(&mut shards)
        .expect("Failed to encode data");

    // TODO
    let proof = 0;

    let mut coded_batches = Vec::new();
    for (i, shard) in shards.into_iter().enumerate() {
        if i == 0 || i == 4 {
            coded_batches.push(None);
        } else {
            let coded_batch = CodedBatch::new(shard, proof, name, &mut signature_service).await;
            coded_batches.push(Some(coded_batch));
        }
    }

    let result = CodedBatch::reconstruct(coded_batches, &committee());
    assert!(result.is_ok());
}

#[tokio::test]
async fn encode_batch() {
    let (tx_batch, rx_batch) = channel(1);
    let (tx_coded_batch, mut rx_coded_batch) = channel(1);

    let (name, secret) = keys().pop().unwrap();
    let committee = committee_with_base_port(12_000);
    let signature_service = SignatureService::new(secret);

    // Create a new test store.
    let path = ".db_test_encode_batch";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Spawn the encoder.
    Encoder::spawn(
        name,
        committee.clone(),
        signature_service,
        store,
        rx_batch,
        tx_coded_batch,
    );

    // Spawn the receivers of the coded shards.
    let (names, handles): (Vec<PublicKey>, Vec<_>) = committee
        .broadcast_addresses(&name)
        .iter()
        .map(|(name, address)| (name, listener(*address)))
        .unzip();
    tokio::task::yield_now().await;

    // Send a batch to the encoder.
    let batch_size = 100;
    let batch = vec![vec![1; 50], vec![2; 50]];
    tx_batch.send((batch, batch_size)).await.unwrap();

    // Ensure the authorities got a valid share.
    for (bytes, name) in try_join_all(handles)
        .await
        .unwrap()
        .into_iter()
        .zip(names.into_iter())
    {
        match bincode::deserialize(&bytes).unwrap() {
            MempoolMessage::CodedBatch(x) => assert!(x.verify(&name, &committee).is_ok()),
            _ => panic!("Unexpected message"),
        }
    }

    // Ensure we got our own share.
    let received = rx_coded_batch.recv().await.unwrap();
    assert!(received.verify(&name, &committee).is_ok());
}
