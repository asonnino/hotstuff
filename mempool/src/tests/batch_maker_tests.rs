use super::*;
use crate::common::{keys, transaction};
use std::time::Duration;
use tokio::sync::mpsc::channel;
use tokio::time::sleep;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let sender = keys()[0].0;
    let stake_map = Arc::new(DashMap::new());
    let mempool_addresses = vec![];
    let indirect_peers = vec![];
    let stake = 0;

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        sender,
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        stake_map.clone(),
        mempool_addresses,
        indirect_peers,
        stake,
    );

    // Send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = BatchWithSender {
        batch: vec![transaction(), transaction()],
        sender,
    };
    // sleep for 1 second to ensure the batch is created
    sleep(Duration::from_secs(1)).await;

    let batch = stake_map.iter().next().unwrap().value().block.clone();
    match bincode::deserialize(&batch).unwrap() {
        MempoolMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let sender = keys()[0].0;
    // Spawn a `BatchMaker` instance.
    let stake_map = Arc::new(DashMap::new());
    let mempool_addresses = vec![];
    let indirect_peers = vec![];
    let stake = 1;

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        sender,
        /* max_batch_size */ 200,
        /* max_batch_delay */ 100, // Ensure the timer is triggered.
        rx_transaction,
        stake_map.clone(),
        mempool_addresses,
        indirect_peers,
        stake,
    );

    // Do not send enough transactions to seal a batch..
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = BatchWithSender {
        batch: vec![transaction()],
        sender,
    };

    // sleep for 1 second to ensure the batch is created
    sleep(Duration::from_secs(1)).await;

    // Find the batch in the stake map which might contain more than one batch
    let mut found = false;
    for batch in stake_map.iter() {
        let batch = batch.value().block.clone();
        match bincode::deserialize(&batch).unwrap() {
            MempoolMessage::Batch(batch) => {
                if batch == expected_batch {
                    found = true;
                }
            }
            _ => panic!("Unexpected message"),
        }
    }
    assert!(found);
}
