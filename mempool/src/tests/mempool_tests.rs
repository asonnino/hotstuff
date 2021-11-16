use super::*;
use crate::common::{committee_with_base_port, keys, listener, transaction};
use futures::future::try_join_all;
use network::SimpleSender;
use std::{collections::HashMap, fs};

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let tx_address = committee.transactions_address(&name).unwrap();
    let mempool_address = committee.mempool_address(&name).unwrap();
    let mut network = SimpleSender::new();

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Mempool` instance.
    let (_tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(1);
    let (tx_mempool_to_consensus, mut rx_mempool_to_consensus) = channel(1);
    Mempool::spawn(
        name,
        committee.clone(),
        parameters,
        signature_service,
        store,
        rx_consensus_to_mempool,
        tx_mempool_to_consensus,
    );

    // Spawn enough mempools' listeners to acknowledge our messages.
    let (names, handles): (Vec<_>, Vec<_>) = committee
        .broadcast_addresses(&name)
        .iter()
        .map(|(name, x)| (*name, listener(*x)))
        .unzip();
    tokio::task::yield_now().await;

    // Make a signature service for every node.
    let mut services: HashMap<_, _> = keys()
        .into_iter()
        .map(|(name, secret)| (name, SignatureService::new(secret)))
        .collect();

    // Send enough transactions to create a batch.
    network.send(tx_address, Bytes::from(transaction())).await;
    network.send(tx_address, Bytes::from(transaction())).await;

    // Ensure the consensus got the coded shards and reply with a vote.
    for (name, serialized) in names
        .into_iter()
        .zip(try_join_all(handles).await.unwrap().into_iter())
    {
        let message: MempoolMessage = bincode::deserialize(&serialized).unwrap();
        match message {
            MempoolMessage::AuthenticatedShard(shard) => {
                let signature_service = services.get_mut(&name).unwrap();
                let vote = BatchVote::new(shard.root, name, signature_service).await;
                let message = MempoolMessage::BatchVote(vote);
                let bytes = bincode::serialize(&message).unwrap();
                network.send(mempool_address, Bytes::from(bytes)).await;
            }
            _ => panic!("Unexpected message"),
        }
    }

    // Ensure we got a certificate
    let certificate = rx_mempool_to_consensus.recv().await.unwrap();
    assert!(certificate.verify(&committee).is_ok());
}
