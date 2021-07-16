use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use network::SimpleSender;
use std::fs;

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

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
        store,
        rx_consensus_to_mempool,
        tx_mempool_to_consensus,
    );

    // Spawn enough mempools' listeners to acknowledge our batches.
    for (_, address) in committee.broadcast_addresses(&name) {
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.transactions_address(&name).unwrap();
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the consensus got the batch digest.
    let received = rx_mempool_to_consensus.recv().await.unwrap();
    assert_eq!(batch_digest(), received);
}
