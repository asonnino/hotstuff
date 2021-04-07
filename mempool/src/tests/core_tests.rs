use super::*;
use crate::common::{committee, keys, payload};
use crate::messages::Transaction;
use crypto::SignatureService;
use std::fs;
use std::time::Duration;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;
use tokio::time::sleep;

async fn core(
    store_path: &str,
) -> (
    Receiver<NetMessage>,
    Sender<MempoolMessage>,
    Sender<ConsensusMempoolMessage>,
    Sender<Transaction>,
) {
    let (tx_network, rx_network) = channel(1);
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_core, rx_core) = channel(1);
    let (tx_consensus_mempool, rx_consensus_mempool) = channel(1);
    let (tx_client, rx_client) = channel(1);

    let (name, secret) = keys().pop().unwrap();
    let parameters = Parameters {
        queue_capacity: 1,
        sync_retry_delay: 10_000,
        max_payload_size: 1,
        min_block_delay: 0,
    };
    let signature_service = SignatureService::new(secret, None);
    let _ = fs::remove_dir_all(store_path);
    let store = Store::new(store_path).unwrap();
    let synchronizer = Synchronizer::new(
        tx_consensus,
        store.clone(),
        name,
        committee(),
        tx_network.clone(),
        parameters.sync_retry_delay,
    );
    let payload_maker = PayloadMaker::new(
        name,
        signature_service,
        parameters.max_payload_size,
        parameters.min_block_delay,
        rx_client,
        tx_core.clone(),
    );
    let mut core = Core::new(
        name,
        committee(),
        parameters,
        store,
        synchronizer,
        payload_maker,
        /* core_channel */ rx_core,
        /* consensus_channel */ rx_consensus_mempool,
        /* network_channel */ tx_network,
    );
    tokio::spawn(async move {
        core.run().await;
    });

    (rx_network, tx_core, tx_consensus_mempool, tx_client)
}

#[tokio::test]
async fn handle_transaction() {
    // Run the core.
    let path = ".db_test_handle_transaction";
    let (mut rx_network, _tx_core, _tx_consensus, tx_client) = core(path).await;

    // Ensure the core transmits the payload to the network.
    tx_client.send(vec![1u8]).await.unwrap();
    tx_client.send(vec![1u8]).await.unwrap();
    assert!(rx_network.recv().await.is_some());
}

#[tokio::test]
async fn handle_request() {
    // Run the core.
    let path = ".db_test_handle_request";
    let (mut rx_network, tx_core, _tx_consensus, _tx_client) = core(path).await;

    // Send a payload to the core.
    let message = MempoolMessage::Payload(payload());
    tx_core.send(message).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    // Send a sync request.
    let (name, _) = keys().pop().unwrap();
    let digest = payload().digest();
    let message = MempoolMessage::PayloadRequest(vec![digest], name);
    tx_core.send(message).await.unwrap();

    // Ensure we transmit a reply.
    assert!(rx_network.recv().await.is_some());
}

#[tokio::test]
async fn get_payload() {
    // Run the core.
    let path = ".db_test_get_payload";
    let (_rx_network, _tx_core, tx_consensus, tx_client) = core(path).await;

    // Send enough transactions to generate a payload.
    tx_client.send(vec![1u8]).await.unwrap();
    tx_client.send(vec![1u8]).await.unwrap();

    // Get the next payload.
    let (sender, receiver) = oneshot::channel();
    let message = ConsensusMempoolMessage::Get(64, sender);
    tx_consensus.send(message).await.unwrap();
    let result = receiver.await.unwrap();
    assert_eq!(result, vec![payload().digest()]);
}
