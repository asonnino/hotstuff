use super::*;
use crate::common::{block, committee, keys};
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;
use std::fs;

#[tokio::test]
async fn verify_empty() {
    let (tx_consensus, _rx_consensus) = channel(1);
    let (tx_network, _rx_network) = channel(1);
    let store_path = ".db_test_verify_empty";
    let _ = fs::remove_dir_all(store_path);
    let store = Store::new(store_path).unwrap();
    let (name, _) = keys().pop().unwrap();
    let mut synchronizer = Synchronizer::new(
        tx_consensus,
        store,
        name,
        committee(),
        tx_network,
        /* sync_retry_delay */ 10_000,
    );
    let result = synchronizer.verify_payload(block()).await;
    assert!(result.is_ok());
    assert!(result.unwrap());
}

#[tokio::test]
async fn verify_wait() {
    let (tx_consensus, mut rx_consensus) = channel(1);
    let (tx_network, mut rx_network) = channel(1);
    let store_path = ".db_test_verify_wait";
    let _ = fs::remove_dir_all(store_path);
    let mut store = Store::new(store_path).unwrap();
    let (name, _) = keys().pop().unwrap();
    let mut synchronizer = Synchronizer::new(
        tx_consensus,
        store.clone(),
        name,
        committee(),
        tx_network,
        /* sync_retry_delay */ 10_000,
    );

    // Make a block with two payloads.
    let mut rng = StdRng::from_seed([0; 32]);
    let mut payload_1 = [0u8; 32];
    rng.fill_bytes(&mut payload_1);
    let mut payload_2 = [0u8; 32];
    rng.fill_bytes(&mut payload_2);
    let payload = vec![Digest(payload_1), Digest(payload_2)];
    let block = Block {
        payload: payload.clone(),
        ..block()
    };
    let digest = block.digest();
    let author = block.author;

    // Ensure the synchronizer replies with WAIT.
    let result = synchronizer.verify_payload(block).await;
    assert!(result.is_ok());
    assert!(!result.unwrap());

    // Ensure the synchronizer tries to sync.
    match rx_network.recv().await {
        Some(NetMessage(bytes, recipient)) => {
            match bincode::deserialize(&bytes).unwrap() {
                MempoolMessage::PayloadRequest(p, s) => {
                    assert!(p.iter().all(|x| payload.contains(x)) && p.len() == payload.len());
                    assert_eq!(s, name);
                }
                _ => assert!(false),
            }
            let address = committee().mempool_address(&author);
            assert_eq!(recipient, vec![address.unwrap()]);
        }
        _ => assert!(false),
    }

    // Add the payload to the store and ensure the driver trigger re-processing.
    let _ = store.write(payload_1.to_vec(), Vec::new()).await;
    let _ = store.write(payload_2.to_vec(), Vec::new()).await;
    match rx_consensus.recv().await {
        Some(ConsensusMessage::LoopBack(b)) => assert_eq!(b.digest(), digest),
        _ => assert!(false),
    }
}
