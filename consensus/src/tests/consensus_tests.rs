use super::*;
use crate::common::{committee, keys, MockMempool};
use crate::config::Parameters;
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn end_to_end() {
    let mut committee = committee();
    committee.increment_base_port(6000);

    // Run all nodes.
    let handles: Vec<_> = keys()
        .into_iter()
        .enumerate()
        .map(|(i, (name, secret))| {
            let config = Config {
                name,
                committee: committee.clone(),
                parameters: Parameters::default(),
            };
            let store_path = format!(".store_test_end_to_end_{}", i);
            let _ = fs::remove_dir_all(&store_path);
            let store = Store::new(&store_path).unwrap();
            let signature_service = SignatureService::new(secret);
            let mempool = MockMempool;
            let (tx_commit, mut rx_commit) = channel(1000);
            tokio::spawn(async move {
                Consensus::run(config, signature_service, store, mempool, tx_commit).await;

                match rx_commit.recv().await {
                    Some(block) => assert_eq!(block, Block::genesis()),
                    _ => assert!(false),
                }
            })
        })
        .collect();

    // Ensure all threads terminated correctly.
    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn dead_node() {
    let mut committee = committee();
    committee.increment_base_port(6100);

    // Run all nodes but one.
    let mut keys = keys();
    let _ = keys.remove(0);
    let handles: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(i, (name, secret))| {
            let config = Config {
                name,
                committee: committee.clone(),
                parameters: Parameters::default(),
            };
            let store_path = format!(".store_test_dead_node_{}", i);
            let _ = fs::remove_dir_all(&store_path);
            let store = Store::new(&store_path).unwrap();
            let signature_service = SignatureService::new(secret);
            let mempool = MockMempool;
            let (tx_commit, mut rx_commit) = channel(1000);
            tokio::spawn(async move {
                Consensus::run(config, signature_service, store, mempool, tx_commit).await;

                match rx_commit.recv().await {
                    Some(block) => assert_eq!(block, Block::genesis()),
                    _ => assert!(false),
                }
            })
        })
        .collect();

    // Ensure all threads terminated correctly.
    assert!(try_join_all(handles).await.is_ok());
}
