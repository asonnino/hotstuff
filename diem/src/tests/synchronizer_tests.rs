use super::*;
use crate::crypto::crypto_tests::keys;
use crate::messages::messages_tests::{block, chain};
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn get_existing_previous_block() {
    let mut chain = chain();
    let block = chain.pop().unwrap();
    let b2 = chain.pop().unwrap();

    // Add the block b2 to the store.
    let path = ".store_test_get_existing_previous_block";
    let _ = fs::remove_dir_all(path).unwrap();
    let mut store = Store::new(path).await.unwrap();
    let key = b2.digest().to_vec();
    let value = bincode::serialize(&b2).unwrap();
    let _ = store.write(key, value).await;

    // Make a new synchronizer.
    let (public_key, _) = keys().pop().unwrap();
    let (tx_network, _) = channel(10);
    let (tx_core, _) = channel(10);
    let timer_manager = TimerManager::new().await;
    let mut synchronizer = Synchronizer::new(
        public_key,
        store,
        tx_network,
        tx_core,
        timer_manager,
        /* sync_retry_delay */ 10_000,
    )
    .await;

    // Ask the predecessor of 'block' to the synchronizer.
    match synchronizer.get_previous_block(&block).await {
        Ok(Some(b)) => assert_eq!(b, b2),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn get_genesis_previous_block() {
    // Make a new synchronizer.
    let path = ".store_test_get_genesis_previous_block";
    let _ = fs::remove_dir_all(path).unwrap();
    let store = Store::new(path).await.unwrap();
    let (public_key, _) = keys().pop().unwrap();
    let (tx_network, _) = channel(10);
    let (tx_core, _) = channel(10);
    let timer_manager = TimerManager::new().await;
    let mut synchronizer = Synchronizer::new(
        public_key,
        store,
        tx_network,
        tx_core,
        timer_manager,
        /* sync_retry_delay */ 10_000,
    )
    .await;

    // Ask the predecessor of 'block' to the synchronizer.
    match synchronizer.get_previous_block(&block()).await {
        Ok(Some(b)) => assert_eq!(b, Block::genesis()),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn get_missing_previous_block() {
    let mut chain = chain();
    let block = chain.pop().unwrap();
    let b2 = chain.pop().unwrap();

    // Make a new synchronizer.
    let path = ".store_test_get_missing_previous_block";
    let _ = fs::remove_dir_all(path).unwrap();
    let mut store = Store::new(path).await.unwrap();
    let (myself, _) = keys().pop().unwrap();
    let (tx_network, mut rx_network) = channel(10);
    let (tx_core, mut rx_core) = channel(10);
    let timer_manager = TimerManager::new().await;
    let mut synchronizer = Synchronizer::new(
        myself.clone(),
        store.clone(),
        tx_network,
        tx_core,
        timer_manager,
        /* sync_retry_delay */ 10_000,
    )
    .await;

    // Ask the predecessor of 'block' to the synchronizer.
    let copy = block.clone();
    let handle = tokio::spawn(async move {
        match synchronizer.get_previous_block(&copy).await {
            Ok(None) => assert!(true),
            _ => assert!(false),
        }
    });

    // Ensure the following operation happen in the right order.
    let mut operations_order = Vec::<u8>::new();
    loop {
        select! {
            value = rx_network.recv().fuse() => {
                match value {
                    Some(NetMessage::SyncRequest(digest, sender)) => {
                        assert_eq!(digest, b2.digest());
                        assert_eq!(sender, myself);
                    },
                    _ => assert!(false)
                }
                operations_order.push(1);
            },
            value = rx_core.recv().fuse() => {
                match value {
                    Some(CoreMessage::LoopBack(b)) => assert_eq!(b, block.clone()),
                    _ => assert!(false)
                }
                operations_order.push(3);
                break;
            },
            () = sleep(Duration::from_millis(100)).fuse() => {
                let key = b2.digest().to_vec();
                let value = bincode::serialize(&b2).unwrap();
                let _ = store.write(key, value).await;
                operations_order.push(2);
            }
        }
    }
    assert_eq!(operations_order, vec![1, 2, 3]);

    // Finally, ensure the synchronizer returns None.
    assert!(handle.await.is_ok());
}
