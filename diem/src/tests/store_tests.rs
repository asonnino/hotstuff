use super::*;
use futures::future::FutureExt as _;
use futures::select;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn create_store() {
    // Create new store.
    let path = ".store_test_create_store";
    let _ = fs::remove_dir_all(path).unwrap();
    let store = Store::new(path).await;
    assert!(store.is_ok());
}

#[tokio::test]
async fn read_write_value() {
    // Create new store.
    let path = ".store_test_read_write_value";
    let _ = fs::remove_dir_all(path).unwrap();
    let mut store = Store::new(path).await.unwrap();

    // Write value to the store.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let value = vec![4u8, 5u8, 6u8, 7u8];
    let result = store.write(key.clone(), value.clone()).await;
    assert!(result.is_ok());

    // Read value.
    let result = store.read(key).await;
    assert!(result.is_ok());
    let read_value = result.unwrap();
    assert!(read_value.is_some());
    assert_eq!(read_value.unwrap(), value);
}

#[tokio::test]
async fn read_unknown_key() {
    // Create new store.
    let path = ".store_test_read_unknown_key";
    let _ = fs::remove_dir_all(path).unwrap();
    let mut store = Store::new(path).await.unwrap();

    // Try to read unknown key.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let result = store.read(key).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn read_notify() {
    // Create new store.
    let path = ".store_test_read_notify";
    let _ = fs::remove_dir_all(path).unwrap();
    let mut store = Store::new(path).await.unwrap();

    // Try to read unknown key.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let value = vec![4u8, 5u8, 6u8, 7u8];
    let mut operations_order = Vec::<u8>::new();
    loop {
        select! {
            result = store.notify_read(key.clone()).fuse() => {
                operations_order.push(2);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), value);
                break;
            },
            () = sleep(Duration::from_millis(100)).fuse() => {
                operations_order.push(1);
                let _ = store.write(key.clone(), value.clone()).await;

            }
        }
    }
    assert_eq!(operations_order, vec![1, 2]);
}
