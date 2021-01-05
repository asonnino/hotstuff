use super::*;
use crate::config::config_tests::committee;
use crate::crypto::crypto_tests::keys;
use futures::future::try_join_all;
use std::fs;

#[tokio::test]
async fn end_to_end() {
    let nodes = keys().len();

    // Make the key files.
    let mut key_files: Vec<_> = keys()
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            let filename = format!(".node_{}.json", i);
            let _ = fs::remove_dir_all(&filename);
            let (name, secret) = key;
            Secret { name, secret }.write(&filename).unwrap();
            filename
        })
        .collect();

    // Make the committee to file.
    let committee_file = ".committee.json";
    let _ = fs::remove_dir_all(committee_file);
    let mut committee = committee();
    committee.set_base_port(6000);
    committee.write(committee_file).unwrap();

    // Run all nodes.
    let handles: Vec<_> = (0..nodes)
        .map(|x| {
            let key_file = key_files.pop().unwrap();
            let store_path = format!(".store_test_end_to_end_{}", x);
            let _ = fs::remove_dir_all(&store_path);
            tokio::spawn(async move {
                let mut rx_channel = Node::make(committee_file, &key_file, &store_path, None)
                    .await
                    .unwrap();

                match rx_channel.recv().await {
                    Some(block) => assert_eq!(block, Block::genesis()),
                    _ => assert!(false),
                }
            })
        })
        .collect();
    
    // Ensure all threads terminated correctly.
    assert!(try_join_all(handles).await.is_ok());
}
