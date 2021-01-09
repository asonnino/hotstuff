use super::*;
use crate::messages::messages_tests::keys;
use futures::future::try_join_all;
use std::fs;

#[tokio::test]
async fn end_to_end() {
    let nodes = keys().len();
    let mut key_files = Secret::print_key_files(".node_test_end_to_end_");
    let committee_file = ".committee_test_end_to_end.json";
    Committee::print_committee(committee_file, 6000);

    // Run all nodes.
    let handles: Vec<_> = (0..nodes)
        .map(|x| {
            let key_file = key_files.pop().unwrap();
            let store_path = format!(".store_test_end_to_end_{}", x);
            let _ = fs::remove_dir_all(&store_path);
            tokio::spawn(async move {
                let mut rx = Node::make(committee_file, &key_file, &store_path, None)
                    .await
                    .unwrap();

                match rx.recv().await {
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
    let nodes = keys().len();
    let mut key_files = Secret::print_key_files(".node_test_dead_node");
    let committee_file = ".committee_test_dead_node.json";
    Committee::print_committee(committee_file, 6100);

    // Run all nodes.
    let handles: Vec<_> = (0..nodes - 1)
        .map(|x| {
            let key_file = key_files.pop().unwrap();
            let store_path = format!(".store_test_dead_node_{}", x);
            let _ = fs::remove_dir_all(&store_path);
            tokio::spawn(async move {
                let mut rx = Node::make(committee_file, &key_file, &store_path, None)
                    .await
                    .unwrap();

                match rx.recv().await {
                    Some(block) => assert_eq!(block, Block::genesis()),
                    _ => assert!(false),
                }
            })
        })
        .collect();

    // Ensure all threads terminated correctly.
    assert!(try_join_all(handles).await.is_ok());
}
