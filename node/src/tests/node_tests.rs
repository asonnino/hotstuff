/*
use super::*;
use crypto::Hash as _;
use crypto::{generate_keypair, PublicKey, SecretKey};
use futures::future::try_join_all;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::fs;

pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

pub trait TestSecret {
    fn print_key_files(prefix: &str) -> Vec<String>;
}

impl TestSecret for Secret {
    fn print_key_files(prefix: &str) -> Vec<String> {
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, key)| {
                let filename = format!("{}_{}.json", prefix, i);
                let _ = fs::remove_file(&filename);
                let (name, secret) = key;
                Self { name, secret }.write(&filename).unwrap();
                filename
            })
            .collect()
    }
}

pub fn committee() -> Committee {
    let authorities: Vec<_> = keys().into_iter().map(|(name, _)| (name, 1)).collect();
    Committee::new(&authorities, /* epoch */ 1)
}

pub trait TestCommittee {
    fn increment_base_port(&mut self, base_port: u16);
    fn print_committee(filename: &str, base_port: u16);
}

impl TestCommittee for Committee {
    fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }

    fn print_committee(filename: &str, base_port: u16) {
        let _ = fs::remove_dir_all(filename);
        let mut committee = committee();
        committee.increment_base_port(base_port);
        committee.write(filename).unwrap();
    }
}

trait TestBlock {
    fn eq(&self, other: &Self) -> bool;
}

impl TestBlock for Block {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

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
                    Some(block) => assert!(block.eq(&Block::genesis())),
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
                    Some(block) => assert!(block.eq(&Block::genesis())),
                    _ => assert!(false),
                }
            })
        })
        .collect();

    // Ensure all threads terminated correctly.
    assert!(try_join_all(handles).await.is_ok());
}
*/