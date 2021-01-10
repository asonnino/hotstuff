use super::*;
use crate::common::{chain, committee, keys};
use crate::mempool::PayloadStatus;
use async_trait::async_trait;
use crypto::SecretKey;
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;
use std::fs;

pub struct MockMempool;

#[async_trait]
impl NodeMempool for MockMempool {
    async fn get(&self) -> Vec<u8> {
        let mut rng = StdRng::from_seed([0; 32]);
        let mut payload = [0u8; 32];
        rng.fill_bytes(&mut payload);
        payload.to_vec()
    }

    async fn verify(&self, _payload: &[u8]) -> PayloadStatus {
        PayloadStatus::Accept
    }

    async fn garbage_collect(&self, _payload: &[u8]) {}
}

async fn core(
    public_key: PublicKey,
    secret_key: SecretKey,
    store: Store,
    tx_network: Sender<NetMessage>,
    tx_commit: Sender<Block>,
) -> Sender<CoreMessage> {
    let signature_service = SignatureService::new(secret_key);
    let leader_elector = LeaderElector::new(committee());
    let mempool = MockMempool {};
    Core::make(
        public_key,
        committee(),
        Parameters::default(),
        store,
        signature_service,
        leader_elector,
        mempool,
        tx_network,
        tx_commit,
    )
    .await
}

fn leader_keys(round: RoundNumber) -> (PublicKey, SecretKey) {
    let leader_elector = LeaderElector::new(committee());
    let leader = leader_elector.get_leader(round);
    keys()
        .into_iter()
        .find(|(public_key, _)| *public_key == leader)
        .unwrap()
}

#[tokio::test]
async fn handle_block() {
    // Make a block and the vote we expect to receive.
    let block = chain(vec![leader_keys(1)]).pop().unwrap();
    let (public_key, secret_key) = keys().pop().unwrap();
    let vote = Vote::new_from_key(block.digest(), block.round, public_key, &secret_key);

    // Run a core instance.
    let path = ".store_test_handle_block";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (tx_network, mut rx_network) = channel(1);
    let (tx_commit, _) = channel(1);
    let core_channel = core(public_key, secret_key, store, tx_network, tx_commit).await;

    // Send a block to the core.
    let message = CoreMessage::Propose(block.clone());
    core_channel.send(message).await.unwrap();

    // Ensure we get a vote back.
    match rx_network.recv().await {
        Some(NetMessage::Vote(v, recipient)) => {
            assert_eq!(v, vote);
            let (next_leader, _) = leader_keys(2);
            assert_eq!(recipient, next_leader);
        }
        _ => assert!(false),
    }
}

#[tokio::test]
async fn make_block() {
    // Get the keys of the leaders of this round and the next.
    let (leader, leader_key) = leader_keys(1);
    let (next_leader, next_leader_key) = leader_keys(2);

    // Make a block, votes, and QC.
    let block = Block::new_from_key(QC::genesis(), None, leader, 1, Vec::new(), &leader_key);
    let hash = block.digest();
    let votes: Vec<_> = keys()
        .iter()
        .map(|(public_key, secret_key)| {
            Vote::new_from_key(hash.clone(), block.round, *public_key, &secret_key)
        })
        .collect();
    let qc = QC {
        hash,
        round: block.round,
        votes: votes
            .iter()
            .cloned()
            .map(|x| (x.author, x.signature))
            .collect(),
    };

    // Run a core instance.
    let path = ".store_test_make_block";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (tx_network, mut rx_network) = channel(1);
    let (tx_commit, _) = channel(1);
    let core_channel = core(next_leader, next_leader_key, store, tx_network, tx_commit).await;

    // Send all votes to the core.
    for vote in votes.clone() {
        let message = CoreMessage::Vote(vote);
        core_channel.send(message).await.unwrap();
    }

    // Ensure the core makes a new block.
    match rx_network.recv().await {
        Some(NetMessage::Block(b)) => {
            assert_eq!(b.round, 2);
            assert_eq!(b.qc, qc);
            assert!(b.tc.is_none());
        }
        _ => assert!(false),
    }
}

#[tokio::test]
async fn commit_block() {
    // Get 3 successive blocks.
    let leaders = vec![leader_keys(1), leader_keys(2), leader_keys(3)];
    let chain = chain(leaders);

    // Run a core instance.
    let path = ".store_test_commit_block";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (tx_network, mut _rx_network) = channel(3);
    let (tx_commit, mut rx_commit) = channel(1);
    let (public_key, secret_key) = keys().pop().unwrap();
    let core_channel = core(public_key, secret_key, store, tx_network, tx_commit).await;

    // Send a 3-chain to the core.
    for block in chain.clone() {
        let message = CoreMessage::Propose(block);
        core_channel.send(message).await.unwrap();
    }

    // Ensure the core commits the head.
    match rx_commit.recv().await {
        Some(b) => assert_eq!(b, Block::genesis()),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn make_timeout() {
    // Make the timeout vote we expect.
    let (public_key, secret_key) = leader_keys(3);
    let timeout = Vote::new_from_key(Digest::default(), 1, public_key, &secret_key);

    // Run a core instance.
    let path = ".store_test_make_timeout";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (tx_network, mut rx_network) = channel(1);
    let (tx_commit, _) = channel(1);
    let signature_service = SignatureService::new(secret_key);
    let leader_elector = LeaderElector::new(committee());
    let mempool = MockMempool {};
    let parameters = Parameters {
        timeout_delay: 100,
        ..Parameters::default()
    };
    let _ = Core::make(
        public_key,
        committee(),
        parameters,
        store,
        signature_service,
        leader_elector,
        mempool,
        tx_network,
        tx_commit,
    )
    .await;

    // Ensure the following operation happen in the right order.
    match rx_network.recv().await {
        Some(NetMessage::Vote(v, recipient)) => {
            assert_eq!(v, timeout);
            let (next_leader, _) = leader_keys(2);
            assert_eq!(recipient, next_leader);
        }
        _ => assert!(false),
    }
}
