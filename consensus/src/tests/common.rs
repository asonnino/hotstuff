use crate::core::RoundNumber;
use crate::mempool::NodeMempool;
use crate::mempool::PayloadStatus;
use crate::messages::{Block, Vote, QC, TC};
use async_trait::async_trait;
use config::Committee;
use crypto::Hash as _;
use crypto::{generate_keypair, Digest, PublicKey, SecretKey, Signature};
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;

// Fixture.
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture.
pub fn committee() -> Committee {
    let authorities: Vec<_> = keys().into_iter().map(|(name, _)| (name, 1)).collect();
    Committee::new(&authorities, /* epoch */ 1)
}

pub trait TestCommittee {
    fn increment_base_port(&mut self, base_port: u16);
}

impl TestCommittee for Committee {
    fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }
}

impl Block {
    pub fn new_from_key(
        qc: QC,
        tc: Option<TC>,
        author: PublicKey,
        round: RoundNumber,
        payload: Vec<u8>,
        secret: &SecretKey,
    ) -> Self {
        let block = Block {
            qc,
            tc,
            author,
            round,
            payload,
            signature: Signature::default(),
        };
        let signature = Signature::new(&block.digest(), secret);
        Self { signature, ..block }
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl Vote {
    pub fn new_from_key(
        hash: Digest,
        round: RoundNumber,
        author: PublicKey,
        secret: &SecretKey,
    ) -> Self {
        let vote = Self {
            hash,
            round,
            author,
            signature: Signature::default(),
        };
        let signature = Signature::new(&vote.digest(), &secret);
        Self { signature, ..vote }
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

// Fixture.
pub fn block() -> Block {
    let (public_key, secret_key) = keys().pop().unwrap();
    Block::new_from_key(QC::genesis(), None, public_key, 1, Vec::new(), &secret_key)
}

// Fixture.
pub fn vote() -> Vote {
    let (public_key, secret_key) = keys().pop().unwrap();
    Vote::new_from_key(block().digest(), 1, public_key, &secret_key)
}

// Fixture.
pub fn qc() -> QC {
    let qc = QC {
        hash: Digest::default(),
        round: 1,
        votes: Vec::new(),
    };
    let digest = qc.digest();
    let mut keys = keys();
    let votes: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();
    QC { votes, ..qc }
}

// Fixture.
pub fn chain(keys: Vec<(PublicKey, SecretKey)>) -> Vec<Block> {
    let mut latest_qc = QC::genesis();
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            // Make a block.
            let (public_key, secret_key) = key;
            let block = Block::new_from_key(
                latest_qc.clone(),
                None,
                *public_key,
                1 + i as RoundNumber,
                Vec::new(),
                secret_key,
            );

            // Make a qc for that block (it will be used for the next block).
            let qc = QC {
                hash: block.digest(),
                round: block.round,
                votes: Vec::new(),
            };
            let digest = qc.digest();
            let votes: Vec<_> = keys
                .iter()
                .map(|(public_key, secret_key)| (*public_key, Signature::new(&digest, secret_key)))
                .collect();
            latest_qc = QC { votes, ..qc };

            // Return the block.
            block
        })
        .collect()
}

// Fixture
pub struct MockMempool;

#[async_trait]
impl NodeMempool for MockMempool {
    async fn get(&mut self) -> Vec<u8> {
        let mut rng = StdRng::from_seed([0; 32]);
        let mut payload = [0u8; 32];
        rng.fill_bytes(&mut payload);
        payload.to_vec()
    }

    async fn verify(&mut self, _payload: &[u8]) -> PayloadStatus {
        PayloadStatus::Accept
    }

    async fn garbage_collect(&mut self, _payload: &[u8]) {}
}
