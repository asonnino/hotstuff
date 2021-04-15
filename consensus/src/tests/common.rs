use crate::config::Committee;
use crate::core::{SeqNumber, HeightNumber, Bool};
use crate::mempool::{ConsensusMempoolMessage, PayloadStatus};
use crate::messages::{Block, Timeout, Vote, QC};
use crypto::Hash as _;
use crypto::{generate_keypair, Digest, PublicKey, SecretKey, Signature};
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;
use tokio::sync::mpsc::Receiver;

// Fixture.
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture.
pub fn committee() -> Committee {
    Committee::new(
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let address = format!("127.0.0.1:{}", i).parse().unwrap();
                let stake = 1;
                (name, 0, stake, address)
            })
            .collect(),
        /* epoch */ 1,
    )
}

impl Committee {
    pub fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }
}

impl Block {
    pub fn new_from_key(
        qc: QC,
        author: PublicKey,
        view: SeqNumber,
        round: SeqNumber,
        height: HeightNumber,
        fallback: Bool,
        payload: Vec<Digest>,
        secret: &SecretKey,
    ) -> Self {
        let block = Block {
            qc,
            tc: None,
            coin: None,
            author,
            view,
            round,
            height,
            fallback,
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
        view: SeqNumber,
        round: SeqNumber,
        height: HeightNumber,
        fallback: Bool,
        proposer: PublicKey,
        author: PublicKey,
        secret: &SecretKey,
    ) -> Self {
        let vote = Self {
            hash,
            view,
            round,
            height,
            fallback,
            proposer,
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

impl Timeout {
    pub fn new_from_key(
        high_qc: QC,
        seq: SeqNumber,
        author: PublicKey,
        secret: &SecretKey,
    ) -> Self {
        let timeout = Self {
            high_qc,
            seq,
            author,
            signature: Signature::default(),
        };
        let signature = Signature::new(&timeout.digest(), &secret);
        Self {
            signature,
            ..timeout
        }
    }
}

impl PartialEq for Timeout {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

// Fixture.
pub fn block() -> Block {
    let (public_key, secret_key) = keys().pop().unwrap();
    Block::new_from_key(QC::genesis(), public_key, 0, 1, 0, 0, Vec::new(), &secret_key)
}

// Fixture.
pub fn vote() -> Vote {
    let (public_key, secret_key) = keys().pop().unwrap();
    Vote::new_from_key(block().digest(), 0, 1, 0, 0, block().author, public_key, &secret_key)
}

// Fixture.
pub fn qc() -> QC {
    let mut keys = keys();
    let (public_key, _) = keys.pop().unwrap();
    let qc = QC {
        hash: Digest::default(),
        view: 0,
        round: 1,
        height: 0,
        fallback: 0,
        proposer: public_key,
        acceptor: public_key,
        votes: Vec::new(),
    };
    let digest = qc.digest();
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
                *public_key,
                0,
                1 + i as SeqNumber,
                0,
                0,
                Vec::new(),
                secret_key,
            );

            // Make a qc for that block (it will be used for the next block).
            let qc = QC {
                hash: block.digest(),
                view: block.view,
                round: block.round,
                height: block.height,
                fallback: block.fallback,
                proposer: block.author,
                acceptor: block.author,
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

impl MockMempool {
    pub fn run(mut consensus_mempool_channel: Receiver<ConsensusMempoolMessage>) {
        tokio::spawn(async move {
            while let Some(message) = consensus_mempool_channel.recv().await {
                match message {
                    ConsensusMempoolMessage::Get(_max, sender) => {
                        let mut rng = StdRng::from_seed([0; 32]);
                        let mut payload = [0u8; 32];
                        rng.fill_bytes(&mut payload);
                        sender.send(vec![Digest(payload)]).unwrap();
                    }
                    ConsensusMempoolMessage::Verify(_block, sender) => {
                        sender.send(PayloadStatus::Accept).unwrap()
                    }
                    ConsensusMempoolMessage::Cleanup(_digests, _round) => (),
                }
            }
        });
    }
}
