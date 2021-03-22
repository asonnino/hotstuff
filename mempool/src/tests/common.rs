use crate::config::Committee;
use crate::messages::Payload;
use consensus::{Block, QC};
use crypto::Hash as _;
use crypto::{generate_keypair, PublicKey, SecretKey, Signature};
use rand::rngs::StdRng;
use rand::SeedableRng as _;

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
                let front = format!("127.0.0.1:{}", i).parse().unwrap();
                let mempool = format!("127.0.0.1:{}", i + keys().len()).parse().unwrap();
                (name, front, mempool)
            })
            .collect(),
        /* epoch */ 1,
    )
}

impl Committee {
    pub fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.front_address.port();
            authority.front_address.set_port(base_port + port);
        }
        for authority in self.authorities.values_mut() {
            let port = authority.mempool_address.port();
            authority.mempool_address.set_port(base_port + port);
        }
    }
}

// Fixture.
pub fn payload() -> Payload {
    let (author, secret) = keys().pop().unwrap();
    let payload = Payload {
        transactions: vec![vec![1u8]],
        author,
        signature: Signature::default(),
    };
    let signature = Signature::new(&payload.digest(), &secret);
    Payload {
        signature,
        ..payload
    }
}

// Fixture.
pub fn block() -> Block {
    let (author, secret) = keys().pop().unwrap();
    let block = Block {
        qc: QC::genesis(),
        tc: None,
        coin: None,
        author,
        view: 0,
        round: 1,
        height: 0,
        fallback: 0,
        payload: Vec::new(),
        signature: Signature::default(),
    };
    let signature = Signature::new(&block.digest(), &secret);
    Block { signature, ..block }
}
