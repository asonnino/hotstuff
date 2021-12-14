use crate::batch_maker::{Batch, Transaction};
use crate::config::Committee;
use crate::mempool::MempoolMessage;
use bytes::Bytes;
use crypto::{generate_keypair, Digest, PublicKey, SecretKey};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// Fixture
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn committee() -> Committee {
    Committee::new(
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let stake = 1;
                let front = format!("127.0.0.1:{}", 100 + i).parse().unwrap();
                let mempool = format!("127.0.0.1:{}", 100 + i).parse().unwrap();
                (name, stake, front, mempool)
            })
            .collect(),
        /*  epoch */ 100,
    )
}

// Fixture.
pub fn committee_with_base_port(base_port: u16) -> Committee {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let port = authority.transactions_address.port();
        authority.transactions_address.set_port(base_port + port);

        let port = authority.mempool_address.port();
        authority.mempool_address.set_port(base_port + port);
    }
    committee
}

// Fixture
pub fn transaction() -> Transaction {
    vec![0; 100]
}

// Fixture
pub fn batch() -> Batch {
    vec![transaction(), transaction()]
}

// Fixture
pub fn serialized_batch() -> Vec<u8> {
    let message = MempoolMessage::Batch(batch());
    bincode::serialize(&message).unwrap()
}

// Fixture
pub fn batch_digest() -> Digest {
    Digest(
        Sha512::digest(&serialized_batch()).as_slice()[..32]
            .try_into()
            .unwrap(),
    )
}

// Fixture
pub fn listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                if let Some(expected) = expected {
                    assert_eq!(received.freeze(), expected);
                }
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}
