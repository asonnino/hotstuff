use crate::batch_maker::{BatchWithSender, Transaction};
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

// Fixture.
pub fn committee_with_base_port(base_port: usize) -> Committee {
    Committee::new(
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let stake = 1;
                let front = format!("127.0.0.1:{}", 100 + i + base_port)
                    .parse()
                    .unwrap();
                let mempool = format!("127.0.0.1:{}", 100 + i + base_port)
                    .parse()
                    .unwrap();
                (name, stake, front, mempool)
            })
            .collect(),
        /*  epoch */ 100,
    )
}

// Fixture
pub fn transaction() -> Transaction {
    vec![0; 100]
}

// Fixture
pub fn batch() -> BatchWithSender {
    BatchWithSender {
        batch: vec![transaction(), transaction()],
        sender: keys()[0].0,
    }
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
    // TODO : fix this function
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
