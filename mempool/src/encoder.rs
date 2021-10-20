use crate::batch_maker::Batch;
use crate::config::Committee;
use crate::mempool::MempoolMessage;
use bytes::Bytes;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use itertools::Itertools as _;
use network::SimpleSender;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use std::convert::TryInto as _;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/encoder_tests.rs"]
pub mod encoder_tests;

pub type MerkleProof = u64; // TODO

#[derive(Debug, Serialize, Deserialize)]
pub struct CodedBatch {
    pub shard: Vec<u8>,
    pub proof: MerkleProof,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Hash for CodedBatch {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.shard);
        hasher.update(self.proof.to_le_bytes());
        hasher.update(self.author);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl CodedBatch {
    pub async fn new(
        shard: Vec<u8>,
        proof: MerkleProof,
        author: PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let coded_batch = Self {
            shard,
            proof,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service
            .request_signature(coded_batch.digest())
            .await;
        Self {
            signature,
            ..coded_batch
        }
    }
}

pub struct Encoder {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_batch: Receiver<(Batch, usize)>,
    network: SimpleSender,
}

impl Encoder {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        rx_batch: Receiver<(Batch, usize)>,
        signature_service: SignatureService,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_batch,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let data_shards =
            (self.committee.total_stake() - self.committee.validity_threshold()) as usize;
        let parity_shards = self.committee.validity_threshold() as usize;

        // Encode and commit to every incoming batch.
        while let Some((batch, batch_size)) = self.rx_batch.recv().await {
            // Encode the payload using RS erasure codes. We can recover with f+1 shards.
            let symbols_length = batch_size / data_shards;
            let parity = vec![0u8; symbols_length * parity_shards];
            let mut shards: Vec<Vec<u8>> = batch
                .into_iter()
                .flatten()
                .chain(parity.into_iter())
                .chunks(symbols_length)
                .into_iter()
                .map(|x| x.collect::<Vec<_>>())
                .collect();

            ReedSolomon::new(data_shards, parity_shards)
                .expect("Failed to initialize RS encoder")
                .encode(&mut shards)
                .expect("Failed to encode data");

            // Commit to each encoded shard.
            // TODO
            let proof = 0;

            // Store the batch (not that we have the Merkle root).
            // TODO

            // Send the Merkle root to the processor so that we can vote for it.
            // TODO

            // Multicast the shards to the committee members.
            for (i, shard) in shards.into_iter().enumerate() {
                let to = self
                    .committee
                    .name(i)
                    .expect("Mismatch between committee and shards");
                let address = self.committee.mempool_address(&to).unwrap();
                let coded_batch =
                    CodedBatch::new(shard, proof, self.name, &mut self.signature_service).await;
                let message = MempoolMessage::CodedBatch(coded_batch);
                let bytes = bincode::serialize(&message).expect("Failed to serialize coded batch");
                self.network.send(address, Bytes::from(bytes)).await;
            }
        }
    }
}
