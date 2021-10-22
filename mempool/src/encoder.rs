use crate::batch_maker::Batch;
use crate::config::Committee;
use crate::ensure;
use crate::error::{MempoolError, MempoolResult};
use crate::mempool::MempoolMessage;
use bytes::Bytes;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use itertools::Itertools as _;
use network::SimpleSender;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use smtree::{
    index::{TreeIndex, MAX_HEIGHT},
    node_template,
    node_template::{HashNodeSmt, MTreeNodeSmt, SumNodeSmt},
    proof::{MerkleProof as _, RandomSamplingProof},
    traits::{
        InclusionProvable, Mergeable, Paddable, PaddingProvable, ProofExtractable, Rand,
        RandomSampleable, Serializable, TypeName,
    },
    tree::SparseMerkleTree,
    utils::{generate_sorted_index_value_pairs, print_output},
};
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/encoder_tests.rs"]
pub mod encoder_tests;

pub type MerkleProof = u64; // TODO

pub const BATCH_STORAGE_PADDING: [u8; 1] = [0; 1];

#[derive(Debug, Serialize, Deserialize, Clone)]
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

    pub fn verify(&self, _name: &PublicKey, _committee: &Committee) -> MempoolResult<()> {
        // TODO
        Ok(())
    }

    pub fn reconstruct(
        coded_batches: Vec<Option<Self>>,
        committee: &Committee,
    ) -> MempoolResult<()> {
        let (data_shards, parity_shards) = committee.shards();
        let r =
            ReedSolomon::new(data_shards, parity_shards).expect("Failed to initialize RS encoder");
        let mut shards: Vec<_> = coded_batches
            .into_iter()
            .map(|x| x.map(|y| y.shard))
            .collect();
        r.reconstruct(&mut shards);
        let result: Vec<_> = shards.into_iter().filter_map(|x| x).collect();
        ensure!(
            r.verify(&result).expect("Failed to verify batch"),
            MempoolError::MalformedBatch
        );
        Ok(())
    }
}

pub struct Encoder {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    store: Store,
    rx_batch: Receiver<(Batch, usize)>,
    tx_coded_batch: Sender<CodedBatch>,
    network: SimpleSender,
}

impl Encoder {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        rx_batch: Receiver<(Batch, usize)>,
        tx_coded_batch: Sender<CodedBatch>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                store,
                rx_batch,
                tx_coded_batch,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let (data_shards, parity_shards) = self.committee.shards();

        // Encode and commit to every incoming batch.
        while let Some((batch, batch_size)) = self.rx_batch.recv().await {
            // Encode the payload using RS erasure codes. We can recover with f+1 shards.
            let symbols_length = batch_size / data_shards;
            let parity = vec![0u8; symbols_length * parity_shards];
            let mut shards: Vec<Vec<u8>> = batch
                .clone()
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

            // ----------- START: MAKE THE MERKLE TREE -----------
            let leaves: Vec<MTreeNodeSmt<blake3::Hasher>> = shards
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, mut shard)| {
                    shard.extend(i.to_le_bytes());
                    MTreeNodeSmt::new(shard)
                })
                .collect();

            //let tree = SparseMerkleTree::<MTreeNodeSmt<blake3::Hasher>>::new_merkle_tree(&leaves);

            // ----------- STOP: MAKE THE MERKLE TREE -----------

            // Now that we have the Merkle root, store the batch. Make sure to not conflict with the coded
            // batch, since they will be both stored under the same key (the Merkle root).
            let proof = 0; // TODO
            let root = Digest::default(); // TODO
            let mut key = root.to_vec();
            key.extend(BATCH_STORAGE_PADDING.to_vec());
            let message = MempoolMessage::Batch(batch);
            let serialized =
                bincode::serialize(&message).expect("Failed to serialize our own batch");
            self.store.write(key, serialized).await;

            // Multicast the shards to the committee members so that we can sign it.
            for (i, shard) in shards.into_iter().enumerate() {
                let to = self
                    .committee
                    .name(i)
                    .expect("Mismatch between committee and shards");
                let coded_batch =
                    CodedBatch::new(shard, proof, self.name, &mut self.signature_service).await;

                if to == self.name {
                    self.tx_coded_batch
                        .send(coded_batch)
                        .await
                        .expect("Failed to send our own coded batch to processor");
                } else {
                    let message = MempoolMessage::CodedBatch(coded_batch);
                    let serialized =
                        bincode::serialize(&message).expect("Failed to serialize coded batch");
                    let address = self.committee.mempool_address(&to).unwrap();
                    self.network.send(address, Bytes::from(serialized)).await;
                }
            }
        }
    }
}
