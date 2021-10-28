use crate::{
    batch_maker::Batch,
    config::Committee,
    ensure,
    error::{MempoolError, MempoolResult},
    mempool::MempoolMessage,
};
use bytes::Bytes;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::{Digest as _, Sha512};
use itertools::Itertools as _;
use network::SimpleSender;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use smtree::{
    index::TreeIndex,
    node_template::MTreeNodeSmt,
    proof::MerkleProof,
    traits::{InclusionProvable, Serializable as _},
    tree::SparseMerkleTree,
};
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/encoder_tests.rs"]
pub mod encoder_tests;

pub type SerializedProof = Vec<u8>;
pub type SerializedRoot = Vec<u8>;
pub type Shard = Vec<u8>;

pub struct Encoder {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    store: Store,
    rx_batch: Receiver<(Batch, usize)>,
    tx_coded_batch: Sender<CodedShard>,
    network: SimpleSender,
}

impl Encoder {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        rx_batch: Receiver<(Batch, usize)>,
        tx_coded_batch: Sender<CodedShard>,
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
            let leaves: Vec<MTreeNodeSmt<blake3::Hasher>> = shards
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, mut shard)| {
                    shard.extend(i.to_le_bytes());
                    MTreeNodeSmt::new(shard)
                })
                .collect();
            let tree = SparseMerkleTree::<MTreeNodeSmt<blake3::Hasher>>::new_merkle_tree(&leaves);
            let root: MTreeNodeSmt<blake3::Hasher> = tree.get_root();

            // Now that we have the Merkle root, store the batch.
            let key = root.serialize();
            let message = MempoolMessage::Batch(shards[0..data_shards].to_vec());
            let value = bincode::serialize(&message).expect("Failed to serialize our own batch");
            self.store.write(key, value).await;

            // Multicast the shards to the committee members so that we can sign it.
            for (i, shard) in shards.into_iter().enumerate() {
                let to = self
                    .committee
                    .name(i)
                    .expect("Mismatch between committee and shards");

                let index_list = vec![TreeIndex::from_u64(tree.get_height(), i as u64)];
                let proof = MerkleProof::<MTreeNodeSmt<blake3::Hasher>>::generate_inclusion_proof(
                    &tree,
                    &index_list,
                )
                .expect("Failed to generate merkle proof");

                let coded_batch = CodedShard::new(
                    shard,
                    proof,
                    root.clone(),
                    self.name,
                    &mut self.signature_service,
                )
                .await;

                if to == self.name {
                    self.tx_coded_batch
                        .send(coded_batch)
                        .await
                        .expect("Failed to send our own coded batch to processor");
                } else {
                    let message = MempoolMessage::CodedShard(coded_batch);
                    let serialized =
                        bincode::serialize(&message).expect("Failed to serialize coded batch");
                    let address = self.committee.mempool_address(&to).unwrap();
                    self.network.send(address, Bytes::from(serialized)).await;
                }
            }
        }
    }
}
