use crate::{
    batch_maker::Batch,
    config::Committee,
    ensure,
    error::{MempoolError, MempoolResult},
};
use crypto::{Digest, PublicKey, Signature, SignatureService};
use itertools::Itertools as _;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use smtree::{
    index::TreeIndex,
    node_template::MTreeNodeSmt,
    proof::MerkleProof,
    traits::{InclusionProvable as _, Serializable as _},
    tree::SparseMerkleTree,
};
use std::convert::TryInto as _;

#[cfg(test)]
#[path = "tests/coded_batch_tests.rs"]
pub mod coded_batch_tests;

/// Represents an erasure-coded shard, generated from a batch of transactions.
pub type Shard = Vec<u8>;

/// Convenient shortcut representing a Merkle tree.
type Tree = SparseMerkleTree<MTreeNodeSmt<blake3::Hasher>>;

/// An erasure-corrected transaction batch.
#[derive(Serialize, Deserialize, Debug)]
pub struct CodedBatch {
    /// All the data shards (not the parity shards) of the erasure-coded transactions batch.
    pub shards: Vec<Shard>,
}

impl CodedBatch {
    /// Encodes (erasure-corrected) a transactions batch.
    pub fn new(batch: Batch, batch_size: usize, committee: &Committee) -> Self {
        let (data_shards, parity_shards) = committee.shards();
        let symbols_length = batch_size / data_shards;
        let parity = vec![0u8; symbols_length * parity_shards];

        let mut shards: Vec<Shard> = batch
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

        Self { shards }
    }

    /// Reconstruct the coded transaction batch from enough shards.
    pub fn reconstruct(
        mut coded_shards: Vec<Option<Shard>>,
        committee: &Committee,
    ) -> MempoolResult<Self> {
        let (data_shards, parity_shards) = committee.shards();

        // Reconstruct the coded batch.
        let decoder = ReedSolomon::new(data_shards, parity_shards)
            .expect("Failed to initialize RS decoder from committee");
        decoder.reconstruct(&mut coded_shards)?;

        // Ensure the reconstruction succeeded.
        let result: Vec<_> = coded_shards.into_iter().filter_map(|x| x).collect();
        ensure!(decoder.verify(&result)?, MempoolError::MalformedCodedBatch);
        Ok(Self { shards: result })
    }

    /// Compute a Merkle tree using the coded shards as leaves.
    pub fn commit(&self) -> Tree {
        let leaves: Vec<_> = self
            .shards
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, shard)| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(&shard);
                hasher.update(&i.to_le_bytes());
                let hash = hasher.finalize();
                MTreeNodeSmt::new(hash.as_bytes().to_vec())
            })
            .collect();

        Tree::new_merkle_tree(&leaves)
    }
}

/// Represents a serialized Merkle proof.
pub type SerializedProof = Vec<u8>;

/// Represents a serialize Merkle root.
pub type SerializedRoot = Vec<u8>;

/// Convenient shortcut representing a Merkle proof.
type Proof = MerkleProof<MTreeNodeSmt<blake3::Hasher>>;

/// A self-authenticated encoded batch shard.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthenticatedShard {
    pub shard: Shard,
    pub destination: usize,
    pub proof: SerializedProof,
    pub root: SerializedRoot,
    pub author: PublicKey,
    pub signature: Signature,
}

impl AuthenticatedShard {
    /// Make a new authenticated batch shard from an encoded shard.
    pub async fn new(
        shard: Shard,
        destination: usize,
        tree: &Tree,
        author: PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        // Sign the Merkle root.
        let serialized_root = tree.get_root().serialize();
        let digest = Digest(serialized_root[0..32].try_into().unwrap());
        let signature = signature_service.request_signature(digest).await;

        // Construct the Merkle proof.
        let index_list = vec![TreeIndex::from_u64(tree.get_height(), destination as u64)];
        let proof = Proof::generate_inclusion_proof(&tree, &index_list)
            .expect("Failed to generate merkle proof");

        Self {
            shard,
            destination,
            proof: proof.serialize(),
            root: serialized_root,
            author,
            signature,
        }
    }

    /// Verify the authenticated shard.
    pub fn verify(&self, name: &PublicKey, committee: &Committee) -> MempoolResult<()> {
        // Deserialize the proof and the tree's root.
        let deserialized_proof =
            Proof::deserialize(&self.proof).map_err(|_| MempoolError::BadInclusionProof)?;
        let deserialized_root =
            MTreeNodeSmt::deserialize(&self.root).map_err(|_| MempoolError::BadInclusionProof)?;

        // Verify the signature on the Merkle root. Note that the conversion of the root into
        // a digest cannot fail since we already deserialized it into a valid Merkle root.
        let digest = Digest(self.root[0..32].try_into().unwrap());
        self.signature.verify(&digest, &self.author)?;

        // Build the leaf of the Merkle Tree.
        let index = committee
            .index(&name)
            .expect("Our public key is not in the committee");

        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.shard);
        hasher.update(&index.to_le_bytes());
        let hash = hasher.finalize();
        let leaf = MTreeNodeSmt::new(hash.as_bytes().to_vec());

        // Check the Merkle proof.
        let ok = deserialized_proof.verify(&leaf, &deserialized_root);
        ensure!(ok, MempoolError::BadInclusionProof);
        Ok(())
    }
}
