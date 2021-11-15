use crate::{
    coded_batch::{AuthenticatedShard, CodedBatch, Shard},
    config::Committee,
    mempool::MempoolMessage,
};
use crypto::{Digest, PublicKey};
use log::warn;
use smtree::traits::Serializable as _;
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto as _,
};
use store::Store;
use tokio::sync::mpsc::Receiver;

/// Indicates a serialized coded batch.
pub type SerializedCodedBatch = Vec<u8>;

pub struct Reconstructor {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Receive the root of the missing batches.
    rx_missing: Receiver<Digest>,
    /// Receives authenticated shards for the roots we requested.
    rx_shard: Receiver<AuthenticatedShard>,
    /// Receives coded batches for the roots we requested.
    rx_batch: Receiver<(CodedBatch, SerializedCodedBatch)>,
    /// Keeps a set of missing batches.
    missing: HashSet<Digest>,
    /// Aggregator helping to reconstruct a batch from its shards.
    collected_shards: HashMap<Digest, Vec<Option<Shard>>>,
}

impl Reconstructor {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        rx_missing: Receiver<Digest>,
        rx_shard: Receiver<AuthenticatedShard>,
        rx_batch: Receiver<(CodedBatch, SerializedCodedBatch)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                rx_missing,
                rx_shard,
                rx_batch,
                missing: HashSet::new(),
                collected_shards: HashMap::new(),
            }
            .run()
            .await;
        });
    }
    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(root) = self.rx_missing.recv() => {
                    self.missing.insert(root);
                }
                Some(shard) = self.rx_shard.recv() => {
                    // Verify the shard.
                    if let Err(e) = shard.verify(&self.name, &self.committee) {
                        warn!("{}", e);
                        continue;
                    }

                    // Ensure we requested this batch.
                    if !self.missing.contains(&shard.root) {
                        // NOTE: Do not print a warning since we will likely receive more shards than
                        // what we need (depending on our sync strategy).
                        continue;
                    }

                    // Add the shard to the aggregator.
                    let size = self.committee.size();
                    let index = shard.destination;
                    let root = shard.root.clone();
                    self
                        .collected_shards
                        .entry(root.clone())
                        .or_insert_with(|| vec![None; size])[index] = Some(shard.shard);

                    // Check if we have enough shards to reconstruct the batch.
                    let (data_shards, _) = self.committee.shards();
                    if self
                        .collected_shards
                        .get(&root)
                        .unwrap()
                        .iter()
                        .filter(|x| x.is_some())
                        .count() >= data_shards
                    {
                        // Reconstruct the batch.
                        let shards = self.collected_shards.remove(&root).unwrap();
                        let mut batch = CodedBatch::reconstruct(shards, &self.committee)
                            .expect("Failed to reconstruct batch from verified shards");

                        // Store the reconstructed batch.
                        batch.compress(&self.committee);
                        let message = MempoolMessage::CodedBatch(batch);
                        let value = bincode::serialize(&message).expect("Failed to serialize coded batch");
                        self.store.write(root.to_vec(), value).await;

                        // Update the missing batch set.
                        self.missing.remove(&root);
                    }

                },
                Some((batch, serialized)) = self.rx_batch.recv() => {
                    // Expand the (compressed) coded batch.
                    let mut coded_batch = batch;
                    if let Err(e) = coded_batch.expand(&self.committee) {
                        warn!("Failed to expand batch: {}", e);
                        continue;
                    }

                    // Re-compute the root of the batch.
                    let tree = coded_batch.commit();
                    let serialized_root = tree.get_root().serialize();
                    let root = Digest(serialized_root[0..32].try_into().unwrap());

                    // Ensure we requested this batch.
                    match self.missing.remove(&root) {
                        true => self.store.write(serialized_root, serialized).await,
                        false => warn!("Received unexpected shard with root {}", root)
                    }
                }
            }
        }
    }
}
