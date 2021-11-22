use crate::{
    coded_batch::{AuthenticatedShard, CodedBatch, Shard},
    config::Committee,
    mempool::MempoolMessage,
};
use crypto::Digest;
use log::{debug, warn};
use smtree::traits::Serializable as _;
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto as _,
};
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/reconstructor_tests.rs"]
pub mod reconstructor_tests;

/// Indicates a serialized coded batch.
pub type SerializedCodedBatch = Vec<u8>;

pub struct Reconstructor {
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
        committee: Committee,
        store: Store,
        rx_missing: Receiver<Digest>,
        rx_shard: Receiver<AuthenticatedShard>,
        rx_batch: Receiver<(CodedBatch, SerializedCodedBatch)>,
    ) {
        tokio::spawn(async move {
            Self {
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
                    //debug!("Registering missing batch {}", root);
                    self.missing.insert(root);
                }
                Some(shard) = self.rx_shard.recv() => {
                    //debug!("Received shard of {}", shard.root);

                    // Verify the shard.
                    let destination = match self.committee.name(shard.destination) {
                        Some(x) => x,
                        None => {
                            warn!("Invalid shard: Unknown destination node");
                            continue;
                        }
                    };
                    if let Err(e) = shard.verify(&destination, &self.committee) {
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
                        debug!("Reconstructing {}", root);

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
                    debug!("Received batch {}", root);
                    if self.missing.remove(&root) {
                        // NOTE: We will likely receive more shards than what we need (depending on our sync strategy).
                        self.collected_shards.remove(&root);
                        self.store.write(serialized_root, serialized).await;
                    }
                }
            }
        }
    }
}
