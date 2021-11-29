use crate::{
    coded_batch::{AuthenticatedShard, CodedBatch},
    config::Committee,
    mempool::MempoolMessage,
    voter::SerializedShard,
};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use network::{CancelHandler, ReliableSender};
use smtree::traits::Serializable as _;
use std::{collections::HashMap, convert::TryInto as _};
use store::Store;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::{sleep, Duration, Instant},
};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// The maximum number of batches that have been created but are not yet certified.
const MAX_PENDING_BATCHES: usize = 50;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The service signing digests.
    signature_service: SignatureService,
    /// the persistent storage.
    store: Store,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Control channel to handle congestions.
    rx_control: Receiver<Vec<Digest>>,
    /// Output channel to deliver shards of transactions batches.
    tx_authenticated_shard: Sender<(AuthenticatedShard, SerializedShard)>,
    /// Send the roots for which we are trying to assemble a certificate.
    tx_root: Sender<Digest>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other mempools.
    network: ReliableSender,
    /// Keeps the handlers of the coded batch multicast.
    pending: HashMap<Digest, Vec<CancelHandler>>,
    /// Number of batches created but not yet certified.
    batch_counter: usize,
}

impl BatchMaker {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        rx_control: Receiver<Vec<Digest>>,
        tx_authenticated_shard: Sender<(AuthenticatedShard, SerializedShard)>,
        tx_root: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                store,
                batch_size,
                max_batch_delay,
                rx_transaction,
                rx_control,
                tx_authenticated_shard,
                tx_root,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                pending: HashMap::new(),
                batch_counter: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
            self.wait().await;
        }
    }

    /// Wait until enough batches are certified and cleanup internal state.
    async fn wait(&mut self) {
        if self.batch_counter < MAX_PENDING_BATCHES {
            return;
        }

        while self.batch_counter >= MAX_PENDING_BATCHES / 2 {
            debug!(
                "Waiting for previous batches to be certified (counter={})",
                self.batch_counter
            );
            let roots = self
                .rx_control
                .recv()
                .await
                .expect("Control channel dropped");
            for root in roots {
                let _ = self.pending.remove(&root);
                self.batch_counter -= 1;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let batch_clone = self.current_batch.clone();
        #[cfg(feature = "benchmark")]
        let batch_size_clone = self.current_batch_size;

        // Increase the batch count.
        self.batch_counter += 1;

        // Encode the payload using RS erasure codes. We can recover with f+1 shards.
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let coded_batch = CodedBatch::new(batch, self.current_batch_size, &self.committee);
        self.current_batch_size = 0;

        // Commit to each encoded shard (i.e. build a Merkle tree using the
        // erasure-coded shards as leaves).
        let tree = coded_batch.commit();
        let serialized_root = tree.get_root().serialize();
        let root = Digest(serialized_root[0..32].try_into().unwrap());

        #[cfg(feature = "benchmark")]
        Self::print_benchmark_info(&batch_clone, batch_size_clone, root.clone());

        // Now that we have the Merkle root, store the coded batch.
        let mut compressed_batch = coded_batch.clone();
        compressed_batch.compress(&self.committee);
        let message = MempoolMessage::CodedBatch(compressed_batch);
        let value = bincode::serialize(&message).expect("Failed to serialize coded batch");
        self.store.write(serialized_root, value).await;
        debug!("Sealed batch {}", root);

        // Send the root to the certificates aggregator.
        self.tx_root
            .send(root.clone())
            .await
            .expect("Failed to send root");

        // Disseminate the coded batch.
        for (i, shard) in coded_batch.shards.into_iter().enumerate() {
            // Make the coded shard.
            let authenticated_shard = AuthenticatedShard::new(
                shard,
                /* destination */ i,
                &tree,
                self.name,
                &mut self.signature_service,
            )
            .await;

            // Multicast the shards to the committee members so that they can sign it.
            let to = self
                .committee
                .name(i)
                .expect("Mismatch between committee and shards");
            let message = MempoolMessage::AuthenticatedShard(authenticated_shard.clone());
            let serialized =
                bincode::serialize(&message).expect("Failed to serialize authenticated shard");

            if to == self.name {
                self.tx_authenticated_shard
                    .send((authenticated_shard, serialized))
                    .await
                    .expect("Failed to send our own coded batch to processor");
            } else {
                let address = self.committee.mempool_address(&to).unwrap();
                let handle = self.network.send(address, Bytes::from(serialized)).await;
                self.pending
                    .entry(root.clone())
                    .or_insert_with(Vec::new)
                    .push(handle);
            }
        }
    }

    #[cfg(feature = "benchmark")]
    fn print_benchmark_info(batch: &Batch, batch_size: usize, root: Digest) {
        // Look for sample txs (they all start with 0) and gather their txs id (the next
        // 8 bytes).
        let tx_ids: Vec<_> = batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        for id in tx_ids {
            // NOTE: This log entry is used to compute performance.
            info!(
                "Batch {:?} contains sample tx {}",
                root,
                u64::from_be_bytes(id)
            );
        }

        // NOTE: This log entry is used to compute performance.
        info!("Batch {:?} contains {} B", root, batch_size);
    }
}
