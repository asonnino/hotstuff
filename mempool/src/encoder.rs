use crate::{
    batch_maker::Batch,
    coded_batch::{AuthenticatedShard, CodedBatch},
    config::Committee,
    mempool::MempoolMessage,
};
use bytes::Bytes;
use crypto::{PublicKey, SignatureService};
use network::SimpleSender;
use smtree::traits::Serializable as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

//#[cfg(test)]
//#[path = "tests/encoder_tests.rs"]
//pub mod encoder_tests;

pub struct Encoder {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    store: Store,
    rx_batch: Receiver<(Batch, usize)>,
    tx_coded_batch: Sender<AuthenticatedShard>,
    network: SimpleSender,
}

impl Encoder {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        rx_batch: Receiver<(Batch, usize)>,
        tx_coded_batch: Sender<AuthenticatedShard>,
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
        // Encode and commit to every incoming batch.
        while let Some((batch, batch_size)) = self.rx_batch.recv().await {
            // Encode the payload using RS erasure codes. We can recover with f+1 shards.
            let coded_batch = CodedBatch::new(batch, batch_size, &self.committee);

            // Commit to each encoded shard (i.e. build a Merkle tree using the erasure-coded shards as leaves).
            let tree = coded_batch.commit();

            // Now that we have the Merkle root, store the coded batch.
            let root = tree.get_root().serialize();
            let mut compressed_batch = coded_batch.clone();
            compressed_batch.compress(&self.committee);
            let message = MempoolMessage::CodedBatch(compressed_batch);
            let value = bincode::serialize(&message).expect("Failed to serialize coded batch");
            self.store.write(root, value).await;

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
                if to == self.name {
                    self.tx_coded_batch
                        .send(authenticated_shard)
                        .await
                        .expect("Failed to send our own coded batch to processor");
                } else {
                    let message = MempoolMessage::AuthenticatedShard(authenticated_shard);
                    let serialized = bincode::serialize(&message)
                        .expect("Failed to serialize authenticated shard");
                    let address = self.committee.mempool_address(&to).unwrap();
                    self.network.send(address, Bytes::from(serialized)).await;
                }
            }
        }
    }
}
