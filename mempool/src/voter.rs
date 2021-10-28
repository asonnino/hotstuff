use crate::{
    coded_batch::AuthenticatedShard,
    config::Committee,
    ensure,
    error::{MempoolError, MempoolResult},
    mempool::MempoolMessage,
};
use bytes::Bytes;
use crypto::{Digest, PublicKey, Signature, SignatureService};
use ed25519_dalek::{Digest as _, Sha512};
use log::warn;
use network::{CancelHandler, ReliableSender};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryInto};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

//#[cfg(test)]
//#[path = "tests/voter_tests.rs"]
//pub mod voter_tests;

/// A vote on a coded batch.
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchVote {
    /// The Merkle root of the coded batch.
    root: Digest,
    /// The signer's identity.
    author: PublicKey,
    /// The signature over of the Merkle root.
    signature: Signature,
}

impl BatchVote {
    pub async fn new(
        root: Digest,
        author: PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        Self {
            root: root.clone(),
            author,
            signature: signature_service.request_signature(root).await,
        }
    }

    pub fn verify(&self, committee: &Committee) -> MempoolResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            MempoolError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.root, &self.author)?;
        Ok(())
    }
}

/// Offset of the storage address to persist authenticated shards.
pub const AUTHENTICATED_SHARD_ADDRESS_OFFSET: u8 = 1;

/// Represents a serialized authenticated shard.
pub type SerializedShard = Vec<u8>;

/// Verify and votes for batch shards.
pub struct Voter {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The service to sign digests.
    signature_service: SignatureService,
    /// Input channel to receive batches.
    rx_authenticated_shard: Receiver<(AuthenticatedShard, SerializedShard)>,
    /// Outputs the votes for our own shards.
    tx_vote: Sender<BatchVote>,
    /// The network sender.
    network: ReliableSender,
    /// Keeps the cancel handle of all the votes we sent.
    pending: HashMap<Digest, CancelHandler>,
}

impl Voter {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        signature_service: SignatureService,
        rx_authenticated_shard: Receiver<(AuthenticatedShard, SerializedShard)>,
        tx_vote: Sender<BatchVote>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                signature_service,
                rx_authenticated_shard,
                tx_vote,
                network: ReliableSender::new(),
                pending: HashMap::new(),
            }
            .run()
            .await
        });
    }

    async fn run(&mut self) {
        while let Some((shard, serialized_shard)) = self.rx_authenticated_shard.recv().await {
            // Verify the shard.
            if let Err(e) = shard.verify(&self.name, &self.committee) {
                warn!("{}", e);
                continue;
            }

            // Store the shard.
            let mut key = shard.root.to_vec();
            key.push(AUTHENTICATED_SHARD_ADDRESS_OFFSET);
            self.store.write(key, serialized_shard).await;

            // Reply with a signature.
            let root = shard.root;
            let vote = BatchVote::new(root.clone(), self.name, &mut self.signature_service).await;
            if shard.author == self.name {
                self.tx_vote.send(vote).await.expect("Failed to send vote");
            } else {
                let address = self
                    .committee
                    .mempool_address(&shard.author)
                    .expect("Author of valid coded shard is not in the committee");
                let message = MempoolMessage::BatchVote(vote);
                let serialized = bincode::serialize(&message).expect("Failed to serialize vote");
                let handle = self.network.send(address, Bytes::from(serialized)).await;
                self.pending.insert(root, handle);
            }
        }
    }
}
