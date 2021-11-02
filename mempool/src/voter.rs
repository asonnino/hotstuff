use crate::{
    coded_batch::AuthenticatedShard,
    config::Committee,
    ensure,
    error::{MempoolError, MempoolResult},
    mempool::MempoolMessage,
};
use bytes::Bytes;
use crypto::{Digest, PublicKey, Signature, SignatureService};
use log::warn;
use network::{CancelHandler, ReliableSender};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// A vote on a coded batch.
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchVote {
    /// The Merkle root of the coded batch.
    pub root: Digest,
    /// The signer's identity.
    pub author: PublicKey,
    /// The signature over of the Merkle root.
    pub signature: Signature,
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
        // TODO: Make signature on root||author.
        self.signature.verify(&self.root, &self.author)?;
        Ok(())
    }
}

/// Offset of the storage address to persist authenticated shards.
pub const AUTHENTICATED_SHARD_ADDRESS_OFFSET: u8 = 1;

/// Maximum number of votes for which we are waiting for a certificate.
const MAX_PENDING_VOTES: usize = 100;

/// Represents a serialized authenticated shard.
pub type SerializedShard = Vec<u8>;

/// Vote for our own batch shards.
pub struct SelfVoter {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The service to sign digests.
    signature_service: SignatureService,
    /// Receives coded shards.
    rx_authenticated_shard: Receiver<(AuthenticatedShard, SerializedShard)>,
    /// Outputs the votes for our own shards.
    tx_vote: Sender<BatchVote>,
}

impl SelfVoter {
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
            let vote = BatchVote::new(shard.root, self.name, &mut self.signature_service).await;
            self.tx_vote.send(vote).await.expect("Failed to send vote");
        }
    }
}

/// Vote for our other nodes' batch shards.
pub struct NodesVoter {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The service to sign digests.
    signature_service: SignatureService,
    /// Receives coded shards.
    rx_authenticated_shard: Receiver<(AuthenticatedShard, SerializedShard)>,
    /// Receives Merkle roots from consensus allowing to clean up internal state.
    rx_cleanup: Receiver<(PublicKey, Digest)>,
    /// The network sender.
    network: ReliableSender,
    /// Keeps the cancel handle of all the votes we sent.
    pending: HashMap<PublicKey, HashMap<Digest, CancelHandler>>,
}

impl NodesVoter {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        signature_service: SignatureService,
        rx_authenticated_shard: Receiver<(AuthenticatedShard, SerializedShard)>,
        rx_cleanup: Receiver<(PublicKey, Digest)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                signature_service,
                rx_authenticated_shard,
                rx_cleanup,
                network: ReliableSender::new(),
                pending: HashMap::new(),
            }
            .run()
            .await
        });
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // Process incoming coded shards.
                Some((shard, serialized_shard)) = self.rx_authenticated_shard.recv() => {
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

                    // Reply with a vote message.
                    let address = self
                        .committee
                        .mempool_address(&shard.author)
                        .expect("Author of valid coded shard is not in the committee");
                    let message = MempoolMessage::BatchVote(vote);
                    let serialized = bincode::serialize(&message).expect("Failed to serialize vote");
                    let handle = self.network.send(address, Bytes::from(serialized)).await;
                    let map = self.pending.entry(shard.author).or_insert_with(HashMap::new);
                    if map.len() >= MAX_PENDING_VOTES {
                        // TODO: Remove the oldest handler rather than a random one.
                        let key = map.keys().next().unwrap().clone();
                        map.retain(|x, _| x != &key);
                    }
                    map.insert(root, handle);
                },
                // Clean up internal state.
                Some((author, root)) = self.rx_cleanup.recv() => {
                    if let Some(map) = self.pending.get_mut(&author) {
                        let _ = map.remove(&root);
                    }
                }
            }
        }
    }
}
