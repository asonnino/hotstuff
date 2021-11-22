use crate::{
    config::{Committee, Stake},
    ensure,
    error::{MempoolError, MempoolResult},
    mempool::MempoolMessage,
    voter::BatchVote,
};
use bytes::Bytes;
use crypto::{Digest, PublicKey, Signature};
use log::{debug, warn};
use network::SimpleSender;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchCertificate {
    pub root: Digest,
    pub author: PublicKey,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl PartialEq for BatchCertificate {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
    }
}
impl Eq for BatchCertificate {}

impl Hash for BatchCertificate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.root.hash(state);
    }
}

impl BatchCertificate {
    pub fn verify(&self, committee: &Committee) -> MempoolResult<()> {
        // Ensure the Certificate has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), MempoolError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, MempoolError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            MempoolError::CertificateRequiresQuorum
        );

        // Check the signatures.
        // TODO: verify signature on root||author.
        Signature::verify_batch(&self.root, &self.votes).map_err(MempoolError::from)
    }
}

struct Aggregator {
    author: PublicKey,
    weight: Stake,
    votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
}

impl Aggregator {
    pub fn new(author: PublicKey) -> Self {
        Self {
            author,
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(
        &mut self,
        root: Digest,
        signature: Signature,
        author: PublicKey,
        committee: &Committee,
    ) -> MempoolResult<Option<BatchCertificate>> {
        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            MempoolError::AuthorityReuse(author)
        );

        self.votes.push((author, signature));
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            return Ok(Some(BatchCertificate {
                author: self.author,
                root,
                votes: self.votes.clone(),
            }));
        }
        Ok(None)
    }
}

pub struct AggregatorService;

impl AggregatorService {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        mut rx_root: Receiver<Digest>,
        mut rx_vote: Receiver<BatchVote>,
        tx_output: Sender<BatchCertificate>,
    ) {
        tokio::spawn(async move {
            let mut aggregators = HashMap::new();
            let mut network = SimpleSender::new();

            loop {
                tokio::select! {
                    Some(root) = rx_root.recv() => {
                        aggregators.insert(root, Aggregator::new(name));
                    },
                    Some(vote) = rx_vote.recv() => {
                        if let Err(e) = vote.verify(&committee) {
                            warn!("{}", e);
                            continue;
                        }
                        let aggregator = match aggregators.get_mut(&vote.root) {
                            Some(x) => x,
                            None => {
                                continue;
                            }
                        };

                        match aggregator.append(vote.root, vote.signature, vote.author, &committee) {
                            Ok(Some(certificate)) => {
                                let root = certificate.root.clone();
                                let _ = aggregators.remove(&root);
                                debug!("Assembled certificate for batch {}", root);

                                tx_output
                                    .send(certificate.clone())
                                    .await
                                    .expect("Failed to output certificate");

                                let addresses = committee
                                    .broadcast_addresses(&name)
                                    .into_iter()
                                    .map(|(_, x)| x)
                                    .collect();
                                let message = MempoolMessage::BatchCertificate(certificate);
                                let serialized = bincode::serialize(&message)
                                    .expect("Failed to serialize certificate");
                                network.broadcast(addresses, Bytes::from(serialized)).await;
                            },
                            Ok(None) => (),
                            Err(e) =>  warn!("{}", e)
                        }
                    }
                }
            }
        });
    }
}
