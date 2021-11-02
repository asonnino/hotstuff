use crate::config::{Committee, Stake};
use crate::ensure;
use crate::error::{MempoolError, MempoolResult};
use crate::voter::BatchVote;
use crypto::{Digest, PublicKey, Signature};
use log::warn;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCertificate {
    pub root: Digest,
    pub author: PublicKey,
    pub votes: Vec<(PublicKey, Signature)>,
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
        mut rx_input: Receiver<BatchVote>,
        tx_output: Sender<BatchCertificate>,
        tx_control: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            let mut aggregators = HashMap::new();
            while let Some(vote) = rx_input.recv().await {
                if let Err(e) = vote.verify(&committee) {
                    warn!("{}", e);
                    continue;
                }
                if vote.author != name {
                    continue;
                }

                let potential_certificate = match aggregators
                    .entry(vote.root.clone())
                    .or_insert_with(|| Aggregator::new(name))
                    .append(vote.root, vote.signature, vote.author, &committee)
                {
                    Ok(x) => x,
                    Err(e) => {
                        warn!("{}", e);
                        continue;
                    }
                };
                if let Some(certificate) = potential_certificate {
                    let root = certificate.root.clone();
                    tx_output
                        .send(certificate)
                        .await
                        .expect("Failed to output certificate");
                    tx_control
                        .send(root)
                        .await
                        .expect("Failed to loopback certified root");
                }
            }
        });
    }
}
