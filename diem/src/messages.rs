use crate::committee::{Committee, Stake};
use crate::crypto::{Digest, Digestible, PublicKey, Signature};
use crate::error::DiemError;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

type Transaction = Vec<u8>;
type RoundNumber = u64;

#[derive(Serialize, Deserialize)]
struct Block {
    qc: QC,
    round: RoundNumber,
    author: PublicKey,
    txs: Vec<Transaction>,
    signature: Signature,
}

impl Digestible for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        let mut hash = [0u8; 64];
        let mut digest = [0u8; 32];
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.author.0);
        for tx in &self.txs {
            hasher.update(tx);
        }
        hash.copy_from_slice(hasher.finalize().as_slice());
        digest.copy_from_slice(&hash[..32]);
        digest
    }
}

#[derive(Serialize, Deserialize)]
pub struct Vote {
    hash: Digest,
    signature: Signature,
    author: PublicKey,
}

impl Digestible for Vote {
    fn digest(&self) -> Digest {
        self.hash
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QC {
    hash: Digest,
    votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Vec<Self> {
        (0..3)
            .map(|x| QC {
                hash: [x as u8; 32],
                votes: Vec::new(),
            })
            .collect()
    }
}

/*
impl QC {
    pub fn check(&self, committee: &Committee) -> Result<(), DagError> {
        // Check the quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            // Check that each authority only appears once.
            diem_ensure!(
                !used.contains(name),
                DiemError::AuthorityReuse(name)
            );
            let voting_rights = committee.stake(name);
            dag_ensure!(voting_rights > 0, DiemError::UnknownAuthority(name));
            used.insert(*name);
            weight += voting_rights;
        }
        dag_ensure!(
            weight >= committee.quorum_threshold(),
            DiemError::QCRequiresQuorum
        );
        // Check the signatures
        // TODO:
        //Signature::verify_batch(&digest, &self.votes)
        true
    }
}
*/

#[derive(Default)]
pub struct SignatureAggregator {
    weight: Stake,
    used: HashSet<PublicKey>,
    pub hash: Digest,
    pub partial: Option<QC>,
}

impl SignatureAggregator {
    pub fn init(&mut self, hash: Digest) {
        self.clear();
        self.hash = hash;
        self.partial = Some(QC {
            hash: hash,
            votes: Vec::new(),
        });
    }

    pub fn clear(&mut self) {
        self.weight = 0;
        self.used.clear();
        self.hash = Digest::default();
        self.partial = None;
    }

    /// Try to append a signature to a (partial) QC.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> Result<Option<QC>, DiemError> {
        let author = vote.author;
        diem_ensure!(
            self.partial.is_some(),
            DiemError::UnexpectedOrLateVote(author)
        );

        // Check that each authority only appears once.
        diem_ensure!(
            !self.used.contains(&author),
            DiemError::AuthorityReuse(author)
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&author);
        diem_ensure!(voting_rights > 0, DiemError::UnknownAuthority(author));

        // Check the signature on the vote.
        vote.signature.check(&vote, &author)?;

        let partial = self.partial.as_mut().unwrap();
        partial.votes.push((author, vote.signature));
        self.used.insert(author);
        self.weight += voting_rights;
        if self.weight >= committee.quorum_threshold() {
            Ok(Some(partial.clone()))
        } else {
            Ok(None)
        }
    }
}
