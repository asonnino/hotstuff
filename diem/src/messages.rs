use crate::committee::{Committee, Stake};
use crate::core::RoundNumber;
use crate::crypto::{Digest, Digestible, PublicKey, Signature};
use crate::error::DiemError;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;

#[derive(Serialize, Deserialize)]
pub struct Block {
    pub qc: QC,
    pub round: RoundNumber,
    author: PublicKey,
    payload: Digest,
    signature: Signature,
}

impl Block {
    pub fn check(&self, committee: &Committee) -> Result<(), DiemError> {
        self.signature.verify(self, &self.author)?;
        self.qc.check(committee)
    }
}

impl Digestible for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        let mut hash = [0u8; 64];
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.author.0);
        hasher.update(self.payload);
        hash.copy_from_slice(hasher.finalize().as_slice());
        hash[..32].try_into().unwrap()
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "B({:?}, {}, {:?}, {:?})",
            self.author,
            self.round,
            self.qc,
            &self.payload[..8]
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct Vote {
    hash: Digest,
    signature: Signature,
    author: PublicKey,
}

impl Vote {
    pub fn new(block: &Block, author: PublicKey) -> Result<Self, DiemError> {
        // TODO
        Ok(Vote {
            hash: block.digest(),
            signature: Signature::default(),
            author,
        })
    }
}

impl Digestible for Vote {
    fn digest(&self) -> Digest {
        self.hash
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "V({:?}, {:?})", self.author, self.hash)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QC {
    pub hash: Digest,
    votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Vec<Self> {
        // The genesis has the following structure:
        // B0 <- |QC0; B1| <- |QC1; B2| <- |QC2; B3| <- ...
        (0..3)
            .map(|x| QC {
                hash: [x as u8; 32],
                votes: Vec::new(),
            })
            .collect()
    }

    pub fn check(&self, committee: &Committee) -> Result<(), DiemError> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), DiemError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, DiemError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            DiemError::QCRequiresQuorum
        );

        // Check the signatures
        Signature::verify_batch(self, &self.votes)
    }
}

impl Digestible for QC {
    fn digest(&self) -> Digest {
        self.hash
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC({:?})", self.hash)
    }
}

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
        self.partial = Some(QC {
            hash: hash,
            votes: Vec::new(),
        });
    }

    pub fn clear(&mut self) {
        self.weight = 0;
        self.used.clear();
        self.partial = None;
    }

    /// Try to append a signature to a (partial) QC.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> Result<Option<QC>, DiemError> {
        let author = vote.author;
        ensure!(
            self.partial.is_some(),
            DiemError::UnexpectedOrLateVote(author)
        );

        // Check that each authority only appears once.
        ensure!(
            !self.used.contains(&author),
            DiemError::AuthorityReuse(author)
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&author);
        ensure!(voting_rights > 0, DiemError::UnknownAuthority(author));

        // Check the signature on the vote.
        vote.signature.verify(&vote, &author)?;

        let partial = self.partial.as_mut().unwrap();
        partial.votes.push((author, vote.signature));
        self.used.insert(author);
        self.weight += voting_rights;
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0;
            self.used.clear();
            Ok(self.partial.take())
        } else {
            Ok(None)
        }
    }
}
