use crate::committee::Committee;
use crate::core::RoundNumber;
use crate::crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use crate::error::{DiemError, DiemResult};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC,
    pub author: PublicKey,
    pub round: RoundNumber,
    pub payload: Digest,
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        qc: QC,
        author: PublicKey,
        round: RoundNumber,
        payload: Digest,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut block = Block {
            qc,
            author,
            round,
            payload,
            signature: Signature::default(),
        };
        block.signature = signature_service.request_signature(block.digest()).await;
        block
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn previous(&self) -> Digest {
        self.qc.hash
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.payload);
        hasher.finalize().as_slice()[..32].try_into().unwrap()
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
    pub hash: Digest,
    pub signature: Signature,
    pub author: PublicKey,
    pub round: RoundNumber,
}

impl Vote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut vote = Vote {
            hash: block.digest(),
            signature: Signature::default(),
            author,
            round: block.round,
        };
        vote.signature = signature_service.request_signature(vote.digest()).await;
        vote
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.hash);
        hasher.update(self.round.to_le_bytes());
        hasher.finalize().as_slice()[..32].try_into().unwrap()
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "V({:?}, {}. {:?})", self.author, self.round, self.hash)
    }
}

#[derive(Serialize, Deserialize)]
pub struct TV {
    pub signature: Signature,
    pub author: PublicKey,
    pub round: RoundNumber,
}

impl TV {
    pub async fn new(
        round: RoundNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut vote = TV {
            signature: Signature::default(),
            author,
            round,
        };
        vote.signature = signature_service.request_signature(vote.digest()).await;
        vote
    }
}

impl Hash for TV {
    fn digest(&self) -> Digest {
        let hash = Sha512::digest(&self.round.to_le_bytes());
        hash.as_slice()[..32].try_into().unwrap()
    }
}

impl fmt::Debug for TV {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TV({:?}, {})", self.author, self.round)
    }
}

pub trait GenericQC: Hash {
    fn check(&self, committee: &Committee) -> DiemResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes().iter() {
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

        // Check the signatures.
        Signature::verify_batch(&self.digest(), self.votes()).map_err(DiemError::from)
    }

    fn votes(&self) -> &Vec<(PublicKey, Signature)>;
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub hash: Digest,
    pub round: RoundNumber,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Self {
        QC::default()
    }
}

impl GenericQC for QC {
    fn votes(&self) -> &Vec<(PublicKey, Signature)> {
        &self.votes
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.hash);
        hasher.update(self.round.to_le_bytes());
        hasher.finalize().as_slice()[..32].try_into().unwrap()
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC({:?}, {})", self.hash, self.round)
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.round == other.round
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TC {
    pub qc: QC,
    pub round: RoundNumber,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl GenericQC for TC {
    fn votes(&self) -> &Vec<(PublicKey, Signature)> {
        &self.votes
    }
}

impl Hash for TC {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.qc.digest());
        hasher.update(self.round.to_le_bytes());
        hasher.finalize().as_slice()[..32].try_into().unwrap()
    }
}

impl fmt::Debug for TC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TC({}, {:?})", self.round, self.qc)
    }
}
