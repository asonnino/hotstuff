use crate::committee::{Committee, Stake};
use crate::core::RoundNumber;
use crate::crypto::{Digest, Digestible, PublicKey, Signature};
use crate::error::{DiemError, DiemResult};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

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
        signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    ) -> Self {
        let mut block = Block {
            qc,
            author,
            round,
            payload,
            signature: Signature::default(),
        };
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = signature_channel.send((block.digest(), sender)).await {
            panic!("Failed to send Block command to Signature Service: {}", e);
        }
        block.signature = receiver
            .await
            .expect("Failed to receive signature from Signature Service");
        block
    }

    pub fn check(&self, committee: &Committee) -> DiemResult<()> {
        self.signature.verify(self, &self.author)?;
        self.qc.check(committee)
    }

    pub fn genesis() -> Self {
        Block::default()
    }
}

impl Digestible for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        let mut hash = [0u8; 64];
        hasher.update(self.author.0);
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.payload);
        hash.copy_from_slice(hasher.finalize().as_slice());
        hash[..32].try_into().expect("Unexpected hash length")
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
}

impl Vote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    ) -> Self {
        let hash = block.digest();
        let mut vote = Vote {
            hash,
            signature: Signature::default(),
            author,
        };
        if author == block.author {
            vote.signature = block.signature.clone();
        } else {
            let (sender, receiver) = oneshot::channel();
            if let Err(e) = signature_channel.send((hash, sender)).await {
                panic!("Failed to send Block command to Signature Service: {}", e);
            }
            vote.signature = receiver
                .await
                .expect("Failed to receive signature from Signature Service");
        }
        vote
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

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub hash: Digest,
    votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn check(&self, committee: &Committee) -> DiemResult<()> {
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
        Signature::verify_batch(self, &self.votes).map_err(DiemError::from)
    }

    pub fn genesis() -> Self {
        QC::default()
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

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
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
    pub fn new(hash: Digest) -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            hash,
            partial: None,
        }
    }

    pub fn init(&mut self, hash: Digest) {
        self.clear();
        self.partial = Some(QC {
            hash,
            votes: Vec::new(),
        });
    }

    pub fn clear(&mut self) {
        self.weight = 0;
        self.used.clear();
        self.partial = None;
    }

    /// Try to append a signature to a (partial) QC.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> DiemResult<Option<QC>> {
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
        // TODO: use self.hash instead of &vote to verify vote.
        // We currently do not know if all votes are on the same message.
        vote.signature.verify(&vote, &author)?;

        let partial = self
            .partial
            .as_mut()
            .expect("Partial QC should not be null at this stage");
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
