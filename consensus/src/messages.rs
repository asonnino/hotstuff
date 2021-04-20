use crate::config::Committee;
use crate::core::{SeqNumber, HeightNumber, Bool};
use crate::error::{ConsensusError, ConsensusResult};
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, BTreeMap};
use std::convert::TryInto;
use std::fmt;
use threshold_crypto::{SignatureShare, PublicKeySet};

#[cfg(test)]
#[path = "tests/messages_tests.rs"]
pub mod messages_tests;

// daniel: Add view, height, fallback in Block, Vote and QC
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC,
    pub tc: Option<TC>,
    pub coin: Option<RandomCoin>,
    pub author: PublicKey,
    pub view: SeqNumber,   // increment by 1 after every async fallback, initially 0
    pub round: SeqNumber,
    pub height: HeightNumber,   // for async block height={1,2}, for sync block height=0
    pub fallback: Bool,  // 1 if async block; 0 if sync block
    pub payload: Vec<Digest>,
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        qc: QC,
        tc: Option<TC>,
        coin: Option<RandomCoin>,
        author: PublicKey,
        view: SeqNumber,
        round: SeqNumber,
        height: HeightNumber,
        fallback: Bool,
        payload: Vec<Digest>,
        mut signature_service: SignatureService,
    ) -> Self {
        let mut block = Self {
            qc,
            tc,
            coin,
            author,
            view,
            round,
            height,
            fallback,
            payload,
            signature: Signature::default(),
        };
        if fallback == 0 {
            block.height = 0;
        }
        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn parent(&self) -> &Digest {
        &self.qc.hash
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        ensure!(
            (self.fallback == 0 && self.height == 0) || (self.fallback == 1 && (self.height == 1 || self.height == 2)),
            ConsensusError::InvalidHeight(self.height)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        // Check the embedded QC.
        if self.qc != QC::genesis() {
            self.qc.verify(committee)?;
        }

        // Check the TC embedded in the block (if any).
        if let Some(ref tc) = self.tc {
            tc.verify(committee)?;
        }
        Ok(())
    }

    pub fn verify_fallback(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        ensure!(
            (self.fallback == 0 && self.height == 0) || (self.fallback == 1 && (self.height == 1 || self.height == 2)),
            ConsensusError::InvalidHeight(self.height)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        // Check the embedded QC.
        if self.qc != QC::genesis() {
            self.qc.verify(committee)?;
        }

        // Check the TC embedded in the block (if any).
        if let Some(ref tc) = self.tc {
            tc.verify(committee)?;
        }

        // Check the coin embedded in the block (if any).
        if let Some(ref coin) = self.coin {
            coin.verify(committee, pk_set)?;
        }
        Ok(())
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.view.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(&self.fallback.to_le_bytes());
        for x in &self.payload {
            hasher.update(x);
        }
        hasher.update(&self.qc.hash);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B(author {}, view {}, round {}, height {}, qc {:?}, payload_len {}, fallback {})",
            self.digest(),
            self.author,
            self.view,
            self.round,
            self.height,
            self.qc,
            self.payload.iter().map(|x| x.size()).sum::<usize>(),
            self.fallback
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}", self.round)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub hash: Digest,
    pub view: SeqNumber,
    pub round: SeqNumber,
    pub height: HeightNumber,
    pub fallback: Bool,
    pub proposer: PublicKey,    // proposer of the block
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        block: &Block,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            hash: block.digest(),
            view: block.view,
            round: block.round,
            height: block.height,
            fallback: block.fallback,
            proposer: block.author,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        ensure!(
            (self.fallback == 0 && self.height == 0) || (self.fallback == 1 && (self.height == 1 || self.height == 2)),
            ConsensusError::InvalidHeight(self.height)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.view.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.fallback.to_le_bytes());
        hasher.update(self.proposer.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Vote(blockhash {}, proposer {}, view {}, round {}, height {}, fallback {}, voter {})", self.hash, self.proposer, self.view, self.round, self.height, self.fallback, self.author)
    }
}

impl fmt::Display for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Vote{}", self.hash)
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub hash: Digest,
    pub view: SeqNumber,
    pub round: SeqNumber,
    pub height: HeightNumber,
    pub fallback: Bool,
    pub proposer: PublicKey,  // proposer of the block
    pub acceptor: PublicKey,    // Node that accepts the QC and builds its f-chain extending it
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis() -> Self {
        QC::default()
    }

    pub fn timeout(&self) -> bool {
        self.hash == Digest::default() && self.round != 0
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), ConsensusError::AuthorityReuseinQC(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::QCRequiresQuorum
        );

        ensure!(
            (self.fallback == 0 && self.height == 0) || (self.fallback == 1 && (self.height == 1 || self.height == 2)),
            ConsensusError::InvalidHeight(self.height)
        );

        // Check the signatures.
        Signature::verify_batch(&self.digest(), &self.votes).map_err(ConsensusError::from)
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.view.to_le_bytes());
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.height.to_le_bytes());
        hasher.update(self.fallback.to_le_bytes());
        hasher.update(self.proposer.0);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC(hash {}, view {}, round {}, height {}, fallback {}, proposer {})", self.hash, self.view, self.round, self.height, self.fallback, self.proposer)
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.view == other.view && self.round == other.round && self.height == other.height && self.proposer == other.proposer && self.fallback == other.fallback
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SignedQC {
    pub qc: QC,
    pub random_coin: Option<RandomCoin>,    // the signed QC to trigger leader election contains no random_coin, the signed QC to update leader's high QC contains random coin
    pub author: PublicKey,
    pub signature: Signature,
}

impl SignedQC {
    pub async fn new(
        qc: QC,
        random_coin: Option<RandomCoin>,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let signed_qc = Self {
            qc,
            random_coin,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(signed_qc.digest()).await;
        Self {
            signature,
            ..signed_qc
        }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        // Check the embedded QC.
        if self.qc != QC::genesis() {
            self.qc.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for SignedQC {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.qc.digest());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for SignedQC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Signed QC(author {}, qc {:?}, random coin {:?})", self.author, self.qc, self.random_coin)
    }
}

// daniel: 
// Nodes sign Hash(seq, high_qc_round)
// For VABA or async fallback, seq=view
// For HotStuff, seq=round
#[derive(Clone, Serialize, Deserialize)]
pub struct Timeout {
    pub high_qc: QC,
    pub seq: SeqNumber,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Timeout {
    pub async fn new(
        high_qc: QC,
        seq: SeqNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let timeout = Self {
            high_qc,
            seq,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(timeout.digest()).await;
        Self {
            signature,
            ..timeout
        }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        // Check the embedded QC.
        if self.high_qc != QC::genesis() {
            self.high_qc.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.seq.to_le_bytes());
        hasher.update(self.high_qc.round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TV(author {}, view/round {}, highqc {:?})", self.author, self.seq, self.high_qc)
    }
}

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Timeout{}", self.seq)
    }
}

// for VABA or async fallback, seq=view
// for HotStuff, seq=round
#[derive(Clone, Serialize, Deserialize)]
pub struct TC {
    pub seq: SeqNumber,
    pub votes: Vec<(PublicKey, Signature, SeqNumber)>,
}

impl TC {
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _, _) in self.votes.iter() {
            ensure!(!used.contains(name), ConsensusError::AuthorityReuseinTC(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::TCRequiresQuorum
        );

        // Check the signatures.
        for (author, signature, high_qc_round) in &self.votes {
            let mut hasher = Sha512::new();
            hasher.update(self.seq.to_le_bytes());
            hasher.update(high_qc_round.to_le_bytes());
            let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
            signature.verify(&digest, &author)?;
        }
        Ok(())
    }

    pub fn high_qc_rounds(&self) -> Vec<SeqNumber> {
        self.votes.iter().map(|(_, _, r)| r).cloned().collect()
    }
}

impl fmt::Debug for TC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TC(view/round {}, highqc_rounds {:?})", self.seq, self.high_qc_rounds())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RandomnessShare {
    pub seq: SeqNumber, // view
    pub author: PublicKey,
    pub signature_share: SignatureShare,
    pub high_qc: Option<QC>,    // attach its height-2 qc in the randomness share as an optimization
}

impl RandomnessShare {
    pub async fn new(
        seq: SeqNumber,
        author: PublicKey,
        mut signature_service: SignatureService,
        high_qc: Option<QC>,
    ) -> Self {
        let mut hasher = Sha512::new();
        hasher.update(seq.to_le_bytes());
        let digest = Digest(hasher.finalize().as_slice()[..32].try_into().unwrap());
        let signature_share = signature_service.request_tss_signature(digest).await.unwrap();
        Self {
            seq,
            author,
            signature_share,
            high_qc,
        }
    }

    pub fn verify(&self, committee: &Committee, pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );
        let tss_pk = pk_set.public_key_share(committee.id(self.author));
        // Check the signature.
        ensure!(
            tss_pk.verify(&self.signature_share, &self.digest()),
            ConsensusError::InvalidThresholdSignature(self.author)
        );

        Ok(())
    }
}

impl Hash for RandomnessShare {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.seq.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for RandomnessShare {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "RandomnessShare (author {}, view {}, sig share {:?})", self.author, self.seq, self.signature_share)
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct RandomCoin {
    pub seq: SeqNumber, // view
    pub leader: PublicKey,  // elected leader of the view
    pub shares: Vec<RandomnessShare>,
}

impl RandomCoin {
    pub fn verify(&self, committee: &Committee,  pk_set: &PublicKeySet) -> ConsensusResult<()> {
        // Ensure the QC has a quorum.
        let mut weight = 0;
        let mut used = HashSet::new();
        for share in self.shares.iter() {
            let name = share.author;
            ensure!(!used.contains(&name), ConsensusError::AuthorityReuseinCoin(name));
            let voting_rights = committee.stake(&name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(name));
            used.insert(name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.random_coin_threshold(),
            ConsensusError::RandomCoinRequiresQuorum
        );

        let mut sigs = BTreeMap::new();
        // Check the random shares.
        for share in &self.shares {
            share.verify(committee, pk_set)?;
            sigs.insert(committee.id(share.author), share.signature_share.clone());
        }
        if let Ok(sig) = pk_set.combine_signatures(sigs.iter()) {
            let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap()) % committee.size();
            let mut keys: Vec<_> = committee.authorities.keys().cloned().collect();
            keys.sort();
            let leader = keys[id];
            ensure!(leader == self.leader, ConsensusError::RandomCoinWithWrongLeader);
        } else {
            ensure!(true, ConsensusError::RandomCoinWithWrongShares);
        }

        Ok(())
    }
}

impl fmt::Debug for RandomCoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "RandomCoin(view {}, leader {})", self.seq, self.leader)
    }
}