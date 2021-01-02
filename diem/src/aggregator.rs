use crate::committee::{Committee, Stake};
use crate::core::RoundNumber;
use crate::crypto::{Digest, Hash, PublicKey};
use crate::error::{DiemError, DiemResult};
use crate::messages::{Vote, QC, TC, TV};
use std::collections::{HashMap, HashSet};

pub struct Aggregator {
    committee: Committee,
    qc_aggregators: HashMap<Digest, Box<QCMaker>>,
    tc_aggregators: HashMap<Digest, Box<TCMaker>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            qc_aggregators: HashMap::new(),
            tc_aggregators: HashMap::new(),
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> DiemResult<Option<QC>> {
        // TODO: self.aggregators is a potential target for DDoS.
        // TODO: How do we cleanup self.aggregators.
        let aggregator = self
            .qc_aggregators
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QCMaker::new(vote.hash, vote.round)));

        // Add the new vote to our aggregator and see if we have a QC.
        aggregator.append(vote, &self.committee)
    }

    pub fn add_timeout(&mut self, timeout: TV, qc: QC) -> DiemResult<Option<TC>> {
        // TODO: self.aggregators is a potential target for DDoS.
        // TODO: How do we cleanup self.aggregators.
        let aggregator = self
            .tc_aggregators
            .entry(timeout.digest())
            .or_insert_with(|| Box::new(TCMaker::new(qc, timeout.round)));

        // Add the new timeout vote to our aggregator and see if we have a TC.
        aggregator.append(timeout, &self.committee)
    }
}

struct QCMaker {
    weight: Stake,
    used: HashSet<PublicKey>,
    pub partial: QC,
}

impl QCMaker {
    pub fn new(hash: Digest, round: RoundNumber) -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            partial: QC {
                hash,
                round,
                votes: Vec::new(),
            },
        }
    }

    /// Try to append a signature to a (partial) QC.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> DiemResult<Option<QC>> {
        let author = vote.author;

        // Check that each authority only appears once.
        ensure!(
            !self.used.contains(&author),
            DiemError::AuthorityReuse(author)
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&author);
        ensure!(voting_rights > 0, DiemError::UnknownAuthority(author));

        // Check the signature on the vote.
        vote.signature.verify(&vote.digest(), &author)?;

        self.partial.votes.push((author, vote.signature));
        self.used.insert(author);
        self.weight += voting_rights;
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC is only made once.
            Ok(Some(self.partial.clone()))
        } else {
            Ok(None)
        }
    }
}

// TODO: Can we avoid code repetition?
struct TCMaker {
    weight: Stake,
    used: HashSet<PublicKey>,
    pub partial: TC,
}

impl TCMaker {
    pub fn new(qc: QC, round: RoundNumber) -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            partial: TC {
                qc,
                round,
                votes: Vec::new(),
            },
        }
    }

    /// Try to append a signature to a (partial) TC.
    pub fn append(&mut self, vote: TV, committee: &Committee) -> DiemResult<Option<TC>> {
        let author = vote.author;

        // Check that each authority only appears once.
        ensure!(
            !self.used.contains(&author),
            DiemError::AuthorityReuse(author)
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&author);
        ensure!(voting_rights > 0, DiemError::UnknownAuthority(author));

        // Check the signature on the vote.
        vote.signature.verify(&vote.digest(), &author)?;

        self.partial.votes.push((author, vote.signature));
        self.used.insert(author);
        self.weight += voting_rights;
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures TC is only made once.
            Ok(Some(self.partial.clone()))
        } else {
            Ok(None)
        }
    }
}
