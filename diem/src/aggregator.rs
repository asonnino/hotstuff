use crate::config::{Committee, Stake};
use crate::crypto::{Digest, Hash, PublicKey, Signature};
use crate::error::{DiemError, DiemResult};
use crate::messages::Vote;
use std::collections::{HashMap, HashSet};

type Votes = Vec<(PublicKey, Signature)>;

pub struct Aggregator {
    committee: Committee,
    aggregators: HashMap<Digest, Box<QuorumMaker>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            aggregators: HashMap::new(),
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> DiemResult<Option<Votes>> {
        // TODO: self.aggregators is a potential target for DDoS.
        // TODO: How do we cleanup self.aggregators.
        let aggregator = self
            .aggregators
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QuorumMaker::new()));

        // Add the new vote to our aggregator and see if we have a QC.
        aggregator.append(vote, &self.committee)
    }
}

struct QuorumMaker {
    weight: Stake,
    used: HashSet<PublicKey>,
    votes: Votes,
}

impl QuorumMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            votes: Vec::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> DiemResult<Option<Votes>> {
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

        self.votes.push((author, vote.signature));
        self.used.insert(author);
        self.weight += voting_rights;
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC/TC is only made once.
            Ok(Some(self.votes.clone()))
        } else {
            Ok(None)
        }
    }
}
