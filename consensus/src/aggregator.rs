use crate::core::RoundNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Vote;
use config::{Committee, Stake};
use crypto::{Digest, Hash, PublicKey, Signature};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
#[path = "tests/aggregator_tests.rs"]
pub mod aggregator_tests;

type Votes = Vec<(PublicKey, Signature)>;

pub struct Aggregator {
    committee: Committee,
    aggregators: HashMap<RoundNumber, HashMap<Digest, Box<QuorumMaker>>>,
    voters: HashMap<RoundNumber, HashSet<PublicKey>>,
}

impl Aggregator {
    pub fn new(committee: Committee) -> Self {
        Self {
            committee,
            aggregators: HashMap::new(),
            voters: HashMap::new(),
        }
    }

    pub fn add_vote(&mut self, vote: Vote) -> ConsensusResult<Option<Votes>> {
        // Record that an authority voted for this round; we accept at most
        // one vote per authority per round.
        if !self
            .voters
            .entry(vote.round)
            .or_insert_with(HashSet::new)
            .insert(vote.author)
        {
            bail!(ConsensusError::AuthorityReuse(vote.author));
        }

        // Add the new vote to our aggregator and see if we have a QC.
        self.aggregators
            .entry(vote.round)
            .or_insert_with(HashMap::new)
            .entry(vote.digest())
            .or_insert_with(|| Box::new(QuorumMaker::new()))
            .append(vote, &self.committee)
    }

    pub fn cleanup(&mut self, round: &RoundNumber) {
        self.aggregators.retain(|k, _| k >= round);
        self.voters.retain(|k, _| k >= round);
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
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<Votes>> {
        let author = vote.author;

        // Check that each authority only appears once.
        ensure!(
            !self.used.contains(&author),
            ConsensusError::AuthorityReuse(author)
        );

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&author);
        ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(author));

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
