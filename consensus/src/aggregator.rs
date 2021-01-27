use crate::config::{Committee, Stake};
use crate::core::RoundNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Vote;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, Signature};
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
        // TODO: ANYONE (not only nodes) may make us run out of memory by sending
        // many votes with different round numbers (as long as they are bigger than
        // our current round) or with different digests.

        // Nodes may send the same timeout vote multiple times to implement the
        // reliable point-to-point abstraction.
        if vote.timeout()
            && !self
                .voters
                .entry(vote.round)
                .or_insert_with(HashSet::new)
                .insert(vote.author)
        {
            return Ok(None);
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
    votes: Votes,
    used: HashSet<PublicKey>,
}

impl QuorumMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, vote: Vote, committee: &Committee) -> ConsensusResult<Option<Votes>> {
        let author = vote.author;

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&author);
        ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(author));

        // Ensure it is the first time this authority votes.
        ensure!(
            self.used.insert(author),
            ConsensusError::AuthorityReuse(author)
        );

        // Check the signature on the vote.
        vote.signature.verify(&vote.digest(), &author)?;

        self.votes.push((author, vote.signature));
        self.weight += voting_rights;
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0; // Ensures QC/TC is only made once.
            Ok(Some(self.votes.clone()))
        } else {
            Ok(None)
        }
    }
}
