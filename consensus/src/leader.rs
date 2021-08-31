use crate::config::Committee;
use crate::core::RoundNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Block, Vote, QC};
use crypto::Hash as _;
use crypto::PublicKey;
use std::collections::HashSet;
use store::Store;

pub type LeaderElector = ReputationLeaderElector;

pub struct ReputationLeaderElector {
    committee: Committee,
    store: Store,
    window_size: usize,
    last_authors_size: usize,
}

impl ReputationLeaderElector {
    pub fn new(committee: Committee, store: Store) -> Self {
        let last_authors_size = committee.validity_threshold() as usize;
        Self {
            committee,
            store,
            window_size: 1,
            last_authors_size,
        }
    }

    fn round_robin(&self, round: RoundNumber) -> PublicKey {
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }

    pub fn next_leader(&self, qc: &QC, round: RoundNumber) -> PublicKey {
        // Use the leader embedded in the QC if there is one and if the block
        // and QC rounds are consecutive.
        if qc.round + 1 == round {
            if let Some(leader) = qc.next_leader {
                return leader;
            }
        }

        // Otherwise, fall back to round robin.
        self.round_robin(round)
    }

    pub async fn elect_future_leader(
        &mut self,
        qc: &QC,
        round: RoundNumber,
    ) -> ConsensusResult<Option<PublicKey>> {
        if qc.round + 1 != round {
            return Ok(None);
        }

        let mut active_validators = HashSet::new();
        let mut last_authors = HashSet::new();
        let mut current_qc = qc.clone();
        let mut i = 0;
        while i < self.window_size || last_authors.len() < self.last_authors_size {
            if current_qc == QC::genesis() {
                break;
            }
            let bytes = self
                .store
                .read(current_qc.id.to_vec())
                .await?
                .expect("We should have all ancestors by now");
            let block: Block = bincode::deserialize(&bytes).expect("Failed to deserialize block");
            if i < self.window_size {
                active_validators.extend(current_qc.voters());
            }
            if last_authors.len() < self.last_authors_size {
                last_authors.insert(block.author);
            }
            current_qc = block.qc;
            i += 1;
        }

        let mut candidates: Vec<_> = active_validators.difference(&last_authors).collect();
        if candidates.is_empty() {
            return Ok(None);
        }
        candidates.sort();
        Ok(Some(
            *candidates.remove(current_qc.round as usize % candidates.len()),
        ))
    }

    pub fn check_block(&self, block: &Block, parent: &Block) -> ConsensusResult<()> {
        let next_leader = match parent.round + 1 == block.round {
            true => self.next_leader(&parent.qc, parent.round),
            false => self.round_robin(block.round),
        };
        ensure!(
            block.author == next_leader,
            ConsensusError::WrongLeader {
                digest: block.digest(),
                leader: block.author,
                round: block.round
            }
        );
        Ok(())
    }

    pub fn check_vote(&self, vote: &Vote, name: PublicKey) -> ConsensusResult<()> {
        ensure!(
            name == self.next_leader(&vote.parent_qc, vote.round),
            ConsensusError::UnexpectedVote {
                digest: vote.digest(),
                round: vote.round
            }
        );
        Ok(())
    }
}
