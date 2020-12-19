use crate::committee::Committee;
use crate::crypto::PublicKey;
use crate::error::{DiemError, DiemResult};
use crate::leader::LeaderElection;
use crate::messages::{Block, Vote, QC};
use crate::store::Store;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use tokio::sync::mpsc::{Receiver, Sender};

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Propose(Block, Vote),
    Vote(Vote),
}

pub struct Core<L: LeaderElection> {
    name: PublicKey,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: (QC, RoundNumber),
    committee: Committee,
    store: Store,
    leader_election: L,
    sender: Sender<CoreMessage>,
    receiver: Receiver<CoreMessage>,
    commit_channel: Sender<Block>,
}

impl<L> Core<L>
where
    L: LeaderElection,
{
    pub fn new(
        name: PublicKey,
        store: Store,
        committee: Committee,
        leader_election: L,
        sender: Sender<CoreMessage>,
        receiver: Receiver<CoreMessage>,
        commit_channel: Sender<Block>,
    ) -> Self {
        // TODO: add genesis to the store?
        Self {
            name,
            round: 3,
            last_voted_round: 2,
            preferred_round: 1,
            highest_qc: (QC::genesis().pop().unwrap(), 1),
            committee,
            store,
            leader_election,
            sender,
            receiver,
            commit_channel,
        }
    }

    async fn get_previous_block(&mut self, block: &Block) -> DiemResult<Block> {
        // TODO
        let bytes = self.store.read(block.qc.hash.to_vec()).await?.unwrap();
        let previous_block = bincode::deserialize(&bytes)?;
        Ok(previous_block)
    }

    async fn handle_propose(&mut self, block: Block, vote: Vote) -> DiemResult<()> {
        // Ignore old messages.
        if block.round < self.round {
            return Ok(());
        }

        // Ensure we are the leader for this round.
        ensure!(
            self.name == self.leader_election.get_leader(block.round),
            DiemError::UnexpectedMessage(CoreMessage::Propose(block, vote))
        );

        // Check the block is well-formed.
        block.check(&self.committee)?;

        // Vote for this block if we can
        let b2 = self.get_previous_block(&block).await?;
        let mut can_vote = b2.round >= self.preferred_round;
        can_vote &= block.round > self.last_voted_round;
        if !can_vote {
            debug!("Cannot vote on {:?}", block);
            return Ok(());
        }

        info!("Voting for block {:?}", block);
        let vote = Vote::new(&block, self.name)?;
        self.sender.send(CoreMessage::Vote(vote)).await?;
        let b1 = self.get_previous_block(&b2).await?;
        self.preferred_round = max(self.preferred_round, b1.round);
        self.last_voted_round = block.round;
        self.round = max(self.round, b2.round + 1);
        let (_, hightest_qc_round) = self.highest_qc;
        if b2.round > hightest_qc_round {
            self.highest_qc = (block.qc, b2.round);
        }

        // Try to commit ancestors.
        let b0 = self.get_previous_block(&b1).await?;
        let mut commit = b0.round + 1 == b1.round;
        commit &= b1.round + 1 == b2.round;
        commit &= b2.round + 1 == block.round;
        if commit {
            self.commit_channel.send(b0).await?
        }
        Ok(())
    }

    async fn handle_vote(&self, _vote: Vote) -> DiemResult<()> {
        // TODO
        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            info!("Received message: {:?}", message);
            let result = match message {
                CoreMessage::Propose(block, vote) => self.handle_propose(block, vote).await,
                CoreMessage::Vote(vote) => self.handle_vote(vote).await,
            };
            match result {
                Ok(()) => debug!("Message successfully processed."),
                Err(e) => error!("{}", e),
            }
        }
    }
}
