use crate::committee::Committee;
use crate::error::DiemError;
use crate::leader::LeaderElection;
use crate::messages::{Block, Vote, QC};
use crate::store::Store;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

pub type RoundNumber = u64;
type CoreResult = Result<(), DiemError>;

#[derive(Serialize, Deserialize, Debug)]
enum CoreMessage {
    Propose(Block, Vote),
    Vote(Vote),
}

struct Core<L: LeaderElection> {
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: QC,
    committee: Committee,
    store: Store,
    leader_election: L,
    sender: Sender<CoreMessage>,
    receiver: Receiver<CoreMessage>,
}

impl<L> Core<L>
where
    L: LeaderElection,
{
    pub fn new(
        store: Store,
        committee: Committee,
        leader_election: L,
        sender: Sender<CoreMessage>,
        receiver: Receiver<CoreMessage>,
    ) -> Self {
        // TODO: add genesis to the store?
        Self {
            round: 3,
            last_voted_round: 2,
            preferred_round: 1,
            highest_qc: QC::genesis().pop().unwrap(),
            committee,
            store,
            leader_election,
            sender,
            receiver,
        }
    }

    fn handle_propose(&self, block: Block, vote: Vote) -> CoreResult {
        Ok(())
    }

    fn handle_vote(&self, vote: Vote) -> CoreResult {
        Ok(())
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.receiver.recv().await {
            debug!("Received message: {:?}", message);
            let result = match message {
                CoreMessage::Propose(block, vote) => self.handle_propose(block, vote),
                CoreMessage::Vote(vote) => self.handle_vote(vote),
            };
            match result {
                Ok(()) => debug!("Message successfully processed."),
                Err(e) => error!("{}", e),
            }
        }
    }
}
