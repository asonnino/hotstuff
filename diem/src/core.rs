use crate::committee::Committee;
use crate::crypto::Digestible;
use crate::crypto::{Digest, PublicKey, Signature};
use crate::error::{DiemError, DiemResult};
use crate::leader::LeaderElection;
use crate::mempool::Mempool;
use crate::messages::{Block, SignatureAggregator, Vote, QC};
use crate::network::NetMessage;
use crate::store::Store;
use crate::synchronizer::Synchronizer;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Block(Block),
    Vote(Vote),
}

struct HighestQC {
    qc: QC,
    round: RoundNumber,
}

pub struct Core<L: LeaderElection> {
    name: PublicKey,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: HighestQC,
    committee: Committee,
    store: Store,
    leader_election: L,
    aggregator: Option<SignatureAggregator>,
    network_channel: Sender<NetMessage>,
    receiver: Receiver<CoreMessage>,
    commit_channel: Sender<Block>,
    signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    mempool: Mempool,
    synchronizer: Synchronizer,
}

impl<L: LeaderElection> Core<L> {
    pub fn new(
        name: PublicKey,
        store: Store,
        committee: Committee,
        leader_election: L,
        network_channel: Sender<NetMessage>,
        receiver: Receiver<CoreMessage>,
        commit_channel: Sender<Block>,
        signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
        mempool: Mempool,
        synchronizer: Synchronizer,
    ) -> Self {
        Self {
            name,
            round: 0,
            last_voted_round: 0,
            preferred_round: 0,
            highest_qc: HighestQC {
                qc: QC::genesis(),
                round: 0,
            },
            committee,
            store,
            leader_election,
            aggregator: None,
            network_channel,
            receiver,
            commit_channel,
            signature_channel,
            mempool,
            synchronizer,
        }
    }

    async fn make_block(&self) {
        let block = Block::new(
            self.highest_qc.qc.clone(),
            self.name,
            self.round + 1,
            self.mempool.get_payload().await,
            self.signature_channel.clone(),
        )
        .await;
        let message = NetMessage::Block(block);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Core failed to send block to the network: {}", e);
        }
    }

    async fn store_block(&mut self, block: &Block) -> DiemResult<()> {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize valid block");
        self.store.write(key, value).await.map_err(DiemError::from)
    }

    async fn process_qc(&mut self, qc: &QC, ancestors: &(Block, Block, Block)) {
        // Get the ancestors of the incoming block:
        // b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        let (b0, b1, b2) = ancestors;

        // Try to commit b0.
        if b0.round + 1 == b1.round && b1.round + 1 == b2.round {
            info!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        // Update the hightest QC and round number.
        if b2.round > self.highest_qc.round {
            self.highest_qc = HighestQC {
                qc: qc.clone(),
                round: b2.round,
            };
        }
        if b2.round >= self.round {
            self.round = b2.round + 1;
            info!("Moved to round {}", self.round);
        }
    }

    async fn try_vote(
        &mut self,
        block: &Block,
        ancestors: &(Block, Block, Block),
    ) -> DiemResult<()> {
        // Get the ancestors of the incoming block:
        // b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        let (_, b1, b2) = ancestors;

        // Prevents bad leaders from proposing blocks with very high round numbers
        // which may cause overflows.
        ensure!(
            block.round < self.round + 100,
            DiemError::UnexpectedMessage(Box::new(CoreMessage::Block(block.clone())))
        );

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_election.get_leader(block.round),
            DiemError::UnexpectedMessage(Box::new(CoreMessage::Block(block.clone())))
        );

        // Check the safety rules.
        let safety_rule_1 = b2.round >= self.preferred_round;
        let safety_rule_2 = block.round > self.last_voted_round;
        if safety_rule_1 && safety_rule_2 {
            debug!("Voting for block {:?}", block);
            let vote = Vote::new(&block, self.name, self.signature_channel.clone()).await;
            let next_leader = self.leader_election.get_leader(self.round + 1);
            if let Err(e) = self
                .network_channel
                .send(NetMessage::Vote(vote, next_leader))
                .await
            {
                panic!("Core failed to send vote to the network: {}", e);
            }

            // Update state preventing to vote for conflicting block.
            self.preferred_round = max(self.preferred_round, b1.round);
            self.last_voted_round = block.round;
        }
        Ok(())
    }

    async fn handle_block(&mut self, block: Block) -> DiemResult<()> {
        // TODO: Unify all checks here (also those in try_vote).
        block.check(&self.committee)?;
        if let Some(ancestors) = self.synchronizer.get_ancestors(&block).await? {
            self.store_block(&block).await?;
            self.process_qc(&block.qc, &ancestors).await;
            self.try_vote(&block, &ancestors).await?;
        }
        Ok(())
    }

    async fn handle_vote(&mut self, vote: Vote) -> DiemResult<()> {
        
        /*
        if self.aggregator.is_none() {
            if let Some(block) = self.get_block(vote.hash).await? {
                ensure!(
                    self.name == self.leader_election.get_leader(block.round),
                    DiemError::UnexpectedMessage(Box::new(CoreMessage::Vote(vote)))
                );
                self.aggregator = Some(SignatureAggregator::new(vote.hash));
            }
        }

        if let Some(_qc) = self
            .aggregator
            .as_mut()
            .unwrap()
            .append(vote, &self.committee)?
        {
            // TODO: Update highest QC with _qc.
            self.make_block().await;
        }
        */
        Ok(())
    }

    pub async fn run(&mut self) {
        // Send the very first block.
        if self.name == self.leader_election.get_leader(1) {
            self.make_block().await;
        }

        // Main loop.
        while let Some(message) = self.receiver.recv().await {
            debug!("Received message: {:?}", message);
            let result = match message {
                CoreMessage::Block(block) => self.handle_block(block).await,
                CoreMessage::Vote(vote) => self.handle_vote(vote).await,
            };
            match result {
                Ok(()) => debug!("Message successfully processed."),
                Err(DiemError::StoreError(e)) => error!("{}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
