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
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Block(Block),
    Vote(Vote),
}

pub struct Core<L: LeaderElection> {
    name: PublicKey,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: QC,
    committee: Committee,
    store: Store,
    leader_election: L,
    aggregators: HashMap<Digest, SignatureAggregator>,
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
            highest_qc: QC::genesis(),
            committee,
            store,
            leader_election,
            aggregators: HashMap::new(),
            network_channel,
            receiver,
            commit_channel,
            signature_channel,
            mempool,
            synchronizer,
        }
    }

    async fn make_block(&self, qc: QC) {
        let block = Block::new(
            qc,
            self.name,
            self.round,
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

    async fn process_qc(&mut self, qc: &QC) {
        if qc.round > self.highest_qc.round {
            self.highest_qc = qc.clone();
        }
        if qc.round >= self.round {
            self.round = qc.round + 1;
            info!("Moved to round {}", self.round);
        }
    }

    async fn try_commit(&mut self, ancestors: &(Block, Block, Block)) {
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
            block.round == self.round,
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
        self.process_qc(&block.qc).await;
        if let Some(ancestors) = self.synchronizer.get_ancestors(&block).await? {
            self.store_block(&block).await?;
            self.try_commit(&ancestors).await;
            self.try_vote(&block, &ancestors).await?;
        }
        Ok(())
    }

    async fn handle_vote(&mut self, vote: Vote) -> DiemResult<()> {
        // TODO: self.aggregators is a potential target for DDoS.
        // TODO: How do we cleanup self.aggregators.
        let aggregator = self
            .aggregators
            .entry(vote.digest())
            .or_insert_with(|| SignatureAggregator::new(vote.hash, vote.round));
        if let Some(qc) = aggregator.append(vote, &self.committee)? {
            let next_round = qc.round + 1;
            let mut propose = self.name == self.leader_election.get_leader(next_round);
            propose &= next_round > self.round;
            if propose {
                self.process_qc(&qc).await;
                self.make_block(qc).await;
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        // Send the very first block.
        if self.name == self.leader_election.get_leader(1) {
            self.round = 1;
            self.make_block(self.highest_qc.clone()).await;
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
