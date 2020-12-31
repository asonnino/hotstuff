use crate::committee::Committee;
use crate::crypto::Hash as _;
use crate::crypto::{Digest, PublicKey, SignatureService};
use crate::error::{DiemError, DiemResult};
use crate::leader::LeaderElector;
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

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Propose(Block),
    Vote(Vote),
    LoopBack(Block),
}

pub struct Core<L: LeaderElector> {
    name: PublicKey,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: QC,
    committee: Committee,
    store: Store,
    leader_elector: L,
    aggregators: HashMap<Digest, Box<SignatureAggregator>>,
    network_channel: Sender<NetMessage>,
    receiver: Receiver<CoreMessage>,
    commit_channel: Sender<Block>,
    signature_service: SignatureService,
    mempool: Mempool,
    synchronizer: Synchronizer,
}

impl<L: LeaderElector> Core<L> {
    pub fn new(
        name: PublicKey,
        store: Store,
        committee: Committee,
        leader_elector: L,
        network_channel: Sender<NetMessage>,
        receiver: Receiver<CoreMessage>,
        commit_channel: Sender<Block>,
        signature_service: SignatureService,
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
            leader_elector,
            aggregators: HashMap::new(),
            network_channel,
            receiver,
            commit_channel,
            signature_service,
            mempool,
            synchronizer,
        }
    }

    async fn make_block(&mut self, qc: QC) -> DiemResult<()> {
        let block = Block::new(
            qc,
            self.name,
            self.round + 1,
            self.mempool.get_payload().await,
            self.signature_service.clone(),
        )
        .await;
        self.process_block(&block).await?;
        let message = NetMessage::Block(block);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Core failed to send block to the network: {}", e);
        }
        Ok(())
    }

    async fn store_block(&mut self, block: &Block) -> DiemResult<()> {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await.map_err(DiemError::from)
    }

    async fn handle_propose(&mut self, block: &Block) -> DiemResult<()> {
        // First, reject old blocks.
        if block.round <= self.round {
            return Ok(());
        }

        // Check the block's round number is as expected. This prevents bad leaders
        // from proposing blocks with very high round numbers which may cause overflows.
        // TODO: Check tc.round + 1 == block.round for TC.
        ensure!(
            block.round == block.qc.round + 1,
            DiemError::MalformedBlock(block.digest())
        );

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            DiemError::WrongLeader {
                digest: block.digest(),
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly signed.
        block.signature.verify(&block.digest(), &block.author)?;

        // Check that the QC embedded in the block is valid.
        block.qc.check(&self.committee)?;

        // If all check pass, process the block.
        self.process_block(&block).await
    }

    async fn process_block(&mut self, block: &Block) -> DiemResult<()> {
        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process all three ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1, b2) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => return Ok(()),
        };

        // If we have all ancestors we 'deliver' the block by adding it to store.
        // Delivering a block means we already processed all its ancestors.
        self.store_block(block).await?;

        // Enter the new round.
        // TODO: Adapt for: self.round = tc.round + 1;
        self.round = block.qc.round + 1;
        info!("Moved to round {}", self.round);

        // Update the highest QC we know.
        if block.qc.round > self.highest_qc.round {
            self.highest_qc = block.qc.clone();
        }

        // Check if the last three ancestors of the block form a 3-chain.
        // If so, we commit b0.
        if b0.round + 1 == b1.round && b1.round + 1 == b2.round {
            info!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        // Check the safety rules to see if we can vote for this new block. If we can,
        // we send our vote to the next leader.
        let safety_rule_1 = b2.round >= self.preferred_round;
        let safety_rule_2 = block.round > self.last_voted_round;
        if safety_rule_1 && safety_rule_2 {
            debug!("Voting for block {:?}", block);

            let vote = Vote::new(&block, self.name, self.signature_service.clone()).await;
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if let Err(e) = self
                .network_channel
                .send(NetMessage::Vote(vote, next_leader))
                .await
            {
                panic!("Core failed to send vote to the network: {}", e);
            }

            // Finally, update our state to ensure we won't vote for conflicting blocks.
            self.preferred_round = max(self.preferred_round, b1.round);
            self.last_voted_round = block.round;
        }

        Ok(())
    }

    async fn handle_vote(&mut self, vote: Vote) -> DiemResult<()> {
        if vote.round < self.round {
            return Ok(());
        }

        // TODO: self.aggregators is a potential target for DDoS.
        // TODO: How do we cleanup self.aggregators.
        let aggregator = self
            .aggregators
            .entry(vote.digest())
            .or_insert_with(|| Box::new(SignatureAggregator::new(vote.hash, vote.round)));

        // Add the new vote to our aggregator and see if we have a QC.
        if let Some(qc) = aggregator.append(vote, &self.committee)? {
            // If we have a QC, we may use it to propose a new block.
            if self.name == self.leader_elector.get_leader(qc.round + 1) {
                self.make_block(qc).await?;
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, send the very first block (if we are the leader).
        if self.name == self.leader_elector.get_leader(1) {
            self.make_block(self.highest_qc.clone())
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes.
        while let Some(message) = self.receiver.recv().await {
            debug!("Received message: {:?}", message);
            let result = match message {
                CoreMessage::Propose(block) => self.handle_propose(&block).await,
                CoreMessage::Vote(vote) => self.handle_vote(vote).await,
                CoreMessage::LoopBack(block) => self.process_block(&block).await,
            };
            match result {
                Ok(()) => debug!("Message successfully processed."),
                Err(DiemError::StoreError(e)) => error!("{}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
