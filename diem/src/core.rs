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
    highest_qc: (QC, RoundNumber),
    committee: Committee,
    store: Store,
    leader_election: L,
    aggregator: Option<SignatureAggregator>,
    sender: Sender<NetMessage>,
    receiver: Receiver<CoreMessage>,
    commit_channel: Sender<Block>,
    signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    mempool: Mempool,
    synchronizer: Synchronizer,
    pending: HashMap<Digest, Block>,
}

impl<L: LeaderElection> Core<L> {
    pub fn new(
        name: PublicKey,
        store: Store,
        committee: Committee,
        leader_election: L,
        sender: Sender<NetMessage>,
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
            highest_qc: (QC::genesis(), 0),
            committee,
            store,
            leader_election,
            aggregator: None,
            sender,
            receiver,
            commit_channel,
            signature_channel,
            mempool,
            synchronizer,
            pending: HashMap::new(),
        }
    }

    async fn store_block(&mut self, block: &Block) -> DiemResult<()> {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize valid block");
        self.store.write(key, value).await.map_err(DiemError::from)
    }

    async fn get_block(&mut self, hash: Digest) -> DiemResult<Option<Block>> {
        match self.store.read(hash.to_vec()).await? {
            Some(bytes) => {
                bincode::deserialize(&bytes).map_err(|e| DiemError::StoreError(e.to_string()))
            }
            None => {
                debug!("Requesting sync for block {:?}", hash);
                // TODO: Do not send sync request if we already have the block in pending.
                self.synchronizer.request(hash).await;
                Ok(None)
            }
        }
    }

    async fn get_previous_block(&mut self, block: &Block) -> DiemResult<Option<Block>> {
        if block.qc == QC::genesis() {
            return Ok(Some(Block::genesis()));
        }
        self.get_block(block.previous()).await
    }

    async fn process_qc(&mut self, qc: &QC, ancestors: &(Block, Block, Block)) {
        let (b0, b1, b2) = ancestors;

        // Try to commit b0.
        if b0.round + 1 == b1.round && b1.round + 1 == b2.round {
            info!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        // Update the hightest QC and round number of the node.
        let (_, highest_qc_round) = self.highest_qc;
        if b2.round > highest_qc_round {
            self.highest_qc = (qc.clone(), b2.round);
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
            if let Err(e) = self.sender.send(NetMessage::Vote(vote, next_leader)).await {
                panic!("Core failed to send vote to the network: {}", e);
            }

            // Update state preventing to vote for conflicting block.
            self.preferred_round = max(self.preferred_round, b1.round);
            self.last_voted_round = block.round;
        }
        Ok(())
    }

    async fn handle_block(&mut self, block: Block) -> DiemResult<()> {
        block.check(&self.committee)?;

        // Get the ancestors of the incoming block:
        // B0 <- |QC0; B1| <- |QC1; B2| <- |QC2; Block|
        let b2 = match self.get_previous_block(&block).await? {
            Some(b) => b,
            None => {
                self.pending.insert(block.previous(), block);
                return Ok(());
            }
        };
        let b1 = self
            .get_previous_block(&b2)
            .await?
            .expect("We should have all ancestors of delivered blocks");
        let b0 = self
            .get_previous_block(&b1)
            .await?
            .expect("We should have all ancestors of delivered blocks");

        // We have all ancestors, deliver the block.
        self.store_block(&block).await?;
        let ancestors = (b0, b1, b2);
        self.process_qc(&block.qc, &ancestors).await;
        self.try_vote(&block, &ancestors).await?;
        if let Some(_retry) = self.pending.remove(&block.digest()) {
            // TODO: send back into the channel and ideally avoid to 
            // re-check the block.
        }
        Ok(())
    }

    async fn make_block(&self) {
        let (highest_qc, _) = &self.highest_qc;
        let block = Block::new(
            highest_qc.clone(),
            self.name,
            self.round + 1,
            self.mempool.get_payload().await,
            self.signature_channel.clone(),
        )
        .await;
        let vote = Vote::new(&block, self.name, self.signature_channel.clone()).await;
        let message = NetMessage::Block(block, vote);
        if let Err(e) = self.sender.send(message).await {
            panic!("Core failed to send block to the network: {}", e);
        }
    }

    async fn handle_vote(&mut self, vote: Vote) -> DiemResult<()> {
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
