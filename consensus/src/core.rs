use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Vote, QC};
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use mempool::NodeMempool;
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Propose(Block),
    Vote(Vote),
    LoopBack(Block),
    SyncRequest(Digest, PublicKey),
}

pub struct Core<Mempool> {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver<Mempool>,
    synchronizer: Synchronizer,
    core_channel: Receiver<CoreMessage>,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    high_qc: QC,
    timer: Timer<RoundNumber>,
    aggregator: Aggregator,
}

impl<Mempool: 'static + NodeMempool> Core<Mempool> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver<Mempool>,
        synchronizer: Synchronizer,
        core_channel: Receiver<CoreMessage>,
        network_channel: Sender<NetMessage>,
        commit_channel: Sender<Block>,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        Self {
            name,
            committee,
            parameters,
            signature_service,
            store,
            leader_elector,
            mempool_driver,
            synchronizer,
            network_channel,
            commit_channel,
            core_channel,
            round: 1,
            last_voted_round: 0,
            preferred_round: 0,
            high_qc: QC::genesis(),
            timer: Timer::new(),
            aggregator,
        }
    }

    async fn store_block(&mut self, block: &Block) -> ConsensusResult<()> {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store
            .write(key, value)
            .await
            .map_err(ConsensusError::from)
    }

    async fn schedule_timer(&mut self) {
        self.timer
            .schedule(self.parameters.timeout_delay, self.round)
            .await;
    }

    async fn transmit(
        &mut self,
        message: &CoreMessage,
        to: Option<PublicKey>,
    ) -> ConsensusResult<()> {
        let addresses = if let Some(to) = to {
            debug!("Sending {:?} to {}", message, to);
            vec![self.committee.address(&to)?]
        } else {
            debug!("Broadcasting {:?}", message);
            self.committee.broadcast_addresses(&self.name)
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    // -- Start Safety Module --
    fn increase_last_voted_round(&mut self, target: RoundNumber) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    fn update_preferred_round(&mut self, target: RoundNumber) {
        self.preferred_round = max(self.preferred_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let safety_rule_2 = block.qc.round >= self.preferred_round;
        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);
        // TODO: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(&block, self.name, self.signature_service.clone()).await)
    }

    fn commit_rule(&self, b0: &Block, b1: &Block, b2: &Block, block: &Block) -> bool {
        b0.round + 1 == b1.round && b1.round + 1 == b2.round && b2.round + 1 == block.round
    }
    // -- End Safety Module --

    // -- Start Pacemaker --
    fn update_high_qc(&mut self, qc: &QC) {
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for round {}", self.round);
        self.increase_last_voted_round(self.round);
        let vote = Vote::new_timeout(self.round, self.name, self.signature_service.clone()).await;
        debug!("Created {:?}", vote);
        self.schedule_timer().await;
        let message = CoreMessage::Vote(vote.clone());
        self.transmit(&message, None).await?;
        self.handle_vote(&vote).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(quorum) = self.aggregator.add_vote(vote.clone())? {
            let qc = QC {
                hash: vote.hash.clone(),
                round: vote.round,
                votes: quorum,
            };
            debug!("Assembled {:?}", qc);
            if !qc.timeout() {
                self.update_high_qc(&qc);
            }
            self.advance_round(&qc).await?
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, qc: &QC) -> ConsensusResult<()> {
        if qc.round < self.round {
            return Ok(());
        }
        self.timer.cancel(self.round).await;
        self.round = qc.round + 1;
        info!("Moved to round {}", self.round);

        // Cleanup the vote aggregator and the mempool driver.
        self.aggregator.cleanup(&self.round);
        self.mempool_driver.cleanup(&self.round).await;

        // Schedule a new timer for this round.
        self.schedule_timer().await;

        // Make a new block if we are the next leader.
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal().await?;
        }
        Ok(())
    }
    // -- End Pacemaker --

    #[async_recursion]
    async fn generate_proposal(&mut self) -> ConsensusResult<()> {
        // Make a new block.
        let block = Block::new(
            self.high_qc.clone(),
            self.name,
            self.round,
            /* payload */ self.mempool_driver.get().await,
            self.signature_service.clone(),
        )
        .await;
        if !block.payload.is_empty() {
            info!("Created non-empty {}", block);
        }
        debug!("Created {:?}", block);

        // Process our new block and broadcast it.
        let message = CoreMessage::Propose(block.clone());
        self.transmit(&message, None).await?;
        self.handle_proposal(&block).await
    }

    #[async_recursion]
    async fn handle_qc(&mut self, block: &Block) -> ConsensusResult<bool> {
        // Check that the QC embedded in the block is valid.
        if block.qc != QC::genesis() {
            block.qc.verify(&self.committee)?;
        }

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process all three ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1, b2) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => return Ok(false),
        };

        if self.commit_rule(&b0, &b1, &b2, block) {
            info!("Committed {}", b0);
            self.mempool_driver.garbage_collect(&b0.payload).await;
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        self.update_preferred_round(b1.round);
        self.update_high_qc(&block.qc);
        self.advance_round(&block.qc).await?;
        Ok(true)
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        self.store_block(block).await?;
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                let message = CoreMessage::Vote(vote);
                self.transmit(&message, Some(next_leader)).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);
        let digest = block.digest();

        // Handle the QC. This may allow us to advance round.
        if !self.handle_qc(block).await? {
            debug!(
                "Processing of {} suspended: missing ancestors",
                block.digest()
            );
            return Ok(());
        }

        // Ensure the block's round is as expected.
        if block.round != self.round {
            debug!(
                "Processing of {} suspended: missing payload",
                block.digest()
            );
            return Ok(());
        }

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly signed.
        block.signature.verify(&digest, &block.author)?;

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block).await? {
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_block(block).await
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = CoreMessage::Propose(block);
            self.transmit(&message, Some(sender)).await?;
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, send the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear back from it.
        self.schedule_timer().await;
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal()
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        CoreMessage::Propose(block) => self.handle_proposal(&block).await,
                        CoreMessage::Vote(vote) => self.handle_vote(&vote).await,
                        CoreMessage::LoopBack(block) => self.process_block(&block).await,
                        CoreMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                    }
                },
                Some(_) = self.timer.notifier.recv() => self.local_timeout_round().await,
                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
