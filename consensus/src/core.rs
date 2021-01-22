use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, GenericQC, Vote, QC, TC};
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, log_enabled, warn, Level};
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
    highest_qc: QC,
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
            round: 0,
            last_voted_round: 0,
            preferred_round: 0,
            highest_qc: QC::genesis(),
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
        let addresses = match to {
            Some(to) => vec![self.committee.address(&to)?],
            None => self.committee.broadcast_addresses(&self.name),
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    #[async_recursion]
    async fn make_block(
        &mut self,
        qc: QC,
        tc: Option<TC>,
        round: RoundNumber,
    ) -> ConsensusResult<()> {
        let payload = self.mempool_driver.get().await;
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            payload,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", block);
        if !block.payload.is_empty() && !log_enabled!(Level::Debug) {
            info!("Created {}", block);
        }
        self.process_block(&block).await?;
        debug!("Broadcasting {:?}", block);
        let message = CoreMessage::Propose(block);
        self.transmit(&message, None).await
    }

    async fn handle_propose(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        // Reject old blocks if we already have them.
        if block.round <= self.round && self.store.read(digest.to_vec()).await?.is_some() {
            return Ok(());
        }

        // Check the block's round number is as expected. This prevents bad leaders
        // from proposing blocks with very high round numbers which may cause overflows.
        let ok = match block.tc {
            Some(ref tc) => block.round == tc.round + 1,
            None => block.round == block.qc.round + 1,
        };
        ensure!(ok, ConsensusError::MalformedBlock(digest));

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

        // Check that the QC embedded in the block is valid.
        if block.qc != QC::genesis() {
            block.qc.verify(&self.committee)?;
        }

        // Check the TC embedded in the block if any.
        if let Some(tc) = &block.tc {
            tc.verify(&self.committee)?;
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(&block).await? {
            return Ok(());
        }

        // If all check pass, process the block.
        self.process_block(&block).await
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

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
        let possible_new_round = match block.tc {
            Some(ref tc) => tc.round + 1,
            None => block.qc.round + 1,
        };
        if self.round < possible_new_round {
            // Cancel the timeout timer for this round and update the round number.
            self.timer.cancel(self.round).await;
            self.round = possible_new_round;
            info!("Moved to round {}", self.round);

            // Cleanup the vote aggregator and the mempool driver.
            self.aggregator.cleanup(&self.round);
            self.mempool_driver.cleanup(&self.round).await;

            // Schedule a new timer for this round.
            self.schedule_timer().await;
        }

        // Update the highest QC we know.
        if block.qc.round > self.highest_qc.round {
            self.highest_qc = block.qc.clone();
        }

        // Check if the last three ancestors of the block form a 3-chain.
        // If so, we commit its head.
        let mut commit_rule = b0.round + 1 == b1.round;
        commit_rule &= b1.round + 1 == b2.round;
        commit_rule &= b2.round + 1 == block.round;
        if commit_rule {
            info!("Committed {}", b0);
            self.mempool_driver.garbage_collect(&b0.payload).await;
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        // Check the safety rules to see if we can vote for this new block.
        // If we can, we send our vote to the next leader.
        let safety_rule_1 = b2.round >= self.preferred_round;
        let safety_rule_2 = block.round > self.last_voted_round;
        if safety_rule_1 && safety_rule_2 {
            debug!("Voted for {:?}", block);

            // Update our state to ensure we won't vote for conflicting blocks.
            self.preferred_round = max(self.preferred_round, b1.round);
            self.last_voted_round = block.round;

            // Send the vote the next leader.
            let vote = Vote::new(&block, self.name, self.signature_service.clone()).await;
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                self.handle_vote(vote).await?;
            } else {
                let message = CoreMessage::Vote(vote);
                debug!("Sending vote to {}", next_leader);
                self.transmit(&message, Some(next_leader)).await?;
            }
        }

        Ok(())
    }

    async fn handle_vote(&mut self, vote: Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }
        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(quorum) = self.aggregator.add_vote(vote.clone())? {
            // We propose a new block if we have a QC or TC, and if we are
            // the leader of the next round.
            let next_round = vote.round + 1;
            if self.name == self.leader_elector.get_leader(next_round) {
                let (qc, tc) = if vote.timeout() {
                    let tc = TC {
                        round: vote.round,
                        votes: quorum,
                    };
                    debug!("Assembled {:?}", tc);
                    (self.highest_qc.clone(), Some(tc))
                } else {
                    let qc = QC {
                        hash: vote.hash,
                        round: vote.round,
                        votes: quorum,
                    };
                    debug!("Assembled {:?}", qc);
                    (qc, None)
                };
                self.make_block(qc, tc, next_round).await?;
            }
        }
        Ok(())
    }

    async fn make_timeout(&mut self) -> ConsensusResult<()> {
        // First move to the next round (the current round timed out).
        self.round += 1;
        info!("Moved to round {}", self.round);

        // Make a timeout vote and send it to the next leader.
        let vote = Vote::new_timeout(self.round, self.name, self.signature_service.clone()).await;
        let next_leader = self.leader_elector.get_leader(self.round + 1);
        if next_leader == self.name {
            self.handle_vote(vote).await?;
        } else {
            debug!("Sending TV to {}", next_leader);
            let message = CoreMessage::Vote(vote);
            self.transmit(&message, Some(next_leader)).await?;
        }

        // Finally, schedule an other timer in case we timeout again.
        self.schedule_timer().await;
        Ok(())
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
        if self.name == self.leader_elector.get_leader(1) {
            self.make_block(self.highest_qc.clone(), None, 1)
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        CoreMessage::Propose(block) => self.handle_propose(&block).await,
                        CoreMessage::Vote(vote) => self.handle_vote(vote).await,
                        CoreMessage::LoopBack(block) => self.process_block(&block).await,
                        CoreMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                    }
                },
                Some(_) = self.timer.notifier.recv() => {
                    warn!("Timeout reached for round {}", self.round);
                    self.make_timeout().await
                },
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
