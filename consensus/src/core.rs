use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, QC, TC};
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::VecDeque;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Block),
    Vote(Vote),
    Timeout(Timeout),
    TC(TC),
    LoopBack(Block),
    SyncRequest(Digest, PublicKey),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    core_channel: Receiver<ConsensusMessage>,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    last_committed_round: RoundNumber,
    high_qc: QC,
    timer: Timer,
    aggregator: Aggregator,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        core_channel: Receiver<ConsensusMessage>,
        network_channel: Sender<NetMessage>,
        commit_channel: Sender<Block>,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        let timer = Timer::new(parameters.timeout_delay);
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
            last_committed_round: 0,
            high_qc: QC::genesis(),
            timer,
            aggregator,
        }
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    fn increase_last_voted_round(&mut self, target: RoundNumber) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> ConsensusResult<Option<Vote>> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let mut safety_rule_2 = block.qc.round + 1 == block.round;
        if let Some(ref tc) = block.tc {
            let mut can_extend = tc.round + 1 == block.round;
            can_extend &= block.qc.round >= *tc.high_qc_rounds().iter().max().expect("Empty TC");
            safety_rule_2 |= can_extend;
        }
        if !(safety_rule_1 && safety_rule_2) {
            return Ok(None);
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);

        // TODO [issue #15]: Write to storage last_voted_round.

        let next_leader = self
            .leader_elector
            .elect_future_leader(&block.qc, block.round)
            .await?;
        Ok(Some(
            Vote::new(
                &block,
                next_leader,
                self.name,
                self.signature_service.clone(),
            )
            .await,
        ))
    }

    async fn commit(&mut self, block: Block) -> ConsensusResult<()> {
        if self.last_committed_round >= block.round {
            return Ok(());
        }

        let mut to_commit = VecDeque::new();
        to_commit.push_back(block.clone());

        // Ensure we commit the entire chain. This is needed after view-change.
        let mut parent = block.clone();
        while self.last_committed_round + 1 < parent.round {
            let ancestor = self
                .synchronizer
                .get_parent_block(&parent)
                .await?
                .expect("We should have all the ancestors by now");
            to_commit.push_front(ancestor.clone());
            parent = ancestor;
        }

        // Save the last committed block.
        self.last_committed_round = block.round;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed B{}({})", block.round, base64::encode(x));
                }
            }
            debug!("Committed {:?}", block);
            if let Err(e) = self.commit_channel.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: RoundNumber) {
        if round < self.round {
            return;
        }
        // Reset the timer and advance round.
        self.timer.reset();
        self.round = round + 1;
        debug!("Moved to round {}", self.round);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup(&self.round);
    }

    async fn local_timeout(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for round {}", self.round);
        self.increase_last_voted_round(self.round);
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.round,
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", timeout);
        self.timer.reset();
        let message = ConsensusMessage::Timeout(timeout.clone());
        Synchronizer::transmit(
            &message,
            &self.name,
            None,
            &self.network_channel,
            &self.committee,
        )
        .await?;
        self.handle_timeout(&timeout).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Ensure the vote is meant for us (ie. we are the next leader).
        self.leader_elector.check_vote(vote, self.name)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block.
            self.generate_proposal(None).await?;
        }
        Ok(())
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        debug!("Processing {:?}", timeout);
        if timeout.round < self.round {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // Process the QC embedded in the timeout.
        self.process_qc(&timeout.high_qc).await;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone())? {
            debug!("Assembled {:?}", tc);

            // Try to advance the round.
            self.advance_round(tc.round).await;

            // Broadcast the TC.
            let message = ConsensusMessage::TC(tc.clone());
            Synchronizer::transmit(
                &message,
                &self.name,
                None,
                &self.network_channel,
                &self.committee,
            )
            .await?;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.next_leader(&self.high_qc, self.round) {
                self.generate_proposal(Some(tc)).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn generate_proposal(&mut self, tc: Option<TC>) -> ConsensusResult<()> {
        // Make a new block.
        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size)
            .await;
        let block = Block::new(
            self.high_qc.clone(),
            tc,
            self.name,
            self.round,
            payload,
            self.signature_service.clone(),
        )
        .await;
        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created B{}({})", block.round, base64::encode(x));
            }
        }
        debug!("Created {:?}", block);

        // Process our new block and broadcast it.
        let message = ConsensusMessage::Propose(block.clone());
        Synchronizer::transmit(
            &message,
            &self.name,
            None,
            &self.network_channel,
            &self.committee,
        )
        .await?;
        self.process_block(&block).await?;

        // Wait for the minimum block delay.
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        Ok(())
    }

    async fn process_qc(&mut self, qc: &QC) {
        // Try to advance round.
        self.advance_round(qc.round).await;

        // Update high QC.
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process both ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Ensure the block proposer is the right leader for the round.
        self.leader_elector.check_block(block, &b1)?;

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        // Check if we can commit the head of the 2-chain.
        // Note that we commit blocks only if we have all its ancestors.
        if b0.round + 1 == b1.round {
            self.commit(b0.clone()).await?;
        }

        // Optimistically cleanup the mempool. This may result in some transactions
        // loss: the client will have to resubmit them.
        self.mempool_driver.cleanup(&b0, &b1, &block).await;

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.round != self.round {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await? {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.next_leader(&block.qc, block.round);
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                let message = ConsensusMessage::Vote(vote);
                Synchronizer::transmit(
                    &message,
                    &self.name,
                    Some(&next_leader),
                    &self.network_channel,
                    &self.committee,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Process the TC (if any). This may also allow us to advance round.
        if let Some(ref tc) = block.tc {
            self.advance_round(tc.round).await;
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone()).await? {
            debug!(
                "Processing of {} suspended: missing payload",
                block.digest()
            );
            return Ok(());
        }

        // Process this block.
        self.process_block(block).await
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = ConsensusMessage::Propose(block);
            Synchronizer::transmit(
                &message,
                &self.name,
                Some(&sender),
                &self.network_channel,
                &self.committee,
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_tc(&mut self, tc: TC) -> ConsensusResult<()> {
        debug!("Processing {:?}", tc);
        if tc.round < self.round {
            return Ok(());
        }

        // Try to advance round.
        self.advance_round(tc.round).await;

        // Propose a block if we are the leader.
        if self.name == self.leader_elector.next_leader(&self.high_qc, self.round) {
            self.generate_proposal(Some(tc)).await?;
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        self.timer.reset();
        if self.name == self.leader_elector.next_leader(&self.high_qc, self.round) {
            self.generate_proposal(None)
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                        ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await,
                        ConsensusMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                        ConsensusMessage::TC(tc) => self.handle_tc(tc).await,
                        ConsensusMessage::LoopBack(block) => self.process_block(&block).await,
                        ConsensusMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                    }
                },
                () = &mut self.timer => self.local_timeout().await,
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
