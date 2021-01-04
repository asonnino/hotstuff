use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::crypto::Hash as _;
use crate::crypto::{Digest, PublicKey, SignatureService};
use crate::error::{DiemError, DiemResult};
use crate::leader::LeaderElector;
use crate::mempool::Mempool;
use crate::messages::{Block, GenericQC, Vote, QC, TC};
use crate::network::NetMessage;
use crate::store::Store;
use crate::synchronizer::Synchronizer;
use futures::future::FutureExt as _;
use futures::select;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Propose(Block),
    Vote(Vote),
    LoopBack(Block),
    SyncRequest(Digest, PublicKey),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool: Mempool,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    highest_qc: QC,
    synchronizer: Synchronizer,
    aggregator: Aggregator,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub async fn make(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        signature_service: SignatureService,
        leader_elector: LeaderElector,
        mempool: Mempool,
        network_channel: Sender<NetMessage>,
        commit_channel: Sender<Block>,
    ) -> Sender<CoreMessage> {
        let (tx, rx) = channel(1000);

        // Make the synchronizer. This instance runs in a background thread
        // and asks other nodes for any block that we may be missing.
        let synchronizer = Synchronizer::new(
            name,
            store.clone(),
            network_channel.clone(),
            tx.clone(),
            parameters.sync_retry_delay,
        )
        .await;

        // Make a votes aggregator. This is the instance that keeps track
        // of incoming votes and aggregates them into QCs.
        let aggregator = Aggregator::new(committee.clone());

        // Run the core in a separate thread.
        tokio::spawn(async move {
            let mut core = Self {
                name,
                committee,
                store,
                signature_service,
                leader_elector,
                mempool,
                network_channel,
                commit_channel,
                round: 0,
                last_voted_round: 0,
                preferred_round: 0,
                highest_qc: QC::genesis(),
                synchronizer,
                aggregator,
            };
            core.run(rx, parameters).await;
        });

        // Return sender channel. The network receiver will use it to
        // send us new messages to process.
        tx
    }

    async fn make_block(&mut self, qc: QC, tc: Option<TC>) -> DiemResult<()> {
        let block = Block::new(
            qc,
            tc,
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
        // Reject old blocks.
        if block.round <= self.round {
            return Ok(());
        }

        // Check the block's round number is as expected. This prevents bad leaders
        // from proposing blocks with very high round numbers which may cause overflows.
        let ok = match block.tc {
            Some(ref tc) => block.round == tc.round + 1,
            None => block.round == block.qc.round + 1,
        };
        ensure!(ok, DiemError::MalformedBlock(block.digest()));

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
        block.qc.verify(&self.committee)?;

        // Check the TC embedded in the block if any.
        if let Some(tc) = &block.tc {
            tc.verify(&self.committee)?;
        }

        // If all check pass, process the block.
        self.process_block(&block).await
    }

    async fn process_block(&mut self, block: &Block) -> DiemResult<()> {
        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and them make us resume processing this block.
        if !self.mempool.ready(&block.payload).await {
            return Ok(());
        }

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
            info!("Moved to round {}", self.round);
            self.round = possible_new_round;
            self.aggregator.cleanup(&self.round);
        }

        // Update the highest QC we know.
        if block.qc.round > self.highest_qc.round {
            self.highest_qc = block.qc.clone();
        }

        // Check if the last three ancestors of the block form a 3-chain.
        // If so, we commit b0.
        let mut commit_rule = b0.round + 1 == b1.round;
        commit_rule &= b1.round + 1 == b2.round;
        commit_rule &= b2.round + 1 == block.round;
        if commit_rule {
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

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(quorum) = self.aggregator.add_vote(vote.clone())? {
            // We propose a new block if we have a QC or TC, and if we are
            // the leader of the next round.
            if self.name == self.leader_elector.get_leader(vote.round + 1) {
                let (qc, tc) = if vote.timeout() {
                    let tc = TC {
                        round: vote.round,
                        votes: quorum,
                    };
                    (self.highest_qc.clone(), Some(tc))
                } else {
                    let qc = QC {
                        hash: vote.hash,
                        round: vote.round,
                        votes: quorum,
                    };
                    (qc, None)
                };
                self.make_block(qc, tc).await?;
            }
        }
        Ok(())
    }

    async fn make_timeout(&mut self) {
        self.round += 1;
        info!("Moved to round {}", self.round);
        let timeout =
            Vote::new_timeout(self.round, self.name, self.signature_service.clone()).await;
        let next_leader = self.leader_elector.get_leader(self.round + 1);
        if let Err(e) = self
            .network_channel
            .send(NetMessage::Vote(timeout, next_leader))
            .await
        {
            panic!("Core failed to send vote to the network: {}", e);
        }
    }

    async fn handle_sync_request(&mut self, digest: Digest, sender: PublicKey) -> DiemResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block =
                bincode::deserialize(&bytes).map_err(|e| DiemError::StoreError(e.to_string()))?;
            if let Err(e) = self
                .network_channel
                .send(NetMessage::SyncReply(block, sender))
                .await
            {
                panic!("Core failed to send sync reply to the network: {}", e);
            }
        }
        Ok(())
    }

    async fn run(&mut self, mut rx: Receiver<CoreMessage>, parameters: Parameters) {
        // Upon booting, send the very first block (if we are the leader).
        if self.name == self.leader_elector.get_leader(1) {
            self.make_block(self.highest_qc.clone(), None)
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes.
        let mut round = self.round;
        loop {
            select! {
                message = rx.recv().fuse() => {
                    if let Some(message) = message {
                        debug!("Received message: {:?}", message);
                        let result = match message {
                            CoreMessage::Propose(block) => self.handle_propose(&block).await,
                            CoreMessage::Vote(vote) => self.handle_vote(vote).await,
                            CoreMessage::LoopBack(block) => self.process_block(&block).await,
                            CoreMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                        };
                        match result {
                            Ok(()) => debug!("Message successfully processed."),
                            Err(DiemError::StoreError(e)) => error!("{}", e),
                            Err(e) => warn!("{}", e),
                        }
                    }
                },
                // TODO: This is a bad way to implement timeouts. Some nodes may
                // potentially wait for 2 sec (instead of 1) before triggering it,
                // and it probably recreate a timer at each loop iteration...
                // TODO: Timeout delay should be a parameter.
                () = sleep(Duration::from_millis(parameters.timeout_delay)).fuse() => {
                    if self.round == round {
                        warn!("Timing out for round {}!", self.round);
                        self.make_timeout().await
                    }
                    round = self.round;
                }
            }
        }
    }
}
