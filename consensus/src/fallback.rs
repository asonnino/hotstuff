use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters, Stake};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, QC, TC, SignedQC, RandomnessShare, RandomCoin};
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use crate::core::{ConsensusMessage, SeqNumber, HeightNumber, Bool};
use async_recursion::async_recursion;
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::NetMessage;
use std::cmp::max;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Duration, sleep};
use std::collections::{HashMap, HashSet, BTreeMap};
use threshold_crypto::PublicKeySet;
use std::convert::TryInto;

pub struct Fallback {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    pk_set: PublicKeySet,   // the set of tss pk
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    core_channel: Receiver<ConsensusMessage>,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: SeqNumber,     // current round
    view: SeqNumber,       // current view
    height: HeightNumber,    // current height, 0 not in fallback, 1 or 2 in fallback
    last_voted_round: SeqNumber,
    high_qc: QC,
    last_committed_round: SeqNumber,    // round of last committed block, initially -1
    fallback: Bool, // 0 if not in async fallback, 1 if in async fallback
    fallback_voted_round: HashMap<PublicKey, SeqNumber>,    // voted round number during fallback for each i
    fallback_voted_height: HashMap<PublicKey, HeightNumber>,  // voted height number during fallback for each i
    fallback_pending_blocks: HashMap<SeqNumber, HashMap<(PublicKey, HeightNumber), Block>>, // buffering the fallback block received when not enter fallback yet, indexed by view
    fallback_qcs: HashMap<PublicKey, QC>,    // highest fallback QC by each node
    fallback_signed_qc_sender: HashMap<SeqNumber, HashSet<PublicKey>>,    // set of nodes that send height-2 signed QC
    fallback_signed_qc_weight: HashMap<SeqNumber, Stake>,    // weight of the above nodes
    fallback_randomness_share_sender: HashMap<SeqNumber, HashSet<PublicKey>>,    // set of nodes that send randomness share
    fallback_randomness_share_weight: HashMap<SeqNumber, Stake>,    // weight of the above nodes
    fallback_randomness_shares: HashMap<SeqNumber, Vec<RandomnessShare>>,    // set of nodes that send randomness share
    fallback_random_coin: HashMap<SeqNumber, RandomCoin>,   // random coin of each fallback
    timer: Timer,
    aggregator: Aggregator,
}

impl Fallback {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        pk_set: PublicKeySet,
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
            pk_set,
            store,
            leader_elector,
            mempool_driver,
            synchronizer,
            network_channel,
            commit_channel,
            core_channel,
            round: 1,
            view: 0,
            height: 0,
            last_voted_round: 0,
            high_qc: QC::genesis(),
            last_committed_round: u64::MAX, // initially -1
            fallback: 0,
            fallback_voted_round: HashMap::new(),
            fallback_voted_height: HashMap::new(),
            fallback_qcs: HashMap::new(),
            fallback_pending_blocks: HashMap::new(),
            fallback_signed_qc_sender: HashMap::new(),
            fallback_signed_qc_weight: HashMap::new(),
            fallback_randomness_share_sender: HashMap::new(),
            fallback_randomness_share_weight: HashMap::new(),
            fallback_randomness_shares: HashMap::new(),
            fallback_random_coin: HashMap::new(),
            timer,
            aggregator,
        }
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    async fn transmit(
        &mut self,
        message: &ConsensusMessage,
        to: Option<PublicKey>,
    ) -> ConsensusResult<()> {
        sleep(Duration::from_millis(self.parameters.network_delay)).await;
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
    // check if qc is nonfallback qc or endorsed fallback qc
    fn valid_qc(&mut self, qc: &QC) -> bool {
        if qc.fallback == 0 {
            return true;
        } else if qc.fallback == 1 {
            match self.leader_elector.get_fallback_leader(qc.view) {
                None => return false,
                Some(leader) => return qc.proposer == leader,
            }
        }
        false
    }
    fn increase_last_voted_round(&mut self, target: SeqNumber) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        if block.fallback == 0 {
            // For non-fallback blocks
            // Check if we can vote for this block.
            let safety_rule_1 = block.round > self.last_voted_round && block.round == block.qc.round+1;
            let safety_rule_2 = block.qc.round >= self.high_qc.round;
            if !(safety_rule_1 && safety_rule_2) {
                return None;
            }
            // Ensure we won't vote for contradicting blocks.
            self.increase_last_voted_round(block.round);
        } else if block.fallback == 1 {
            // For fallback blocks
            let voted_height = match self.fallback_voted_height.get(&block.author) {
                Some(h) => h,
                None => {
                    warn!("Receiving block from {} outside the committee", block.author);
                    return None;
                }
            };
            let voted_round = match self.fallback_voted_round.get(&block.author) {
                Some(r) => r,
                None => {
                    warn!("Receiving block from {} outside the committee", block.author);
                    return None;
                }
            };
            if block.height <= *voted_height || block.round <= *voted_round {
                return None;
            }
            match block.height {
                1 => {
                    match &block.tc {
                        None => return None,
                        Some(tc) => {
                            let safety_rule_1 = (block.qc.view > self.high_qc.view) || (block.qc.view == self.high_qc.view && block.qc.round >= self.high_qc.round);
                            let safety_rule_2 = (block.round == block.qc.round+1) && (block.view == tc.seq);
                            if !(safety_rule_1 && safety_rule_2) {
                                return None;
                            }
                        }
                    }
                },
                2 => {
                    let safety_rule = (block.round == block.qc.round+1) && (block.view == self.view) && (block.height == block.qc.height+1);
                    if !(safety_rule) {
                        return None;
                    }
                },
                _ => return None,
            }
            self.fallback_voted_height.insert(block.author, block.height);
            self.fallback_voted_round.insert(block.author, block.round);
        }
        
        // TODO: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(&block, self.name, self.signature_service.clone()).await)
    }
    // -- End Safety Module --

    // -- Start Pacemaker --
    fn update_high_qc(&mut self, qc: &QC) {
        if qc.view > self.high_qc.view || (qc.view == self.high_qc.view && qc.round > self.high_qc.round) {
            self.high_qc = qc.clone();
        }
    }

    // Executed when entering a new fallback
    fn init_fallback_state(&mut self) {
        self.height = 1;
        for (node, _) in &self.committee.authorities {
            self.fallback_voted_height.insert(*node, 0);
            self.fallback_voted_round.insert(*node, 0);
            self.fallback_qcs.insert(*node, self.high_qc.clone());
        }
    }

    fn clean_fallback_state(&mut self, view: &SeqNumber) {
        self.height = 0;
        // Cleanup the pending fallback blocks
        self.fallback_pending_blocks.retain(|v, _| v >= &view);
        self.fallback_signed_qc_sender.retain(|v, _| v >= &view);
        self.fallback_signed_qc_weight.retain(|v, _| v >= &view);
        self.fallback_randomness_share_sender.retain(|v, _| v >= &view);
        self.fallback_randomness_share_weight.retain(|v, _| v >= &view);
        self.fallback_randomness_shares.retain(|v, _| v >= &view);
        self.fallback_random_coin.retain(|v, _| v >= &view);
    }

    async fn local_timeout_view(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for view {}", self.view);
        self.fallback = 1;  // Enter fallback and stop voting for non-fallback blocks
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.view,
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", timeout);
        self.timer.reset();
        let message = ConsensusMessage::Timeout(timeout.clone());
        self.transmit(&message, None).await?;
        self.handle_timeout(&timeout).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.view < self.view || (vote.fallback == 0 && vote.round < self.round) {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process non-fallback QC directly.
            if qc.fallback == 0 {
                // Process the QC.
                self.process_qc(&qc).await;

                // Make a new block if we are the next leader and not in fallback
                if self.fallback == 0 && self.name == self.leader_elector.get_leader(self.round) {
                    // Simulate DDOS attack on the leader
                    if self.parameters.ddos {
                        sleep(Duration::from_millis(2 * self.parameters.timeout_delay)).await;
                    }
                    self.generate_proposal(None, None, self.high_qc.clone()).await?;
                }
            }
            // Fallback QC of my proposed block
            if qc.fallback == 1 && qc.proposer == self.name {
                // Update the highest fallback QC
                let fallback_high_qc = self.fallback_qcs.get(&self.name).unwrap();
                if qc.view > fallback_high_qc.view || (qc.view == fallback_high_qc.view && qc.height > fallback_high_qc.height) {
                    self.fallback_qcs.insert(self.name, qc.clone());
                }
                // Propose height-2 fallback block
                if qc.height == 1 {
                    self.height = 2;
                    self.generate_proposal(None, None, qc.clone()).await?;
                }
                // Sign and multicast height-2 QC
                if qc.height == 2 {
                    let signed_qc = SignedQC::new(qc, self.name, self.signature_service.clone()).await;
                    let message = ConsensusMessage::SignedQC(signed_qc.clone());
                    self.transmit(&message, None).await?;
                    self.handle_signed_qc(signed_qc).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        debug!("Processing {:?}", timeout);
        if timeout.seq < self.view {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // Process the QC embedded in the timeout.
        self.process_qc(&timeout.high_qc).await;

        // Add the new vote to our aggregator and see if we have a quorum.
        // Enter the fallback
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone())? {
            debug!("Assembled {:?}", tc);
            info!("-------------------------------------------------------- Enter fallback of view {} --------------------------------------------------------", tc.seq);

            // Enter fallback
            self.fallback = 1;

            // Update the view to be the view of the TC.
            self.advance_view(tc.seq).await;

            // Initialize fallback states
            self.init_fallback_state();

            // Make a new block for its fallback chain
            self.height = 1;
            self.generate_proposal(Some(tc), None, self.high_qc.clone()).await?;

            self.process_pending_blocks().await?;
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: SeqNumber) {
        if round < self.round {
            return;
        }
        self.timer.reset();
        self.round = round + 1;
        debug!("Moved to round {}", self.round);
    }

    // #[async_recursion]
    async fn advance_view(&mut self, view: SeqNumber) {
        if view <= self.view {
            return;
        }
        debug!("advance_view: previous view {} with leader {:?} and highqc {:?}, new view {}", self.view, self.leader_elector.get_fallback_leader(self.view), self.high_qc, view);
        self.timer.reset();
        self.view = view;
        info!("-------------------------------------------------------- Enter view {} --------------------------------------------------------", view);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup_async(&self.view, &self.round);
        
        self.clean_fallback_state(&view);
    }

    // -- End Pacemaker --

    #[async_recursion]
    async fn generate_proposal(&mut self, tc: Option<TC>, coin: Option<RandomCoin>, qc: QC) -> ConsensusResult<()> {
        // Make a new block.
        let payload = self
            .mempool_driver
            .get(self.parameters.max_payload_size)
            .await;
        let block = Block::new(
            qc.clone(),
            tc,
            coin,
            self.name,
            self.view,
            qc.round+1,   // The chain always has consecutive round numbers
            self.height,
            self.fallback,
            payload,
            self.signature_service.clone(),
        )
        .await;
        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                info!("Created B{}({})", block.round, base64::encode(x));
            }
        }
        debug!("Created {:?}", block);

        // Process our new block and broadcast it.
        let message = ConsensusMessage::Propose(block.clone());
        self.transmit(&message, None).await?;
        self.process_block(&block).await?;
        
        // Wait for the minimum block delay. 
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        Ok(())
    }

    async fn process_pending_blocks(&mut self) -> ConsensusResult<()> {
        if let Some(map) = self.fallback_pending_blocks.remove(&self.view) {
            for (_, block) in map {
                if let Err(e) = self.process_block(&block).await {
                    warn!("Failed to process pending blocks: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn process_qc(&mut self, qc: &QC) {
        // Fallback QC only handled after the fallback
        if !self.valid_qc(qc) {
            return
        }
        self.advance_round(qc.round).await;
        self.update_high_qc(qc);
    }

    #[async_recursion]
    async fn commit(&mut self, block: &Block) -> ConsensusResult<()> {
        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process all three ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        // Cleanup the mempool.
        self.mempool_driver.cleanup(&b0, &b1, &block).await;

        if b0.round <= self.last_committed_round && self.last_committed_round != u64::MAX {
            return Ok(());
        }

        // The chain should have consecutive round numbers by construction.
        let mut consecutive_rounds = b0.round + 1 == b1.round;
        consecutive_rounds &= b1.round + 1 == block.round;
        ensure!(consecutive_rounds || block.qc == QC::genesis(), ConsensusError::NonConsecutiveRounds{rd1: b0.round, rd2: b1.round, rd3: block.round});
        
        // The new commit rule requires blocks of the same view.
        let same_view = b0.view == b1.view;
        // For fallback blocks, they need to be proposed by the fallback leader.
        let endorsed = self.valid_qc(&b1.qc) && self.valid_qc(&block.qc);
        if same_view && endorsed {
            if !b0.payload.is_empty() {
                info!("Committed {}", b0);

                #[cfg(feature = "benchmark")]
                for x in &b0.payload {
                    info!("Committed B{}({})", b0.round, base64::encode(x));
                }
            }

            self.last_committed_round = b0.round;
            debug!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }

        Ok(())
    }

    // #[async_recursion]
    // async fn print_chain(&mut self, block: &Block) -> ConsensusResult<()> {
    //     debug!("-------------------------------------------------------- printing chain start --------------------------------------------------------");
    //     let mut current_block = block.clone();
    //     while current_block.qc != QC::genesis() {
    //         let parent = match self.synchronizer.get_previous_block(&current_block).await? {
    //             Some(b) => b,
    //             None => {
    //                 debug!("Processing of {} suspended: missing parent", current_block.digest());
    //                 break;
    //             }
    //         };
    //         debug!("{:?}", current_block);
    //         current_block = parent;
    //     }
    //     debug!("{:?}", current_block);
    //     debug!("-------------------------------------------------------- printing chain end --------------------------------------------------------");
    //     Ok(())
    // }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        // debug!("{:?}", self.print_chain(block).await?);

        if let Some(coin) = block.coin.clone() {
            self.handle_random_coin(coin).await?;
        }

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        self.commit(block).await?;

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.fallback == 0 && block.round != self.round {
            return Ok(());
        }
        if block.fallback == 1 && block.view != self.view {
            return Ok(());
        }
        if block.fallback != self.fallback {
            return Ok(());
        }

        // See if we can propose a fallback block extending the fallback QC
        if block.qc.fallback == 1 && (block.qc.view == self.view && block.qc.height >= self.height) {
            let fallback_high_qc = self.fallback_qcs.get(&block.qc.proposer).unwrap();
            if block.qc.view > fallback_high_qc.view || (block.qc.view == fallback_high_qc.view && block.qc.height > fallback_high_qc.height) {
                self.fallback_qcs.insert(block.qc.proposer, block.qc.clone());
            }
            self.height = block.qc.height+1;
            if block.qc.height == 1 {
                if self.fallback == 1 {
                    self.height = 2;
                    self.generate_proposal(None, None, block.qc.clone()).await?;
                }
            }
            if block.qc.height == 2 {
                // sign and multicast height-2 QC
                let message = ConsensusMessage::SignedQC(SignedQC::new(block.qc.clone(), self.name, self.signature_service.clone()).await);
                self.transmit(&message, None).await?;
            }
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let mut next_leader = self.leader_elector.get_leader(self.round + 1);
            // For fallback blocks, send vote back to the proposer.
            if block.fallback == 1 {
                next_leader = block.author;
            }
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                let message = ConsensusMessage::Vote(vote);
                self.transmit(&message, Some(next_leader)).await?;
            }
        }

        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.fallback == 1 || block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly formed.
        block.verify_fallback(&self.committee, &self.pk_set)?;

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone()).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // Not in fallback, process the fallback blocks when later enter the fallback
        // It is necessary to receive 2f+1 timeout messages with QCs to update the high_qc
        if block.fallback == 1 && self.fallback == 0 {
            if block.height == 1 || block.height == 2 {
                let map = self.fallback_pending_blocks.entry(block.view).or_insert(HashMap::new());
                map.insert((block.author, block.height), block.clone());
            }
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
            let message = ConsensusMessage::Propose(block);
            self.transmit(&message, Some(sender)).await?;
        }
        Ok(())
    }

    // With async fallback, do not handle TC directly. The reason is that any node needs to receive 2f+1 Timeout to update the high QC.
    async fn handle_tc(&mut self, _: TC) -> ConsensusResult<()> {
        Ok(())
    }

    // When receiving 2f+1 height-2 fallback QC, send randomness share
    async fn handle_signed_qc(&mut self, signed_qc: SignedQC) -> ConsensusResult<()> {
        // Already receive from the sender.
        let set = self.fallback_signed_qc_sender.entry(signed_qc.qc.view).or_insert(HashSet::new());
        if set.contains(&signed_qc.author) {
            return Ok(());
        }

        if signed_qc.qc.fallback == 0 || signed_qc.qc.height != 2 || signed_qc.qc.view < self.view {
            return Ok(());
        }
        signed_qc.verify(&self.committee)?;
        
        set.insert(signed_qc.author);
        let weight = self.fallback_signed_qc_weight.entry(signed_qc.qc.view).or_insert(0);
        
        // Collected 2f+1 height-2 fallback QC, send randomness share
        *weight += self.committee.stake(&signed_qc.author);
        if *weight >= self.committee.quorum_threshold() {
            *weight = 0; // Only send randomness share once
            // Multicast the randomness share
            let randomness_share = RandomnessShare::new(signed_qc.qc.view, self.name, self.signature_service.clone()).await;
            let message = ConsensusMessage::RandomnessShare(randomness_share.clone());
            self.transmit(&message, None).await?;
            self.handle_randomness_share(randomness_share).await?;
        }
        Ok(())
    }

    async fn handle_randomness_share(&mut self, randomness_share: RandomnessShare) -> ConsensusResult<()> {
        if self.fallback_random_coin.contains_key(&randomness_share.seq) {
            return Ok(())
        }

        let set = self.fallback_randomness_share_sender.entry(randomness_share.seq).or_insert(HashSet::new());
        if randomness_share.seq < self.view || set.contains(&randomness_share.author) {
            return Ok(());
        }

        randomness_share.verify(&self.committee, &self.pk_set)?;

        let weight = self.fallback_randomness_share_weight.entry(randomness_share.seq).or_insert(0);
        let shares = self.fallback_randomness_shares.entry(randomness_share.seq).or_insert(Vec::new());
        set.insert(randomness_share.author);
        shares.push(randomness_share.clone());

        *weight += self.committee.stake(&randomness_share.author);
        // Collected enough shares, send random coin
        if *weight >= self.committee.random_coin_threshold() {
            *weight = 0; // Only send random coin once
            let mut sigs = BTreeMap::new();
            // Check the random shares.
            for share in shares.clone() {
                sigs.insert(self.committee.id(share.author.clone()), share.signature_share.clone());
            }
            if let Ok(sig) = self.pk_set.combine_signatures(sigs.iter()) {
                let id = usize::from_be_bytes((&sig.to_bytes()[0..8]).try_into().unwrap()) % self.committee.size();
                let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                keys.sort();
                let leader = keys[id];
                debug!("Random coin of view {} elects leader id {}", randomness_share.seq, id);
                // Multicast the random coin
                let random_coin = RandomCoin {seq: randomness_share.seq, leader, shares: shares.to_vec()};
                self.handle_random_coin(random_coin.clone()).await?;
            } else {
                error!("Wrong random coin shares!");
            }
        }
        Ok(())
    }

    async fn handle_random_coin(&mut self, random_coin: RandomCoin) -> ConsensusResult<()> {
        if random_coin.seq < self.view || self.fallback_random_coin.contains_key(&random_coin.seq) {
            return Ok(())
        }

        random_coin.verify(&self.committee, &self.pk_set)?;
        
        let view = random_coin.seq;
        self.fallback_random_coin.insert(view, random_coin.clone());
        let message = ConsensusMessage::RandomCoin(random_coin.clone());
        self.transmit(&message, None).await?;

        self.leader_elector.add_random_coin(random_coin.clone());

        // if self.fallback == 1 {
        let fallback_leader = self.leader_elector.get_fallback_leader(view).unwrap();
        if let Some(voted_round) = self.fallback_voted_round.get(&fallback_leader) {
            self.last_voted_round = max(self.last_voted_round, *voted_round);
        }
        if let Some(qc) = self.fallback_qcs.get_mut(&fallback_leader).cloned() {
            self.process_qc(&qc).await;
        }
        // }
        // Exit fallback
        self.fallback = 0;
        self.advance_view(view+1).await;
        self.timer.reset();
        if self.name == self.leader_elector.get_leader(self.round) {
            // Simulate DDOS attack on the leader
            if self.parameters.ddos {
                sleep(Duration::from_millis(2 * self.parameters.timeout_delay)).await;
            }
            self.generate_proposal(None, Some(random_coin), self.high_qc.clone())
                .await
                .expect("Failed to send the first block after fallback");
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        self.timer.reset();
        if self.name == self.leader_elector.get_leader(self.round) {
            // Simulate DDOS attack on the leader
            if self.parameters.ddos {
                sleep(Duration::from_millis(2 * self.parameters.timeout_delay)).await;
            }
            self.generate_proposal(None, None, self.high_qc.clone())
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
                        ConsensusMessage::SignedQC(signed_qc) => self.handle_signed_qc(signed_qc).await,
                        ConsensusMessage::RandomnessShare(randomness_share) => self.handle_randomness_share(randomness_share).await,
                        ConsensusMessage::RandomCoin(random_coin) => self.handle_random_coin(random_coin).await,
                        ConsensusMessage::LoopBack(block) => self.process_block(&block).await,
                        ConsensusMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                    }
                },
                () = &mut self.timer => self.local_timeout_view().await,
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
