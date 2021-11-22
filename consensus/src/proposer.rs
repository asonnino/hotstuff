use crate::{
    config::{Committee, Stake},
    consensus::{ConsensusMessage, Round},
    messages::{Block, QC, TC},
};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use log::{debug, info};
use mempool::BatchCertificate;
use network::{CancelHandler, ReliableSender};
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};

/// The maximum number of batches from other nodes that we include in our block.
/// NOTE: This parameter heavily influences performance.
const MAX_BATCHES_FROM_OTHERS: usize = 30;

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<BatchCertificate>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_mempool: Receiver<BatchCertificate>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    tx_mempool: Sender<Digest>,
    buffer: HashSet<BatchCertificate>,
    network: ReliableSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_mempool: Receiver<BatchCertificate>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
        tx_mempool: Sender<Digest>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_mempool,
                rx_message,
                tx_loopback,
                tx_mempool,
                buffer: HashSet::new(),
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            /* payload */ self.buffer.drain().collect(),
            self.signature_service.clone(),
        )
        .await;

        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", block, x.root);
            }
        }
        debug!("Created {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        // Control system: Wait for 2f+1 nodes to acknowledge our block before
        // continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    async fn run(&mut self) {
        let mut others_payloads = 0;
        loop {
            tokio::select! {
                Some(payload) = self.rx_mempool.recv() => {
                    /*
                    if payload.author == self.name {
                        debug!("Adding our own certificate to payload {}", payload.root);

                        self
                            .tx_mempool
                            .send(payload.root.clone())
                            .await
                            .expect("Failed to send back digest to mempool");

                        self.buffer.insert(payload);
                    } else if others_payloads < MAX_BATCHES_FROM_OTHERS  {
                        debug!("Adding others' certificate to payload {}", payload.root);
                        self.buffer.insert(payload);
                        others_payloads += 1;
                    }
                    */
                    if payload.author == self.name {
                        self
                            .tx_mempool
                            .send(payload.root.clone())
                            .await
                            .expect("Failed to send back digest to mempool");
                    }
                    if others_payloads < MAX_BATCHES_FROM_OTHERS  {
                        self.buffer.insert(payload);
                        others_payloads += 1;
                    }
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => {
                        self.make_block(round, qc, tc).await;
                        others_payloads = 0;
                    },
                    ProposerMessage::Cleanup(digests) => {
                        for x in &digests {
                            if self.buffer.remove(x) {
                                others_payloads -= 1;
                            }
                        }
                    }
                }
            }
        }
    }
}
