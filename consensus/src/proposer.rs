use crate::config::Committee;
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info};
use network::SimpleSender;
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<Digest>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    max_payload_size: usize,
    rx_mempool: Receiver<Digest>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: HashSet<Digest>,
    network: SimpleSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        max_payload_size: usize,
        rx_mempool: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                max_payload_size,
                rx_mempool,
                rx_message,
                tx_loopback,
                buffer: HashSet::new(),
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // Gather the payload.
        let digest_len = Digest::default().size();
        let payload = self
            .buffer
            .iter()
            .take(self.max_payload_size / digest_len)
            .cloned()
            .collect();
        for x in &payload {
            self.buffer.remove(x);
        }

        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            payload,
            self.signature_service.clone(),
        )
        .await;

        if !block.payload.is_empty() {
            info!("Created {}", block);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                // NOTE: This log entry is used to compute performance.
                info!("Created {} -> {:?}", block, x);
            }
        }
        debug!("Created {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let addresses = self.committee.broadcast_addresses(&self.name);
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        self.network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_mempool.recv() => {
                    self.buffer.insert(digest);
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(digests) => {
                        for x in &digests {
                            self.buffer.remove(x);
                        }
                    }
                }
            }
        }
    }
}
