use crate::config::Committee;
use crate::consensus::{ConsensusMessage, Round};
use crate::messages::{Block, QC, TC};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, info};
use network::SimpleSender;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct ProposerMessage(pub Round, pub QC, pub Option<TC>);

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_mempool: Receiver<Digest>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: Vec<Digest>,
    network: SimpleSender,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_mempool: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_mempool,
                rx_message,
                tx_loopback,
                buffer: Vec::new(),
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            /* payload */ self.buffer.drain(..).collect(),
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
                    self.buffer.push(digest);
                },
                Some(ProposerMessage(round, qc, tc)) = self.rx_message.recv() => {
                    self.make_block(round, qc, tc).await;
                }
            }
        }
    }
}
