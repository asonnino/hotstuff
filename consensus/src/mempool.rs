use crate::core::SeqNumber;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use crypto::Digest;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum PayloadStatus {
    Accept,
    Reject,
    Wait,
}

#[derive(Debug)]
pub enum ConsensusMempoolMessage {
    Get(usize, oneshot::Sender<Vec<Digest>>),
    Verify(Box<Block>, oneshot::Sender<PayloadStatus>),
    Cleanup(Vec<Digest>, SeqNumber),
}

pub struct MempoolDriver {
    mempool_channel: Sender<ConsensusMempoolMessage>,
}

impl MempoolDriver {
    pub fn new(mempool_channel: Sender<ConsensusMempoolMessage>) -> Self {
        Self { mempool_channel }
    }

    pub async fn get(&mut self, max: usize) -> Vec<Digest> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMempoolMessage::Get(max, sender);
        self.mempool_channel
            .send(message)
            .await
            .expect("Failed to send message to mempool");
        receiver
            .await
            .expect("Failed to receive payload from mempool")
    }

    pub async fn verify(&mut self, block: Block) -> ConsensusResult<bool> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMempoolMessage::Verify(Box::new(block), sender);
        self.mempool_channel
            .send(message)
            .await
            .expect("Failed to send message to mempool");
        match receiver
            .await
            .expect("Failed to receive payload status from mempool")
        {
            PayloadStatus::Accept => Ok(true),
            PayloadStatus::Reject => bail!(ConsensusError::InvalidPayload),
            PayloadStatus::Wait => Ok(false),
        }
    }

    pub async fn cleanup(&mut self, b0: &Block, b1: &Block, block: &Block) {
        let digests = b0
            .payload
            .iter()
            .cloned()
            .chain(b1.payload.iter().cloned())
            .chain(block.payload.iter().cloned())
            .collect();
        let message = ConsensusMempoolMessage::Cleanup(digests, block.round);
        self.mempool_channel
            .send(message)
            .await
            .expect("Failed to send message to mempool");
    }

    pub async fn cleanup_async(&mut self, b0: &Block) {
        let digests = b0
            .payload
            .iter()
            .cloned()
            .collect();
        let message = ConsensusMempoolMessage::Cleanup(digests, b0.round);
        self.mempool_channel
            .send(message)
            .await
            .expect("Failed to send message to mempool");
    }
}
