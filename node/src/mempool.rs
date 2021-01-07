use crate::core::CoreMessage;
use crate::messages::Block;
use rand::Rng as _;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

pub type Mempool = FakeMempool;

pub struct FakeMempool {
    channel: Sender<oneshot::Sender<Vec<u8>>>,
}

impl FakeMempool {
    pub fn new() -> Self {
        let (tx, mut rx): (Sender<oneshot::Sender<Vec<u8>>>, _) = channel(100);
        tokio::spawn(async move {
            while let Some(sender) = rx.recv().await {
                // This is a fake mempool, so we just generate a
                // random 32 bytes payload.
                let mut rng = rand::thread_rng();
                let payload = [
                    rng.gen::<u128>().to_le_bytes(),
                    rng.gen::<u128>().to_le_bytes(),
                ]
                .concat();
                let _ = sender.send(payload);
            }
        });
        Self { channel: tx }
    }

    pub async fn get_payload(&self) -> Vec<u8> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(sender).await {
            panic!("Failed to request payload from mempool: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive payload from mempool")
    }

    pub async fn ready(&self, _block: &Block, _channel: Sender<CoreMessage>) -> bool {
        // This function is called by the core upon processing a new block
        // to ask the mempool if it has all the block data. This is useful
        // in case the payload is a hash, certificate, or does any represent
        // the txs data. This function returns True if the core can process
        // the block. Otherwise, it does whatever it needs to do to get the
        // block data and schedule re-processing of the block by sending a
        // `CoreMessage::Propose` message to the provided core channel.
        true
    }
}
