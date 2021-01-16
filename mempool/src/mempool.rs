use async_trait::async_trait;

pub enum PayloadStatus {
    Accept,
    Reject,
    Wait(Vec<u8>),
}

#[async_trait]
pub trait NodeMempool: Send + Sync {
    /// Consensus calls this method whenever it needs to create a new block.
    /// The mempool needs to promptly provide a payload.
    async fn get(&mut self) -> Vec<u8>;

    /// Consensus calls this method when receiving a new block. The mempool should
    /// return Accept if the block can be processed right away, Wait(missing_value)
    /// if the block should be processed when missing_value is the storage, or
    /// Reject if the payload is invalid and the block should be dropped.
    async fn verify(&mut self, payload: &[u8]) -> PayloadStatus;

    /// Consensus calls this method upon commit a block. The mempool can use the
    /// knowledge that a block is committed to clean up its internal state.
    async fn garbage_collect(&mut self, payload: &[u8]);
}
