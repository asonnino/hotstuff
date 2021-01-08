use async_trait::async_trait;
use rand::Rng as _;
use std::marker::Send;

#[async_trait]
pub trait NodeMempool: Send + Sync {
    /// Consensus calls this method whenever it needs to create a new block.
    /// The mempool needs to promptly provide a payload.
    async fn get(&self) -> Vec<u8>;

    /// Consensus calls this method when receiving a new block. The mempool should
    /// return Ok(None) if the block can be processed right away, Ok(precondition)
    /// if the block should be processed when precondition is the storage, or Err
    /// if the block is invalid and should be dropped.
    async fn verify(&self, payload: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;

    /// Consensus calls this method upon commit a block. The mempool can use the
    /// knowledge that a block is committed to clean up its internal state.
    async fn cleanup(&self, payload: &[u8]);
}

pub struct MockMempool;

impl MockMempool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl NodeMempool for MockMempool {
    async fn get(&self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        [
            rng.gen::<u128>().to_le_bytes(),
            rng.gen::<u128>().to_le_bytes(),
        ]
        .concat()
    }

    async fn verify(&self, _payload: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        Ok(None)
    }

    async fn cleanup(&self, _payload: &[u8]) {}
}
