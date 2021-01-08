use async_trait::async_trait;
use rand::Rng as _;
use std::marker::Send;

#[async_trait]
pub trait NodeMempool: Send + Sync {
    async fn get(&self) -> Vec<u8>;

    async fn verify(&self, payload: &[u8]) -> Result<bool, Box<dyn std::error::Error>>;

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

    async fn verify(&self, _payload: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(true)
    }

    async fn cleanup(&self, _payload: &[u8]) {}
}
