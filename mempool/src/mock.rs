use async_trait::async_trait;
use consensus::mempool::{NodeMempool, PayloadStatus};
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;

pub struct MockMempool;

impl MockMempool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl NodeMempool for MockMempool {
    async fn get(&self) -> Vec<u8> {
        let mut rng = StdRng::from_seed([0; 32]);
        let mut payload = [0u8; 32];
        rng.fill_bytes(&mut payload);
        payload.to_vec()
    }

    async fn verify(&self, _payload: &[u8]) -> PayloadStatus {
        PayloadStatus::Accept
    }

    async fn garbage_collect(&self, _payload: &[u8]) {}
}
