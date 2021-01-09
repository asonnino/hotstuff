use rand::Rng as _;
use crate::mempool::{PayloadStatus, NodeMempool};
use async_trait::async_trait;

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

    async fn verify(&self, _payload: &[u8]) -> PayloadStatus {
        PayloadStatus::Accept
    }

    async fn garbage_collect(&self, _payload: &[u8]) {}
}
