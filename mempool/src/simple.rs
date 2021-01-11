use crate::error::{MempoolError, MempoolResult};
use crate::messages::{Payload, PayloadMaker, Transaction};
use async_trait::async_trait;
use config::Committee;
use consensus::mempool::{NodeMempool, PayloadStatus};
use crypto::{Digest, Hash, PublicKey, SignatureService};
use rand::rngs::StdRng;
use rand::RngCore as _;
use rand::SeedableRng as _;
use std::collections::VecDeque;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct Parameters {
    queue_capacity: usize,
    max_payload_size: usize,
}

pub struct SimpleMempool {
    queue: VecDeque<Digest>,
    payload_maker: PayloadMaker,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    network_channel: Sender<Payload>,
}

impl SimpleMempool {
    pub fn new(
        committee: Committee,
        name: PublicKey,
        signature_service: SignatureService,
        _receiver: Receiver<Transaction>,
        parameters: Parameters,
        network_channel: Sender<Payload>,
        store: Store,
    ) -> Self {
        let payload_maker = PayloadMaker::new(name, signature_service, parameters.max_payload_size);
        Self {
            queue: VecDeque::new(),
            payload_maker,
            committee,
            parameters,
            store,
            network_channel,
        }
        /*
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                //
            }
        )};
        */
    }

    async fn store_payload(&mut self, digest: &Digest, payload: &Payload) -> MempoolResult<()> {
        let key = digest.to_vec();
        let value = bincode::serialize(payload).expect("Failed to serialize payload");
        self.store
            .write(key, value)
            .await
            .map_err(MempoolError::from)
    }

    pub async fn handle_transaction(&mut self, transaction: Transaction) -> MempoolResult<()> {
        // Drop the transaction if our mempool is full.
        if self.queue.len() >= self.parameters.queue_capacity {
            return Ok(());
        }

        // Otherwise, try to add the transaction to the next payload
        // we will add to the queue.
        if let Some(payload) = self.payload_maker.add(transaction).await {
            let digest = payload.digest();
            self.store_payload(&digest, &payload).await?;
            self.queue.push_front(digest);
            // Share this new payload with all other nodes.
            if let Err(e) = self.network_channel.send(payload).await {
                panic!("Failed to send block through network channel: {}", e);
            }
        }
        Ok(())
    }

    pub async fn handle_payload(&mut self, payload: Payload) -> MempoolResult<()> {
        // Ensure the author of the payload has stake.
        let author = payload.author;
        ensure!(
            self.committee.stake(&author) > 0,
            MempoolError::UnknownAuthority(author)
        );

        // Verify that the payload is correctly signed.
        let digest = payload.digest();
        payload.signature.verify(&digest, &author)?;

        // Store payload.
        self.store_payload(&digest, &payload).await?;
        Ok(())
    }
}

#[async_trait]
impl NodeMempool for SimpleMempool {
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
