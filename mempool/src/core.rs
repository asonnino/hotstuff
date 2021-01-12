use crate::error::{MempoolError, MempoolResult};
use crate::messages::{Payload, PayloadMaker, Transaction};
use crate::network::NetMessage;
use async_trait::async_trait;
use config::Committee;
use consensus::mempool::{NodeMempool, PayloadStatus};
use crypto::{Digest, Hash, PublicKey, SignatureService};
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct Parameters {
    queue_capacity: usize,
    max_payload_size: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum CoreMessage {
    Transaction(Transaction),
    Payload(Payload),
    Request(Digest, PublicKey),
}

pub struct Core {
    queue: VecDeque<Digest>,
    payload_maker: PayloadMaker,
    committee: Committee,
    name: PublicKey,
    parameters: Parameters,
    store: Store,
    network_channel: Sender<NetMessage>,
}

impl Core {
    pub fn new(
        committee: Committee,
        name: PublicKey,
        signature_service: SignatureService,
        _receiver: Receiver<Transaction>,
        parameters: Parameters,
        network_channel: Sender<NetMessage>,
        store: Store,
    ) -> Self {
        let payload_maker = PayloadMaker::new(name, signature_service, parameters.max_payload_size);
        Self {
            queue: VecDeque::with_capacity(parameters.queue_capacity),
            payload_maker,
            committee,
            name,
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
            let message = NetMessage::Share(payload);
            if let Err(e) = self.network_channel.send(message).await {
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

        // Verify that the payload does not exceed the maximum size.
        ensure!(
            payload.size() <= self.parameters.max_payload_size,
            MempoolError::PayloadTooBig
        );

        // Verify that the payload is correctly signed.
        let digest = payload.digest();
        payload.signature.verify(&digest, &author)?;

        // Store payload.
        // TODO: A bad node may make us store a lot of crap...
        self.store_payload(&digest, &payload).await?;
        Ok(())
    }

    pub async fn handle_request(&mut self, digest: Digest, sender: PublicKey) -> MempoolResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let payload = bincode::deserialize(&bytes)?;
            let message = NetMessage::Reply(payload, sender);
            if let Err(e) = self.network_channel.send(message).await {
                panic!("Failed to send message through network channel: {}", e);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NodeMempool for Core {
    async fn get(&mut self) -> Vec<u8> {
        self.queue.pop_back().map_or_else(Vec::new, |x| x.to_vec())
    }

    async fn verify(&mut self, digest: &[u8]) -> PayloadStatus {
        let bytes = digest.to_vec();
        match self.store.read(bytes.clone()).await {
            Ok(Some(_)) => PayloadStatus::Accept,
            Ok(None) => match digest.try_into() {
                Ok(digest) => {
                    debug!("Requesting sync for missing payload {:?}", digest);
                    let message = NetMessage::Request(digest, self.name);
                    if let Err(e) = self.network_channel.send(message).await {
                        panic!("Failed to send block through network channel: {}", e);
                    }
                    PayloadStatus::Wait(bytes)
                }
                Err(e) => {
                    warn!("Received invalid payload digest: {}", e);
                    PayloadStatus::Reject
                }
            },
            Err(e) => {
                error!("{}", e);
                PayloadStatus::Reject
            }
        }
    }

    async fn garbage_collect(&mut self, _payload: &[u8]) {}
}
