use crate::error::{MempoolError, MempoolResult};
use crate::mempool::{ConsensusMessage, Parameters};
use crate::messages::{Payload, PayloadMaker, Transaction};
use crate::network::NetMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, Hash, PublicKey, SignatureService};
use futures::future::FutureExt as _;
use futures::select;
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Deserialize, Serialize, Debug)]
pub enum CoreMessage {
    Transaction(Transaction),
    Payload(Payload),
    SyncRequest(Digest, PublicKey),
}

pub struct Core {
    queue: VecDeque<Digest>,
    payload_maker: PayloadMaker,
    committee: Committee,
    name: PublicKey,
    parameters: Parameters,
    store: Store,
    network_channel: Sender<NetMessage>,
    core_channel: Receiver<CoreMessage>,
    consensus_channel: Receiver<ConsensusMessage>,
}

impl Core {
    pub fn new(
        committee: Committee,
        name: PublicKey,
        signature_service: SignatureService,
        parameters: Parameters,
        store: Store,
        network_channel: Sender<NetMessage>,
        core_channel: Receiver<CoreMessage>,
        consensus_channel: Receiver<ConsensusMessage>,
    ) -> Self {
        Self {
            queue: VecDeque::with_capacity(parameters.queue_capacity),
            payload_maker: PayloadMaker::new(name, signature_service, parameters.max_payload_size),
            committee,
            name,
            parameters,
            store,
            network_channel,
            core_channel,
            consensus_channel,
        }
    }

    async fn store_payload(&mut self, digest: &Digest, payload: &Payload) -> MempoolResult<()> {
        let key = digest.to_vec();
        let value = bincode::serialize(payload).expect("Failed to serialize payload");
        self.store
            .write(key, value)
            .await
            .map_err(MempoolError::from)
    }

    async fn transmit(
        &mut self,
        message: &CoreMessage,
        to: Option<PublicKey>,
    ) -> MempoolResult<()> {
        let addresses = if let Some(to) = to {
            vec![self.committee.address(&to)?]
        } else {
            self.committee.broadcast_addresses(&self.name)
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    async fn handle_transaction(&mut self, transaction: Transaction) -> MempoolResult<()> {
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
            let message = CoreMessage::Payload(payload);
            self.transmit(&message, None).await?;
        }
        Ok(())
    }

    async fn handle_payload(&mut self, payload: Payload) -> MempoolResult<()> {
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

    async fn handle_request(&mut self, digest: Digest, requestor: PublicKey) -> MempoolResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let payload = bincode::deserialize(&bytes)?;
            let message = CoreMessage::Payload(payload);
            self.transmit(&message, Some(requestor)).await?;
        }
        Ok(())
    }

    fn next_payload(&mut self) -> Option<Digest> {
        self.queue.pop_back()
    }

    async fn verify_payload(&mut self, digest: Digest) -> MempoolResult<bool> {
        match self.store.read(digest.to_vec()).await? {
            Some(_) => Ok(true),
            None => {
                debug!("Requesting sync for missing payload {:?}", digest);
                let message = CoreMessage::SyncRequest(digest, self.name);
                self.transmit(&message, None).await?;
                Ok(false)
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            select! {
                message = self.core_channel.recv().fuse() => {
                    if let Some(message) = message {
                        let result = match message {
                            CoreMessage::Transaction(tx) => self.handle_transaction(tx).await,
                            CoreMessage::Payload(payload) => self.handle_payload(payload).await,
                            CoreMessage::SyncRequest(digest, sender) => {
                                self.handle_request(digest, sender).await
                            }
                        };
                        match result {
                            Ok(()) => (),
                            Err(MempoolError::StoreError(e)) => error!("{}", e),
                            Err(MempoolError::SerializationError(e)) => error!("Store corrupted. {}", e),
                            Err(e) => warn!("{}", e),
                        }
                    }
                },
                message = self.consensus_channel.recv().fuse() => {
                    if let Some(message) = message {
                        match message {
                            ConsensusMessage::Get(sender) => {
                                let _ = sender.send(self.next_payload());
                            },
                            ConsensusMessage::Verify(digest, sender) => {
                                let result = self.verify_payload(digest).await;
                                match result {
                                    Ok(_) => (),
                                    Err(MempoolError::StoreError(ref e)) => error!("{}", e),
                                    Err(ref e) => warn!("{}", e),
                                }
                                let _ = sender.send(result);
                            }
                        }
                    }
                },
                complete => break,
            }
        }
    }
}
