use crate::config::{Committee, Parameters};
use crate::error::{MempoolError, MempoolResult};
use crate::mempool::ConsensusMessage;
use crate::messages::{Payload, PayloadMaker, Transaction};
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::{debug, error, info, warn};
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

#[derive(Deserialize, Serialize, Debug)]
pub enum CoreMessage {
    Payload(Payload),
    PayloadRequest(Digest, PublicKey),
}

pub struct Core {
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    core_channel: Receiver<CoreMessage>,
    consensus_channel: Receiver<ConsensusMessage>,
    client_channel: Receiver<Transaction>,
    network_channel: Sender<NetMessage>,
    queue: HashSet<Digest>,
    payload_maker: PayloadMaker,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        core_channel: Receiver<CoreMessage>,
        consensus_channel: Receiver<ConsensusMessage>,
        client_channel: Receiver<Transaction>,
        network_channel: Sender<NetMessage>,
    ) -> Self {
        let queue = HashSet::with_capacity(parameters.queue_capacity);
        let payload_maker = PayloadMaker::new(name, signature_service, parameters.max_payload_size);
        info!("Max payload size: {} B", parameters.max_payload_size);
        Self {
            name,
            committee,
            parameters,
            store,
            core_channel,
            consensus_channel,
            client_channel,
            network_channel,
            queue,
            payload_maker,
        }
    }

    async fn store_payload(&mut self, key: Vec<u8>, payload: &Payload) -> MempoolResult<()> {
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
            debug!("Sending {:?} to {}", message, to);
            vec![self.committee.mempool_address(&to)?]
        } else {
            debug!("Broadcasting {:?}", message);
            self.committee.broadcast_addresses(&self.name)
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    async fn process_own_payload(
        &mut self,
        digest: &Digest,
        payload: Payload,
    ) -> MempoolResult<()> {
        let bytes = digest.to_vec();

        #[cfg(feature = "benchmark")]
        info!(
            "Payload {} contains {} B",
            base64::encode(&bytes),
            payload.size()
        );

        #[cfg(feature = "benchmark")]
        if payload.sample_txs > 0 {
            info!(
                "Payload {} contains {} sample tx(s)",
                base64::encode(&bytes),
                payload.sample_txs
            );
        }

        // Wait for the minimum block delay.
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;

        // Store the payload.
        self.store_payload(bytes, &payload).await?;

        // Share the payload with all other nodes.
        let message = CoreMessage::Payload(payload);
        self.transmit(&message, None).await
    }

    async fn handle_transaction(&mut self, transaction: Transaction) -> MempoolResult<()> {
        // Drop the transaction if our mempool is full.
        ensure!(
            self.queue.len() < self.parameters.queue_capacity,
            MempoolError::MempoolFull
        );

        // Otherwise, try to add the transaction to the next payload
        // we will add to the queue.
        if let Some(payload) = self.payload_maker.add(transaction).await {
            let digest = payload.digest();
            self.process_own_payload(&digest, payload).await?;
            self.queue.insert(digest);
        }
        Ok(())
    }

    async fn handle_payload(&mut self, payload: Payload) -> MempoolResult<()> {
        // Ensure the author of the payload is in the committee.
        let author = payload.author;
        ensure!(
            self.committee.exists(&author),
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
        // TODO [issue #18]: A bad node may make us store a lot of junk. There is no
        // limit to how many payloads they can send us, and we will store them all.
        self.store_payload(digest.to_vec(), &payload).await?;

        // Add the payload to the queue.
        self.queue.insert(digest);
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

    async fn get_payload(&mut self, max: usize) -> MempoolResult<Vec<Digest>> {
        if self.queue.is_empty() {
            let payload = self.payload_maker.make().await;
            if payload.size() == 0 {
                return Ok(Vec::new());
            }
            let digest = payload.digest();
            self.process_own_payload(&digest, payload).await?;
            return Ok(vec![digest]);
        }
        let digests = self.queue.iter().take(max / 32).cloned().collect();
        for x in &digests {
            self.queue.remove(x);
        }
        Ok(digests)
    }

    async fn verify_payload(&mut self, digests: Vec<Digest>) -> MempoolResult<Vec<Digest>> {
        let mut missing = Vec::new();
        for digest in digests {
            if self.store.read(digest.to_vec()).await?.is_none() {
                debug!(
                    "Requesting sync for payload {}",
                    base64::encode(&digest.to_vec())
                );
                let message = CoreMessage::PayloadRequest(digest.clone(), self.name);
                self.transmit(&message, None).await?;
                missing.push(digest);
            }
        }
        Ok(missing)
    }

    fn cleanup(&mut self, digests: Vec<Digest>) {
        for x in &digests {
            self.queue.remove(x);
        }
    }

    pub async fn run(&mut self) {
        let log = |result: Result<&(), &MempoolError>| match result {
            Ok(()) => (),
            Err(MempoolError::StoreError(e)) => error!("{}", e),
            Err(MempoolError::SerializationError(e)) => error!("Store corrupted. {}", e),
            Err(e) => warn!("{}", e),
        };

        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        CoreMessage::Payload(payload) => self.handle_payload(payload).await,
                        CoreMessage::PayloadRequest(digest, sender) => self.handle_request(digest, sender).await,
                    }
                },
                Some(message) = self.consensus_channel.recv() => {
                    match message {
                        ConsensusMessage::Get(max, sender) => {
                            let result = self.get_payload(max).await;
                            log(result.as_ref().map(|_| &()));
                            let _ = sender.send(result);
                        },
                        ConsensusMessage::Verify(digests, sender) => {
                            let result = self.verify_payload(digests).await;
                            log(result.as_ref().map(|_| &()));
                            let _ = sender.send(result);
                        },
                        ConsensusMessage::Cleanup(digests) => self.cleanup(digests),
                    }
                    Ok(())
                },
                Some(tx) = self.client_channel.recv() => self.handle_transaction(tx).await,
                else => break,
            };
            log(result.as_ref());
        }
    }
}
