use crate::core::Core;
use crate::error::MempoolResult;
use crate::network::{NetReceiver, NetSender};
use async_trait::async_trait;
use config::Committee;
use consensus::mempool::{NodeMempool, PayloadStatus};
use crypto::{Digest, PublicKey, SignatureService};
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

pub struct Parameters {
    pub queue_capacity: usize,
    pub max_payload_size: usize,
}

#[derive(Debug)]
pub enum ConsensusMessage {
    Get(oneshot::Sender<Option<Digest>>),
    Verify(Digest, oneshot::Sender<MempoolResult<bool>>),
}

pub struct SimpleMempool {
    channel: Sender<ConsensusMessage>,
}

impl SimpleMempool {
    pub fn new(
        committee: Committee,
        name: PublicKey,
        signature_service: SignatureService,
        parameters: Parameters,
        store: Store,
    ) -> Self {
        let (tx_network, rx_network) = channel(1000);
        let (tx_core, rx_core) = channel(1000);
        let (tx_consensus, rx_consensus) = channel(1000);
        let (tx_client, rx_client) = channel(1000);

        // TODO: Better committee structure: all mod use the
        // same port.
        let mut address = committee
            .address(&name)
            .expect("Our own public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());

        // Run the mempool network receiver.
        let network_receiver = NetReceiver::new(address.clone(), tx_core);
        tokio::spawn(async move {
            network_receiver.run().await;
        });

        // Run the mempool network sender.
        let mut network_sender = NetSender::new(rx_network);
        tokio::spawn(async move {
            network_sender.run().await;
        });

        // Run the front end that receives client transactions.
        let front = NetReceiver::new(address, tx_client);
        tokio::spawn(async move {
            front.run().await;
        });

        // Run the core.
        let mut core = Core::new(
            committee,
            name,
            signature_service,
            parameters,
            store,
            tx_network,
            rx_core,
            rx_consensus,
            rx_client,
        );
        tokio::spawn(async move {
            core.run().await;
        });

        Self {
            channel: tx_consensus,
        }
    }
}

#[async_trait]
impl NodeMempool for SimpleMempool {
    async fn get(&mut self) -> Vec<u8> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMessage::Get(sender);
        self.channel
            .send(message)
            .await
            .expect("Consensus channel closed");
        receiver
            .await
            .expect("Failed to receive payload from core")
            .map_or_else(Vec::new, |x| x.to_vec())
    }

    async fn verify(&mut self, digest: &[u8]) -> PayloadStatus {
        let (sender, receiver) = oneshot::channel();
        let message = match digest.try_into() {
            Ok(x) => ConsensusMessage::Verify(x, sender),
            Err(_) => return PayloadStatus::Reject,
        };
        self.channel
            .send(message)
            .await
            .expect("Consensus channel closed");
        match receiver.await.expect("Failed to receive payload from core") {
            Ok(true) => PayloadStatus::Accept,
            Ok(false) => PayloadStatus::Wait(digest.to_vec()),
            Err(_) => PayloadStatus::Reject,
        }
    }

    async fn garbage_collect(&mut self, _payload: &[u8]) {}
}
