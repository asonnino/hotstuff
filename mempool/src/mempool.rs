use crate::config::Config;
use crate::core::Core;
use crate::error::MempoolResult;
use crate::network::{NetReceiver, NetSender};
use async_trait::async_trait;
use consensus::mempool::{NodeMempool, PayloadStatus};
use crypto::{Digest, SignatureService};
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/mempool_tests.rs"]
pub mod mempool_tests;

#[derive(Debug)]
pub enum ConsensusMessage {
    Get(oneshot::Sender<MempoolResult<Digest>>),
    Verify(Digest, oneshot::Sender<MempoolResult<bool>>),
}

pub struct SimpleMempool {
    channel: Sender<ConsensusMessage>,
}

impl SimpleMempool {
    pub fn new(config: Config, signature_service: SignatureService, store: Store) -> Self {
        let (tx_network, rx_network) = channel(1000);
        let (tx_core, rx_core) = channel(1000);
        let (tx_consensus, rx_consensus) = channel(1000);
        let (tx_client, rx_client) = channel(1000);

        // Run the front end that receives client transactions.
        let mut address = config
            .committee
            .front_address(&config.name)
            .expect("Our own public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());

        let front = NetReceiver::new(address, tx_client);
        tokio::spawn(async move {
            front.run().await;
        });

        // Run the mempool network sender and receiver.
        let mut address = config
            .committee
            .mempool_address(&config.name)
            .expect("Our own public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        let network_receiver = NetReceiver::new(address, tx_core);
        tokio::spawn(async move {
            network_receiver.run().await;
        });

        let mut network_sender = NetSender::new(rx_network);
        tokio::spawn(async move {
            network_sender.run().await;
        });

        // Run the core.
        let mut core = Core::new(
            config,
            signature_service,
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
            .unwrap_or_else(|_| Digest::default())
            .to_vec()
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
