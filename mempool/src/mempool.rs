use crate::config::{Committee, Parameters};
use crate::core::Core;
use crate::error::MempoolResult;
use crate::front::Front;
use async_trait::async_trait;
use consensus::{NodeMempool, PayloadStatus};
use crypto::{Digest, PublicKey, SignatureService};
use network::{NetReceiver, NetSender};
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/mempool_tests.rs"]
pub mod mempool_tests;

#[derive(Debug)]
pub enum ConsensusMessage {
    Get(oneshot::Sender<MempoolResult<Vec<Digest>>>),
    Verify(Vec<Digest>, oneshot::Sender<MempoolResult<Vec<Digest>>>),
}

pub struct Mempool {
    channel: Sender<ConsensusMessage>,
}

impl Mempool {
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
    ) -> MempoolResult<Self> {
        let (tx_network, rx_network) = channel(1000);
        let (tx_core, rx_core) = channel(1000);
        let (tx_consensus, rx_consensus) = channel(1000);
        let (tx_client, rx_client) = channel(1000);

        // Run the front end that receives client transactions.
        let address = committee.front_address(&name).map(|mut x| {
            x.set_ip("0.0.0.0".parse().unwrap());
            x
        })?;

        let front = Front::new(address, tx_client);
        tokio::spawn(async move {
            front.run().await;
        });

        // Run the mempool network sender and receiver.
        let address = committee.mempool_address(&name).map(|mut x| {
            x.set_ip("0.0.0.0".parse().unwrap());
            x
        })?;
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
            name,
            committee,
            parameters,
            signature_service,
            store,
            /* core_channel */ rx_core,
            /* consensus_channel */ rx_consensus,
            /* client_channel */ rx_client,
            /* network_channel */ tx_network,
        );
        tokio::spawn(async move {
            core.run().await;
        });

        Ok(Self {
            channel: tx_consensus,
        })
    }
}

#[async_trait]
impl NodeMempool for Mempool {
    async fn get(&mut self) -> Vec<Vec<u8>> {
        let (sender, receiver) = oneshot::channel();
        let message = ConsensusMessage::Get(sender);
        self.channel
            .send(message)
            .await
            .expect("Consensus channel closed");
        receiver
            .await
            .expect("Failed to receive payload from core")
            .unwrap_or_default()
            .iter()
            .map(|x| x.to_vec())
            .collect()
    }

    async fn verify(&mut self, payload: &[Vec<u8>]) -> PayloadStatus {
        if payload.is_empty() {
            return PayloadStatus::Accept;
        }

        let (sender, receiver) = oneshot::channel();
        let message = match payload.iter().map(|x| x[..].try_into()).collect() {
            Ok(x) => ConsensusMessage::Verify(x, sender),
            Err(_) => return PayloadStatus::Reject,
        };
        self.channel
            .send(message)
            .await
            .expect("Consensus channel closed");
        match receiver.await.expect("Failed to receive payload from core") {
            Ok(missing) if missing.is_empty() => PayloadStatus::Accept,
            Ok(missing) => PayloadStatus::Wait(missing.iter().map(|x| x.to_vec()).collect()),
            Err(_) => PayloadStatus::Reject,
        }
    }

    async fn garbage_collect(&mut self, _payload: &[Vec<u8>]) {}
}
