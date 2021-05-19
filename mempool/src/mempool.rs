use crate::config::{Committee, Parameters};
use crate::core::Core;
use crate::error::MempoolResult;
use crate::front::Front;
use crate::payload::PayloadMaker;
use crate::synchronizer::Synchronizer;
use consensus::{ConsensusMempoolMessage, ConsensusMessage};
use crypto::{PublicKey, SignatureService};
use log::info;
use network::{NetReceiver, NetSender};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/mempool_tests.rs"]
pub mod mempool_tests;

pub struct Mempool;

impl Mempool {
    pub fn run(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        signature_service: SignatureService,
        consensus_channel: Sender<ConsensusMessage>,
        consensus_mempool_channel: Receiver<ConsensusMempoolMessage>,
    ) -> MempoolResult<()> {
        info!(
            "Mempool queue capacity set to {} payloads",
            parameters.queue_capacity
        );
        info!(
            "Mempool max payload size set to {} B",
            parameters.max_payload_size
        );
        info!(
            "Mempool min block delay set to {} ms",
            parameters.min_block_delay
        );

        let (tx_network, rx_network) = channel(10000);
        let (tx_core, rx_core) = channel(10000);
        let (tx_client, rx_client) = channel(10000);

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
        let network_receiver = NetReceiver::new(address, tx_core.clone());
        tokio::spawn(async move {
            network_receiver.run().await;
        });

        let mut network_sender = NetSender::new(rx_network);
        tokio::spawn(async move {
            network_sender.run().await;
        });

        // Build and run the synchronizer.
        let synchronizer = Synchronizer::new(
            consensus_channel,
            store.clone(),
            name,
            committee.clone(),
            tx_network.clone(),
            parameters.sync_retry_delay,
        );

        // Build and run the payload maker.
        let payload_maker = PayloadMaker::new(
            name,
            signature_service,
            parameters.max_payload_size,
            parameters.min_block_delay,
            rx_client,
            tx_core,
        );

        // Run the core.
        let mut core = Core::new(
            name,
            committee,
            parameters,
            store,
            synchronizer,
            payload_maker,
            /* core_channel */ rx_core,
            consensus_mempool_channel,
            /* network_channel */ tx_network,
        );
        tokio::spawn(async move {
            core.run().await;
        });

        Ok(())
    }
}
