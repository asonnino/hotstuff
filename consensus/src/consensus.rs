use crate::config::{Committee, Parameters};
use crate::core::{ConsensusMessage, Core};
use crate::error::ConsensusResult;
use crate::leader::LeaderElector;
use crate::mempool::{MempoolDriver};
use crate::messages::Block;
use crate::synchronizer::Synchronizer;
use crypto::{PublicKey, SignatureService};
use log::info;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

pub struct Consensus;

/*
impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        signature_service: SignatureService,
        tx_core: Sender<ConsensusMessage>,
        rx_core: Receiver<ConsensusMessage>,
        tx_consensus_mempool: Sender<ConsensusMempoolMessage>,
        tx_commit: Sender<Block>,
    ) -> ConsensusResult<()> {
        // NOTE: The following log entries are used to compute performance.
        info!(
            "Consensus timeout delay set to {} ms",
            parameters.timeout_delay
        );
        info!(
            "Consensus synchronizer retry delay set to {} ms",
            parameters.sync_retry_delay
        );
        info!(
            "Consensus max payload size set to {} B",
            parameters.max_payload_size
        );
        info!(
            "Consensus min block delay set to {} ms",
            parameters.min_block_delay
        );

        let (tx_network, rx_network) = channel(1_000);

        // Make the network sender and receiver.
        let address = committee.address(&name).map(|mut x| {
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

        // The leader elector algorithm.
        let leader_elector = LeaderElector::new(committee.clone());

        // Make the mempool driver which will mediate our requests to the mempool.
        let mempool_driver = MempoolDriver::new(tx_consensus_mempool);

        // Make the synchronizer. This instance runs in a background thread
        // and asks other nodes for any block that we may be missing.
        let synchronizer = Synchronizer::new(
            name,
            committee.clone(),
            store.clone(),
            /* network_channel */ tx_network.clone(),
            /* core_channel */ tx_core,
            parameters.sync_retry_delay,
        )
        .await;

        let mut core = Core::new(
            name,
            committee,
            parameters,
            signature_service,
            store,
            leader_elector,
            mempool_driver,
            synchronizer,
            /* core_channel */ rx_core,
            /* network_channel */ tx_network,
            /* commit_channel */ tx_commit,
        );
        tokio::spawn(async move {
            core.run().await;
        });

        Ok(())
    }
}
*/
