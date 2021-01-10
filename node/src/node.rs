use config::Config as _;
use config::{Committee, Parameters, Secret};
use consensus::core::Core;
use consensus::error::{ConsensusError, ConsensusResult};
use consensus::leader::LeaderElector;
use consensus::messages::Block;
use consensus::network::{NetReceiver, NetSender};
use crypto::SignatureService;
use log::info;
use mempool::simple::SimpleMempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};

#[cfg(test)]
#[path = "tests/node_tests.rs"]
pub mod node_tests;

pub struct Node;

impl Node {
    pub async fn make(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> ConsensusResult<Receiver<Block>> {
        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;

        // Retrieve node's information.
        let name = secret.name;
        let secret_key = secret.secret;
        let mut address = committee.address(&name)?;
        address.set_ip("0.0.0.0".parse().unwrap());

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path)?;

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Choose the mempool and leader election algorithm.
        let mempool = SimpleMempool::new();
        let leader_elector = LeaderElector::new(committee.clone());

        // Create the commit channel from which we can read the sequence of
        // committed blocks.
        let (tx_commit, rx_commit) = channel(1000);

        // Now wire together the network sender, core, and network receiver.
        let network_channel = NetSender::make(name, committee.clone());
        let core_channel = Core::make(
            name,
            committee,
            parameters,
            store,
            signature_service,
            leader_elector,
            mempool,
            network_channel,
            tx_commit,
        )
        .await;
        let () = NetReceiver::make(&address, core_channel).await;

        // Return the commit receiver.
        info!("Node {:?} successfully booted on {}", name, address);
        Ok(rx_commit)
    }

    pub fn print_key_file(filename: &str) -> ConsensusResult<()> {
        Secret::new().write(filename).map_err(ConsensusError::from)
    }
}
