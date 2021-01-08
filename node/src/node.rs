use crate::config::Config as _;
use crate::config::{Committee, Parameters, Secret};
use crate::core::Core;
use crate::crypto::SignatureService;
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MockMempool;
use crate::messages::Block;
use crate::network::{NetReceiver, NetSender};
use crate::store::Store;
use log::info;
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
        let address = match committee.address(&name) {
            Some(mut address) => {
                address.set_ip("0.0.0.0".parse().unwrap());
                address
            }
            None => bail!(ConsensusError::ConfigError(
                committee_file.to_string(),
                "Node name is not in the committee".to_string()
            )),
        };

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
        let mempool = MockMempool::new();
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
        Secret::new().write(filename)
    }
}
