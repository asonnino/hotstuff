use crate::core::Core;
use crate::leader::LeaderElector;
use crate::mempool::NodeMempool;
use crate::messages::Block;
use crate::network::{NetReceiver, NetSender};
use config::{Committee, Parameters};
use crypto::{PublicKey, SignatureService};
use store::Store;
use tokio::sync::mpsc::Sender;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

pub struct Consensus;

impl Consensus {
    pub async fn run<Mempool: 'static + NodeMempool>(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        signature_service: SignatureService,
        mempool: Mempool,
        commit_channel: Sender<Block>,
    ) {
        let leader_elector = LeaderElector::new(committee.clone());

        let mut address = committee
            .address(&name)
            .expect("Our own public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());

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
            commit_channel,
        )
        .await;
        let () = NetReceiver::make(&address, core_channel).await;
    }
}
