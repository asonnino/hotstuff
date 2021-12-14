use crypto::PublicKey;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Deserialize, Serialize)]
pub struct Parameters {
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            gc_depth: 50,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,
        }
    }
}

impl Parameters {
    pub fn log(&self) {
        // NOTE: These log entries are used to compute performance.
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);
    }
}

pub type EpochNumber = u128;
pub type Stake = u32;

#[derive(Clone, Deserialize, Serialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// Address to receive client transactions.
    pub transactions_address: SocketAddr,
    /// Address to receive messages from other nodes.
    pub mempool_address: SocketAddr,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    pub epoch: EpochNumber,
}

impl Committee {
    pub fn new(info: Vec<(PublicKey, Stake, SocketAddr, SocketAddr)>, epoch: EpochNumber) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, stake, transactions_address, mempool_address)| {
                    let authority = Authority {
                        stake,
                        transactions_address,
                        mempool_address,
                    };
                    (name, authority)
                })
                .collect(),
            epoch,
        }
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the address to receive client transactions.
    pub fn transactions_address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.transactions_address)
    }

    /// Returns the mempool addresses of a specific node.
    pub fn mempool_address(&self, name: &PublicKey) -> Option<SocketAddr> {
        self.authorities.get(name).map(|x| x.mempool_address)
    }

    /// Returns the mempool addresses of all nodes except `myself`.
    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<(PublicKey, SocketAddr)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, x)| (*name, x.mempool_address))
            .collect()
    }
}
