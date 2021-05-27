use crate::error::{ConsensusError, ConsensusResult};
use crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

pub type Stake = u32;
pub type EpochNumber = u128;

#[derive(Serialize, Deserialize)]
pub enum Protocol {
    HotStuff,
    AsyncHotStuff,
    TwoChainVABA,
    Others,
}

impl Default for Protocol {
    fn default() -> Self {
        Protocol::HotStuff
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Parameters {
    pub timeout_delay: u64,
    pub sync_retry_delay: u64,
    pub network_delay: u64,
    pub max_payload_size: usize,
    pub min_block_delay: u64,
    pub ddos: bool,
    pub exp: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 5000,
            sync_retry_delay: 10_000,
            min_block_delay: 100,
            network_delay: 100,
            max_payload_size: 500,
            ddos: false,
            exp: 1,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Authority {
    pub name: PublicKey,
    pub id: usize,  // id of the node in the tss public key share set
    pub stake: Stake,
    pub address: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    pub epoch: EpochNumber,
}

impl Committee {
    pub fn new(info: Vec<(PublicKey, usize, Stake, SocketAddr)>, epoch: EpochNumber) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, id, stake, address)| {
                    let authority = Authority {
                        name,
                        id,
                        stake,
                        address,
                    };
                    (name, authority)
                })
                .collect(),
            epoch,
        }
    }

    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    pub fn id(&self, name: PublicKey) -> usize {
        self.authorities.get(&name).unwrap().id
    }

    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * ((total_votes - 1) / 3) + 1
    }

    pub fn large_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        total_votes
    }

    pub fn random_coin_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes - 1) / 3 + 1
    }

    pub fn address(&self, name: &PublicKey) -> ConsensusResult<SocketAddr> {
        self.authorities
            .get(name)
            .map(|x| x.address)
            .ok_or_else(|| ConsensusError::NotInCommittee(*name))
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<SocketAddr> {
        self.authorities
            .values()
            .filter(|x| x.name != *myself)
            .map(|x| x.address)
            .collect()
    }
}
