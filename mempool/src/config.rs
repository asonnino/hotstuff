use crate::error::{MempoolError, MempoolResult};
use crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    pub queue_capacity: usize,
    pub sync_retry_delay: u64,
    pub max_payload_size: usize,
    pub min_block_delay: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            sync_retry_delay: 10_000,
            max_payload_size: 100_000,
            min_block_delay: 100,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Authority {
    pub name: PublicKey,
    pub front_address: SocketAddr,
    pub mempool_address: SocketAddr,
}

pub type EpochNumber = u128;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    pub epoch: EpochNumber,
}

impl Committee {
    pub fn new(info: Vec<(PublicKey, SocketAddr, SocketAddr)>, epoch: EpochNumber) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, front_address, mempool_address)| {
                    let authority = Authority {
                        name,
                        front_address,
                        mempool_address,
                    };
                    (name, authority)
                })
                .collect(),
            epoch,
        }
    }

    pub fn exists(&self, name: &PublicKey) -> bool {
        self.authorities.contains_key(name)
    }

    pub fn front_address(&self, name: &PublicKey) -> MempoolResult<SocketAddr> {
        self.authorities
            .get(name)
            .map(|x| x.front_address)
            .ok_or_else(|| MempoolError::NotInCommittee(*name))
    }

    pub fn mempool_address(&self, name: &PublicKey) -> MempoolResult<SocketAddr> {
        self.authorities
            .get(name)
            .map(|x| x.mempool_address)
            .ok_or_else(|| MempoolError::NotInCommittee(*name))
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<SocketAddr> {
        self.authorities
            .values()
            .filter(|x| x.name != *myself)
            .map(|x| x.mempool_address)
            .collect()
    }
}
