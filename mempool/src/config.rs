use crate::error::{MempoolError, MempoolResult};
use crypto::PublicKey;
use std::collections::HashMap;
use std::net::SocketAddr;

pub struct Config {
    pub name: PublicKey,
    pub committee: Committee,
    pub parameters: Parameters,
}

pub struct Parameters {
    pub queue_capacity: usize,
    pub max_payload_size: usize,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            max_payload_size: 100_000,
        }
    }
}

#[derive(Clone)]
pub struct Authority {
    pub name: PublicKey,
    pub front_address: SocketAddr,
    pub mempool_address: SocketAddr,
}

pub type EpochNumber = u128;

#[derive(Clone)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    epoch: EpochNumber,
}

impl Committee {
    pub fn new(authorities: &[PublicKey], epoch: EpochNumber) -> Self {
        let authorities = authorities
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let (front_port, mempool_port) = (i, i + authorities.len());
                let authority = Authority {
                    name: *name,
                    front_address: format!("127.0.0.1:{}", front_port).parse().unwrap(),
                    mempool_address: format!("127.0.0.1:{}", mempool_port).parse().unwrap(),
                };
                (*name, authority)
            })
            .collect();
        Self { authorities, epoch }
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
