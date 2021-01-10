use crypto::{generate_keypair, generate_production_keypair, PublicKey, SecretKey};
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use thiserror::Error;

#[cfg(test)]
#[path = "tests/config_tests.rs"]
pub mod config_tests;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },

    #[error("Node {0:?} is not in the committee")]
    NotInCommittee(PublicKey),
}

pub type Stake = u32;
pub type EpochNumber = u128;

pub trait Config: Serialize + DeserializeOwned {
    fn read(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    pub timeout_delay: u64,
    pub sync_retry_delay: u64,
}

impl Config for Parameters {}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 10_000,
            sync_retry_delay: 10_000,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Secret {
    pub name: PublicKey,
    pub secret: SecretKey,
}

impl Secret {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Config for Secret {}

impl Default for Secret {
    fn default() -> Self {
        let mut rng = StdRng::from_seed([0; 32]);
        let (name, secret) = generate_keypair(&mut rng);
        Self { name, secret }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub name: PublicKey,
    pub stake: Stake,
    pub address: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    epoch: EpochNumber,
}

impl Committee {
    pub fn new(authorities: &[(PublicKey, Stake)], epoch: EpochNumber) -> Self {
        let authorities = authorities
            .iter()
            .enumerate()
            .map(|(i, (name, stake))| {
                let authority = Authority {
                    name: *name,
                    stake: *stake,
                    address: format!("127.0.0.1:{}", i).parse().unwrap(),
                };
                (*name, authority)
            })
            .collect();
        Self { authorities, epoch }
    }

    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    fn total_votes(&self) -> Stake {
        self.authorities.values().map(|x| x.stake).sum()
    }

    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        2 * self.total_votes() / 3 + 1
    }

    pub fn address(&self, name: &PublicKey) -> Result<SocketAddr, ConfigError> {
        self.authorities
            .get(name)
            .map(|x| x.address)
            .ok_or_else(|| ConfigError::NotInCommittee(*name))
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<SocketAddr> {
        self.authorities
            .values()
            .filter(|x| x.name != *myself)
            .map(|x| x.address)
            .collect()
    }
}

impl Config for Committee {}
