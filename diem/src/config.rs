use crate::crypto::{generate_production_keypair, PublicKey, SecretKey};
use crate::error::{DiemError, DiemResult};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;

#[cfg(test)]
#[path = "tests/config_tests.rs"]
pub mod config_tests;

pub type Stake = u32;
pub type EpochNumber = u128;

pub trait Config: Serialize + DeserializeOwned {
    fn read(path: &str) -> DiemResult<Self> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| DiemError::ConfigError(path.to_string(), e.to_string()))
    }

    fn write(&self, path: &str) -> DiemResult<()> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| DiemError::ConfigError(path.to_string(), e.to_string()))
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
            timeout_delay: 1_000,
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub name: PublicKey,
    pub stake: Stake,
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    epoch: EpochNumber,
}

impl Committee {
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

    pub fn address(&self, name: &PublicKey) -> Option<String> {
        self.authorities
            .get(name)
            .map(|x| format!("{}:{}", x.host, x.port))
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<String> {
        self.authorities
            .values()
            .filter(|x| x.name != *myself)
            .map(|x| format!("{}:{}", x.host, x.port))
            .collect()
    }
}

impl Config for Committee {}
