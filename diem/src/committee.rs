use crate::crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;

pub type Stake = u32;
pub type EpochNumber = u128;

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

    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        (self.total_votes() + 2) / 3
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

    /// Create a new committee by reading it from file.
    pub fn read(path: &str) -> Result<Self, std::io::Error> {
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(data.as_slice())?)
    }

    /// Write the committee to a json file.
    pub fn write(&self, path: &str) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let data = serde_json::to_string_pretty(self).unwrap();
        writer.write_all(data.as_ref())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    #[cfg(test)]
    pub fn new(names: Vec<PublicKey>, base_port: u16) -> Self {
        let authorities = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let authority = Authority {
                    name: *name,
                    stake: 1,
                    host: "127.0.0.1".to_string(),
                    port: base_port + i as u16,
                };
                (*name, authority)
            })
            .collect();
        Self {
            authorities,
            epoch: 1,
        }
    }
}
