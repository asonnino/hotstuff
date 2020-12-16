use serde::{Deserialize, Serialize};
//use std::fs::{self, OpenOptions};
//use std::io::{BufWriter, Write};
use crate::crypto::PublicKey;

pub type Stake = u32;

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub name: PublicKey,
    pub stake: Stake,
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    authorities: Vec<Authority>,
}

impl Committee {
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        for auth in &self.authorities {
            if *name == auth.name {
                return auth.stake;
            }
        }
        0
    }

    fn total_votes(&self) -> Stake {
        self.authorities.iter().map(|x| x.stake).sum()
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

    /*
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

    /// Create a test committee with equal stake and 1 worker per primary.
    pub fn debug_new(nodes: Vec<NodeID>, base_port: u16) -> Self {
        let instance_id = 100;
        let mut rng = StdRng::from_seed([0; 32]);
        let authorities = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let primary = Machine {
                    name: *node,
                    host: "127.0.0.1".to_string(),
                    port: base_port + i as u16,
                };
                let worker = Machine {
                    name: get_keypair(&mut rng).0,
                    host: "127.0.0.1".to_string(),
                    port: base_port + (i + nodes.len()) as u16,
                };
                Authority {
                    primary,
                    workers: vec![(0, worker)],
                    stake: 1,
                }
            })
            .collect();
        Self {
            authorities,
            instance_id,
        }
    }
    */
}
