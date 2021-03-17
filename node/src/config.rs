use crate::node::NodeError;
use consensus::{Committee as ConsensusCommittee, Parameters as ConsensusParameters};
use crypto::{generate_keypair, generate_production_keypair, PublicKey, SecretKey, SecretShare};
use mempool::{Committee as MempoolCommittee, Parameters as MempoolParameters};
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;

pub trait Export: Serialize + DeserializeOwned {
    fn read(path: &str) -> Result<Self, NodeError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| NodeError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), NodeError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| NodeError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Parameters {
    pub consensus: ConsensusParameters,
    pub mempool: MempoolParameters,
    pub protocol: u8,
}

impl Export for Parameters {}

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

impl Export for Secret {}

impl Default for Secret {
    fn default() -> Self {
        let mut rng = StdRng::from_seed([0; 32]);
        let (name, secret) = generate_keypair(&mut rng);
        Self { name, secret }
    }
}

impl Export for SecretShare {}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Committee {
    pub consensus: ConsensusCommittee,
    pub mempool: MempoolCommittee,
}

impl Export for Committee {}
