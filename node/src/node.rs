use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Block, Consensus, ConsensusError, Protocol};
use crypto::{SignatureService, SecretShare};
use log::{info, warn};
use mempool::{Mempool, MempoolError};
use store::{Store, StoreError};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use threshold_crypto::SecretKeySet;
use threshold_crypto::serde_impl::SerdeSecret;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },

    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error(transparent)]
    ConsensusError(#[from] ConsensusError),

    #[error(transparent)]
    MempoolError(#[from] MempoolError),
}

pub struct Node {
    pub commit: Receiver<Block>,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        tss_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> Result<Self, NodeError> {
        let (tx_commit, rx_commit) = channel(10000);
        let (tx_consensus, rx_consensus) = channel(10000);
        let (tx_consensus_mempool, rx_consensus_mempool) = channel(10000);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        info!("committee {:?}", committee);
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;
        let tss_keys = SecretShare::read(tss_file)?;
        let pk_set = tss_keys.pkset.clone();

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path)?;

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key, Some(tss_keys.secret.into_inner()));

        let protocol = match parameters.protocol {
            0 => Protocol::HotStuff,
            1 => Protocol::AsyncHotStuff,
            2 => Protocol::TwoChainVABA,
            _ => {
                warn!("Undefined protocol type!");
                Protocol::Others
            }
        };

        // Make a new mempool.
        Mempool::run(
            name,
            committee.mempool,
            parameters.mempool,
            store.clone(),
            signature_service.clone(),
            tx_consensus.clone(),
            rx_consensus_mempool,
        )?;

        // Run the consensus core.
        Consensus::run(
            name,
            committee.consensus,
            parameters.consensus,
            store.clone(),
            signature_service,
            pk_set,
            tx_consensus,
            rx_consensus,
            tx_consensus_mempool,
            tx_commit,
            protocol,
        )
        .await?;

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit })
    }

    pub fn print_key_file(filename: &str) -> Result<(), NodeError> {
        Secret::new().write(filename)
    }

    // Print the threshold signature keys to the corresponding files
    pub fn print_threshold_key_file(filenames: Vec<&str>) -> Result<(), NodeError> {
        let size = filenames.len();
        let threshold = (size - 1) / 3; // The threshold for TSS is f
        let mut rng = rand::thread_rng();
        let sk_set = SecretKeySet::random(threshold, &mut rng);
        let pk_set = sk_set.public_keys();
        
        for id in 0..size {
            let sk_share = sk_set.secret_key_share(id);
            let pk_share = pk_set.public_key_share(id);
            SecretShare::new(id, pk_share, SerdeSecret(sk_share.clone()), pk_set.clone()).write(filenames[id])?;
        }
        return Ok(());
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // This is where we can further process committed block.
        }
    }
}
