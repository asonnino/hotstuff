use crate::config::Export as _;
use crate::config::{Committee, Parameters, Secret};
use consensus::{Block, Consensus, ConsensusError};
use crypto::SignatureService;
use log::info;
use mempool::Payload;
use mempool::{MempoolError, SimpleMempool};
use store::{Store, StoreError};
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};

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
    store: Store,
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> Result<Self, NodeError> {
        let (tx_commit, rx_commit) = channel(1000);

        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Make the data store.
        let store = Store::new(store_path)?;

        // Run the signature service.
        let signature_service = SignatureService::new(secret_key);

        // Make a new mempool.
        let mempool = SimpleMempool::new(
            name,
            committee.mempool,
            parameters.mempool,
            signature_service.clone(),
            store.clone(),
        )?;

        // Run the consensus core.
        Consensus::run(
            name,
            committee.consensus,
            parameters.consensus,
            signature_service,
            store.clone(),
            mempool,
            /* commit_channel */ tx_commit,
        )
        .await?;

        info!("Node {} successfully booted", name);
        Ok(Self {
            commit: rx_commit,
            store,
        })
    }

    pub fn print_key_file(filename: &str) -> Result<(), NodeError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(block) = self.commit.recv().await {
            let id = format!("{}", block);

            // Skip empty blocks.
            if block.payload.is_empty() {
                continue;
            }

            // Load the payload from storage.
            let bytes = self
                .store
                .read(block.payload)
                .await
                .expect("Failed to read committed block from store")
                .unwrap_or_else(|| panic!("Payload missing from store"));

            // Deserialize the payload.
            let payload: Payload =
                bincode::deserialize(&bytes).expect("Failed to deserialize payload");

            #[cfg(feature = "benchmark")]
            for tx in payload.transactions {
                // Check if it contains a special transaction.
                if tx.windows(2).all(|x| x[0] == x[1]) && tx.contains(&5u8) {
                    info!("{} contains a special transaction", id);
                }
            }

            #[cfg(not(feature = "benchmark"))]
            info!("{} committed with {} txs", id, payload.transactions.len());
        }
    }
}
