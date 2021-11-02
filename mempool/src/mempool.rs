use crate::{
    aggregator::{AggregatorService, BatchCertificate},
    batch_maker::{BatchMaker, Transaction},
    certificate_verifier::CertificateVerifier,
    coded_batch::{AuthenticatedShard, CodedBatch},
    config::{Committee, Parameters},
    voter::{BatchVote, NodesVoter, SelfVoter, SerializedShard},
};
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use log::{info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

//#[cfg(test)]
//#[path = "tests/mempool_tests.rs"]
//pub mod mempool_tests;

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

/// The message exchanged between the nodes' mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum MempoolMessage {
    CodedBatch(CodedBatch),
    AuthenticatedShard(AuthenticatedShard),
    BatchVote(BatchVote),
    BatchCertificate(BatchCertificate),
}

/// The messages sent by the consensus and the mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConsensusMempoolMessage {
    /// The consensus notifies the mempool that it need to sync the target
    /// missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The consensus notifies the mempool of a round update.
    Cleanup(Round),
}

pub struct Mempool;

impl Mempool {
    pub fn spawn(
        // The public key of this authority.
        name: PublicKey,
        // The committee information.
        committee: Committee,
        // The service signing digests.
        signature_service: SignatureService,
        // The configuration parameters.
        parameters: Parameters,
        // The persistent storage.
        store: Store,
        // Receives messages from consensus.
        rx_consensus: Receiver<ConsensusMempoolMessage>,
        // Sends messages to consensus.
        tx_consensus: Sender<BatchCertificate>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        let (tx_aggregator, rx_aggregator) = channel(CHANNEL_CAPACITY);

        // Spawn all mempool tasks.
        Self::handle_consensus_messages(
            name,
            committee.clone(),
            store.clone(),
            &parameters,
            rx_consensus,
        );
        Self::handle_clients_transactions(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            &parameters,
            tx_consensus.clone(),
            tx_aggregator.clone(),
            rx_aggregator,
        );
        Self::handle_mempool_messages(
            name,
            committee.clone(),
            signature_service,
            store,
            tx_consensus,
            tx_aggregator,
        );

        info!(
            "Mempool successfully booted on {}",
            committee
                .mempool_address(&name)
                .expect("Our public key is not in the committee")
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from the consensus.
    fn handle_consensus_messages(
        _name: PublicKey,
        _committee: Committee,
        _store: Store,
        _parameters: &Parameters,
        _rx_consensus: Receiver<ConsensusMempoolMessage>,
    ) {
        // The `Synchronizer` is responsible to keep the mempool in sync with the others. It handles the commands
        // it receives from the consensus (which are mainly notifications that we are out of sync).
        /*
        Synchronizer::spawn(
            self.name,
            self.committee,
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_consensus,
        );
        */
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        parameters: &Parameters,
        tx_consensus: Sender<BatchCertificate>,
        tx_aggregator: Sender<BatchVote>,
        rx_aggregator: Receiver<BatchVote>,
    ) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_voter, rx_voter) = channel(CHANNEL_CAPACITY);
        let (tx_control, rx_control) = channel(CHANNEL_CAPACITY);

        let mut address = committee
            .transactions_address(&name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        BatchMaker::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            parameters.batch_size,
            parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            rx_control,
            /* tx_authenticated_shard */ tx_voter,
        );

        SelfVoter::spawn(
            name,
            committee.clone(),
            store,
            signature_service,
            /* rx_authenticated_shard */ rx_voter,
            tx_aggregator,
        );

        AggregatorService::spawn(
            name,
            committee,
            /* rx_input */ rx_aggregator,
            /* tx_output */ tx_consensus,
            tx_control,
        );

        info!("Mempool listening to client transactions on {}", address);
    }

    /// Spawn all tasks responsible to handle messages from other mempools.
    fn handle_mempool_messages(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        store: Store,
        tx_consensus: Sender<BatchCertificate>,
        tx_aggregator: Sender<BatchVote>,
    ) {
        let (tx_voter, rx_voter) = channel(CHANNEL_CAPACITY);
        let (tx_certificate_verifier, rx_certificate_verifier) = channel(CHANNEL_CAPACITY);
        let (tx_cleanup, rx_cleanup) = channel(CHANNEL_CAPACITY);

        let mut address = committee
            .mempool_address(&name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            MempoolReceiverHandler {
                tx_voter,
                tx_aggregator,
                tx_certificate_verifier,
            },
        );

        NodesVoter::spawn(
            name,
            committee.clone(),
            store,
            signature_service,
            /* rx_authenticated_shard */ rx_voter,
            rx_cleanup,
        );

        CertificateVerifier::spawn(
            committee,
            /* rx_input */ rx_certificate_verifier,
            /* tx_output */ tx_consensus,
            tx_cleanup,
        );

        /*
        Helper::spawn(
            committee.clone(),
            store.clone(),
            /* rx_request */ rx_helper,
        );
        */

        info!("Mempool listening to mempool messages on {}", address);
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        self.tx_batch_maker
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}

/// Defines how the network receiver handles incoming mempool messages.
#[derive(Clone)]
struct MempoolReceiverHandler {
    tx_voter: Sender<(AuthenticatedShard, SerializedShard)>,
    tx_aggregator: Sender<BatchVote>,
    tx_certificate_verifier: Sender<BatchCertificate>,
}

#[async_trait]
impl MessageHandler for MempoolReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(MempoolMessage::CodedBatch(..)) => (), // TODO
            Ok(MempoolMessage::AuthenticatedShard(shard)) => self
                .tx_voter
                .send((shard, serialized.to_vec()))
                .await
                .expect("Failed to send shard"),
            Ok(MempoolMessage::BatchVote(vote)) => self
                .tx_aggregator
                .send(vote)
                .await
                .expect("Failed to send vote"),
            Ok(MempoolMessage::BatchCertificate(certificate)) => self
                .tx_certificate_verifier
                .send(certificate)
                .await
                .expect("Failed to send certificate"),
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}
