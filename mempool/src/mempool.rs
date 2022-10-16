use crate::batch_maker::{BatchMaker, BatchWithSender, Transaction};
use crate::config::{Committee, Parameters};
use crate::helper::Helper;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::quorum_waiter::QuorumWaiter;
use crate::synchronizer::Synchronizer;
use crate::topologies::Topology;
use crate::TopologyBuilder;
use async_trait::async_trait;
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::{info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/mempool_tests.rs"]
pub mod mempool_tests;

/// The default channel capacity for each channel of the mempool.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus round number.
pub type Round = u64;

/// The message exchanged between the nodes' mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum MempoolMessage {
    Ack(Digest),
    Batch(BatchWithSender),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
}

/// The messages sent by the consensus and the mempool.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConsensusMempoolMessage {
    /// The consensus notifies the mempool that it need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The consensus notifies the mempool of a round update.
    Cleanup(Round),
}

pub struct Mempool<Builder>
where
    Builder: TopologyBuilder,
{
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
    /// Send messages to consensus.
    tx_consensus: Sender<Digest>,
    /// Topology of the network
    topology: Builder::Topology,
}

impl<Builder> Mempool<Builder>
where
    Builder: TopologyBuilder,
{
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        rx_consensus: Receiver<ConsensusMempoolMessage>,
        tx_consensus: Sender<Digest>,
        mut topology_builder: Builder,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();
        topology_builder.set_params(&parameters, name);
        let mut peers = committee.broadcast_addresses(&name);
        peers.push((name, "127.0.0.1:0".parse().unwrap()));

        let topology = topology_builder
            .build(peers)
            .expect("Failed to build topology");
        // Define a mempool instance.
        let mut mempool = Self {
            name,
            committee,
            parameters,
            store,
            tx_consensus,
            topology,
        };
        let (tx_ack, rx_ack) = channel(CHANNEL_CAPACITY);

        // Spawn all mempool tasks.
        mempool.handle_consensus_messages(rx_consensus);
        mempool.handle_clients_transactions(rx_ack);
        mempool.handle_mempool_messages(tx_ack);

        info!(
            "Mempool successfully booted on {}",
            mempool
                .committee
                .mempool_address(&mempool.name)
                .expect("Our public key is not in the committee")
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle messages from the consensus.
    fn handle_consensus_messages(&self, rx_consensus: Receiver<ConsensusMempoolMessage>) {
        // The `Synchronizer` is responsible to keep the mempool in sync with the others. It handles the commands
        // it receives from the consensus (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_consensus,
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&mut self, rx_ack: Receiver<(SocketAddr, Digest)>) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .transactions_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other mempools that share the same `id` as us. Finally,
        // it gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.name,
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            /* tx_message */ tx_quorum_waiter,
            /* mempool_addresses */ self.topology.broadcast_peers(self.name).to_vec(),
        );

        // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // the batch to the `Processor`.
        QuorumWaiter::spawn(
            self.committee.clone(),
            /* stake */ self.committee.stake(&self.name),
            /* rx_message */ rx_quorum_waiter,
            /* tx_batch */ tx_processor,
            /* rx_ack */ rx_ack,
        );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the consensus.
        Processor::spawn(
            self.committee.clone(),
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ self.tx_consensus.clone(),
            self.topology.clone(),
        );

        info!("Mempool listening to client transactions on {}", address);
    }

    /// Spawn all tasks responsible to handle messages from other mempools.
    fn handle_mempool_messages(&self, tx_ack: Sender<(SocketAddr, Digest)>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);

        // Receive incoming messages from other mempools.
        let mut address = self
            .committee
            .mempool_address(&self.name)
            .expect("Our public key is not in the committee");
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            MempoolReceiverHandler {
                tx_helper,
                tx_processor,
                tx_ack,
            },
        );

        // The `Helper` is dedicated to reply to batch requests from other mempools.
        Helper::spawn(
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,
        );

        // This `Processor` hashes and stores the batches we receive from the other mempools. It then forwards the
        // batch's digest to the consensus.
        Processor::spawn(
            self.committee.clone(),
            self.store.clone(),
            /* rx_batch */ rx_processor,
            /* tx_digest */ self.tx_consensus.clone(),
            self.topology.clone(),
        );

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
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        _peer: SocketAddr,
        message: Bytes,
    ) -> Result<(), Box<dyn Error>> {
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
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,
    tx_processor: Sender<(
        SerializedBatchMessage,
        Digest,
        Option<(SocketAddr, PublicKey)>,
    )>,
    tx_ack: Sender<(SocketAddr, Digest)>,
}

#[async_trait]
impl MessageHandler for MempoolReceiverHandler {
    async fn dispatch(
        &self,
        writer: &mut Writer,
        peer: SocketAddr,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(MempoolMessage::Batch(batch_with_sender)) => {
                // Send the batch to the digest processor.
                let digest = Digest::hash(&serialized);
                self.tx_processor
                    .send((
                        serialized.to_vec(),
                        digest,
                        Some((peer, batch_with_sender.sender)),
                    ))
                    .await
                    .expect("Failed to send batch");
            }
            Ok(MempoolMessage::BatchRequest(missing, requestor)) => self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request"),
            Ok(MempoolMessage::Ack(digest)) => {
                info!("Received ACK for batch {} from {}", digest, peer);
                self.tx_ack
                    .send((peer, digest))
                    .await
                    .expect("failed to send ack");
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}
