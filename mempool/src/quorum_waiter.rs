use std::iter::FromIterator;
use std::net::SocketAddr;

use crate::config::{Committee, Stake};
use crate::processor::SerializedBatchMessage;
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use std::collections::HashSet;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `MempoolMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<(SerializedBatchMessage, Digest)>,
    /// Channel to receive acknowledgements from the network.
    rx_ack: Receiver<(SocketAddr, Digest)>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<(SerializedBatchMessage, Digest)>,
        rx_ack: Receiver<(SocketAddr, Digest)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
                rx_ack,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
            let digest = Digest::hash(&batch);

            // Set of publickeys which already sent an acknowledgement.
            let mut acks =
                HashSet::<PublicKey>::from_iter(handlers.iter().map(|(pk, _)| pk.clone()));

            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the consensus (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = self.stake;
            while total_stake < self.committee.quorum_threshold() {
                tokio::select! {
                    Some(stake) = wait_for_quorum.next() => {
                        total_stake += stake;
                    }
                    Some((addr, received_digest)) = self.rx_ack.recv() => {
                        let name = self.committee.get_public_key(&addr);
                        if let Some(name) = name {
                            if received_digest == digest && !acks.contains(name) {
                                let stake = self.committee.stake(&name);
                                total_stake += stake;
                                acks.insert(*name);
                            }
                        }
                    }
                };
            }

            self.tx_batch
                .send((batch, digest))
                .await
                .expect("Failed to deliver batch");
        }
    }
}
