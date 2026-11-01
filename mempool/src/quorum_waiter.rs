use std::sync::Arc;
use std::time::Duration;

use crate::config::{Committee, Stake};
use crate::processor::SerializedBatchMessage;
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    sync::Semaphore,
    time::timeout,
};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

/// Extra batch dissemination time for the f last nodes (in ms).
const DISSEMINATION_DEADLINE: u64 = 500;
/// Bounds the queue handling the extra dissemination.
const DISSEMINATION_QUEUE_MAX: usize = 10_000;

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
    tx_batch: Sender<SerializedBatchMessage>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
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
        // Bounds the number of batches whose dissemination to the f slower nodes is in progress.
        let dissemination_slots = Arc::new(Semaphore::new(DISSEMINATION_QUEUE_MAX));

        while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
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
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                if total_stake >= self.committee.quorum_threshold() {
                    self.tx_batch
                        .send(batch)
                        .await
                        .expect("Failed to deliver batch");
                    break;
                }
            }

            // Give a bit of extra time to disseminate the batch to slower nodes rather than
            // immediately dropping the handles. This runs in its own task so that its deadline
            // holds even while this loop is blocked waiting for the quorum of a later batch.
            // If all slots are taken, the handles are dropped immediately.
            // TODO: We should allocate resource per peer (not in total).
            if let Ok(permit) = dissemination_slots.clone().try_acquire_owned() {
                tokio::spawn(async move {
                    let _ = timeout(Duration::from_millis(DISSEMINATION_DEADLINE), async move {
                        while wait_for_quorum.next().await.is_some() {}
                    })
                    .await;
                    drop(permit);
                });
            }
        }
    }
}
