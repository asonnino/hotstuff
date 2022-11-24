use std::time::Duration;

use crate::config::{Committee, Stake};
use crate::processor::SerializedBatchMessage;
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use network::CancelHandler;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::sleep,
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

    // async fn empty_buffer()

    /// Main loop.
    async fn run(&mut self) {
        // Hold the dissemination handlers of the f slower nodes.
        let mut pending = FuturesUnordered::new();
        let mut pending_counter = 0;

        // while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
        loop {
            tokio::select! {
                Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv() => {
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
                    // immediately dropping the handles.
                    // TODO: We should allocate resource per peer (not in total).
                    if pending_counter >= DISSEMINATION_QUEUE_MAX {
                        pending.push(async move {
                            tokio::select! {
                                _ = async move {while let Some(_) = wait_for_quorum.next().await {}} => (),
                                () = sleep(Duration::from_millis(DISSEMINATION_DEADLINE)) => ()
                            }
                        });
                        pending_counter += 1;
                    }
                },
                Some(_) = pending.next() =>  {
                    if pending_counter > 0 {
                        pending_counter -= 1;
                    }
                }
            }
        }
    }
}
