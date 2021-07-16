use crate::config::Committee;
use crate::mempool::{ConsensusMempoolMessage, MempoolMessage, Round};
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

// The `Synchronizer` is responsible to keep the mempool in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the consensus.
    rx_message: Receiver<ConsensusMempoolMessage>,
    /// A network sender to send requests to the other mempools.
    network: SimpleSender,
    /// Loosely keep track of the consensus's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the consensus. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<ConsensusMempoolMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new(),
                round: Round::default(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the consensus' messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle consensus' messages.
                Some(message) = self.rx_message.recv() => match message {
                    ConsensusMempoolMessage::Synchronize(digests, target) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            // Ensure we do not send twice the same sync request.
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // Register the digest as missing.
                            missing.push(digest.clone());
                            debug!("Requesting sync for batch {}", digest);

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(digest, (self.round, tx_cancel, now));
                        }

                        // Send sync request to a single node. If this fails, we will send it
                        // to other nodes when a timer times out.
                        let address = match self.committee.mempool_address(&target) {
                            Some(address) => address,
                            None => {
                                error!("Consensus asked us to sync with an unknown node: {}", target);
                                continue;
                            }
                        };
                        let message = MempoolMessage::BatchRequest(missing, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network.send(address, Bytes::from(serialized)).await;
                    },
                    ConsensusMempoolMessage::Cleanup(round) => {
                        // Keep track of the consensus' round number.
                        self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for (r, handler, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        self.pending.retain(|_, (r, _, _)| r > &mut gc_round);
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                        // The sync request for this batch has been canceled.
                    },
                    Err(e) => error!("{}", e)
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to a bunch of other nodes (selected at random).
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        let addresses = self.committee
                            .broadcast_addresses(&self.name)
                            .iter()
                            .map(|(_, address)| *address)
                            .collect();
                        let message = MempoolMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network
                            .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                            .await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}
