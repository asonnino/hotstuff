use crate::config::Committee;
use crate::core::MempoolMessage;
use crate::error::{MempoolError, MempoolResult};
use bytes::Bytes;
use consensus::{Block, ConsensusMessage, SeqNumber};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::NetMessage;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

enum SynchronizerMessage {
    Sync(HashSet<Digest>, Block),
    Clean(SeqNumber),
}

pub struct Synchronizer {
    inner_channel: Sender<SynchronizerMessage>,
    store: Store,
}

impl Synchronizer {
    pub fn new(
        consensus_channel: Sender<ConsensusMessage>,
        store: Store,
        name: PublicKey,
        committee: Committee,
        network_channel: Sender<NetMessage>,
        sync_retry_delay: u64,
    ) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<SynchronizerMessage>) = channel(10000);

        let store_copy = store.clone();
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut pending = HashMap::new();
            let mut requests = HashMap::new();

            let timer = sleep(Duration::from_millis(5000));
            tokio::pin!(timer);
            loop {
                tokio::select! {
                    Some(message) = rx_inner.recv() => match message {
                        SynchronizerMessage::Sync(mut missing, block) => {
                            // TODO [issue #7]: A bad node may make us run out of memory by sending many blocks
                            // with different round numbers or different payloads.

                            let block_digest = block.digest();
                            let author = block.author;
                            let round = block.round;
                            if pending.contains_key(&block_digest) {
                                continue;
                            }

                            let wait_for = missing.iter().cloned().map(|x| (x, store_copy.clone())).collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            pending.insert(block_digest, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, block, rx_cancel);
                            waiting.push(fut);

                            let missing: Vec<_> = missing
                                .drain()
                                .filter(|x| !requests.contains_key(x))
                                .collect();
                            if !missing.is_empty() {
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Failed to measure time")
                                    .as_millis();
                                for x in &missing {
                                    requests.insert(x.clone(), (round, now));
                                }

                                let message = MempoolMessage::PayloadRequest(missing.clone(), name);
                                Self::transmit(
                                    &message,
                                    &name,
                                    Some(&author),
                                    &committee,
                                    &network_channel
                                )
                                .await
                                .expect("Failed to send payload sync request");
                            }
                        },
                        SynchronizerMessage::Clean(mut round) => {
                            for (r, handler) in pending.values() {
                                if r <= &round {
                                    let _ = handler.send(()).await;
                                }
                            }
                            pending.retain(|_, (r, _)| r > &mut round);
                            requests.retain(|_, (r, _)| r > &mut round);
                        }
                    },
                    Some(result) = waiting.next() => {
                        match result {
                            Ok(Some(block)) => {
                                debug!("mempool sync loopback block {:?}", block);
                                let _ = pending.remove(&block.digest());
                                for x in &block.payload {
                                    let _ = requests.remove(x);
                                }
                                let message = ConsensusMessage::LoopBack(block);
                                if let Err(e) = consensus_channel.send(message).await {
                                    panic!("Failed to send message to consensus: {}", e);
                                }
                            },
                            Ok(None) => (),
                            Err(e) => error!("{}", e)
                        }
                    },
                    () = &mut timer => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();
                        let retransmit: Vec<_> = requests
                            .iter()
                            .filter(|(_, (_, timestamp))| timestamp + (sync_retry_delay as u128) < now)
                            .map(|(digest, _)| digest)
                            .cloned()
                            .collect();
                        if !retransmit.is_empty() {
                            let message = MempoolMessage::PayloadRequest(retransmit, name);
                            Self::transmit(
                                &message,
                                &name,
                                None,
                                &committee,
                                &network_channel
                            )
                            .await
                            .expect("Failed to send payload sync request");
                        }
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(5000));
                    },
                    else => break,
                }
            }
        });
        Self {
            inner_channel: tx_inner,
            store,
        }
    }

    async fn waiter(
        mut missing: Vec<(Digest, Store)>,
        deliver: Block,
        mut handler: Receiver<()>,
    ) -> MempoolResult<Option<Block>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(MempoolError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    pub async fn transmit(
        message: &MempoolMessage,
        from: &PublicKey,
        to: Option<&PublicKey>,
        committee: &Committee,
        network_channel: &Sender<NetMessage>,
    ) -> MempoolResult<()> {
        let addresses = if let Some(to) = to {
            debug!("Sending {:?} to {}", message, to);
            vec![committee.mempool_address(to)?]
        } else {
            debug!("Broadcasting {:?}", message);
            committee.broadcast_addresses(&from)
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    pub async fn verify_payload(&mut self, block: Block) -> MempoolResult<bool> {
        let mut missing = HashSet::new();
        for digest in &block.payload {
            if self.store.read(digest.to_vec()).await?.is_none() {
                debug!("Requesting sync for payload {}", digest);
                missing.insert(digest.clone());
            }
        }

        if missing.is_empty() {
            return Ok(true);
        }
        let message = SynchronizerMessage::Sync(missing, block);
        if let Err(e) = self.inner_channel.send(message).await {
            panic!("Failed to send message to synchronizer core: {}", e);
        }
        Ok(false)
    }

    pub async fn cleanup(&mut self, round: SeqNumber) {
        let message = SynchronizerMessage::Clean(round);
        debug!("cleanup round {}", round);
        if let Err(e) = self.inner_channel.send(message).await {
            panic!("Failed to send message to synchronizer core: {}", e);
        }
    }
}
