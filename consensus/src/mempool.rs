use crate::consensus::{Round, CHANNEL_CAPACITY};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use crypto::Digest;
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::error;
use mempool::ConsensusMempoolMessage;
use std::collections::{HashMap, HashSet};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use crypto::Hash as _;

pub struct MempoolDriver {
    store: Store,
    tx_mempool: Sender<ConsensusMempoolMessage>,
    tx_payload_buffer: Sender<PayloadBufferMessage>,
    tx_payload_waiter: Sender<PayloadWaiterMessage>,
}

impl MempoolDriver {
    pub fn new(
        store: Store,
        rx_mempool: Receiver<Digest>,
        tx_mempool: Sender<ConsensusMempoolMessage>,
        tx_consensus: Sender<Block>,
    ) -> Self {
        let (tx_payload_buffer, rx_payload_buffer) = channel(CHANNEL_CAPACITY);
        let (tx_payload_waiter, rx_payload_waiter) = channel(CHANNEL_CAPACITY);

        // Spawn the payload buffer.
        PayloadBuffer::spawn(rx_mempool, rx_payload_buffer);

        // Spawn the payload waiter.
        PayloadWaiter::spawn(store.clone(), rx_payload_waiter, tx_consensus);

        // Returns the mempool driver.
        Self {
            store,
            tx_mempool,
            tx_payload_buffer,
            tx_payload_waiter,
        }
    }

    pub async fn get(&mut self, max_size: usize) -> Vec<Digest> {
        let (sender, receiver) = oneshot::channel();
        self.tx_payload_buffer
            .send((max_size, sender))
            .await
            .expect("Failed to request payload");
        receiver.await.expect("Failed to receive payload")
    }

    pub async fn verify(&mut self, block: Block) -> ConsensusResult<bool> {
        let mut missing = Vec::new();
        for x in &block.payload {
            if self.store.read(x.to_vec()).await?.is_none() {
                missing.push(x.clone());
            }
        }

        if missing.is_empty() {
            return Ok(true);
        }

        let message = ConsensusMempoolMessage::Synchronize(missing.clone(), block.author);
        self.tx_mempool
            .send(message)
            .await
            .expect("Failed to send sync message");

        self.tx_payload_waiter
            .send(PayloadWaiterMessage::Wait(missing, block))
            .await
            .expect("Failed to send message to payload waiter");

        Ok(false)
    }

    pub async fn cleanup(&mut self, b0: &Block, b1: &Block, block:&Block) {

        // TODO: remove digests from queue.

        let round = b0.round;
        self.tx_mempool
            .send(ConsensusMempoolMessage::Cleanup(round))
            .await
            .expect("Failed to send cleanup message");
        self.tx_payload_waiter
            .send(PayloadWaiterMessage::Cleanup(round))
            .await
            .expect("Failed to send cleanup message");
    }
}

type PayloadBufferMessage = (/* max size */ usize, oneshot::Sender<Vec<Digest>>);

struct PayloadBuffer;

impl PayloadBuffer {
    pub fn spawn(mut rx_mempool: Receiver<Digest>, mut rx_messages: Receiver<PayloadBufferMessage>) {
        tokio::spawn(async move {
            let mut queue = HashSet::new();
            loop {
                tokio::select! {
                    Some(digest) = rx_mempool.recv() => {
                        queue.insert(digest);
                    },
                    Some((max, sender)) = rx_messages.recv() => {
                        let digest_len = Digest::default().size();
                        let digests = queue.iter().take(max / digest_len).cloned().collect();
                        for x in &digests {
                            queue.remove(x);
                        }
                        let _ = sender.send(digests);
                    }
                }
            }
        });
    }
}

#[derive(Debug)]
enum PayloadWaiterMessage {
    Wait(Vec<Digest>, Block),
    Cleanup(Round),
}

struct PayloadWaiter {
    store: Store,
    rx_message: Receiver<PayloadWaiterMessage>,
    tx_consensus: Sender<Block>,
}

impl PayloadWaiter {
    pub fn spawn(
        store: Store,
        rx_message: Receiver<PayloadWaiterMessage>,
        tx_consensus: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                rx_message,
                tx_consensus,
            }
            .run()
            .await;
        });
    }

    async fn waiter(
        mut missing: Vec<(Digest, Store)>,
        deliver: Block,
        mut handler: Receiver<()>,
    ) -> ConsensusResult<Option<Block>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(ConsensusError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();
        let mut pending = HashMap::new();

        let store_copy = self.store.clone();
        loop {
            tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    PayloadWaiterMessage::Wait(missing, block) => {
                        let block_digest = block.digest();

                        if pending.contains_key(&block_digest) {
                            continue;
                        }

                        let (tx_cancel, rx_cancel) = channel(1);
                        pending.insert(block_digest, (block.round, tx_cancel));
                        let wait_for = missing.iter().cloned().map(|x| (x, store_copy.clone())).collect();
                        let fut = Self::waiter(wait_for, block, rx_cancel);
                        waiting.push(fut);
                    },
                    PayloadWaiterMessage::Cleanup(mut round) => {
                        for (r, handler) in pending.values() {
                            if r <= &round {
                                let _ = handler.send(()).await;
                            }
                        }
                        pending.retain(|_, (r, _)| r > &mut round);
                    }
                },
                Some(result) = waiting.next() => {
                    match result {
                        Ok(Some(block)) => {
                            let _ = pending.remove(&block.digest());
                            self.tx_consensus.send(block).await.expect("Failed to send consensus message");
                        },
                        Ok(None) => (),
                        Err(e) => error!("{}", e)
                    }
                }
            }
        }
    }
}
