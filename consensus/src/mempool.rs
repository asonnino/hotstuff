use crate::core::{CoreMessage, SeqNumber};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::future::FutureExt as _;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/mempool_tests.rs"]
pub mod mempool_tests;

pub enum PayloadStatus {
    Accept,
    Reject,
    Wait(Vec<Vec<u8>>),
}

#[async_trait]
pub trait NodeMempool: Send + Sync {
    /// Consensus calls this method whenever it needs to create a new block.
    /// The mempool needs to promptly provide at most 'max' payloads.
    async fn get(&mut self, max: usize) -> Vec<Vec<u8>>;

    /// Consensus calls this method when receiving a new block. The mempool should
    /// return Accept if the block can be processed right away, Wait(missing_values)
    /// if the block should be processed when missing_value is the storage, or
    /// Reject if the payload is invalid and the block should be dropped.
    async fn verify(&mut self, payload: &[Vec<u8>]) -> PayloadStatus;

    /// Consensus calls this method upon commit a block. The mempool can use the
    /// knowledge that a block is committed to clean up its internal state.
    async fn garbage_collect(&mut self, payload: &[Vec<u8>]);
}

// TODO [issue #3] Merge the mempool driver with the synchronizer.

type DriverMessage = (Vec<Vec<u8>>, Block, Receiver<()>);

pub struct MempoolDriver<Mempool> {
    inner_channel: Sender<DriverMessage>,
    mempool: Mempool,
    pending: HashMap<SeqNumber, Sender<()>>,
}

impl<Mempool: 'static + NodeMempool> MempoolDriver<Mempool> {
    pub fn new(mempool: Mempool, core_channel: Sender<CoreMessage>, store: Store) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<DriverMessage>) = channel(1000);
        let mut waiting = FuturesUnordered::new();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some((missing, block, handler)) = rx_inner.recv().fuse() => {
                        let missing = missing.into_iter().map(|x| (x, store.clone())).collect();
                        let fut = Self::waiter(missing, block, handler);
                        waiting.push(fut);
                    },
                    Some(result) = waiting.next() => {
                        match result {
                            Ok(Some(block)) => {
                                let message = CoreMessage::LoopBack(block);
                                if let Err(e) = core_channel.send(message).await {
                                    panic!("Failed to send message through core channel: {}", e);
                                }
                            },
                            Ok(None) => (),
                            Err(e) => error!("{}", e)
                        }
                    },
                    else => break,
                }
            }
        });
        Self {
            inner_channel: tx_inner,
            mempool,
            pending: HashMap::new(),
        }
    }

    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
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

    pub async fn verify(&mut self, block: &Block) -> ConsensusResult<bool> {
        match self.mempool.verify(&block.payload).await {
            PayloadStatus::Accept => Ok(true),
            PayloadStatus::Reject => bail!(ConsensusError::InvalidPayload),
            PayloadStatus::Wait(missing) => {
                if !self.pending.contains_key(&block.round) {
                    let (tx_cancel, rx_cancel) = channel(1);
                    let round = block.round;
                    self.pending.insert(round, tx_cancel);
                    debug!("Registering missing payload for {:?}", block);
                    let message = (missing, block.clone(), rx_cancel);
                    if let Err(e) = self.inner_channel.send(message).await {
                        panic!("Failed to send request to synchronizer: {}", e);
                    }
                }
                Ok(false)
            }
        }
    }

    pub async fn cleanup(&mut self, b0: &Block, b1: &Block) {
        // Cleanup the driver.
        let round = b1.round;
        for (k, v) in &self.pending {
            if !v.is_closed() && k <= &round {
                let _ = v.send(()).await;
            }
        }
        self.pending.retain(|k, _| k > &round);

        // Cleanup the mempool.
        let payloads = [&b0.payload[..], &b1.payload[..]].concat();
        self.mempool.garbage_collect(&payloads).await;
    }

    pub async fn get(&mut self, max: usize) -> Vec<Vec<u8>> {
        self.mempool.get(max).await
    }
}
