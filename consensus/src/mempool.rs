use crate::core::{CoreMessage, RoundNumber};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use async_trait::async_trait;
use futures::future::FutureExt as _;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use std::collections::HashMap;
use std::marker::Send;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[allow(dead_code)]
pub enum PayloadStatus {
    Accept,
    Reject,
    Wait(Vec<u8>),
}

#[async_trait]
pub trait NodeMempool: Send + Sync {
    /// Consensus calls this method whenever it needs to create a new block.
    /// The mempool needs to promptly provide a payload.
    async fn get(&self) -> Vec<u8>;

    /// Consensus calls this method when receiving a new block. The mempool should
    /// return Accept if the block can be processed right away, Wait(missing_value)
    /// if the block should be processed when missing_value is the storage, or
    /// Reject if the payload is invalid and the block should be dropped.
    async fn verify(&self, payload: &[u8]) -> PayloadStatus;

    /// Consensus calls this method upon commit a block. The mempool can use the
    /// knowledge that a block is committed to clean up its internal state.
    async fn garbage_collect(&self, payload: &[u8]);
}

type DriverMessage = (Vec<u8>, Block, Receiver<()>);

pub struct MempoolDriver<Mempool> {
    inner_channel: Sender<DriverMessage>,
    mempool: Mempool,
    pending: HashMap<RoundNumber, Sender<()>>,
}

impl<Mempool: 'static + NodeMempool> MempoolDriver<Mempool> {
    pub fn new(mempool: Mempool, core_channel: Sender<CoreMessage>, store: Store) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<DriverMessage>) = channel(1000);
        let mut waiting = FuturesUnordered::new();
        tokio::spawn(async move {
            loop {
                select! {
                    message = rx_inner.recv().fuse() => {
                        if let Some((wait_on, block, handler)) = message {
                            let fut = Self::waiter(store.clone(), wait_on, block, handler);
                            waiting.push(fut);
                        }
                    },
                    result = waiting.select_next_some() => {
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
                    }
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
        mut store: Store,
        wait_on: Vec<u8>,
        deliver: Block,
        mut handler: Receiver<()>,
    ) -> ConsensusResult<Option<Block>> {
        select! {
            result = store.notify_read(wait_on).fuse() => {
                let _ = result?;
                Ok(Some(deliver))
            },
            _ = handler.recv().fuse() => {
                Ok(None)
            }
        }
    }

    pub async fn cleanup(&mut self, round: &RoundNumber) {
        for (k, v) in &self.pending {
            if !v.is_closed() && k < round {
                let _ = v.send(()).await;
            }
        }
        self.pending.retain(|k, _| k < round);
    }

    pub async fn verify(&mut self, block: &Block) -> ConsensusResult<bool> {
        match self.mempool.verify(&block.payload).await {
            PayloadStatus::Accept => Ok(true),
            PayloadStatus::Reject => bail!(ConsensusError::InvalidPayload),
            PayloadStatus::Wait(wait_on) => {
                if !self.pending.contains_key(&block.round) {
                    let (tx_cancel, rx_cancel) = channel(1);
                    let round = block.round;
                    self.pending.insert(round, tx_cancel);
                    debug!("Registering missing payload for {:?}", block);
                    let message = (wait_on, block.clone(), rx_cancel);
                    if let Err(e) = self.inner_channel.send(message).await {
                        panic!("Failed to send request to synchronizer: {}", e);
                    }
                }
                Ok(false)
            }
        }
    }

    pub async fn garbage_collect(&self, payload: &[u8]) {
        self.mempool.garbage_collect(payload).await;
    }

    pub async fn get(&self) -> Vec<u8> {
        self.mempool.get().await
    }
}
