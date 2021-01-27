use crate::core::{CoreMessage, RoundNumber};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use futures::future::FutureExt as _;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use mempool::{NodeMempool, PayloadStatus};
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

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
                tokio::select! {
                    Some((wait_on, block, handler)) = rx_inner.recv().fuse() => {
                        let fut = Self::waiter(store.clone(), wait_on, block, handler);
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
        mut store: Store,
        wait_on: Vec<u8>,
        deliver: Block,
        mut handler: Receiver<()>,
    ) -> ConsensusResult<Option<Block>> {
        tokio::select! {
            result = store.notify_read(wait_on) => {
                let _ = result?;
                Ok(Some(deliver))
            },
            _ = handler.recv() => Ok(None),
        }
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

    pub async fn garbage_collect(&mut self, block: &Block) {
        // Cleanup the driver.
        let round = block.round;
        for (k, v) in &self.pending {
            if !v.is_closed() && k < &round {
                let _ = v.send(()).await;
            }
        }
        self.pending.retain(|k, _| k < &round);

        // Cleanup the mempool.
        self.mempool.garbage_collect(&block.payload).await;
    }

    pub async fn get(&mut self) -> Vec<u8> {
        self.mempool.get().await
    }
}
