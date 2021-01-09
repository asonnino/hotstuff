use crate::core::{CoreMessage, RoundNumber};
use crate::error::ConsensusResult;
use crate::messages::Block;
use crate::store::Store;
use async_trait::async_trait;
use futures::future::FutureExt as _;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use rand::Rng as _;
use std::collections::HashMap;
use std::marker::Send;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[async_trait]
pub trait NodeMempool: Send + Sync {
    /// Consensus calls this method whenever it needs to create a new block.
    /// The mempool needs to promptly provide a payload.
    async fn get(&self) -> Vec<u8>;

    /// Consensus calls this method when receiving a new block. The mempool should
    /// return Ok(None) if the block can be processed right away, Ok(precondition)
    /// if the block should be processed when precondition is the storage, or Err
    /// if the block is invalid and should be dropped.
    async fn verify(&self, payload: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;

    /// Consensus calls this method upon commit a block. The mempool can use the
    /// knowledge that a block is committed to clean up its internal state.
    async fn cleanup(&self, payload: &[u8]);
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

    pub async fn verify(&mut self, wait_on: Vec<u8>, block: &Block) {
        // call Verify

        if !self.pending.contains_key(&block.round) {
            let (tx_cancel, rx_cancel) = channel(1);
            let round = block.round;
            self.pending.insert(round, tx_cancel);
            debug!("Registering missing payload for {:?}", block);
            let message = (block.payload.clone(), block.clone(), rx_cancel);
            if let Err(e) = self.inner_channel.send(message).await {
                panic!("Failed to send request to synchronizer: {}", e);
            }
        }
    }

    pub async fn timeout(&mut self, round: RoundNumber) {
        // TODO: write it better
        for (k, v) in &self.pending {
            if !v.is_closed() && k < &round {
                let _ = v.send(()).await;
            }
        }
        self.pending.retain(|k, _| k < &round);
    }
}

// ---

pub struct MockMempool;

impl MockMempool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl NodeMempool for MockMempool {
    async fn get(&self) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        [
            rng.gen::<u128>().to_le_bytes(),
            rng.gen::<u128>().to_le_bytes(),
        ]
        .concat()
    }

    async fn verify(&self, _payload: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        Ok(None)
    }

    async fn cleanup(&self, _payload: &[u8]) {}
}
