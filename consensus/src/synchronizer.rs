use crate::core::CoreMessage;
use crate::error::ConsensusResult;
use crate::messages::{Block, QC};
use crate::network::NetMessage;
use crate::timer::Timer;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use std::collections::HashSet;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

pub struct Synchronizer {
    store: Store,
    inner_channel: Sender<Block>,
}

impl Synchronizer {
    pub async fn new(
        name: PublicKey,
        store: Store,
        network_channel: Sender<NetMessage>,
        core_channel: Sender<CoreMessage>,
        sync_retry_delay: u64,
    ) -> Self {
        let (tx_inner, mut rx_inner): (_, Receiver<Block>) = channel(1000);
        let mut timer = Timer::new();
        timer.schedule(sync_retry_delay, true).await;

        let store_copy = store.clone();
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut pending = HashSet::new();
            loop {
                tokio::select! {
                    Some(block) = rx_inner.recv() => {
                        if pending.insert(block.digest()) {
                            let previous = block.previous().clone();
                            let fut = Self::waiter(store_copy.clone(), previous.clone(), block);
                            waiting.push(fut);
                            let sync_request = NetMessage::SyncRequest(previous, name);
                            if let Err(e) = network_channel.send(sync_request).await {
                                panic!("Failed to send Sync Request to network: {}", e);
                            }
                        }
                    },
                    Some(result) = waiting.next() => {
                        match result {
                            Ok(block) => {
                                let _ = pending.remove(&block.digest());
                                let message = CoreMessage::LoopBack(block);
                                if let Err(e) = core_channel.send(message).await {
                                    panic!("Failed to send message through core channel: {}", e);
                                }
                            },
                            Err(e) => error!("{}", e)
                        }
                    },
                    Some(_) = timer.notifier.recv() => {
                        // This ensure liveness in case Sync Requests are lost.
                        // It should not happen in theory, but the internet is wild.
                        for digest in &pending {
                            let sync_request = NetMessage::SyncRequest(digest.clone(), name);
                            if let Err(e) = network_channel.send(sync_request).await {
                                panic!("Failed to send Sync Request to network: {}", e);
                            }
                        }
                        timer
                            .schedule(sync_retry_delay, true)
                            .await;
                    },
                    else => break,
                }
            }
        });
        Self {
            store,
            inner_channel: tx_inner,
        }
    }

    async fn waiter(mut store: Store, wait_on: Digest, deliver: Block) -> ConsensusResult<Block> {
        let _ = store.notify_read(wait_on.to_vec()).await?;
        Ok(deliver)
    }

    async fn get_previous_block(&mut self, block: &Block) -> ConsensusResult<Option<Block>> {
        if block.qc == QC::genesis() {
            return Ok(Some(Block::genesis()));
        }
        let previous = block.previous();
        match self.store.read(previous.to_vec()).await? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => {
                debug!("Requesting sync for block {:?}", previous);
                if let Err(e) = self.inner_channel.send(block.clone()).await {
                    panic!("Failed to send request to synchronizer: {}", e);
                }
                Ok(None)
            }
        }
    }

    pub async fn get_ancestors(
        &mut self,
        block: &Block,
    ) -> ConsensusResult<Option<(Block, Block, Block)>> {
        let b2 = match self.get_previous_block(block).await? {
            Some(b) => b,
            None => return Ok(None),
        };
        let b1 = self
            .get_previous_block(&b2)
            .await?
            .expect("We should have all ancestors of delivered blocks");
        let b0 = self
            .get_previous_block(&b1)
            .await?
            .expect("We should have all ancestors of delivered blocks");
        Ok(Some((b0, b1, b2)))
    }
}