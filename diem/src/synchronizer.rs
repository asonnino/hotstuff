use crate::core::CoreMessage;
use crate::error::{DiemError, DiemResult};
use crate::messages::{Block, QC};
use crate::network::NetMessage;
use crate::store::Store;
use futures::future::FutureExt as _;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use tokio::sync::mpsc::{channel, Sender};

pub struct Synchronizer {
    store: Store,
    inner_channel: Sender<Block>,
    network_channel: Sender<NetMessage>,
}

impl Synchronizer {
    pub async fn new(
        store: Store,
        network_channel: Sender<NetMessage>,
        core_channel: Sender<CoreMessage>,
    ) -> Self {
        let (tx, mut rx) = channel(1000);
        let synchronizer = Self {
            store: store.clone(),
            inner_channel: tx,
            network_channel,
        };
        tokio::spawn(async move {
            // TODO: The field 'waiting' may be target of DDoS.
            let mut waiting = FuturesUnordered::new();
            loop {
                select! {
                    message = rx.next().fuse() => {
                        if let Some(block) = message {
                            let fut = Self::waiter(store.clone(), block);
                            waiting.push(fut);
                        }
                    }
                    result = waiting.select_next_some() => {
                        match result {
                            Ok(block) => {
                                let message = CoreMessage::Block(block);
                                if let Err(e) = core_channel.send(message).await {
                                    panic!("Synchronizer failed to send message through core channel: {}", e);
                                }
                            },
                            Err(e) => error!("{}", e)
                        }
                    }
                }
            }
        });
        synchronizer
    }

    async fn waiter(mut store: Store, block: Block) -> DiemResult<Block> {
        let previous = block.previous();
        let _ = store.notify_read(previous.to_vec()).await?;
        Ok(block)
    }

    async fn get_previous_block(&mut self, block: &Block) -> DiemResult<Option<Block>> {
        if block.qc == QC::genesis() {
            return Ok(Some(Block::genesis()));
        }
        let previous = block.previous();
        match self.store.read(previous.to_vec()).await? {
            Some(bytes) => {
                bincode::deserialize(&bytes).map_err(|e| DiemError::StoreError(e.to_string()))
            }
            None => {
                debug!("Requesting sync for block {:?}", previous);
                if let Err(e) = self.inner_channel.send(block.clone()).await {
                    panic!("Failed to send request to synchronizer: {}", e);
                }
                let sync_request = NetMessage::SyncRequest(previous);
                if let Err(e) = self.network_channel.send(sync_request).await {
                    panic!("Failed to send Sync Request to network: {}", e);
                }
                Ok(None)
            }
        }
    }

    pub async fn get_ancestors(
        &mut self,
        block: &Block,
    ) -> DiemResult<Option<(Block, Block, Block)>> {
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
