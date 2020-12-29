use crate::crypto::Digest;
use crate::error::{DiemError, DiemResult};
use crate::messages::Block;
use crate::network::NetMessage;
use crate::store::Store;
use futures::future::FutureExt as _;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use tokio::sync::mpsc::{channel, Sender};

pub struct Synchronizer {
    inner_channel: Sender<Digest>,
    network_channel: Sender<NetMessage>,
}

impl Synchronizer {
    pub async fn new(
        store: Store,
        inner_channel: Sender<DiemResult<Block>>,
        network_channel: Sender<NetMessage>,
    ) -> Self {
        let (tx, mut rx) = channel(1000);
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            loop {
                select! {
                    msg = rx.next().fuse() => {
                        if let Some(digest) = msg {
                            let fut = Self::waiter(store.clone(), digest);
                            waiting.push(fut);
                        }
                    }
                    result = waiting.select_next_some() => {
                        // TODO: This should be an output channel?
                        if let Err(e) = inner_channel.send(result).await {
                            panic!("Synchronizer failed to send message through output channel: {}", e);
                        }
                    }
                }
            }
        });
        Self {
            inner_channel: tx,
            network_channel,
        }
    }

    async fn waiter(mut store: Store, digest: Digest) -> DiemResult<Block> {
        let bytes = store.notify_read(digest.to_vec()).await?;
        bincode::deserialize(&bytes).map_err(|e| DiemError::StoreError(e.to_string()))
    }

    pub async fn request(&mut self, digest: Digest) {
        if let Err(e) = self.inner_channel.send(digest).await {
            panic!("Failed to send request to synchronizer: {}", e);
        }
        let sync_request = NetMessage::SyncRequest(digest);
        if let Err(e) = self.network_channel.send(sync_request).await {
            panic!("Failed to send Sync Request to network: {}", e);
        }
    }
}
