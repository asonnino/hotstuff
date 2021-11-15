use crate::{config::Committee, mempool::MempoolMessage};
use bytes::Bytes;
use crypto::{Digest, PublicKey};
use log::warn;
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;

//#[cfg(test)]
//#[path = "tests/helper_tests.rs"]
//pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch
/// requests.
pub struct Helper {
    // The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive shard and batch requests.
    rx_request: Receiver<(Digest, PublicKey, bool)>,
    /// A network sender to send the batches to the other mempools.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Digest, PublicKey, bool)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                rx_request,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((root, origin, want_shard)) = self.rx_request.recv().await {
            // TODO: Do some accounting to prevent bad nodes from monopolizing
            // our resources.

            // get the requestors address.
            let address = match self.committee.mempool_address(&origin) {
                Some(x) => x,
                None => {
                    warn!("Received batch request from unknown authority: {}", origin);
                    continue;
                }
            };

            // Check if we have the requested data.
            let data = match want_shard {
                true => {
                    let mut key = root.to_vec();
                    key.extend(self.name.to_vec());
                    self.store
                        .read(key)
                        .await
                        .expect("Failed to read store")
                        .map(|serialized| {
                            match bincode::deserialize(&serialized)
                                .expect("Failed to deserialized authenticated shard")
                            {
                                MempoolMessage::AuthenticatedShard(shard) => {
                                    let message = MempoolMessage::ShardReply(shard);
                                    bincode::serialize(&message).expect("Failed to serialize shard")
                                }
                                _ => panic!("Authenticated shard stored in unexpected format"),
                            }
                        })
                }
                false => self
                    .store
                    .read(root.to_vec())
                    .await
                    .expect("Failed to read store"),
            };

            // Reply to the requestor.
            if let Some(data) = data {
                self.network.send(address, Bytes::from(data)).await
            }
        }
    }
}
