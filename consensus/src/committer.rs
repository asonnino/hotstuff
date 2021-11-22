use crate::consensus::Round;
use crate::messages::{Block, QC};
use crypto::{Digest, Hash as _};
use log::{debug, info, warn};
use std::collections::{HashSet, VecDeque};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct Committer {
    store: Store,
    rx_to_commit: Receiver<Block>,
    rx_ready_to_commit: Receiver<Block>,
    tx_committed: Sender<Block>,
    last_committed_round: Round,
    pending: HashSet<Digest>,
}

impl Committer {
    pub fn spawn(
        store: Store,
        rx_to_commit: Receiver<Block>,
        rx_ready_to_commit: Receiver<Block>,
        tx_committed: Sender<Block>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                rx_to_commit,
                rx_ready_to_commit,
                tx_committed,
                last_committed_round: 0,
                pending: HashSet::new(),
            }
            .run()
            .await
        });
    }

    async fn get_parent_block(&mut self, block: &Block) -> Option<Block> {
        if block.qc == QC::genesis() {
            return Some(Block::genesis());
        }

        let parent = block.parent();
        self.store
            .read(parent.to_vec())
            .await
            .expect("Failed to read store")
            .map(|x| bincode::deserialize(&x).expect("Fail to deserialize block"))
    }

    async fn commit(&mut self, block: Block) {
        if self.last_committed_round >= block.round {
            return;
        }

        // Ensure we commit the entire chain. This is needed after view-change.
        let mut to_commit = VecDeque::new();
        let mut parent = block.clone();
        while self.last_committed_round + 1 < parent.round {
            let ancestor = self
                .get_parent_block(&parent)
                .await
                .expect("We should have all the ancestors by now");
            to_commit.push_front(ancestor.clone());
            parent = ancestor;
        }
        to_commit.push_front(block.clone());

        // Save the last committed block.
        self.last_committed_round = block.round;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            if !block.payload.is_empty() {
                info!("Committed {}", block);

                #[cfg(feature = "benchmark")]
                for x in &block.payload {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", block, x.root);
                }
            }
            debug!("Committed {:?}", block);
            if let Err(e) = self.tx_committed.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(block) = self.rx_to_commit.recv() => {
                    // Check whether we got the payload in the storage.
                    let mut got_payload = true;
                    for x in &block.payload {
                        got_payload &= self
                            .store
                            .read(x.root.to_vec())
                            .await
                            .expect("Failed to read from store")
                            .is_some();

                        if !got_payload {
                            break;
                        }
                    }

                    // Commit only if we got the payload. If we don't we have to wait
                    // for the mempool driver to get it and let us know.
                    if got_payload {
                        self.commit(block).await;
                    } else {
                        self.pending.insert(block.digest());
                    }
                },
                Some(block) = self.rx_ready_to_commit.recv() => {
                    if self.pending.remove(&block.digest()) {
                        self.commit(block).await;
                    }
                }
            }
        }
    }
}
