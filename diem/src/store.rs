use crate::error::DiemError;
use crate::messages::Block;
use log::error;
use rocksdb::DB;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

type Key = Vec<u8>;
type Value = Vec<u8>;
type StoreResult<T> = Result<T, DiemError>;

pub enum StoreCommand {
    Write(Key, Value, oneshot::Sender<StoreResult<()>>),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
}

pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub async fn new(path: String) -> Result<Self, DiemError> {
        let db = DB::open_default(path)?;
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value, sender) => {
                        let response = db.put(key, value).map_err(DiemError::from);
                        if sender.send(response).is_err() {
                            error!("Failed to reply to store command: Receiver dropped");
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(key).map_err(DiemError::from);
                        if sender.send(response).is_err() {
                            error!("Failed to reply to store command: Receiver dropped");
                        }
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) -> StoreResult<()> {
        let (sender, receiver) = oneshot::channel();
        self.channel
            .send(StoreCommand::Write(key, value, sender))
            .await?;
        receiver.await?
    }

    pub async fn read(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        self.channel.send(StoreCommand::Read(key, sender)).await?;
        receiver.await?
    }

    pub async fn get_previous_block(&mut self, block: &Block) -> StoreResult<Block> {
        // TODO
        let bytes = self.read(block.qc.hash.to_vec()).await?.unwrap();
        let previous_block = bincode::deserialize(&bytes)?;
        Ok(previous_block)
    }
}
