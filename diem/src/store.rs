use log::error;
use rocksdb;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub enum StoreCommand {
    Write(Key, Value, oneshot::Sender<Result<(), StoreError>>),
    Read(Key, oneshot::Sender<Result<Option<Value>, StoreError>>),
}

pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub async fn new(path: String) -> Result<Self, StoreError> {
        let db = rocksdb::DB::open_default(path)?;
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value, sender) => {
                        let response = db.put(key, value).map_err(StoreError::from);
                        if sender.send(response).is_err() {
                            error!("Failed to reply to store command: Receiver dropped");
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(key).map_err(StoreError::from);
                        if sender.send(response).is_err() {
                            error!("Failed to reply to store command: Receiver dropped");
                        }
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) -> Result<(), StoreError> {
        let (sender, receiver) = oneshot::channel();
        self.channel
            .send(StoreCommand::Write(key, value, sender))
            .await?;
        receiver.await?
    }

    pub async fn read(&mut self, key: Key) -> Result<Option<Value>, StoreError> {
        let (sender, receiver) = oneshot::channel();
        self.channel.send(StoreCommand::Read(key, sender)).await?;
        receiver.await?
    }
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("RocksDB internal error: {0}")]
    RocksDBError(rocksdb::Error),

    #[error("Internal store channel Error: {0}")]
    ChannelError(String),
}

impl From<rocksdb::Error> for StoreError {
    fn from(e: rocksdb::Error) -> Self {
        StoreError::RocksDBError(e)
    }
}

impl From<oneshot::error::RecvError> for StoreError {
    fn from(e: oneshot::error::RecvError) -> Self {
        StoreError::ChannelError(e.to_string())
    }
}

impl From<SendError<StoreCommand>> for StoreError {
    fn from(e: SendError<StoreCommand>) -> Self {
        StoreError::ChannelError(e.to_string())
    }
}
