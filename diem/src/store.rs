use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

type StoreResult<T> = Result<T, StoreError>;

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

type Key = Vec<u8>;
type Value = Vec<u8>;

pub enum StoreCommand {
    Write(Key, Value, oneshot::Sender<StoreResult<()>>),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
}

pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub async fn new(path: String) -> StoreResult<Self> {
        let db = rocksdb::DB::open_default(path)?;
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value, sender) => {
                        let response = db.put(key, value).map_err(StoreError::from);
                        sender
                            .send(response)
                            .expect("Failed to reply to store write command: Receiver dropped");
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(key).map_err(StoreError::from);
                        sender
                            .send(response)
                            .expect("Failed to reply to store read command: Receiver dropped");
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
}
