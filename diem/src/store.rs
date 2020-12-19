use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

pub type StoreError = rocksdb::Error;
type StoreResult<T> = Result<T, StoreError>;

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
                        let response = db.put(key, value);
                        let _ = sender.send(response);
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(key);
                        let _ = sender.send(response);
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) -> StoreResult<()> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::Write(key, value, sender))
            .await
        {
            panic!("Failed to send Write Command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Write Command from store")
    }

    pub async fn read(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read Command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read Command from store")
    }
}
