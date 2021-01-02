use crate::committee::Committee;
use crate::core::CoreMessage;
use crate::crypto::{Digest, PublicKey};
use crate::error::{DiemError, DiemResult};
use crate::messages::{Block, Vote};
use bytes::Bytes;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt as _;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Serialize, Deserialize, Debug)]
pub enum NetMessage {
    Block(Block),
    Vote(Vote, PublicKey),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block, PublicKey),
}

pub struct NetSender {
    name: PublicKey,
    committee: Committee,
    channel: Receiver<NetMessage>,
}

impl NetSender {
    pub async fn run(&'static mut self) {
        let addresses = self.committee.broadcast_addresses(&self.name);
        while let Some(message) = self.channel.recv().await {
            debug!("Sending message {:?}", message);
            let (message, address) = match message {
                NetMessage::Block(block) => (CoreMessage::Propose(block), None),
                NetMessage::Vote(vote, to) => {
                    let message = CoreMessage::Vote(vote);
                    let address = self
                        .committee
                        .address(&to)
                        .expect("Network address of next leader unknown");
                    (message, Some(address))
                }
                NetMessage::SyncRequest(digest, from) => {
                    (CoreMessage::SyncRequest(digest, from), None)
                }
                NetMessage::SyncReply(block, to) => {
                    (CoreMessage::Propose(block), self.committee.address(&to))
                }
            };

            let bytes = match bincode::serialize(&message) {
                Ok(bytes) => bytes,
                Err(e) => panic!("Failed to serialize Core message: {}", e),
            };
            let addresses = addresses.clone();
            tokio::spawn(async move {
                let result = match address {
                    Some(address) => Self::send(bytes, address).await,
                    None => Self::broadcast(bytes, addresses).await,
                };
                match result {
                    Ok(()) => debug!("Successfully sent message"),
                    Err(e) => warn!("Failed to send message. {}", e),
                }
            });
        }
    }

    async fn send(message: Vec<u8>, address: String) -> DiemResult<()> {
        let stream = TcpStream::connect(address).await?;
        let (read, write) = stream.into_split();
        let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
        let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
        transport_write.send(Bytes::from(message)).await?;
        let _ = transport_read.next().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to receive Ack from peer")
        })?;
        Ok(())
    }

    async fn broadcast(message: Vec<u8>, addresses: Vec<String>) -> DiemResult<()> {
        for address in addresses {
            let message = message.clone();
            tokio::spawn(async move {
                let _ = Self::send(message, address);
            });
        }
        Ok(())
    }
}

pub struct NetReceiver;

impl NetReceiver {
    pub async fn run(address: String, core_channel: Sender<CoreMessage>) {
        let listener = TcpListener::bind(&address)
            .await
            .expect("Failed to bind to TCP port");

        info!("Core listening on address {}", address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", DiemError::from(e));
                    continue;
                }
            };

            let core_channel = core_channel.clone();
            tokio::spawn(async move {
                debug!("Incoming request from {}", peer);
                let (read, write) = socket.into_split();
                let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
                let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
                while let Some(frame) = transport_read.next().await {
                    match frame {
                        Ok(bytes) => {
                            match bincode::deserialize(&bytes) {
                                Ok(message) => {
                                    if let Err(e) = core_channel.send(message).await {
                                        panic!("Failed to send message to Core: {}", e);
                                    }
                                }
                                Err(e) => warn!("{}", DiemError::from(e)),
                            }

                            let response = Bytes::from("Ack");
                            if let Err(e) = transport_write.send(response).await {
                                warn!("Failed to reply with read result: {}", e);
                            }
                        }
                        Err(e) => warn!("{}", DiemError::from(e)),
                    }
                }
                debug!("Connection closed by peer {}", peer);
            });
        }
    }
}
