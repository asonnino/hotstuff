use crate::core::CoreMessage;
use crate::crypto::{Digest, PublicKey};
use crate::error::DiemError;
use crate::messages::{Block, Vote};
use bytes::Bytes;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::stream::StreamExt as _;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Serialize, Deserialize)]
pub enum NetMessage {
    Block(Block),
    Vote(Vote, PublicKey),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block, PublicKey),
}

pub struct NetSender {
    channel: Receiver<NetMessage>,
}

impl NetSender {
    pub async fn run(&mut self) {
        while let Some(_message) = self.channel.recv().await {
            //
        }
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
