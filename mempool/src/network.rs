use crate::core::CoreMessage;
use crate::error::{MempoolError, MempoolResult};
use crate::messages::Payload;
use bytes::{Bytes, BytesMut};
use config::Committee;
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug)]
pub enum NetMessage {
    Share(Payload),
    Request(Digest, PublicKey),
    Reply(Payload, PublicKey),
}

pub struct NetSender {
    name: PublicKey,
    committee: Committee,
    receiver: Receiver<NetMessage>,
}

impl NetSender {
    pub fn new(name: PublicKey, committee: Committee, receiver: Receiver<NetMessage>) -> Self {
        Self {
            name,
            committee,
            receiver,
        }
    }

    pub async fn run(&mut self) {
        let mut senders = HashMap::<_, Sender<_>>::new();
        while let Some(message) = self.receiver.recv().await {
            debug!("Sending {:?}", message);
            // We receive messages from the core. Some of them need to be sent only
            // only a specific peer and others need to be broadcast. In the latter
            // case, the function `make_messages` returns multiple messages: one for
            // each node (except ourself).
            for (bytes, address) in self.make_messages(message) {
                // We keep alive one TCP connection per peer, each of which is handled
                // by a separate thread (called worker). We communicate with our workers
                // with a dedicated channel kept by the HashMap called `senders`. If the
                // a connection die, we make a new one.
                match senders.get(&address) {
                    // Check that we have an alive connection with this node...
                    Some(tx) if !tx.is_closed() => {
                        if let Err(e) = tx.send(bytes).await {
                            panic!("Failed to send message to inner worker: {} ", e);
                        }
                    }
                    // ... If we don't, we make a new one.
                    _ => {
                        let tx = Self::spawn_worker(address).await;
                        if !tx.is_closed() {
                            if let Err(e) = tx.send(bytes).await {
                                panic!("Failed to send message to inner worker: {} ", e);
                            }
                            senders.insert(address, tx);
                        }
                    }
                }
            }
        }
    }

    fn make_messages(&self, message: NetMessage) -> Vec<(Bytes, SocketAddr)> {
        // Extract the message content and destination address.
        let (message, address) = match message {
            NetMessage::Share(payload) => (CoreMessage::Payload(payload), None),
            NetMessage::Request(digest, from) => (CoreMessage::Request(digest, from), None),
            NetMessage::Reply(payload, to) => match self.committee.address(&to) {
                Ok(address) => (CoreMessage::Payload(payload), Some(address)),
                Err(_) => return vec![],
            },
        };

        // Encode the message and find the network address of the receiver.
        let bytes = match bincode::serialize(&message) {
            Ok(bytes) => Bytes::from(bytes),
            Err(e) => panic!("Failed to serialize core message: {}", e),
        };
        match address {
            Some(address) => vec![(bytes, address)],
            None => self
                .committee
                .broadcast_addresses(&self.name)
                .into_iter()
                .map(|x| (bytes.clone(), x))
                .collect(),
        }
    }

    async fn spawn_worker(address: SocketAddr) -> Sender<Bytes> {
        // Each worker handle a TCP connection with on address.
        let (tx, mut rx) = channel(1000);
        tokio::spawn(async move {
            let stream = match TcpStream::connect(address).await {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("Failed to connect to {}: {}", address, e);
                    return;
                }
            };
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
            while let Some(message) = rx.recv().await {
                if let Err(e) = transport.send(message).await {
                    warn!("Failed to send message to {}: {}", address, e);
                    return;
                }
            }
        });
        tx
    }
}

pub struct NetReceiver<Message> {
    address: SocketAddr,
    core_channel: Sender<Message>,
}

impl<Message: 'static + Send + DeserializeOwned> NetReceiver<Message> {
    pub fn new(address: SocketAddr, core_channel: Sender<Message>) -> Self {
        Self {
            address,
            core_channel,
        }
    }

    pub async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind to TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", MempoolError::from(e));
                    continue;
                }
            };
            info!("Connection established with peer {}", peer);
            Self::spawn_worker(socket, peer, self.core_channel.clone()).await;
        }
    }

    async fn spawn_worker(socket: TcpStream, peer: SocketAddr, core_channel: Sender<Message>) {
        tokio::spawn(async move {
            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
            while let Some(frame) = transport.next().await {
                match frame
                    .map_err(MempoolError::from)
                    .and_then(|x| Ok(bincode::deserialize(&x)?))
                {
                    Ok(message) => {
                        //debug!("Received {:?}", message);
                        if let Err(e) = core_channel.send(message).await {
                            panic!("Failed to send message to Core: {}", e);
                        }
                    }
                    Err(e) => warn!("{}", e),
                }
            }
            info!("Connection closed by peer {}", peer);
        });
    }
}
