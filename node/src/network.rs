use crate::config::Committee;
use crate::core::CoreMessage;
use crate::crypto::{Digest, PublicKey};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Block, Vote};
use bytes::{Bytes, BytesMut};
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
pub mod network_tests;

#[derive(Serialize, Deserialize, Debug)]
pub enum NetMessage {
    Block(Block),
    Vote(Vote, PublicKey),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block, PublicKey),
}

pub struct NetSender;

impl NetSender {
    pub async fn make(name: PublicKey, committee: Committee) -> Sender<NetMessage> {
        let mut senders = HashMap::<_, Sender<_>>::new();
        let (tx, mut rx) = channel(1000);
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                debug!("Sending {:?}", message);

                // We receive messages from the core. Some of them need to be sent only
                // only a specific node and others need to be broadcast. In the latter
                // case, the function `make_messages` returns multiple messages: one for 
                // each node (except ourself).
                for (bytes, address) in Self::make_messages(message, &committee, name) {
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
                            let tx = Self::make_worker(address).await;
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
        });
        tx
    }

    fn make_messages(
        message: NetMessage,
        committee: &Committee,
        name: PublicKey,
    ) -> Vec<(Bytes, SocketAddr)> {
        // Extract the message content and destination address.
        let (message, address) = match message {
            NetMessage::Block(block) => (CoreMessage::Propose(block), None),
            NetMessage::Vote(vote, to) => {
                let message = CoreMessage::Vote(vote);
                let address = committee
                    .address(&to)
                    .expect("Network address of next leader unknown");
                (message, Some(address))
            }
            NetMessage::SyncRequest(digest, from) => (CoreMessage::SyncRequest(digest, from), None),
            NetMessage::SyncReply(block, to) => {
                (CoreMessage::Propose(block), committee.address(&to))
            }
        };

        // Encode the message and find the network address of the receiver.
        let bytes = match bincode::serialize(&message) {
            Ok(bytes) => Bytes::from(bytes),
            Err(e) => panic!("Failed to serialize Core message: {}", e),
        };
        match address {
            Some(address) => vec![(bytes, address)],
            None => committee
                .broadcast_addresses(&name)
                .into_iter()
                .map(|x| (bytes.clone(), x))
                .collect(),
        }
    }

    async fn make_worker(address: SocketAddr) -> Sender<Bytes> {
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

pub struct NetReceiver;

impl NetReceiver {
    pub async fn make(address: &SocketAddr, core_channel: Sender<CoreMessage>) {
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to bind to TCP port");

        debug!("Listening on {}", address);
        tokio::spawn(async move {
            loop {
                let (socket, peer) = match listener.accept().await {
                    Ok(value) => value,
                    Err(e) => {
                        warn!("{}", ConsensusError::from(e));
                        continue;
                    }
                };
                info!("Connection established with peer {}", peer);

                let core_channel = core_channel.clone();
                tokio::spawn(async move {
                    let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
                    while let Some(frame) = transport.next().await {
                        if let Err(e) = Self::handle_message(frame, &core_channel).await {
                            warn!("{}", e);
                        }
                    }
                    info!("Connection closed by peer {}", peer);
                });
            }
        });
    }

    async fn handle_message(
        frame: Result<BytesMut, std::io::Error>,
        core_channel: &Sender<CoreMessage>,
    ) -> ConsensusResult<()> {
        let bytes = frame?;
        let message = bincode::deserialize(&bytes)?;
        debug!("Received {:?}", message);
        if let Err(e) = core_channel.send(message).await {
            panic!("Failed to send message to Core: {}", e);
        }
        Ok(())
    }
}
