use crate::config::Committee;
use crate::core::CoreMessage;
use crate::crypto::{Digest, PublicKey};
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::{Block, Vote};
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt as _;
use futures::select;
use futures::sink::SinkExt as _;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Sender};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

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

struct SenderWorker;

impl SenderWorker {
    async fn make(address: SocketAddr) -> Sender<Bytes> {
        let (tx, mut rx) = channel(1000);
        tokio::spawn(async move {
            let mut stream = match TcpStream::connect(address).await {
                Ok(stream) => stream,
                Err(e) => {
                    warn!("Failed to connect to {}: {}", address, e);
                    return;
                }
            };
            let (read, write) = stream.split();
            let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
            let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
            while let Some(message) = rx.recv().await {
                match transport_write.send(message).await {
                    Ok(()) => {
                        let _ = transport_read.next().await;
                    }
                    Err(e) => {
                        warn!("Failed to send message to {}: {}", address, e);
                        return;
                    }
                }
            }
        });
        tx
    }
}

pub struct NetSender;

impl NetSender {
    pub async fn make(name: PublicKey, committee: Committee) -> Sender<NetMessage> {
        let mut senders: HashMap<_, Sender<_>> = HashMap::new();
        let (tx, mut rx) = channel(1000);
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                debug!("Sending {:?}", message);
                for (bytes, address) in Self::make_messages(message, &committee, name) {
                    match senders.get(&address) {
                        Some(tx) if !tx.is_closed() => {
                            if let Err(e) = tx.send(bytes).await {
                                panic!("Net Sender failed to send message to Worker: {} ", e);
                            }
                        }
                        _ => {
                            let tx = SenderWorker::make(address).await;
                            if tx.is_closed() {
                                warn!("Failed to connect to {}", address);
                            } else {
                                if let Err(e) = tx.send(bytes).await {
                                    panic!("Net Sender failed to send message to Worker: {} ", e);
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
}

/*
pub struct NetSender;

impl NetSender {
    pub async fn make(name: PublicKey, committee: Committee) -> Sender<NetMessage> {
        let (tx, mut rx) = channel(1000);
        let mut waiting = FuturesUnordered::new();
        tokio::spawn(async move {
            loop {
                select! {
                    message = rx.recv().fuse() => {
                        if let Some(message) = message {
                            debug!("Sending {:?}", message);
                            for (bytes, address) in Self::make_messages(message, &committee, name) {
                                let fut = Self::send(bytes, address);
                                waiting.push(fut);
                            }
                        }
                    }
                    result = waiting.select_next_some() => {
                        if let Err(e) = result {
                            warn!("Failed to send message. {}", e);
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

    /// Send a message to a specific network address.
    async fn send(message: Bytes, address: SocketAddr) -> ConsensusResult<()> {
        let stream = TcpStream::connect(address).await?;
        let (read, write) = stream.into_split();
        let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
        let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
        transport_write.send(message).await?;
        if let None | Some(Err(_)) = transport_read.next().await {
            bail!(ConsensusError::NetworkError(io::Error::new(
                io::ErrorKind::Other,
                "Failed to receive Ack from peer"
            )))
        }
        Ok(())
    }
}
*/

pub struct NetReceiver;

impl NetReceiver {
    pub async fn make(address: &SocketAddr, core_channel: Sender<CoreMessage>) {
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to bind to TCP port");

        debug!("Listening on address {}", address);
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
                    let (read, write) = socket.into_split();
                    let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
                    let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
                    while let Some(frame) = transport_read.next().await {
                        if let Err(e) =
                            Self::handle_message(frame, &core_channel, &mut transport_write).await
                        {
                            warn!("{}", e);
                        }
                    }
                    info!("Connection closed by peer {}", peer);
                });
            }
        });
    }

    async fn handle_message(
        frame: Result<BytesMut, io::Error>,
        core_channel: &Sender<CoreMessage>,
        transport_write: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    ) -> ConsensusResult<()> {
        let bytes = frame?;
        let message = bincode::deserialize(&bytes)?;
        if let Err(e) = core_channel.send(message).await {
            panic!("Failed to send message to Core: {}", e);
        }
        let response = Bytes::from("Ack");
        transport_write.send(response).await?;
        Ok(())
    }
}
