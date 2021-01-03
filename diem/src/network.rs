use crate::committee::Committee;
use crate::core::CoreMessage;
use crate::crypto::{Digest, PublicKey};
use crate::error::{DiemError, DiemResult};
use crate::messages::{Block, Vote};
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt as _;
use futures::select;
use futures::sink::SinkExt as _;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Serialize, Deserialize, Debug)]
pub enum NetMessage {
    Block(Block),
    Vote(Vote, PublicKey),
    SyncRequest(Digest, PublicKey),
    SyncReply(Block, PublicKey),
}

pub struct NetSender;

impl NetSender {
    pub async fn new(name: PublicKey, committee: Committee, mut rx: Receiver<NetMessage>) {
        let mut waiting = FuturesUnordered::new();
        tokio::spawn(async move {
            loop {
                select! {
                    message = rx.recv().fuse() => {
                        if let Some(message) = message {
                            debug!("Sending message {:?}", message);
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
    }

    fn make_messages(
        message: NetMessage,
        committee: &Committee,
        name: PublicKey,
    ) -> Vec<(Bytes, String)> {
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

        // Encore the message and find the network address of the receiver.
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
    async fn send(message: Bytes, address: String) -> DiemResult<()> {
        let stream = TcpStream::connect(address).await?;
        let (read, write) = stream.into_split();
        let mut transport_write = FramedWrite::new(write, LengthDelimitedCodec::new());
        let mut transport_read = FramedRead::new(read, LengthDelimitedCodec::new());
        transport_write.send(message).await?;
        // TODO: Better error detect; let's make sure we receive Ack for SyncRequests.
        // Also, let's ensure we don't return Some(Err(e)).
        let _ = transport_read.next().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to receive Ack from peer")
        })?;
        Ok(())
    }
}

pub struct NetReceiver;

impl NetReceiver {
    pub async fn new(address: String, core_channel: Sender<CoreMessage>) {
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
    }

    async fn handle_message(
        frame: Result<BytesMut, io::Error>,
        core_channel: &Sender<CoreMessage>,
        transport_write: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    ) -> DiemResult<()> {
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
