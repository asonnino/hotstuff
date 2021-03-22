use super::*;
use crate::common::{block, committee, keys, payload};
use crate::config::Parameters;
use bytes::Bytes;
use consensus::{Block, PayloadStatus};
use crypto::Hash as _;
use futures::future::try_join_all;
use futures::sink::SinkExt as _;
use std::fs;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn end_to_end() {
    let mut committee = committee();
    committee.increment_base_port(5000);

    // Run all mempools.
    let mempool_handles: Vec<_> = keys()
        .into_iter()
        .enumerate()
        .map(|(i, (name, secret))| {
            let committee = committee.clone();
            let parameters = Parameters {
                queue_capacity: 1,
                sync_retry_delay: 10_000,
                max_payload_size: 1,
                min_block_delay: 0,
            };
            let signature_service = SignatureService::new(secret, None);
            let store_path = format!(".db_test_end_to_end_{}", i);
            let _ = fs::remove_dir_all(&store_path);
            let store = Store::new(&store_path).unwrap();
            let (tx_consensus, _rx_consensus) = channel(1);
            let (tx_consensus_mempool, rx_consensus_mempool) = channel(1);

            tokio::spawn(async move {
                Mempool::run(
                    name,
                    committee,
                    parameters,
                    store,
                    signature_service,
                    tx_consensus,
                    rx_consensus_mempool,
                )
                .unwrap();
                sleep(Duration::from_millis(100)).await;

                let payload = vec![payload().digest()];
                let block = Block { payload, ..block() };
                let (sender, receiver) = oneshot::channel();
                let message = ConsensusMempoolMessage::Verify(Box::new(block), sender);
                tx_consensus_mempool.send(message).await.unwrap();
                match receiver.await {
                    Ok(PayloadStatus::Accept) => assert!(true),
                    _ => assert!(false),
                }
            })
        })
        .collect();

    // Wait for the mempools to boot.
    sleep(Duration::from_millis(50)).await;

    // Send a payload to all mempools.
    let client_handles: Vec<_> = keys()
        .into_iter()
        .map(|(name, _)| {
            let address = committee.clone().front_address(&name).unwrap();
            tokio::spawn(async move {
                let stream = TcpStream::connect(address).await.unwrap();
                let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
                let transaction = vec![1u8];
                let bytes = Bytes::from(transaction.to_vec());
                transport.send(bytes.clone()).await.unwrap();
                transport.send(bytes.clone()).await.unwrap();
            })
        })
        .collect();

    // Ensure all transactions are sent.
    assert!(try_join_all(client_handles).await.is_ok());

    // Ensure all threads terminated correctly.
    assert!(try_join_all(mempool_handles).await.is_ok());
}
