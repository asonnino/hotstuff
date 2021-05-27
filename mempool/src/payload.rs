use crate::core::MempoolMessage;
use crate::messages::{Payload, Transaction};
use crypto::{PublicKey, SignatureService};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

struct Runner {
    transactions: Vec<Transaction>,
    size: usize,
    max_size: usize,
    min_block_delay: u64,
    name: PublicKey,
    signature_service: SignatureService,
    client_channel: Receiver<Transaction>,
    core_channel: Sender<MempoolMessage>,
    request_channel: Receiver<oneshot::Sender<Payload>>,
}

impl Runner {
    fn new(
        name: PublicKey,
        signature_service: SignatureService,
        max_size: usize,
        min_block_delay: u64,
        client_channel: Receiver<Transaction>,
        core_channel: Sender<MempoolMessage>,
        request_channel: Receiver<oneshot::Sender<Payload>>,
    ) -> Self {
        Self {
            transactions: Vec::with_capacity(max_size),
            size: 0,
            max_size,
            min_block_delay,
            name,
            signature_service,
            client_channel,
            core_channel,
            request_channel,
        }
    }

    async fn add(&mut self, tx: Transaction) -> Option<Payload> {
        let length = tx.len();
        let ret = match self.size + length > self.max_size {
            true => Some(self.make().await),
            false => None,
        };

        self.transactions.push(tx);
        self.size += length;
        ret
    }

    async fn make(&mut self) -> Payload {
        let transactions = self.transactions.drain(..).collect();

        // Cleanup state.
        self.size = 0;

        // Make a payload.
        Payload::new(transactions, self.name, self.signature_service.clone()).await
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(transaction) = self.client_channel.recv() => {
                    if let Some(payload) = self.add(transaction).await {
                        let message = MempoolMessage::OwnPayload(payload);
                        if let Err(e) = self.core_channel.send(message).await {
                            panic!("Failed to send payload to the core: {}", e);
                        }

                        // Wait for the minimum block delay.
                        sleep(Duration::from_millis(self.min_block_delay)).await;
                    }
                },
                Some(sender) = self.request_channel.recv() => {
                    let _ = sender.send(self.make().await);
                },
                else => break,
            }
        }
    }
}

pub struct PayloadMaker {
    request_channel: Sender<oneshot::Sender<Payload>>,
}

impl PayloadMaker {
    pub fn new(
        name: PublicKey,
        signature_service: SignatureService,
        max_size: usize,
        min_block_delay: u64,
        client_channel: Receiver<Transaction>,
        core_channel: Sender<MempoolMessage>,
    ) -> Self {
        let (tx_request, rx_request) = channel(10000);
        tokio::spawn(async move {
            Runner::new(
                name,
                signature_service,
                max_size,
                min_block_delay,
                client_channel,
                core_channel,
                rx_request,
            )
            .run()
            .await;
        });
        Self {
            request_channel: tx_request,
        }
    }

    pub async fn make(&mut self) -> Option<Payload> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.request_channel.send(sender).await {
            panic!("Failed to request payload from the inner runner: {}", e);
        }
        let payload = receiver
            .await
            .expect("Failed to receive payload from the inner runner");
        match payload.size() {
            0 => None,
            _ => Some(payload),
        }
    }
}
