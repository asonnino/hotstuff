use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::sink::SinkExt as _;
use log::info;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for hotstuff nodes.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs.'")
        .args_from_usage("--transactions=<INT> 'The number of transactions for the benchmark'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let address = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let transactions = matches
        .value_of("transactions")
        .unwrap()
        .parse::<usize>()
        .context("The number of transactions must be a non-negative integer")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<usize>()
        .context("The rate of transactions must be a non-negative integer")?;

    info!("Node address: {}", address);
    info!("Number of transactions: {}", transactions);
    info!("Transactions size: {} B", size);
    info!("Transactions rate: {} tx/s", rate);
    let client = Client {
        address,
        transactions,
        size,
        rate,
    };
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    address: SocketAddr,
    transactions: usize,
    size: usize,
    rate: usize,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 16 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 16 bytes",
            ));
        }

        // Adapt for the case where the transaction rate is zero.
        let (batches, burst) = match self.rate {
            0 => (1, self.rate),
            _ => (self.transactions / self.rate + 1, self.rate),
        };

        // Connect to the mempool.
        let stream = TcpStream::connect(self.address)
            .await
            .context(format!("failed to connect to {}", self.address))?;
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

        // Submit all transactions.
        let interval = interval(Duration::from_millis(1000));
        tokio::pin!(interval);
        info!("Start sending transactions");
        for x in 0..batches {
            interval.as_mut().tick().await;
            if self.rate == 0 {
                self.send_burst(&mut transport, burst, x as u64).await?;
            } else {
                timeout(
                    Duration::from_millis(1000),
                    self.send_burst(&mut transport, burst, x as u64),
                )
                .await
                .context("transaction rate too high for this client")??;
            }
        }
        info!("Finished sending transactions");
        Ok(())
    }

    async fn send_burst(
        &self,
        transport: &mut Framed<TcpStream, LengthDelimitedCodec>,
        load: usize,
        nonce: u64,
    ) -> Result<()> {
        for x in 0..load {
            let mut tx = BytesMut::with_capacity(self.size);
            tx.put_u64(nonce);
            tx.put_u64(x as u64);
            tx.resize(self.size, 0u8);
            transport
                .send(tx.freeze())
                .await
                .context("Failed to send transaction")?;
        }
        Ok(())
    }
}
