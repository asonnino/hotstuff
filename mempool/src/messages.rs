use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::fmt;

pub type Transaction = Vec<u8>;

#[derive(Deserialize, Serialize)]
pub struct Payload {
    pub transactions: Vec<Transaction>,
    pub author: PublicKey,
    pub signature: Signature,
    #[cfg(feature = "benchmark")]
    pub special_txs: usize,
}

impl Payload {
    pub async fn new(
        transactions: Vec<Transaction>,
        author: PublicKey,
        mut signature_service: SignatureService,
        #[cfg(feature = "benchmark")] special_txs: usize,
    ) -> Self {
        let payload = Self {
            transactions,
            author,
            signature: Signature::default(),
            #[cfg(feature = "benchmark")]
            special_txs,
        };
        let signature = signature_service.request_signature(payload.digest()).await;
        Self {
            signature,
            ..payload
        }
    }

    pub fn size(&self) -> usize {
        self.transactions.iter().map(|x| x.len()).sum()
    }
}

impl Hash for Payload {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        for transaction in &self.transactions {
            hasher.update(transaction);
        }
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Payload({}, {})", self.digest(), self.size())
    }
}

pub struct PayloadMaker {
    transactions: Vec<Transaction>,
    size: usize,
    max_size: usize,
    name: PublicKey,
    signature_service: SignatureService,
    #[cfg(feature = "benchmark")]
    special_txs: usize,
}

impl PayloadMaker {
    pub fn new(name: PublicKey, signature_service: SignatureService, max_size: usize) -> Self {
        Self {
            transactions: Vec::with_capacity(max_size),
            size: 0,
            max_size,
            name,
            signature_service,
            #[cfg(feature = "benchmark")]
            special_txs: 0,
        }
    }

    pub async fn add(&mut self, tx: Transaction) -> Option<Payload> {
        let length = tx.len();
        let ret = match self.size + length > self.max_size {
            true => Some(self.make().await),
            false => None,
        };
        
        #[cfg(feature = "benchmark")]
        if tx.windows(2).all(|x| x[0] == x[1]) && tx.contains(&5u8) {
            // Count the number of special transactions in the payload.
            self.special_txs += 1;
        }

        self.transactions.push(tx);
        self.size += length;
        ret
    }

    pub async fn make(&mut self) -> Payload {
        self.size = 0;
        let transactions = self.transactions.drain(..).collect();
        let payload = Payload::new(
            transactions,
            self.name,
            self.signature_service.clone(),
            #[cfg(feature = "benchmark")]
            self.special_txs,
        )
        .await;

        #[cfg(feature = "benchmark")]
        {
            self.special_txs = 0;
        }

        payload
    }
}
