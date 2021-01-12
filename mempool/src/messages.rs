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
}

impl Payload {
    pub async fn new(
        transactions: Vec<Transaction>,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let payload = Self {
            transactions,
            author,
            signature: Signature::default(),
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
        write!(f, "Payload({:?})", self.digest())
    }
}

pub struct PayloadMaker {
    transactions: Vec<Transaction>,
    size: usize,
    max_size: usize,
    name: PublicKey,
    signature_service: SignatureService,
}

impl PayloadMaker {
    pub fn new(name: PublicKey, signature_service: SignatureService, max_size: usize) -> Self {
        Self {
            transactions: Vec::with_capacity(max_size),
            size: 0,
            max_size,
            name,
            signature_service,
        }
    }

    pub async fn add(&mut self, transaction: Transaction) -> Option<Payload> {
        let length = transaction.len();
        let ret = if self.size + length > self.max_size {
            self.size = 0;
            let transactions = self.transactions.drain(..).collect();
            Some(Payload::new(transactions, self.name, self.signature_service.clone()).await)
        } else {
            None
        };
        self.transactions.push(transaction);
        self.size += length;
        ret
    }
}
