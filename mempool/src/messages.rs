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
        write!(f, "Payload({}, {})", self.digest(), self.size())
    }
}
