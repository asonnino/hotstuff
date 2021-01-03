use rand::Rng as _;
use std::convert::TryInto;

type Payload = Vec<u8>;

pub struct Mempool;

impl Mempool {
    pub async fn get_payload(&self) -> Payload {
        let mut rng = rand::thread_rng();
        [
            rng.gen::<u128>().to_le_bytes(),
            rng.gen::<u128>().to_le_bytes(),
        ]
        .concat()
        .try_into()
        .unwrap()
    }

    pub async fn ready(&self, _payload: &Payload) -> bool {
        // This function is called by the core upon processing a new block
        // to ask the mempool if it has all the block data. This is useful
        // in case the payload is a hash, certificate, or does any represent
        // the txs data. This function returns True if the core can process
        // the block. Otherwise, it does whatever it needs to do to get the
        // block data, schedule a re-processing of the block, and returns False.
        true
    }
}
