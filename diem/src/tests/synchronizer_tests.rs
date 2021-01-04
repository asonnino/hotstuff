/*
use super::*;
use crate::crypto::generate_keypair;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

#[tokio::test]
async fn get_previous_block() {
    // Make a new synchronizer.
    let mut rng = StdRng::from_seed([0; 32]);
    let (public_key, secret_key) = generate_keypair(&mut rng);
    let path = ".store_test_get_previous_block";
    let store = Store::new(path).await;
    let (tx_network, mut rx_network) = channel(10);
    let (tx_core, mut rx_core) = channel(10);
    let mut synchronizer = Synchronizer::new(public_key, store, tx_network, tx_core).await;

    let qc = QC {
        hash: [1u8; 32],
        round: 1,
        votes: Vec::new(),
    };
    let block = Block {
        qc,
        tc: None,
        author: public_key,
        round: qc.round + 1,
        payload: Vec::new(),
        signature: Signature::default(),
    };
    let digest = block.digest();
    let signature = Signature::new(&digest, &secret_key);
    let block = Block {signature: signature, ..block};

    let result = synchronizer.get_previous_block(&block).await;

}
*/
