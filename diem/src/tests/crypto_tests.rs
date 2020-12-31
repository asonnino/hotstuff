use super::{Digest, Hash, PublicKey, Signature};
use ed25519_dalek::Digest as _;
use ed25519_dalek::Signer as _;
use ed25519_dalek::{Keypair, Sha512};
use rand::{rngs::StdRng, SeedableRng};
use std::convert::TryInto;

impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        Sha512::digest(self).as_slice()[..32].try_into().unwrap()
    }
}

#[test]
fn signature_test() {
    // Make keypair.
    let mut rng = StdRng::from_seed([0; 32]);
    let keypair = Keypair::generate(&mut rng);
    let public_key = PublicKey(keypair.public.to_bytes());

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let bytes = keypair.sign(&message.digest()).to_bytes();
    let mut part1 = [0; 32];
    let mut part2 = [0; 32];
    part1.clone_from_slice(&bytes[..32]);
    part2.clone_from_slice(&bytes[32..64]);
    let signature = Signature { part1, part2 };

    // Verify the signature.
    assert!(signature.verify(&message.digest(), &public_key).is_ok());
}
