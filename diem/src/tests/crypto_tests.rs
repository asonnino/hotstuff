use super::*;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use rand::{rngs::StdRng, SeedableRng};

impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        Sha512::digest(self).as_slice()[..32].try_into().unwrap()
    }
}

impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_base64())
    }
}

#[test]
fn import_export_public_key() {
    // Make keypair.
    let mut rng = StdRng::from_seed([0; 32]);
    let (public_key, _) = generate_keypair(&mut rng);

    // Export and re-import key.
    let export = public_key.to_base64();
    let import = PublicKey::from_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    // Make keypair.
    let mut rng = StdRng::from_seed([0; 32]);
    let (_, secret_key) = generate_keypair(&mut rng);

    // Export and re-import key.
    let export = secret_key.to_base64();
    let import = SecretKey::from_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), secret_key);
}

#[test]
fn verify_valid_signature() {
    // Make keypair.
    let mut rng = StdRng::from_seed([0; 32]);
    let (public_key, secret_key) = generate_keypair(&mut rng);

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn verify_invalid_signature() {
    // Make keypair.
    let mut rng = StdRng::from_seed([0; 32]);
    let (public_key, secret_key) = generate_keypair(&mut rng);

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    let bad_message: &[u8] = b"Bad message!";
    let digest = bad_message.digest();
    assert!(signature.verify(&digest, &public_key).is_err());
}

#[test]
fn verify_valid_batch() {
    // Make keypairs.
    let mut rng = StdRng::from_seed([0; 32]);
    let (pub1, sec1) = generate_keypair(&mut rng);
    let (pub2, sec2) = generate_keypair(&mut rng);
    let (pub3, sec3) = generate_keypair(&mut rng);

    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let sig1 = Signature::new(&digest, &sec1);
    let sig2 = Signature::new(&digest, &sec2);
    let sig3 = Signature::new(&digest, &sec3);

    // Verify the signature.
    let signatures = vec![(pub1, sig1), (pub2, sig2), (pub3, sig3)];
    assert!(Signature::verify_batch(&digest, &signatures).is_ok());
}

#[test]
fn verify_invalid_batch() {
    // Make keypairs.
    let mut rng = StdRng::from_seed([0; 32]);
    let (pub1, sec1) = generate_keypair(&mut rng);
    let (pub2, sec2) = generate_keypair(&mut rng);
    let (pub3, _) = generate_keypair(&mut rng);

    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let sig1 = Signature::new(&digest, &sec1);
    let sig2 = Signature::new(&digest, &sec2);
    let sig3 = Signature::default();

    // Verify the signature.
    let signatures = vec![(pub1, sig1), (pub2, sig2), (pub3, sig3)];
    assert!(Signature::verify_batch(&digest, &signatures).is_err());
}

#[tokio::test]
async fn signature_service() {
    // Make keypair.
    let mut rng = StdRng::from_seed([0; 32]);
    let (public_key, secret_key) = generate_keypair(&mut rng);

    // Spawn the signature service.
    let mut service = SignatureService::new(secret_key).await;

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest).await;

    // Verify the signature we received.
    assert!(signature.verify(&digest, &public_key).is_ok());
}
