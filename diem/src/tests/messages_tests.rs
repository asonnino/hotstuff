use super::*;
use crate::config::config_tests::committee;
use crate::crypto::crypto_tests::keys;
use crate::crypto::generate_keypair;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

// Fixture.
pub fn vote() -> Vote {
    let (public_key, secret_key) = keys().pop().unwrap();
    let vote = Vote {
        hash: Digest::default(),
        signature: Signature::default(),
        author: public_key,
        round: 1,
    };
    let digest = vote.digest();
    let signature = Signature::new(&digest, &secret_key);
    Vote {
        signature: signature,
        ..vote
    }
}

// Fixture.
pub fn qc() -> QC {
    let qc = QC {
        hash: Digest::default(),
        round: 1,
        votes: Vec::new(),
    };
    let digest = qc.digest();
    let mut keys = keys();
    let votes: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();
    QC { votes, ..qc }
}

#[test]
fn verify_valid_qc() {
    assert!(qc().verify(&committee()).is_ok());
}

#[test]
fn verify_qc_authority_reuse() {
    // Modify QC to reuse one authority.
    let mut qc = qc();
    let _ = qc.votes.pop();
    let vote = qc.votes[0].clone();
    qc.votes.push(vote.clone());

    // Verify the QC.
    match qc.verify(&committee()) {
        Err(DiemError::AuthorityReuse(name)) => assert_eq!(name, vote.0),
        _ => assert!(false),
    }
}

#[test]
fn verify_qc_unknown_authority() {
    let mut qc = qc();

    // Modify QC to add one unknown authority.
    let mut rng = StdRng::from_seed([1; 32]);
    let (unknown, _) = generate_keypair(&mut rng);
    let (_, sig) = qc.votes.pop().unwrap();
    qc.votes.push((unknown, sig));

    // Verify the QC.
    match qc.verify(&committee()) {
        Err(DiemError::UnknownAuthority(name)) => assert_eq!(name, unknown),
        _ => assert!(false),
    }
}

#[test]
fn verify_qc_insufficient_stake() {
    // Modify QC to remove one authority.
    let mut qc = qc();
    let _ = qc.votes.pop();

    // Verify the QC.
    match qc.verify(&committee()) {
        Err(DiemError::QCRequiresQuorum) => assert!(true),
        _ => assert!(false),
    }
}
