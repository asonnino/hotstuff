use super::*;
use crate::crypto::{generate_keypair, SecretKey};
use crate::messages::GenericQC as _;
use crate::messages::QC;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

fn make_vote(public_key: &PublicKey, secret_key: &SecretKey, digest: Digest) -> Vote {
    let vote = Vote {
        hash: digest,
        signature: Signature::default(),
        author: *public_key,
        round: 1,
    };
    let digest = vote.digest();
    let signature = Signature::new(&digest, secret_key);
    Vote {
        signature: signature,
        ..vote
    }
}

#[test]
fn add_vote() {
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let names = keys.iter().map(|x| x.0).collect();
    let committee = Committee::new(names, 6200);
    let mut aggregator = Aggregator::new(committee);

    // Add a vote to the aggregator.
    let (public_key, secret_key) = &keys[0];
    let vote = make_vote(public_key, secret_key, Digest::default());

    // Ensure the aggregator returns None.
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn make_quorum() {
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let names = keys.iter().map(|x| x.0).collect();
    let committee = Committee::new(names, 6200);
    let mut aggregator = Aggregator::new(committee.clone());

    // Add 2f+1 votes to the aggregator and ensure it returns the cryptographic
    // material to make a valid QC.
    let (public_key, secret_key) = &keys[0];
    let vote = make_vote(public_key, secret_key, Digest::default());
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let (public_key, secret_key) = &keys[1];
    let vote = make_vote(public_key, secret_key, Digest::default());
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let (public_key, secret_key) = &keys[2];
    let vote = make_vote(public_key, secret_key, Digest::default());
    match aggregator.add_vote(vote.clone()) {
        Ok(Some(votes)) => {
            let qc = QC {
                hash: vote.hash,
                round: vote.round,
                votes,
            };
            assert!(qc.verify(&committee).is_ok());
        }
        _ => assert!(false),
    }
}

#[test]
fn authority_reuse() {
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let names = keys.iter().map(|x| x.0).collect();
    let committee = Committee::new(names, 6200);
    let mut aggregator = Aggregator::new(committee.clone());

    // Add a vote.
    let (public_key, secret_key) = &keys[0];
    let vote = make_vote(public_key, secret_key, Digest::default());
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Add a vote from the same authority.
    let vote = make_vote(public_key, secret_key, Digest::default());
    match aggregator.add_vote(vote) {
        Err(DiemError::AuthorityReuse(name)) => assert_eq!(&name, public_key),
        _ => assert!(false),
    }
}

#[test]
fn unknown_authority() {
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let names = keys.iter().map(|x| x.0).collect();
    let committee = Committee::new(names, 6200);
    let mut aggregator = Aggregator::new(committee.clone());

    // Add a vote from an unknown authority.
    let (public_key, secret_key) = generate_keypair(&mut rng);
    let vote = make_vote(&public_key, &secret_key, Digest::default());
    match aggregator.add_vote(vote) {
        Err(DiemError::UnknownAuthority(name)) => assert_eq!(name, public_key),
        _ => assert!(false),
    }
}

#[test]
fn cleanup() {
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let names = keys.iter().map(|x| x.0).collect();
    let committee = Committee::new(names, 6200);
    let mut aggregator = Aggregator::new(committee.clone());

    // Add a vote and ensure it is in the aggregator memory.
    let (public_key, secret_key) = &keys[0];
    let vote = make_vote(public_key, secret_key, Digest::default());
    let result = aggregator.add_vote(vote);
    assert!(result.is_ok());
    assert_eq!(aggregator.aggregators.len(), 1);
    assert_eq!(aggregator.voters.len(), 1);

    // Clean up the aggregator.
    aggregator.cleanup(&2);
    assert!(aggregator.aggregators.is_empty());
    assert!(aggregator.voters.is_empty());
}
