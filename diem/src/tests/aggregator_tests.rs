use super::*;
use crate::config::config_tests::committee;
use crate::crypto::generate_keypair;
use crate::messages::messages_tests::{qc, vote};
use crate::messages::GenericQC as _;
use crate::messages::QC;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

#[test]
fn add_vote() {
    let mut aggregator = Aggregator::new(committee());
    let result = aggregator.add_vote(vote());
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn make_quorum() {
    let mut aggregator = Aggregator::new(committee());

    // Add 2f+1 votes to the aggregator and ensure it returns the cryptographic
    // material to make a valid QC.
    let mut votes = qc().votes().clone();

    let (author, signature) = votes.pop().unwrap();
    let vote_x = Vote {
        author,
        signature,
        ..vote()
    };
    let result = aggregator.add_vote(vote_x);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let (author, signature) = votes.pop().unwrap();
    let vote_x = Vote {
        author,
        signature,
        ..vote()
    };
    let result = aggregator.add_vote(vote_x);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    let (author, signature) = votes.pop().unwrap();
    let vote_x = Vote {
        author,
        signature,
        ..vote()
    };
    match aggregator.add_vote(vote_x.clone()) {
        Ok(Some(votes)) => {
            let qc = QC {
                hash: vote_x.hash,
                round: vote_x.round,
                votes,
            };
            assert!(qc.verify(&committee()).is_ok());
        }
        _ => assert!(false),
    }
}

#[test]
fn authority_reuse() {
    let mut aggregator = Aggregator::new(committee());

    // Add a vote.
    let result = aggregator.add_vote(vote());
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // Add a vote from the same authority.
    match aggregator.add_vote(vote()) {
        Err(DiemError::AuthorityReuse(name)) => assert_eq!(name, vote().author),
        _ => assert!(false),
    }
}

#[test]
fn unknown_authority() {
    let mut aggregator = Aggregator::new(committee());

    // Add a vote from an unknown authority.
    let mut rng = StdRng::from_seed([1; 32]);
    let (unknown, _) = generate_keypair(&mut rng);
    let vote = Vote {
        author: unknown,
        ..vote()
    };
    match aggregator.add_vote(vote) {
        Err(DiemError::UnknownAuthority(name)) => assert_eq!(name, unknown),
        _ => assert!(false),
    }
}

#[test]
fn cleanup() {
    let mut aggregator = Aggregator::new(committee());

    // Add a vote and ensure it is in the aggregator memory.
    let result = aggregator.add_vote(vote());
    assert!(result.is_ok());
    assert_eq!(aggregator.aggregators.len(), 1);
    assert_eq!(aggregator.voters.len(), 1);

    // Clean up the aggregator.
    aggregator.cleanup(&2);
    assert!(aggregator.aggregators.is_empty());
    assert!(aggregator.voters.is_empty());
}
