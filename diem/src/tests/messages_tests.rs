use super::*;
use crate::crypto::generate_keypair;
use rand::rngs::StdRng;
use rand::SeedableRng as _;

fn qc_and_committee() -> (QC, Committee) {
    let qc = QC {
        hash: Digest::default(),
        round: 1,
        votes: Vec::new(),
    };

    let mut rng = StdRng::from_seed([0; 32]);
    let (pub1, sec1) = generate_keypair(&mut rng);
    let (pub2, sec2) = generate_keypair(&mut rng);
    let (pub3, sec3) = generate_keypair(&mut rng);
    let (pub4, _) = generate_keypair(&mut rng);

    let digest = qc.digest();
    let sig1 = Signature::new(&digest, &sec1);
    let sig2 = Signature::new(&digest, &sec2);
    let sig3 = Signature::new(&digest, &sec3);

    let votes = vec![(pub1, sig1), (pub2, sig2), (pub3, sig3)];
    let qc = QC { votes, ..qc };

    let names = vec![pub1, pub2, pub3, pub4];
    let committee = Committee::new(names, 6000);

    (qc, committee)
}

#[test]
fn verify_valid_qc() {
    let (qc, committee) = qc_and_committee();
    assert!(qc.verify(&committee).is_ok());
}

#[test]
fn verify_qc_authority_reuse() {
    let (mut qc, committee) = qc_and_committee();

    // Modify QC to reuse one authority.
    let _ = qc.votes.pop();
    let vote = qc.votes[0].clone();
    qc.votes.push(vote.clone());

    // Verify the QC.
    match qc.verify(&committee) {
        Err(DiemError::AuthorityReuse(name)) => assert_eq!(name, vote.0),
        _ => assert!(false),
    }
}

#[test]
fn verify_qc_unknown_authority() {
    let (mut qc, committee) = qc_and_committee();

    // Modify QC to add one unknown authority.
    let mut rng = StdRng::from_seed([1; 32]);
    let unknown = generate_keypair(&mut rng).0;
    let (_, sig) = qc.votes.pop().unwrap();
    qc.votes.push((unknown, sig));

    // Verify the QC.
    match qc.verify(&committee) {
        Err(DiemError::UnknownAuthority(name)) => assert_eq!(name, unknown),
        _ => assert!(false),
    }
}

#[test]
fn verify_qc_insufficient_stake() {
    let (mut qc, committee) = qc_and_committee();

    // Modify QC to remove one authority.
    let _ = qc.votes.pop();

    // Verify the QC.
    match qc.verify(&committee) {
        Err(DiemError::QCRequiresQuorum) => assert!(true),
        _ => assert!(false),
    }
}
