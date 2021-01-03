use super::*;
use crate::crypto::generate_keypair;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::fmt;

impl PartialEq for Committee {
    fn eq(&self, other: &Self) -> bool {
        self.authorities == other.authorities
    }
}

impl fmt::Debug for Committee {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self.authorities.keys().collect::<Vec<_>>())
    }
}

impl PartialEq for Authority {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.stake == other.stake
    }
}

impl fmt::Debug for Authority {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self.name)
    }
}

#[test]
fn validity_threshold() {
    let mut rng = StdRng::from_seed([0; 32]);
    let names = (0..4).map(|_| generate_keypair(&mut rng).0).collect();
    let committee = Committee::new(names, 6100);
    assert_eq!(committee.validity_threshold(), 2);
}

#[test]
fn quorum_threshold() {
    let mut rng = StdRng::from_seed([0; 32]);
    let names = (0..4).map(|_| generate_keypair(&mut rng).0).collect();
    let committee = Committee::new(names, 6100);
    assert_eq!(committee.quorum_threshold(), 3);
}

#[test]
fn read_write() {
    let mut rng = StdRng::from_seed([0; 32]);
    let names = (0..4).map(|_| generate_keypair(&mut rng).0).collect();
    let committee = Committee::new(names, 6100);
    let filename = ".committee_test_read_write";
    let result = committee.write(filename);
    assert!(result.is_ok());
    let read_committee = Committee::read(filename);
    assert!(read_committee.is_ok());
    assert_eq!(read_committee.unwrap(), committee);
}
