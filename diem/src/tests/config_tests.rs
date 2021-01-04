use super::*;
use crate::crypto::crypto_tests::keys;
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

// Fixture.
pub fn committee() -> Committee {
    let names = keys().iter().map(|(public_key, _)| *public_key).collect();
    Committee::new(names, 0)
}

#[test]
fn quorum_threshold() {
    assert_eq!(committee().quorum_threshold(), 3);
}

#[test]
fn committee_read_write() {
    let committee = committee();
    let filename = ".committee_test_read_write";
    let result = committee.write(filename);
    assert!(result.is_ok());
    let read_committee = Committee::read(filename);
    assert!(read_committee.is_ok());
    assert_eq!(read_committee.unwrap(), committee);
}
