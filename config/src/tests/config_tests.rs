use super::*;
use crypto::crypto::generate_keypair;
use std::fmt;
use std::fs;

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
    let mut rng = StdRng::from_seed([0; 32]);
    let keys: Vec<_> = (0..4).map(|_| generate_keypair(&mut rng)).collect();
    let authorities: Vec<_> = keys.into_iter().map(|(name, _)| (name, 1)).collect();
    Committee::new(&authorities, /* epoch */ 1)
}

#[test]
fn quorum_threshold() {
    assert_eq!(committee().quorum_threshold(), 3);
}

#[test]
fn committee_read_write() {
    let committee = committee();
    let filename = ".committee_test_read_write.json";
    let _ = fs::remove_file(filename);
    let result = committee.write(filename);
    assert!(result.is_ok());
    let read_committee = Committee::read(filename);
    assert!(read_committee.is_ok());
    assert_eq!(read_committee.unwrap(), committee);
}
