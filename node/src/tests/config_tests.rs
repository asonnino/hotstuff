use super::*;
use crate::crypto::crypto_tests::keys;
use std::fmt;
use std::fs;

impl Committee {
    pub fn set_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }
}

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
    let names: Vec<_> = keys().iter().map(|(public_key, _)| *public_key).collect();
    let authorities = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let authority = Authority {
                name: *name,
                stake: 1,
                address: format!("127.0.0.1:{}", i).parse().unwrap(),
            };
            (*name, authority)
        })
        .collect();
    Committee {
        authorities,
        epoch: 1,
    }
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
