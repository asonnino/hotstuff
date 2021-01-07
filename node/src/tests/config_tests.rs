use super::*;
use crate::crypto::crypto_tests::keys;
use std::fmt;
use std::fs;

impl Secret {
    pub fn print_key_files(prefix: &str) -> Vec<String> {
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, key)| {
                let filename = format!("{}_{}.json", prefix, i);
                let _ = fs::remove_file(&filename);
                let (name, secret) = key;
                Self { name, secret }.write(&filename).unwrap();
                filename
            })
            .collect()
    }
}

impl Committee {
    pub fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }
    
    pub fn print_committee(filename: &str, base_port: u16) {
        let _ = fs::remove_dir_all(filename);
        let mut committee = committee();
        committee.increment_base_port(base_port);
        committee.write(filename).unwrap();
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
    let authorities: Vec<_> = keys().into_iter().map(|(name, _)| (name, 1)).collect();
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
