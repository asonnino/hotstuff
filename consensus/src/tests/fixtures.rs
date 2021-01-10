use config::config::Config as _;
use config::config::{Committee, Secret};
use crypto::crypto::{generate_keypair, PublicKey, SecretKey};
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::fs;

// Fixture.
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

pub trait MockSecret {
    fn print_key_files(prefix: &str) -> Vec<String>;
}

impl MockSecret for Secret {
    fn print_key_files(prefix: &str) -> Vec<String> {
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

// Fixture.
pub fn committee() -> Committee {
    let authorities: Vec<_> = keys().into_iter().map(|(name, _)| (name, 1)).collect();
    Committee::new(&authorities, /* epoch */ 1)
}

pub trait MockCommittee {
    fn increment_base_port(&mut self, base_port: u16);
    fn print_committee(filename: &str, base_port: u16);
}

impl MockCommittee for Committee {
    fn increment_base_port(&mut self, base_port: u16) {
        for authority in self.authorities.values_mut() {
            let port = authority.address.port();
            authority.address.set_port(base_port + port);
        }
    }

    fn print_committee(filename: &str, base_port: u16) {
        let _ = fs::remove_dir_all(filename);
        let mut committee = committee();
        committee.increment_base_port(base_port);
        committee.write(filename).unwrap();
    }
}
