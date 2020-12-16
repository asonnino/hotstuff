use crate::error::DiemError;
use ed25519_dalek as dalek;
use ed25519_dalek::ed25519;
use serde::{de, ser, Deserialize, Serialize};

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type Digest = [u8; 32];

pub trait Digestible {
    fn digest(&self) -> Digest;
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct PublicKey(pub [u8; 32]);

impl PublicKey {
    pub fn to_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        let bytes = base64::decode(s)?;
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes[..32]);
        Ok(Self(key))
    }
}

impl From<PublicKey> for Vec<u8> {
    fn from(x: PublicKey) -> Self {
        x.0.to_vec()
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.to_base64())?;
        Ok(())
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.to_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::from_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Signature {
    part1: [u8; 32],
    part2: [u8; 32],
}

impl Signature {
    pub fn check<D>(&self, data: &D, public_key: &PublicKey) -> Result<(), DiemError>
    where
        D: Digestible,
    {
        let mut bytes = [0u8; 64];
        bytes[..32].clone_from_slice(&self.part1);
        bytes[32..64].clone_from_slice(&self.part2);
        let signature = ed25519::signature::Signature::from_bytes(&bytes)?;
        let public_key = dalek::PublicKey::from_bytes(&public_key.0)?;
        let digest = data.digest();
        public_key.verify_strict(&digest, &signature)?;
        Ok(())
    }
}
