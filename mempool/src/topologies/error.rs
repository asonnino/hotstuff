use thiserror::Error;

/// Errors that can occur when building a topology
#[derive(Debug, Error)]
pub enum TopologyError {
    #[error("Missing params : '{param}'")]
    MissingParameters { param: String },
    #[error("No peers found.")]
    NoPeers,
    #[error("Invalid parameter : {param}")]
    InvalidParameter { param: String },
}
