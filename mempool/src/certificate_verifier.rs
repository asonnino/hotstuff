use crate::{aggregator::BatchCertificate, config::Committee};
use crypto::{Digest, PublicKey};
use log::warn;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct CertificateVerifier;

impl CertificateVerifier {
    pub fn spawn(
        committee: Committee,
        mut rx_input: Receiver<BatchCertificate>,
        tx_output: Sender<BatchCertificate>,
        tx_cleanup: Sender<(PublicKey, Digest)>,
    ) {
        tokio::spawn(async move {
            while let Some(certificate) = rx_input.recv().await {
                if let Err(e) = certificate.verify(&committee) {
                    warn!("{}", e);
                    continue;
                }

                tx_cleanup
                    .send((certificate.author, certificate.root.clone()))
                    .await
                    .expect("Failed to loopback root");

                tx_output
                    .send(certificate)
                    .await
                    .expect("Failed to send certificate");
            }
        });
    }
}
