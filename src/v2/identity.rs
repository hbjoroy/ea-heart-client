//! Client certificates for mutual TLS with Heart's worker listener.
//!
//! The certificate's SPIFFE ID (URI SAN) identifies the worker service to
//! Heart; no bearer token is needed. [`ClientIdentity::from_files`] re-reads
//! the PEM files at most every 30 seconds during TLS handshakes, so a renewed
//! certificate (for example from cert-manager) is used for new connections
//! without restarting the worker.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rustls::{
    client::ResolvesClientCert,
    crypto::CryptoProvider,
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    sign::CertifiedKey,
    SignatureScheme,
};

use super::client::WorkerError;

const RELOAD_INTERVAL: Duration = Duration::from_secs(30);

/// A worker's client certificate and private key.
pub struct ClientIdentity {
    source: Source,
}

enum Source {
    Fixed { certificate: Vec<u8>, key: Vec<u8> },
    Files { certificate: PathBuf, key: PathBuf },
}

impl ClientIdentity {
    /// PEM certificate chain (leaf first) and private key held in memory.
    pub fn from_pem(certificate: &[u8], key: &[u8]) -> Self {
        Self {
            source: Source::Fixed {
                certificate: certificate.to_vec(),
                key: key.to_vec(),
            },
        }
    }

    /// PEM files that are re-read when they change.
    pub fn from_files(certificate: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Self {
            source: Source::Files {
                certificate: certificate.into(),
                key: key.into(),
            },
        }
    }

    pub(crate) fn resolver(
        self,
        provider: Arc<CryptoProvider>,
    ) -> Result<Arc<dyn ResolvesClientCert>, WorkerError> {
        let (certificate, key, files) = match self.source {
            Source::Fixed { certificate, key } => (certificate, key, None),
            Source::Files { certificate, key } => {
                let read = |path: &PathBuf| {
                    std::fs::read(path).map_err(|_| WorkerError::InvalidClientIdentity)
                };
                (read(&certificate)?, read(&key)?, Some((certificate, key)))
            }
        };
        let current = certified_key(&certificate, &key, &provider)
            .ok_or(WorkerError::InvalidClientIdentity)?;
        Ok(Arc::new(Resolver {
            provider,
            files,
            state: Mutex::new(State {
                current,
                certificate,
                key,
                checked: Instant::now(),
            }),
        }))
    }
}

fn certified_key(
    certificate: &[u8],
    key: &[u8],
    provider: &CryptoProvider,
) -> Option<Arc<CertifiedKey>> {
    let chain = CertificateDer::pem_slice_iter(certificate)
        .collect::<Result<Vec<_>, _>>()
        .ok()
        .filter(|chain| !chain.is_empty())?;
    let key = PrivateKeyDer::from_pem_slice(key).ok()?;
    // Rejects a key that does not match the certificate.
    CertifiedKey::from_der(chain, key, provider)
        .ok()
        .map(Arc::new)
}

struct Resolver {
    provider: Arc<CryptoProvider>,
    files: Option<(PathBuf, PathBuf)>,
    state: Mutex<State>,
}

struct State {
    current: Arc<CertifiedKey>,
    certificate: Vec<u8>,
    key: Vec<u8>,
    checked: Instant,
}

impl Resolver {
    fn current(&self) -> Arc<CertifiedKey> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((certificate_path, key_path)) = &self.files {
            if state.checked.elapsed() >= RELOAD_INTERVAL {
                state.checked = Instant::now();
                if let (Ok(certificate), Ok(key)) =
                    (std::fs::read(certificate_path), std::fs::read(key_path))
                {
                    if certificate != state.certificate || key != state.key {
                        // A half-written pair (new certificate, old key) is
                        // skipped and retried; the previous pair stays in use.
                        if let Some(next) = certified_key(&certificate, &key, &self.provider) {
                            tracing::info!("reloaded Heart worker client certificate");
                            state.current = next;
                            state.certificate = certificate;
                            state.key = key;
                        }
                    }
                }
            }
        }
        state.current.clone()
    }
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeartWorkerClientCertificate")
    }
}

impl ResolvesClientCert for Resolver {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(name: &str) -> (String, String) {
        let key = rcgen::generate_simple_self_signed(vec![name.to_owned()]).unwrap();
        (key.cert.pem(), key.key_pair.serialize_pem())
    }

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    #[test]
    fn rejects_mismatched_or_missing_material() {
        let (cert, _) = pair("a");
        let (_, other_key) = pair("b");
        assert!(
            ClientIdentity::from_pem(cert.as_bytes(), other_key.as_bytes())
                .resolver(provider())
                .is_err()
        );
        assert!(ClientIdentity::from_pem(b"", b"")
            .resolver(provider())
            .is_err());
        assert!(
            ClientIdentity::from_files("/nonexistent/tls.crt", "/nonexistent/tls.key")
                .resolver(provider())
                .is_err()
        );
    }

    #[test]
    fn files_are_reloaded_and_half_written_pairs_skipped() {
        let dir =
            std::env::temp_dir().join(format!("heart-client-identity-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert_path, key_path) = (dir.join("tls.crt"), dir.join("tls.key"));
        let (first_cert, first_key) = pair("first");
        std::fs::write(&cert_path, &first_cert).unwrap();
        std::fs::write(&key_path, &first_key).unwrap();

        let identity = ClientIdentity::from_files(&cert_path, &key_path);
        let Source::Files { .. } = identity.source else {
            unreachable!()
        };
        let resolver = Resolver {
            provider: provider(),
            files: Some((cert_path.clone(), key_path.clone())),
            state: Mutex::new(State {
                current: certified_key(first_cert.as_bytes(), first_key.as_bytes(), &provider())
                    .unwrap(),
                certificate: first_cert.clone().into_bytes(),
                key: first_key.clone().into_bytes(),
                checked: Instant::now(),
            }),
        };
        let leaf = |key: Arc<CertifiedKey>| key.cert[0].clone();
        let first = leaf(resolver.current());
        let expire = || {
            resolver.state.lock().unwrap().checked = Instant::now() - RELOAD_INTERVAL;
        };

        // New certificate written before its key: keep the first pair.
        let (second_cert, second_key) = pair("second");
        std::fs::write(&cert_path, &second_cert).unwrap();
        expire();
        assert_eq!(leaf(resolver.current()), first);

        // Complete pair: switch.
        std::fs::write(&key_path, &second_key).unwrap();
        expire();
        let second = leaf(resolver.current());
        assert_ne!(second, first);

        // Not re-read before the interval passes.
        std::fs::write(&cert_path, &first_cert).unwrap();
        std::fs::write(&key_path, &first_key).unwrap();
        assert_eq!(leaf(resolver.current()), second);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
