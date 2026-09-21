//! # Config
//! The rustls settings every TLS task shares, and how its
//! errors map onto the runtime's

use crate::RuntimeError;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    crypto::CryptoProvider,
    server::WebPkiClientVerifier,
    pki_types::{
        CertificateDer, PrivateKeyDer,
        pem::{self, PemObject},
    },
};
use rustls_platform_verifier::Verifier;
use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

/// The one crypto backend, made once
fn provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();

    PROVIDER
        .get_or_init(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .clone()
}

/// The client settings every connection without extra roots
/// shares
///
/// Built once. A failure is kept too
pub(crate) fn client() -> Result<Arc<ClientConfig>, RuntimeError> {
    static CLIENT: OnceLock<Result<Arc<ClientConfig>, RuntimeError>> = OnceLock::new();

    CLIENT
        .get_or_init(|| build_client(Verifier::new(provider())))
        .clone()
}

/// A certificate chain and its private key, as PEM
#[derive(Debug, Clone)]
pub(crate) enum Keys {
    /// Two files, read when the settings are built
    Files(PathBuf, PathBuf),

    /// Two buffers already in memory
    Pem(Arc<[u8]>, Arc<[u8]>),
}

/// What a client connect asks for beyond the defaults
#[derive(Debug, Clone, Default)]
pub(crate) struct ClientSettings {
    /// Roots to trust on top of the system's, as PEM
    pub(crate) roots: Option<Arc<[u8]>>,

    /// Protocols offered through ALPN, most wanted first
    pub(crate) alpn: Arc<[Vec<u8>]>,

    /// A certificate and key to show a server that asks for one
    pub(crate) identity: Option<(Arc<[u8]>, Arc<[u8]>)>,
}

/// Client settings for a connect
///
/// ## Returns
/// The shared defaults when nothing is asked for. `BadCertificate`
/// when a certificate or key doesn't parse
pub(crate) fn client_with(settings: &ClientSettings) -> Result<Arc<ClientConfig>, RuntimeError> {
    if settings.roots.is_none() && settings.alpn.is_empty() && settings.identity.is_none() {
        return client();
    }

    let verifier = match &settings.roots {
        Some(pem) => Verifier::new_with_extra_roots(certificates(pem)?, provider()),
        None => Verifier::new(provider()),
    };

    let builder = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier.map_err(tls_error)?));

    let mut config = match &settings.identity {
        Some((cert, key)) => builder
            .with_client_auth_cert(certificates(cert)?, private_key(key)?)
            .map_err(|_| RuntimeError::BadCertificate)?,

        None => builder.with_no_client_auth(),
    };

    config.alpn_protocols = settings.alpn.to_vec();

    Ok(Arc::new(config))
}

/// Every certificate in `pem`
///
/// ## Returns
/// `BadCertificate` when there are none, or one doesn't parse
fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, RuntimeError> {
    let found: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<_, _>>()
        .map_err(|_| RuntimeError::BadCertificate)?;

    match found.is_empty() {
        true => Err(RuntimeError::BadCertificate),
        false => Ok(found),
    }
}

/// The private key in `pem`
fn private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, RuntimeError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|_| RuntimeError::BadCertificate)
}

/// Client settings around a platform verifier
fn build_client(
    verifier: Result<Verifier, rustls::Error>,
) -> Result<Arc<ClientConfig>, RuntimeError> {
    let verifier = verifier.map_err(tls_error)?;

    let config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();

    Ok(Arc::new(config))
}

/// What a listener serves with
#[derive(Debug, Clone)]
pub(crate) struct ServerSettings {
    /// The certificate chain and key
    pub(crate) keys: Keys,

    /// Protocols accepted through ALPN, most wanted first
    pub(crate) alpn: Arc<[Vec<u8>]>,

    /// Roots a client's certificate has to chain to, when one is
    /// required
    pub(crate) client_roots: Option<Arc<[u8]>>,
}

/// Server settings for a listener
///
/// ## Returns
/// `CheckError` when a file can't be read, and `BadCertificate`
/// when one doesn't parse or the key doesn't fit the certificate
pub(crate) fn server_with(settings: &ServerSettings) -> Result<Arc<ServerConfig>, RuntimeError> {
    let (chain, key) = match &settings.keys {
        Keys::Files(cert, key) => {
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
                .map_err(pem_error)?
                .collect::<Result<_, _>>()
                .map_err(pem_error)?;

            (chain, PrivateKeyDer::from_pem_file(key).map_err(pem_error)?)
        }

        Keys::Pem(cert, key) => (certificates(cert)?, private_key(key)?),
    };

    if chain.is_empty() {
        return Err(RuntimeError::BadCertificate);
    }

    let builder = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?;

    let builder = match &settings.client_roots {
        Some(pem) => {
            let mut roots = RootCertStore::empty();

            for root in certificates(pem)? {
                roots.add(root).map_err(|_| RuntimeError::BadCertificate)?;
            }

            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()
                .map_err(|_| RuntimeError::BadCertificate)?;

            builder.with_client_cert_verifier(verifier)
        }

        None => builder.with_no_client_auth(),
    };

    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(|_| RuntimeError::BadCertificate)?;

    config.alpn_protocols = settings.alpn.to_vec();

    Ok(Arc::new(config))
}

/// Turns a rustls failure into the runtime's
///
/// A refused certificate is told apart from everything else
pub(crate) fn tls_error(error: rustls::Error) -> RuntimeError {
    match error {
        rustls::Error::InvalidCertificate(_) => RuntimeError::BadCertificate,
        _ => RuntimeError::TlsFailed,
    }
}

/// Turns a PEM failure into the runtime's
///
/// A file that can't be read keeps the kernel's reason
fn pem_error(error: pem::Error) -> RuntimeError {
    match error {
        pem::Error::Io(error) => RuntimeError::CheckError(error.raw_os_error()),
        _ => RuntimeError::BadCertificate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refused certificate is its own error, and every other
    /// failure is a TLS one
    #[test]
    fn a_refused_certificate_is_told_apart() {
        assert_eq!(
            tls_error(rustls::Error::InvalidCertificate(
                rustls::CertificateError::UnknownIssuer
            )),
            RuntimeError::BadCertificate,
        );

        assert_eq!(
            tls_error(rustls::Error::HandshakeNotComplete),
            RuntimeError::TlsFailed,
        );
    }

    /// Roots that aren't certificates are refused before anything
    /// is built around them
    #[test]
    fn roots_that_are_not_certificates_are_refused() {
        for pem in [&b""[..], b"not a certificate"] {
            let settings = ClientSettings {
                roots: Some(Arc::from(pem)),
                ..ClientSettings::default()
            };

            assert_eq!(
                client_with(&settings).map(|_| ()),
                Err(RuntimeError::BadCertificate)
            );
        }
    }

    /// A certificate file that isn't there keeps the kernel's
    /// reason
    #[test]
    fn a_missing_certificate_file_is_not_found() {
        let missing = PathBuf::from("/nonexistent/atap/cert.pem");

        let settings = ServerSettings {
            keys: Keys::Files(missing.clone(), missing),
            alpn: Arc::from([]),
            client_roots: None,
        };

        assert_eq!(
            server_with(&settings).map(|_| ()),
            Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        );
    }
}
