//! # Config
//! The rustls settings every TLS task shares, and how its
//! errors map onto the runtime's

use crate::RuntimeError;
use rustls::{
    ClientConfig, ServerConfig,
    crypto::CryptoProvider,
    pki_types::{
        CertificateDer, PrivateKeyDer,
        pem::{self, PemObject},
    },
};
use rustls_platform_verifier::Verifier;
use std::{
    path::Path,
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
/// Built once, since asking macOS for its verifier isn't free. A
/// failure is kept too, so it isn't retried on every connect
pub(crate) fn client() -> Result<Arc<ClientConfig>, RuntimeError> {
    static CLIENT: OnceLock<Result<Arc<ClientConfig>, RuntimeError>> = OnceLock::new();

    CLIENT
        .get_or_init(|| build_client(Verifier::new(provider())))
        .clone()
}

/// Client settings that also trust the roots in `pem`
///
/// ## Returns
/// `BadCertificate` when `pem` holds no certificate that parses
pub(crate) fn client_trusting(pem: &[u8]) -> Result<Arc<ClientConfig>, RuntimeError> {
    let roots: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<_, _>>()
        .map_err(|_| RuntimeError::BadCertificate)?;

    if roots.is_empty() {
        return Err(RuntimeError::BadCertificate);
    }

    build_client(Verifier::new_with_extra_roots(roots, provider()))
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

/// Server settings from a certificate chain and a private key,
/// both PEM files
///
/// ## Returns
/// `CheckError` when a file can't be read, and `BadCertificate`
/// when one doesn't parse or the key doesn't fit the certificate
pub(crate) fn server(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>, RuntimeError> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .map_err(pem_error)?
        .collect::<Result<_, _>>()
        .map_err(pem_error)?;

    if chain.is_empty() {
        return Err(RuntimeError::BadCertificate);
    }

    let key = PrivateKeyDer::from_pem_file(key).map_err(pem_error)?;

    let config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_error)?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|_| RuntimeError::BadCertificate)?;

    Ok(Arc::new(config))
}

/// Turns a rustls failure into the runtime's
///
/// A refused certificate is told apart from everything else,
/// since it is the one a caller can do something about
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
        assert_eq!(client_trusting(b"").map(|_| ()), Err(RuntimeError::BadCertificate));
        assert_eq!(
            client_trusting(b"not a certificate").map(|_| ()),
            Err(RuntimeError::BadCertificate),
        );
    }

    /// A certificate file that isn't there keeps the kernel's
    /// reason
    #[test]
    fn a_missing_certificate_file_is_not_found() {
        let missing = Path::new("/nonexistent/atap/cert.pem");

        assert_eq!(
            server(missing, missing).map(|_| ()),
            Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        );
    }
}
