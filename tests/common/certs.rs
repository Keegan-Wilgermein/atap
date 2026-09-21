//! # Certs
//! A throwaway certificate authority, and the certificates a TLS
//! test serves and connects with

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::{fs, path::PathBuf, process};
use time::{Duration as Span, OffsetDateTime};

/// A test's certificate authority, and the server certificate and
/// key it issued, written where `Tls::listen` can read them
pub struct Certs {
    /// The authority, as PEM, for the client to trust
    pub ca: String,

    /// The server's certificate
    pub cert: PathBuf,

    /// The server's private key
    pub key: PathBuf,

    /// The server's certificate and key, as PEM
    pub cert_pem: String,
    pub key_pem: String,

    /// A client certificate and key from the same authority, as PEM
    pub client_cert: String,
    pub client_key: String,
}

/// Mints a certificate authority and a `localhost` certificate
/// signed by it
///
/// Valid from a day ago to a day from now. macOS refuses server
/// certificates valid for too long, and wants the name in the
/// subject alternative names and server auth in the extended key
/// usage
pub fn certs(name: &str) -> Certs {
    let now = OffsetDateTime::now_utc();

    let ca_key = KeyPair::generate().unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();

    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca.distinguished_name
        .push(DnType::CommonName, "atap test authority");
    ca.not_before = now - Span::days(1);
    ca.not_after = now + Span::days(1);

    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca, ca_key);

    let leaf_key = KeyPair::generate().unwrap();
    let mut leaf = CertificateParams::new(vec!["localhost".to_string()]).unwrap();

    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    leaf.distinguished_name
        .push(DnType::CommonName, "localhost");
    leaf.not_before = now - Span::days(1);
    leaf.not_after = now + Span::days(1);

    let leaf_cert = leaf.signed_by(&leaf_key, &issuer).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client = CertificateParams::new(Vec::<String>::new()).unwrap();

    client.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    client
        .distinguished_name
        .push(DnType::CommonName, "atap test client");
    client.not_before = now - Span::days(1);
    client.not_after = now + Span::days(1);

    let client_cert = client.signed_by(&client_key, &issuer).unwrap();

    let dir = std::env::temp_dir().join(format!("atap-tls-{}-{name}", process::id()));
    fs::create_dir_all(&dir).unwrap();

    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");

    fs::write(&cert, leaf_cert.pem()).unwrap();
    fs::write(&key, leaf_key.serialize_pem()).unwrap();

    Certs {
        ca: ca_cert.pem(),
        cert,
        key,
        cert_pem: leaf_cert.pem(),
        key_pem: leaf_key.serialize_pem(),
        client_cert: client_cert.pem(),
        client_key: client_key.serialize_pem(),
    }
}
