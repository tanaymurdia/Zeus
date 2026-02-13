use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig};
use rcgen::generate_simple_self_signed;
use std::{error::Error, net::SocketAddr, sync::Arc};

pub fn init() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn make_server_endpoint(bind_addr: SocketAddr) -> Result<(Endpoint, Vec<u8>), Box<dyn Error>> {
    let (server_config, cert_der) = configure_server()?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok((endpoint, cert_der))
}

pub fn make_client_endpoint(
    bind_addr: SocketAddr,
    server_cert_der: &[u8],
) -> Result<Endpoint, Box<dyn Error>> {
    let client_config = configure_client(server_cert_der)?;
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

fn configure_server() -> Result<(ServerConfig, Vec<u8>), Box<dyn Error>> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().to_vec();
    let priv_key = cert.signing_key.serialize_der();

    let priv_key = rustls::pki_types::PrivateKeyDer::Pkcs8(priv_key.into());
    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;

    let mut transport_config = TransportConfig::default();
    transport_config.max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_millis(500)));
    transport_config.datagram_receive_buffer_size(Some(10 * 1024 * 1024));
    transport_config.datagram_send_buffer_size(10 * 1024 * 1024);

    server_config.transport_config(Arc::new(transport_config));

    Ok((server_config, cert_der))
}

fn configure_client(server_cert_der: &[u8]) -> Result<ClientConfig, Box<dyn Error>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(server_cert_der))?;

    let mut client_config = ClientConfig::with_root_certificates(Arc::new(roots))?;

    let mut transport_config = TransportConfig::default();
    transport_config.max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_millis(500)));
    transport_config.datagram_receive_buffer_size(Some(10 * 1024 * 1024));

    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}

pub fn make_promiscuous_endpoint(
    bind_addr: SocketAddr,
) -> Result<(Endpoint, Vec<u8>), Box<dyn Error>> {
    let (server_config, cert_der) = configure_server()?;

    let roots = rustls::RootCertStore::empty();
    let mut rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    rustls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(SkipServerVerification));

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)?;
    let mut client_config = ClientConfig::new(Arc::new(crypto));

    let mut transport_config = TransportConfig::default();
    transport_config.max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(std::time::Duration::from_millis(500)));
    transport_config.datagram_receive_buffer_size(Some(10 * 1024 * 1024));
    transport_config.datagram_send_buffer_size(10 * 1024 * 1024);
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = Endpoint::server(server_config, bind_addr)?;
    endpoint.set_default_client_config(client_config);
    Ok((endpoint, cert_der))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_endpoint_creation() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let result = make_server_endpoint(addr);
        assert!(result.is_ok());
        let (endpoint, cert) = result.unwrap();
        assert!(!cert.is_empty());
        endpoint.close(0u32.into(), b"test done");
    }

    #[tokio::test]
    async fn test_promiscuous_endpoint() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let result = make_promiscuous_endpoint(addr);
        assert!(result.is_ok());
        let (endpoint, _) = result.unwrap();
        endpoint.close(0u32.into(), b"test done");
    }
}
