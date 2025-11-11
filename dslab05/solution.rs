use hmac::{Hmac, Mac};
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConnection, RootCertStore, ServerConnection, StreamOwned};
use rustls::pki_types::ServerName;
use sha2::Sha256;
use std::io::{Read, Write};
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

pub struct SecureClient<L: Read + Write> {
    tls_link: StreamOwned<ClientConnection, L>,
    mac: Hmac<Sha256>,
}

pub struct SecureServer<L: Read + Write> {
    tls_link: StreamOwned<ServerConnection, L>,
    mac: Hmac<Sha256>,
}

impl<L: Read + Write> SecureClient<L> {
    /// Creates a new instance of `SecureClient`.
    ///
    /// `SecureClient` communicates with `SecureServer` via `link`.
    /// The messages include a HMAC tag calculated using `hmac_key`.
    /// A certificate of `SecureServer` is signed by `root_cert`.
    /// We are connecting with `server_hostname`.
    
    fn create_tls_stream(stream: L, root_cert: &str, server_hostname: ServerName<'static>) -> StreamOwned<ClientConnection, L> {
        let mut root_store = RootCertStore::empty();
    
        root_store.add_parsable_certificates(rustls::pki_types::CertificateDer::from_pem_slice(
            root_cert.as_bytes()
        ));

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
    
        let connection =
            ClientConnection::new(Arc::new(client_config), server_hostname.try_into().unwrap()).unwrap();
    
        StreamOwned::new(connection, stream)
    }

    pub fn new(
        link: L,
        hmac_key: &[u8],
        root_cert: &str,
        server_hostname: ServerName<'static>,
    ) -> Self {
        SecureClient {
            tls_link: SecureClient::create_tls_stream(link, root_cert, server_hostname),
            mac: HmacSha256::new_from_slice(hmac_key).unwrap(),
        }
    }

    /// Sends the data to the server. The sent message follows the
    /// format specified in the description of the assignment.
    pub fn send_msg(&mut self, data: Vec<u8>) {
        let msg_len = data.len() as u32;
        let msg_len_bytes = msg_len.to_be_bytes();

        let mut mac = self.mac.clone();
        mac.update(data.as_slice());
        let tag = mac.finalize().into_bytes();

        let total_len = 4 + data.len() + 32;
        let mut full_msg = Vec::with_capacity(total_len);
        full_msg.extend_from_slice(&msg_len_bytes);
        full_msg.extend_from_slice(data.as_slice());
        full_msg.extend_from_slice(&tag);

        let _res = self.tls_link.write_all(&full_msg);
    }
}

impl<L: Read + Write> SecureServer<L> {
    /// Creates a new instance of `SecureServer`.
    ///
    /// `SecureServer` receives messages from `SecureClients` via `link`.
    /// HMAC tags of the messages are verified against `hmac_key`.
    /// The private key of the `SecureServer`'s certificate is `server_private_key`,
    /// and the full certificate chain is `server_full_chain`.

    fn create_tls_stream(stream: L, server_full_chain: &str, server_private_key: &str) -> StreamOwned<ServerConnection, L> {
        let certs =
            rustls::pki_types::CertificateDer::pem_slice_iter(server_full_chain.as_bytes())
                .flatten()
                .collect();

        let private_key =
            rustls::pki_types::PrivateKeyDer::from_pem_slice(server_private_key.as_bytes())
                .unwrap();

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .unwrap();

        let connection = ServerConnection::new(Arc::new(server_config)).unwrap();

        StreamOwned::new(connection, stream)
    } 
    
    pub fn new(
        link: L,
        hmac_key: &[u8],
        server_private_key: &str,
        server_full_chain: &str,
    ) -> Self {
        SecureServer {
            tls_link: SecureServer::create_tls_stream(link, server_full_chain, server_private_key),
            mac: HmacSha256::new_from_slice(hmac_key).unwrap(),
        }
    }

    /// Receives the next incoming message and returns the message's content
    /// (i.e., without the message size and without the HMAC tag) if the
    /// message's HMAC tag is correct. Otherwise, returns `SecureServerError`.
    pub fn recv_message(&mut self) -> Result<Vec<u8>, SecureServerError> {
        let mut msg_len_bytes: [u8; 4] = [0, 0, 0, 0];
        self.tls_link.read_exact(&mut msg_len_bytes).unwrap();
        let msg_len = u32::from_be_bytes(msg_len_bytes);

        let mut msg = Vec::with_capacity(msg_len as usize);
        msg.resize(msg_len as usize, 0);
        let _ = self.tls_link.read_exact(&mut msg);

        let mut mac_tag = Vec::with_capacity(32);
        mac_tag.resize(32, 0);
        let _res = self.tls_link.read_exact(&mut mac_tag);

        let mut mac = self.mac.clone();
        mac.update(&msg);

        if let false = mac.verify_slice(&mac_tag).is_ok() {
            return Result::Err(SecureServerError::InvalidHmac)
        }

        Result::Ok(msg)
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SecureServerError {
    /// The HMAC tag of a message is invalid.
    InvalidHmac,
}
