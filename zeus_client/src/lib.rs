use quinn::{Connection, Endpoint};
use std::net::SocketAddr;
use thiserror::Error;
use zeus_common::{GhostSerializer, ed25519_dalek::SigningKey};
use zeus_transport::make_promiscuous_endpoint;
// use tokio::io::AsyncWriteExt; // Implicitly used? No, I added it manually. But warning says unused.
// Wait, stream.write_all comes from AsyncWriteExt. If I delete it, code might break if not in scope.
// But the warning says unused. Let's delete it and see. If it breaks, I'll put it back.
// Actually, earlier compilation said "AsyncWriteExt reimported". I had it twice.
// Step 867 removed one, Step 877 removed valid one?
// Let's rely on the warning. The warning said it's unused.
// Ah, `quinn::SendStream` implements `tokio::io::AsyncWrite`? No, `quinn` has its own async write?
// Actually `quinn` depends on `tokio`.
// Let's comment it out.

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Transport Error: {0}")]
    Transport(String), // Converting Box<dyn Error> to String to avoid Send/Sync issues
    #[error("Connection Error: {0}")]
    Connection(#[from] quinn::ConnectionError),
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Connect Error: {0}")]
    Connect(#[from] quinn::ConnectError),
    #[error("Write Error: {0}")]
    Write(#[from] quinn::WriteError),
    #[error("Closed Stream: {0}")]
    ClosedStream(#[from] quinn::ClosedStream),
}

pub struct ZeusClient {
    endpoint: Endpoint,
    connection: Option<Connection>,
    signing_key: SigningKey,
    local_id: u64,
}

impl ZeusClient {
    pub fn new(local_id: u64) -> Result<Self, ClientError> {
        // Bind to ephemeral port on loopback (IPv4)
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (endpoint, _) =
            make_promiscuous_endpoint(addr).map_err(|e| ClientError::Transport(e.to_string()))?;

        let (signing_key, _) = GhostSerializer::generate_keypair();

        Ok(Self {
            endpoint,
            connection: None,
            signing_key,
            local_id,
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn local_id(&self) -> u64 {
        self.local_id
    }

    pub async fn connect(&mut self, seed: SocketAddr) -> Result<(), ClientError> {
        let connection = self.endpoint.connect(seed, "localhost")?.await?;
        self.connection = Some(connection);
        Ok(())
    }

    pub async fn send_state(
        &self,
        pos: (f32, f32, f32),
        vel: (f32, f32, f32),
    ) -> Result<(), ClientError> {
        if let Some(conn) = &self.connection {
            // 1. Serialize Signed Update
            let mut serializer = GhostSerializer::new();
            serializer.set_keypair(self.signing_key.clone());

            let msg_bytes = serializer.serialize(self.local_id, pos, vel);

            // 2. Send Stream
            let mut stream = conn.open_uni().await?;
            stream.write_all(&msg_bytes).await?;
            stream.finish()?;
        }
        Ok(())
    }

    /// Try to read server status datagram (non-blocking poll)
    /// Returns Option<(entity_count, node_count)>
    pub fn try_read_server_status(&self) -> Option<(u16, u8)> {
        if let Some(conn) = &self.connection {
            // Try to read a datagram (non-blocking check not directly available)
            // We'll rely on the game loop calling this periodically
            // For now, return None - the async version will be used
            None
        } else {
            None
        }
    }

    /// Async read server status - call from background task
    pub async fn read_server_status(&self) -> Option<(u16, u8)> {
        if let Some(conn) = &self.connection {
            if let Ok(datagram) = conn.read_datagram().await {
                if datagram.len() >= 4 && datagram[0] == 0xAA {
                    let entity_count = ((datagram[1] as u16) << 8) | (datagram[2] as u16);
                    let node_count = datagram[3];
                    return Some((entity_count, node_count));
                }
            }
        }
        None
    }

    /// Get connection for direct access
    pub fn connection(&self) -> Option<Connection> {
        self.connection.clone()
    }

    /// Read raw datagram from server
    pub async fn read_datagram(&self) -> Option<Vec<u8>> {
        if let Some(conn) = &self.connection {
            match conn.read_datagram().await {
                Ok(datagram) => return Some(datagram.to_vec()),
                Err(e) => {
                    eprintln!("[ZeusClient] Read Datagram Error: {}", e);
                    return None;
                }
            }
        }
        None
    }
}
