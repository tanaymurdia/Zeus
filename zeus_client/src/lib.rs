use quinn::{Connection, Endpoint};
use std::net::SocketAddr;
use thiserror::Error;
use zeus_common::{GhostSerializer, ed25519_dalek::SigningKey};
use zeus_transport::make_promiscuous_endpoint;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Transport Error: {0}")]
    Transport(String),
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
            let mut serializer = GhostSerializer::new();
            serializer.set_keypair(self.signing_key.clone());

            let msg_bytes = serializer.serialize(self.local_id, pos, vel);

            let mut stream = conn.open_uni().await?;
            stream.write_all(&msg_bytes).await?;
            stream.finish()?;
        }
        Ok(())
    }

    pub fn try_read_server_status(&self) -> Option<(u16, u8)> {
        if let Some(_conn) = &self.connection {
            None
        } else {
            None
        }
    }

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

    pub fn connection(&self) -> Option<Connection> {
        self.connection.clone()
    }

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
