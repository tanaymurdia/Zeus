use zeus_client::ZeusClient;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_client_connect_and_send() {
    // 1. Start a Seed Node (Server)
    // We use zeus_transport to make a server, similar to NodeActor
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (server_endpoint, _) = zeus_transport::make_server_endpoint(addr).expect("Failed to create server");
    let server_addr = server_endpoint.local_addr().unwrap();
    
    // Spawn Server Logic
    tokio::spawn(async move {
        if let Some(conn) = server_endpoint.accept().await {
            let connection = conn.await.expect("Server connection failed");
            // Accept unidirectional stream
            if let Ok(mut recv) = connection.accept_uni().await {
                // Read everything (expecting ~100 bytes of signed update)
                let _ = recv.read_to_end(1024 * 10).await;
            }
        }
    });

    // 2. Start Client
    let mut client = ZeusClient::new(999).expect("Failed to create client");
    
    // 3. Connect (Mocking 'localhost' cert trust by assuming client uses promiscuous endpoint which it does)
    client.connect(server_addr).await.expect("Failed to connect");
    
    // 4. Send State
    // Should succeed
    client.send_state((10.0, 0.0, 0.0), (1.0, 0.0, 0.0)).await.expect("Failed to send state");
    
    sleep(Duration::from_millis(100)).await;
}
