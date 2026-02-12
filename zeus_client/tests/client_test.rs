use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use zeus_client::ZeusClient;

#[tokio::test]
async fn test_client_connect_and_send() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (server_endpoint, _) =
        zeus_transport::make_server_endpoint(addr).expect("Failed to create server");
    let server_addr = server_endpoint.local_addr().unwrap();

    tokio::spawn(async move {
        if let Some(conn) = server_endpoint.accept().await {
            let connection = conn.await.expect("Server connection failed");

            if let Ok(mut recv) = connection.accept_uni().await {
                let _ = recv.read_to_end(1024 * 10).await;
            }
        }
    });

    let mut client = ZeusClient::new(999).expect("Failed to create client");

    client
        .connect(server_addr)
        .await
        .expect("Failed to connect");

    client
        .send_state((10.0, 0.0, 0.0), (1.0, 0.0, 0.0))
        .await
        .expect("Failed to send state");

    sleep(Duration::from_millis(100)).await;
}
