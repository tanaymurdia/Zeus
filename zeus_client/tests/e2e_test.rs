use std::net::SocketAddr;
use zeus_client::ZeusClient;
use zeus_node::node_actor::NodeActor;
use zeus_transport::make_server_endpoint;

use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_full_mesh_propagation() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (server_endpoint, _) = make_server_endpoint(addr).expect("Failed to create server");
    let server_addr = server_endpoint.local_addr().unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let _ = NodeActor::new(0.0, 5.0, 0.0);

    let tx_loop = tx.clone();
    tokio::spawn(async move {
        let mut _conn_handle: Option<quinn::Connection> = None;

        while let Some(event) = rx.recv().await {
            match event {
                zeus_node::NetworkEvent::NewConnection(conn) => {
                    println!("Test Node: Accepted Connection");
                    _conn_handle = Some(conn.clone());

                    let tx_reader = tx_loop.clone();
                    tokio::spawn(async move {
                        loop {
                            match conn.accept_uni().await {
                                Ok(mut recv) => {
                                    if let Ok(bytes) = recv.read_to_end(1024 * 64).await {
                                        let _ = tx_reader
                                            .send(zeus_node::NetworkEvent::Payload(
                                                conn.clone(),
                                                bytes,
                                                true,
                                            ))
                                            .await;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    });
                }
                zeus_node::NetworkEvent::Payload(_conn, bytes, _is_stream) => {
                    if bytes.len() == 96 {
                        if let Ok(msg) =
                            zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&bytes)
                        {
                            if let Some(ghost) = msg.state() {
                                let id = ghost.entity_id();
                                assert_eq!(id, 101);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    });

    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(conn) = server_endpoint.accept().await {
            let connection = conn.await.expect("Conn failed");
            let _ = tx_clone
                .send(zeus_node::NetworkEvent::NewConnection(connection))
                .await;
        }
    });
    let mut client1 = ZeusClient::new(101).expect("Client 1 fail");
    client1
        .connect(server_addr)
        .await
        .expect("Client 1 connect fail");

    let mut client2 = ZeusClient::new(102).expect("Client 2 fail");
    client2
        .connect(server_addr)
        .await
        .expect("Client 2 connect fail");

    sleep(Duration::from_millis(100)).await;
    client1
        .send_state((50.0, 0.0, 0.0), (1.0, 0.0, 0.0))
        .await
        .expect("Send fail");

    sleep(Duration::from_millis(200)).await;
}
