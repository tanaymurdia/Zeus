use zeus_client::ZeusClient;

use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use zeus_transport::make_server_endpoint;

#[tokio::test]
async fn test_20_clients_concurrent_spam() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (server_endpoint, _) = make_server_endpoint(addr).expect("Failed to create server");
    let server_addr = server_endpoint.local_addr().unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    tokio::spawn(async move {
        while let Some(conn) = server_endpoint.accept().await {
            if let Ok(connection) = conn.await {
                let counter_ref = counter_clone.clone();

                tokio::spawn(async move {
                    loop {
                        match connection.accept_uni().await {
                            Ok(mut recv) => {
                                if let Ok(_) = recv.read_to_end(1024).await {
                                    counter_ref.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(_) => break,
                        }
                    }
                });
            }
        }
    });
    let client_count = 20;
    let updates_per_client = 50;
    let mut tasks = Vec::new();

    for i in 0..client_count {
        tasks.push(tokio::spawn(async move {
            let mut client = ZeusClient::new(1000 + i as u64).unwrap();
            client.connect(server_addr).await.expect("Connect failed");

            for j in 0..updates_per_client {
                client
                    .send_state((i as f32, j as f32, 0.0), (1.0, 1.0, 1.0))
                    .await
                    .expect("Send failed");
                if j % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            client.endpoint().close(0u32.into(), b"done");
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    let mut attempts = 0;
    while counter.load(Ordering::Relaxed) < client_count * updates_per_client {
        sleep(Duration::from_millis(100)).await;
        attempts += 1;
        if attempts > 50 {
            break;
        }
    }

    let total = counter.load(Ordering::Relaxed);
    println!(
        "Test: Received {} / {} packets",
        total,
        client_count * updates_per_client
    );

    assert_eq!(total, client_count * updates_per_client);
}

#[tokio::test]
async fn test_data_integrity_sequence() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (server_endpoint, _) = make_server_endpoint(addr).expect("Server fail");
    let server_addr = server_endpoint.local_addr().unwrap();

    let (tx, mut rx) = mpsc::channel(100);

    tokio::spawn(async move {
        if let Some(conn) = server_endpoint.accept().await {
            let connection = conn.await.unwrap();
            loop {
                if let Ok(mut recv) = connection.accept_uni().await {
                    if let Ok(buf) = recv.read_to_end(1024 * 64).await {
                        tx.send(buf).await.unwrap();
                    }
                } else {
                    break;
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut client = ZeusClient::new(999).unwrap();
        client.connect(server_addr).await.unwrap();

        for x in 0..10 {
            client
                .send_state((x as f32, 0.0, 0.0), (0.0, 0.0, 0.0))
                .await
                .unwrap();
            sleep(Duration::from_millis(10)).await;
        }
    });

    let mut expected_x = 0.0;
    for _ in 0..10 {
        if let Some(bytes) = rx.recv().await {
            if let Ok(ghost) = zeus_common::flatbuffers::root::<zeus_common::Ghost>(&bytes) {
                let pos = ghost.position().unwrap();
                println!("Received X={}", pos.x());
                assert_eq!(pos.x(), expected_x);
                expected_x += 1.0;
            }
        }
    }
}
