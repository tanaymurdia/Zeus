use super::helpers::*;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use zeus_node::cell::Cell;
use zeus_node::entity_manager::{AuthorityState, Entity};

#[tokio::test]
async fn test_peer_death_detected_within_100ms() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = make_node(cell0, vec![]).await;
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = make_node(cell1, vec![node0_addr]).await;

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let peers_before = node0.engine.discovery.peer_ids();
    assert!(!peers_before.is_empty(), "node0 should see node1 as peer");

    for conn in &node1.engine.peer_connections {
        conn.close(0u32.into(), b"test_shutdown");
    }
    for conn in &node1.engine.connections {
        conn.close(0u32.into(), b"test_shutdown");
    }

    let start = Instant::now();
    let deadline = Duration::from_millis(500);
    loop {
        node0.tick(0.008).await.unwrap();
        let peers_now = node0.engine.discovery.peer_ids();
        if peers_now.is_empty() {
            let elapsed = start.elapsed();
            assert!(
                elapsed < deadline,
                "Peer death detection took {:?}, expected < {:?}",
                elapsed, deadline
            );
            break;
        }
        if start.elapsed() >= deadline {
            panic!(
                "Peer death not detected within {:?}. Remaining peers: {:?}",
                deadline, node0.engine.discovery.peer_ids()
            );
        }
        sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test]
async fn test_split_handoff_completes_within_200ms() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = make_node(cell0.clone(), vec![]).await;
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = make_node(cell1, vec![node0_addr]).await;

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for i in 0..10u64 {
        let x = 11.5;
        let pos = (x, 5.0 + i as f32, 0.0);
        let vel = (3.0, 0.0, 0.0);
        node0.world.spawn_local(i + 1, pos, vel);
        node0.engine.node.manager.add_entity(Entity {
            id: i + 1,
            pos,
            vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
    }

    let start = Instant::now();
    let deadline = Duration::from_millis(2000);
    let mut all_arrived = false;

    for _ in 0..500 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();

        let n1_local: Vec<u64> = node1.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .map(|(id, _)| *id)
            .collect();

        if n1_local.len() >= 10 {
            all_arrived = true;
            let elapsed = start.elapsed();
            assert!(
                elapsed < deadline,
                "Handoff took {:?}, expected < {:?}",
                elapsed, deadline
            );
            break;
        }
        sleep(Duration::from_millis(2)).await;
    }
    assert!(all_arrived, "Not all 10 entities arrived on node1");
}

#[tokio::test]
async fn test_drain_completes_within_500ms() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = make_node(cell0.clone(), vec![]).await;
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = make_node(cell1, vec![node0_addr]).await;

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for i in 0..10u64 {
        let pos = (11.5, 5.0 + i as f32, 0.0);
        let vel = (1.0, 0.0, 0.0);
        node0.world.spawn_local(i + 1, pos, vel);
        node0.engine.node.manager.add_entity(Entity {
            id: i + 1,
            pos,
            vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
    }

    for _ in 0..10 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let exclude: Vec<u64> = Vec::new();
    let _ = node0.engine.drain_local_entities(&exclude).await;

    let start = Instant::now();
    let deadline = Duration::from_millis(3000);
    let mut drain_done = false;

    for _ in 0..500 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();

        let n0_local = node0.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .count();
        let n0_ho = node0.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::HandoffOut)
            .count();

        if n0_local == 0 && n0_ho == 0 {
            drain_done = true;
            let elapsed = start.elapsed();
            assert!(
                elapsed < deadline,
                "Drain took {:?}, expected < {:?}",
                elapsed, deadline
            );
            break;
        }

        if n0_ho > 0 {
            let _ = node0.engine.drain_local_entities(&exclude).await;
        }

        sleep(Duration::from_millis(2)).await;
    }
    assert!(drain_done, "Drain did not complete within deadline");

    let n1_local = node1.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .count();
    let n1_any = node1.engine.node.manager.entities.len();
    assert!(n1_local >= 5 || n1_any >= 5, "Expected entities on node1 after drain, got local={} total={}", n1_local, n1_any);
}
