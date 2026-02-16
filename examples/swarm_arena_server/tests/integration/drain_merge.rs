use super::helpers::*;
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::cell::Cell;
use zeus_node::engine::ZeusConfig;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::GameLoop;

#[tokio::test]
async fn test_drain_mode_entity_conservation_two_nodes() {
    let keep_cell = Cell::new(0.0, 24.0, -1.0, 13.0, -12.0, 12.0);
    let drain_cell = Cell::new(0.0, 24.0, 13.0, 25.0, -12.0, 12.0);

    let config_keep = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(keep_cell.clone()),
    };
    let mut world_keep = TestDroneWorld::new();
    for i in 0..10 {
        let y = 1.0 + (i as f32) * 1.1;
        world_keep.spawn_drone((12.0, y, 0.0), (0.0, 0.0, 0.0));
    }
    let mut node_keep = GameLoop::new(config_keep, world_keep).await.unwrap();
    let addr_keep = node_keep.engine.endpoint.local_addr().unwrap();

    for id in 1..=10u64 {
        let (pos, vel) = node_keep.world.drones[&id];
        node_keep.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let config_drain = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_keep],
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(drain_cell.clone()),
    };
    let mut world_drain = TestDroneWorld::new();
    world_drain.next_id = 100;
    for i in 0..5 {
        let y = 14.0 + (i as f32) * 2.0;
        world_drain.spawn_drone((12.0, y, 0.0), (0.0, 0.0, 0.0));
    }
    let mut node_drain = GameLoop::new(config_drain, world_drain).await.unwrap();

    for id in 100..105u64 {
        let (pos, vel) = node_drain.world.drones[&id];
        node_drain.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..30 {
        node_keep.tick(0.016).await.unwrap();
        node_drain.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let _ = node_drain.engine.drain_local_entities(&[]).await;

    for _ in 0..100 {
        node_keep.tick(0.016).await.unwrap();
        node_drain.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let drain_local: usize = node_drain.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let drain_ho: usize = node_drain.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::HandoffOut).count();
    let keep_local: usize = node_keep.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();

    assert_eq!(drain_local, 0, "Draining node should have 0 local entities");
    assert_eq!(drain_ho, 0, "Draining node should have 0 HandoffOut after completion");
    assert!(keep_local >= 13, "Keeping node should have absorbed drained entities: got {}", keep_local);
}

#[tokio::test]
async fn test_drain_preserves_physics_state_on_receiving_node() {
    let cell_a = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let cell_b = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell_a.clone()),
    };
    let world_a = TestDroneWorld::new();
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let mut world_b = TestDroneWorld::new();
    world_b.next_id = 200;
    for i in 0..8 {
        let x = 55.0 + (i as f32) * 5.0;
        world_b.spawn_drone((x, 50.0, 50.0), (1.0, 0.5, -0.3));
    }
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();
    for id in 200..208u64 {
        let (pos, vel) = node_b.world.drones[&id];
        node_b.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let _ = node_b.engine.drain_local_entities(&[]).await;

    for _ in 0..80 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let a_arrived_count = node_a.world.local_ids.len();
    assert!(a_arrived_count >= 6, "Node A physics world should have received drained entities: got {}", a_arrived_count);
}

#[tokio::test]
async fn test_drain_repeated_calls_idempotent() {
    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_b = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell_a),
    };
    let mut world_a = TestDroneWorld::new();
    for i in 0..5 {
        world_a.spawn_drone((10.0 + i as f32 * 5.0, 25.0, 25.0), (0.0, 0.0, 0.0));
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    for id in 1..=5u64 {
        let (pos, vel) = node_a.world.drones[&id];
        node_a.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 50.0,
        cell: Some(cell_b),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let d1 = node_a.engine.drain_local_entities(&[]).await;
    let d2 = node_a.engine.drain_local_entities(&[]).await;
    assert_eq!(d2, 0, "Second drain should have nothing to drain since all are already HandoffOut");
    assert_eq!(d1, 5, "First drain should drain all 5");
}
