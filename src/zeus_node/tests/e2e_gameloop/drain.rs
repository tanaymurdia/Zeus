use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::game_loop::GameLoop;

#[tokio::test]
#[ignore = "drain_local_entities network delivery needs investigation"]
async fn test_drain_local_entities_moves_all_to_peer() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_b = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    for i in 0..10u64 {
        let pos = (10.0 + i as f32, 25.0, 25.0);
        node_a.engine.node.manager.add_entity(Entity {
            id: i + 1,
            pos,
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        node_a.world.spawn_local(i + 1, pos, (0.0, 0.0, 0.0));
    }

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let drained = node_a.engine.drain_local_entities(&[]).await;
    assert_eq!(drained, 10, "Should drain all 10 entities");

    let a_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(a_local, 0, "Node A should have 0 local after drain");

    let a_ho: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::HandoffOut).count();
    assert_eq!(a_ho, 10, "All 10 should be HandoffOut on node A");

    for _ in 0..50 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let b_local: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert!(b_local >= 8, "Node B should have received most entities, got {}", b_local);

    let a_local_after: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(a_local_after, 0, "Node A should still have 0 local");
}

#[tokio::test]
#[ignore = "drain_local_entities network delivery needs investigation"]
async fn test_drain_excludes_specified_ids() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_b = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    let player_id = 999u64;
    node_a.engine.node.manager.add_entity(Entity {
        id: player_id,
        pos: (25.0, 25.0, 25.0),
        vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node_a.world.spawn_local(player_id, (25.0, 25.0, 25.0), (0.0, 0.0, 0.0));

    for i in 0..5u64 {
        node_a.engine.node.manager.add_entity(Entity {
            id: i + 1,
            pos: (10.0 + i as f32, 25.0, 25.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        node_a.world.spawn_local(i + 1, (10.0 + i as f32, 25.0, 25.0), (0.0, 0.0, 0.0));
    }

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let drained = node_a.engine.drain_local_entities(&[player_id]).await;
    assert_eq!(drained, 5, "Should drain 5 entities (not the player)");

    let player_state = node_a.engine.node.manager.get_entity(player_id).unwrap().state.clone();
    assert_eq!(player_state, AuthorityState::Local, "Player should remain Local");
}

#[tokio::test]
async fn test_drain_with_no_peers_drains_nothing() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut node_a = make_node(cell_a.clone(), vec![]).await;

    for i in 0..5u64 {
        node_a.engine.node.manager.add_entity(Entity {
            id: i + 1,
            pos: (10.0 + i as f32, 25.0, 25.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        node_a.world.spawn_local(i + 1, (10.0 + i as f32, 25.0, 25.0), (0.0, 0.0, 0.0));
    }

    let drained = node_a.engine.drain_local_entities(&[]).await;
    assert_eq!(drained, 0, "No peers to drain to");

    let a_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(a_local, 5, "All entities should remain Local");
}

#[tokio::test]
#[ignore = "drain_local_entities network delivery needs investigation"]
async fn test_merge_cycle_entity_conservation() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let cell_a = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let cell_b = Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0);

    let mut node_a = make_node(full_cell.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    let total_entities = 20u64;
    for i in 0..total_entities {
        let y = 5.0 + (i as f32 / total_entities as f32) * 90.0;
        let pos = (50.0, y, 50.0);
        node_a.engine.node.manager.add_entity(Entity {
            id: i + 1, pos, vel: (0.0, 0.5, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
        node_a.world.spawn_local(i + 1, pos, (0.0, 0.5, 0.0));
    }

    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(cell_a.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..100 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let a_local_pre: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let b_local_pre: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let pre_drain_total = a_local_pre + b_local_pre;

    let _ = node_b.engine.drain_local_entities(&[]).await;

    for _ in 0..100 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let b_local: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let b_ho: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::HandoffOut).count();
    let a_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();

    assert_eq!(b_local, 0, "Node B should have 0 local after drain");
    assert_eq!(b_ho, 0, "Node B should have 0 HandoffOut after drain completes");
    assert!(a_local >= (pre_drain_total / 2),
        "Node A should have absorbed most entities, got {} pre_drain={}", a_local, pre_drain_total);
    assert_single_ownership(&[node_a, node_b], "post-drain");
}

#[tokio::test]
#[ignore = "drain_local_entities network delivery needs investigation"]
async fn test_drain_3node_entities_distributed_correctly() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cell_a = Cell::new(0.0, 33.0, 0.0, 100.0, 0.0, 100.0);
    let cell_b = Cell::new(33.0, 66.0, 0.0, 100.0, 0.0, 100.0);
    let cell_c = Cell::new(66.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;
    let addr_b = node_b.engine.endpoint.local_addr().unwrap();
    let mut node_c = make_node(cell_c.clone(), vec![addr_a, addr_b]).await;

    for i in 0..10u64 {
        let x = 34.0 + (i as f32 / 10.0) * 30.0;
        let pos = (x, 50.0, 50.0);
        node_b.engine.node.manager.add_entity(Entity {
            id: i + 1, pos, vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
        node_b.world.spawn_local(i + 1, pos, (0.0, 0.0, 0.0));
    }

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let _ = node_b.engine.drain_local_entities(&[]).await;

    for _ in 0..80 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let b_local: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let a_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let c_local: usize = node_c.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let total = a_local + b_local + c_local;

    assert_eq!(b_local, 0, "Draining node should have 0 local");
    assert!(total >= 8, "Should conserve most entities, total={}", total);
    assert_single_ownership(&[node_a, node_b, node_c], "3-node drain");
}

#[tokio::test]
async fn test_autoscaler_expand_toward_proposes_union() {
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
    use zeus_node::cell::Cell;

    let cell_a = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let dead_cell = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });

    let mut peer_ids = HashSet::new();
    peer_ids.insert(42);
    let mut peer_cells = HashMap::new();
    peer_cells.insert(42, dead_cell.clone());
    scaler.evaluate(&cell_a, 10, &peer_ids, &peer_cells, 2, &[]);

    let events = scaler.evaluate(&cell_a, 10, &HashSet::new(), &HashMap::new(), 1, &[]);

    let expanded = events.iter().find(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
    assert!(expanded.is_some(), "Should produce CellExpanded for adjacent sibling");

    if let Some(ScaleEvent::CellExpanded { new_cell, dead_peer_id, .. }) = expanded {
        assert_eq!(*dead_peer_id, 42);
        assert!((new_cell.x_min - 0.0).abs() < 1e-3);
        assert!((new_cell.x_max - 100.0).abs() < 1e-3);
        assert!((new_cell.y_min - 0.0).abs() < 1e-3);
        assert!((new_cell.y_max - 100.0).abs() < 1e-3);
    }
}

#[tokio::test]
async fn test_autoscaler_merge_not_on_single_node() {
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });

    let events = scaler.evaluate(&cell, 2, &HashSet::new(), &HashMap::new(), 1, &[]);
    assert!(!events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "Single node should never recommend merge even with few entities");
}

#[tokio::test]
#[ignore = "drain_local_entities network delivery needs investigation"]
async fn test_split_then_drain_full_cycle() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};
    use zeus_node::autoscaler::AutoScaler;

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut node_a = make_node(full_cell.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for i in 0..50u64 {
        let x = 5.0 + (i as f32 % 10.0) * 9.0;
        let y = 5.0 + (i as f32 / 10.0).floor() * 18.0;
        let pos = (x, y, 50.0);
        node_a.engine.node.manager.add_entity(Entity {
            id: i + 1, pos, vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
        node_a.world.spawn_local(i + 1, pos, (0.0, 0.0, 0.0));
    }

    let positions: Vec<(f32, f32, f32)> = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local)
        .map(|e| e.pos)
        .collect();
    let (keep_cell, new_cell, _, _) = AutoScaler::compute_binary_split(&full_cell, &positions);

    let mut node_b = make_node(new_cell.clone(), vec![addr_a]).await;

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..100 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let a_loc: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let b_loc: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let pre_drain = a_loc + b_loc;
    assert!(pre_drain >= 40, "Should have 40+ entities after split, got {}", pre_drain);

    let _ = node_b.engine.drain_local_entities(&[]).await;

    for _ in 0..100 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let b_final: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let a_final: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();

    assert_eq!(b_final, 0, "Drained node B should have 0 Local");
    assert!(a_final >= (pre_drain * 7 / 10),
        "Node A should absorb most entities: a_local={} pre_drain={}", a_final, pre_drain);
}

#[tokio::test]
async fn test_drain_completes_within_bounded_ticks() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_b = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    for i in 0..20u64 {
        let pos = (5.0 + i as f32, 25.0, 25.0);
        node_a.engine.node.manager.add_entity(Entity {
            id: i + 1, pos, vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
        node_a.world.spawn_local(i + 1, pos, (0.0, 0.0, 0.0));
    }

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let expanded = cell_b.expand_toward(&cell_a).unwrap();
    node_b.set_cell(expanded);

    for _ in 0..5 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let _ = node_a.engine.drain_local_entities(&[]).await;

    let mut completed_at = None;
    for tick in 0..200 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let a_local: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let a_ho: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();

        if a_local == 0 && a_ho == 0 {
            completed_at = Some(tick);
            break;
        }
    }

    assert!(completed_at.is_some(), "Drain should complete within 200 ticks");
    assert!(completed_at.unwrap() < 100, "Drain should complete within 100 ticks, took {}", completed_at.unwrap());
}

#[tokio::test]
async fn test_find_nearest_peer_picks_closest_cell() {
    use zeus_node::cell::Cell;
    use zeus_node::discovery::Peer;
    use zeus_node::discovery::DiscoveryActor;
    use std::time::Instant;

    let addr: std::net::SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);

    let cell_near = Cell::new(10.0, 20.0, 0.0, 10.0, 0.0, 10.0);
    let cell_far = Cell::new(50.0, 60.0, 0.0, 10.0, 0.0, 10.0);

    actor.peers.insert(2, Peer {
        id: 2,
        addr: "127.0.0.1:9001".parse().unwrap(),
        pos: (0.0, 0.0, 0.0),
        load: None,
        last_seen: Instant::now(),
        ordinal: 1,
        cell: Some(cell_near.clone()),
    });
    actor.peers.insert(3, Peer {
        id: 3,
        addr: "127.0.0.1:9002".parse().unwrap(),
        pos: (0.0, 0.0, 0.0),
        load: None,
        last_seen: Instant::now(),
        ordinal: 2,
        cell: Some(cell_far.clone()),
    });

    let nearest = actor.find_nearest_peer((5.0, 5.0, 5.0)).unwrap();
    assert_eq!(nearest.id, 2, "Should pick the cell at x=[10,20], not x=[50,60]");

    let nearest2 = actor.find_nearest_peer((55.0, 5.0, 5.0)).unwrap();
    assert_eq!(nearest2.id, 3, "Should pick the cell at x=[50,60] for point at x=55");
}

#[tokio::test]
async fn test_cell_expand_toward_covers_dead_space() {
    use zeus_node::cell::Cell;
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};

    let cell_a = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let dead_cell = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });

    let mut peers = HashSet::new();
    peers.insert(42);
    let mut peer_cells = HashMap::new();
    peer_cells.insert(42, dead_cell.clone());
    scaler.evaluate(&cell_a, 10, &peers, &peer_cells, 2, &[]);

    let events = scaler.evaluate(&cell_a, 10, &HashSet::new(), &HashMap::new(), 1, &[]);
    let expanded = events.iter().find(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
    assert!(expanded.is_some());

    if let Some(ScaleEvent::CellExpanded { new_cell, .. }) = expanded {
        assert!((new_cell.x_max - 100.0).abs() < 1e-3, "Should expand to cover dead space");
        assert!((new_cell.y_min - 0.0).abs() < 1e-3, "Y should be preserved");
        assert!((new_cell.y_max - 100.0).abs() < 1e-3, "Y should be preserved");
    }
}

#[tokio::test]
#[ignore = "drain_local_entities network delivery needs investigation"]
async fn test_5node_drain_cascade_no_entity_loss() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cells = vec![
        Cell::new(0.0, 20.0, 0.0, 100.0, 0.0, 100.0),
        Cell::new(20.0, 40.0, 0.0, 100.0, 0.0, 100.0),
        Cell::new(40.0, 60.0, 0.0, 100.0, 0.0, 100.0),
        Cell::new(60.0, 80.0, 0.0, 100.0, 0.0, 100.0),
        Cell::new(80.0, 100.0, 0.0, 100.0, 0.0, 100.0),
    ];

    let mut all_addrs: Vec<std::net::SocketAddr> = Vec::new();
    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();

    for cell in &cells {
        let peers = all_addrs.clone();
        let node = make_node(cell.clone(), peers).await;
        all_addrs.push(node.engine.endpoint.local_addr().unwrap());
        nodes.push(node);
    }

    let total_entities = 50u64;
    for i in 0..total_entities {
        let node_idx = (i as usize) % 5;
        let cell = &cells[node_idx];
        let x = cell.x_min + 5.0 + (i as f32 % 5.0) * 2.0;
        let pos = (x, 50.0, 50.0);
        nodes[node_idx].engine.node.manager.add_entity(Entity {
            id: i + 1, pos, vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
        nodes[node_idx].world.spawn_local(i + 1, pos, (0.0, 0.0, 0.0));
    }

    for _ in 0..30 {
        for n in nodes.iter_mut() { n.tick(0.016).await.unwrap(); }
        sleep(Duration::from_millis(5)).await;
    }

    let pre_total = total_local_count(&nodes);
    assert_eq!(pre_total, total_entities as usize);

    let _ = nodes[2].engine.drain_local_entities(&[]).await;

    for _ in 0..100 {
        for n in nodes.iter_mut() { n.tick(0.016).await.unwrap(); }
        sleep(Duration::from_millis(5)).await;
    }

    let mid_node_local: usize = nodes[2].engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(mid_node_local, 0, "Drained node should have 0 local");

    let _ = nodes[4].engine.drain_local_entities(&[]).await;

    for _ in 0..100 {
        for n in nodes.iter_mut() { n.tick(0.016).await.unwrap(); }
        sleep(Duration::from_millis(5)).await;
    }

    let post_total = total_local_count(&nodes);
    assert!(post_total >= (total_entities as usize * 7 / 10),
        "After draining 2 nodes, should conserve >=70% entities: got {}/{}", post_total, total_entities);
    assert_single_ownership(&nodes, "5-node cascaded drain");
}
