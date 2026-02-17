use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::GameLoop;

#[tokio::test]
async fn test_authority_unique_during_split() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 10.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 30.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..5u64 {
        let x = 3.0 + (i as f32) * 3.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (1.0, 0.0, 0.0));
    }

    let mut duplicate_found = false;
    for tick in 0..100 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();

        let local_on_0: HashSet<u64> = node0
            .engine
            .node
            .manager
            .entities
            .iter()
            .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
            .map(|(id, _)| *id)
            .collect();
        let local_on_1: HashSet<u64> = node1
            .engine
            .node
            .manager
            .entities
            .iter()
            .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
            .map(|(id, _)| *id)
            .collect();

        let overlap: Vec<u64> = local_on_0.intersection(&local_on_1).copied().collect();
        if !overlap.is_empty() {
            eprintln!(
                "[tick {}] DUPLICATE Local authority: {:?} (node0 local={:?}, node1 local={:?})",
                tick, overlap, local_on_0, local_on_1
            );
            duplicate_found = true;
        }

        if tick % 20 == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    }

    assert!(
        !duplicate_found,
        "No entity should be Local on two nodes simultaneously during split"
    );
}

#[tokio::test]
async fn test_sequential_3node_split() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    for i in 0..8u64 {
        let x = 2.0 + (i as f32) * 3.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (0.1, 0.0, 0.0));
    }

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.set_boundary(12.0);
    for _ in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    node0.set_boundary(8.0);
    node1.set_boundary(16.0);

    for _ in 0..120 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let mut entity_seen_on: HashMap<u64, Vec<usize>> = HashMap::new();
    for (node_idx, node) in [&node0, &node1, &node2].iter().enumerate() {
        for (id, _entity) in &node.engine.node.manager.entities {
            entity_seen_on.entry(*id).or_default().push(node_idx);
        }
    }

    let unique_entities = entity_seen_on.len();
    assert!(
        unique_entities >= 6,
        "At least 6 of 8 entities should be tracked across 3 nodes (got {}). Map: {:?}",
        unique_entities, entity_seen_on
    );

    let mut lost = Vec::new();
    for id in 1..=8u64 {
        let exists_anywhere = node0.engine.node.manager.get_entity(id).is_some()
            || node1.engine.node.manager.get_entity(id).is_some()
            || node2.engine.node.manager.get_entity(id).is_some()
            || node0.engine.remote_entity_states.contains_key(&id)
            || node1.engine.remote_entity_states.contains_key(&id)
            || node2.engine.remote_entity_states.contains_key(&id);
        if !exists_anywhere {
            lost.push(id);
        }
    }

    assert!(
        lost.len() <= 1,
        "At most 1 entity can be lost during 3-node split. Lost: {:?}",
        lost
    );
}

#[tokio::test]
async fn test_entity_count_conserved_during_rapid_boundary_change() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 20.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..10u64 {
        let x = 2.0 + (i as f32) * 2.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (0.1, 0.0, 0.0));
    }
    for i in 0..10u64 {
        let x = 25.0 + (i as f32) * 2.0;
        node1.world.spawn_local(100 + i + 1, (x, 0.0, 0.0), (-0.1, 0.0, 0.0));
    }

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for tick in 0..50 {
        let new_boundary = 20.0 - (tick as f32 * 0.3);
        node0.set_boundary(new_boundary.max(3.0));
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
    }

    for _ in 0..100 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }

    let mut total_local = 0;
    for node in [&node0, &node1] {
        for entity in node.engine.node.manager.entities.values() {
            if entity.state == zeus_node::entity_manager::AuthorityState::Local {
                total_local += 1;
            }
        }
    }

    assert!(
        total_local >= 12,
        "At least 12 of 20 entities should remain Local across both nodes (got {})",
        total_local
    );
}

#[tokio::test]
async fn test_sdk_should_split() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let game_loop = GameLoop::new(config, TestWorld::new()).await.unwrap();

    assert!(!game_loop.should_split(0), "0 entities should not trigger split");
    assert!(!game_loop.should_split(39), "39 entities should not trigger split (threshold is 40)");
    assert!(game_loop.should_split(40), "40 entities should trigger split");
    assert!(game_loop.should_split(50), "50 entities should trigger split");
}

#[tokio::test]
async fn test_3d_cell_entity_stays_local_at_world_edge() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell_a),
    };
    let world = TestWorld::new();
    let mut node = GameLoop::new(config, world).await.unwrap();

    node.world.spawn_local(1, (25.0, 49.0, 25.0), (0.0, 10.0, 0.0));
    node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1,
        pos: (25.0, 49.0, 25.0),
        vel: (0.0, 10.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..30 {
        node.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let entity = node.engine.node.manager.get_entity(1);
    assert!(
        entity.is_some(),
        "Entity should still exist on node (not lost)"
    );
    let e = entity.unwrap();
    assert_eq!(
        e.state,
        AuthorityState::Local,
        "Entity at world edge with no neighbor should stay Local, not HandoffOut. State: {:?}",
        e.state
    );
}

#[tokio::test]
async fn test_3d_cell_handoff_across_z_boundary() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell_a = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 100.0, 0.0, 100.0, 50.0, 100.0);

    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell_a.clone()),
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let world1 = TestWorld::new();
    let mut node1 = GameLoop::new(config1, world1).await.unwrap();
    let node1_id = node1.engine.discovery.local_id;

    sleep(Duration::from_millis(100)).await;
    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.engine.discovery.update_peer_cell(node1_id, cell_b.clone());

    node0.world.spawn_local(42, (50.0, 50.0, 48.0), (0.0, 0.0, 10.0));
    node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 42,
        pos: (50.0, 50.0, 48.0),
        vel: (0.0, 0.0, 10.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    let mut handoff_occurred = false;
    for _ in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        if let Some(e) = node1.engine.node.manager.get_entity(42) {
            if e.state == AuthorityState::Local {
                handoff_occurred = true;
                break;
            }
        }
    }

    assert!(
        handoff_occurred,
        "Entity should handoff across Z boundary from node0 to node1 via 3D cell routing"
    );
}

#[tokio::test]
async fn test_3d_cell_offer_rejected_if_outside_cell() {
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut node = zeus_node::node_actor::NodeActor::new_3d(cell, 1.0);

    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let pos = zeus_common::Vec3::new(75.0, 25.0, 25.0);
    let vel = zeus_common::Vec3::new(0.0, 0.0, 0.0);
    let sig = builder.create_vector(&[0u8; 64]);
    let ghost = zeus_common::Ghost::create(
        &mut builder,
        &zeus_common::GhostArgs {
            entity_id: 99,
            position: Some(&pos),
            velocity: Some(&vel),
            signature: Some(sig),
        },
    );
    let msg = zeus_common::HandoffMsg::create(
        &mut builder,
        &zeus_common::HandoffMsgArgs {
            entity_id: 99,
            type_: zeus_common::HandoffType::Offer,
            state: Some(ghost),
        },
    );
    builder.finish(msg, None);
    let buf = builder.finished_data();
    let msg = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(buf).unwrap();

    node.handle_handoff_msg(msg, None);
    assert!(
        node.manager.get_entity(99).is_none(),
        "Offer with entity position (75,25,25) outside cell [0-50,0-50,0-50] should be rejected"
    );
}

#[tokio::test]
async fn test_3d_cell_offer_accepted_if_inside_cell() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut node = zeus_node::node_actor::NodeActor::new_3d(cell, 1.0);

    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let pos = zeus_common::Vec3::new(25.0, 25.0, 25.0);
    let vel = zeus_common::Vec3::new(0.0, 0.0, 0.0);
    let sig = builder.create_vector(&[0u8; 64]);
    let ghost = zeus_common::Ghost::create(
        &mut builder,
        &zeus_common::GhostArgs {
            entity_id: 99,
            position: Some(&pos),
            velocity: Some(&vel),
            signature: Some(sig),
        },
    );
    let msg = zeus_common::HandoffMsg::create(
        &mut builder,
        &zeus_common::HandoffMsgArgs {
            entity_id: 99,
            type_: zeus_common::HandoffType::Offer,
            state: Some(ghost),
        },
    );
    builder.finish(msg, None);
    let buf = builder.finished_data();
    let msg = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(buf).unwrap();

    node.handle_handoff_msg(msg, None);
    let entity = node.manager.get_entity(99);
    assert!(entity.is_some(), "Offer inside cell should be accepted");
    assert_eq!(entity.unwrap().state, AuthorityState::HandoffIn);
}

#[tokio::test]
async fn test_no_broadcast_to_all_peers_on_3d_exit() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell_a = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);

    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell_a),
    };
    let world = TestWorld::new();
    let mut node = GameLoop::new(config, world).await.unwrap();

    node.world.spawn_local(1, (25.0, 99.0, 50.0), (0.0, 10.0, 0.0));
    node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1,
        pos: (25.0, 99.0, 50.0),
        vel: (0.0, 10.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..20 {
        node.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let e = node.engine.node.manager.get_entity(1).unwrap();
    assert_eq!(
        e.state,
        AuthorityState::Local,
        "Entity exiting on Y face with no peer should remain Local (no blind broadcast). State: {:?}",
        e.state
    );
}

#[tokio::test]
async fn test_autoscaler_split_recommendation() {
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 10,
        warmup_threshold: 10,
        merge_threshold: 2,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 8,
        startup_grace_ticks: 0,
    });

    let positions: Vec<(f32, f32, f32)> = (0..15).map(|i| (i as f32 * 5.0, 50.0, 50.0)).collect();
    let peer_ids = HashSet::new();
    let peer_cells = HashMap::new();

    let events = scaler.evaluate(&cell, 15, &peer_ids, &peer_cells, 1, &positions);
    let split = events.iter().find(|e| matches!(e, ScaleEvent::WarmupRecommended { .. }));
    assert!(split.is_some(), "Should recommend split with 15 entities > threshold 10");

    if let Some(ScaleEvent::WarmupRecommended { projected_cell: keep_cell, projected_new_cell: new_cell, .. }) = split {
        let union = keep_cell.union(new_cell);
        assert!((union.x_min - cell.x_min).abs() < 1e-3);
        assert!((union.x_max - cell.x_max).abs() < 1e-3);
        assert!((union.y_min - cell.y_min).abs() < 1e-3);
        assert!((union.y_max - cell.y_max).abs() < 1e-3);
        assert!((union.z_min - cell.z_min).abs() < 1e-3);
        assert!((union.z_max - cell.z_max).abs() < 1e-3);
    }
}

#[tokio::test]
async fn test_autoscaler_merge_recommendation() {
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });

    let peer_ids = HashSet::new();
    let peer_cells = HashMap::new();
    let events = scaler.evaluate(&cell, 2, &peer_ids, &peer_cells, 2, &[]);
    assert!(events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)));
}

#[tokio::test]
async fn test_autoscaler_cell_expansion_on_peer_death() {
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
    use zeus_node::cell::Cell;

    let my_cell = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let dead_cell = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig::default());

    let mut peer_ids = HashSet::new();
    peer_ids.insert(42);
    let mut peer_cells = HashMap::new();
    peer_cells.insert(42, dead_cell.clone());
    scaler.evaluate(&my_cell, 10, &peer_ids, &peer_cells, 2, &[]);

    let empty_peers = HashSet::new();
    let empty_cells = HashMap::new();
    let events = scaler.evaluate(&my_cell, 10, &empty_peers, &empty_cells, 1, &[]);

    let expanded = events.iter().find(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
    assert!(expanded.is_some(), "Should expand cell when adjacent peer dies");
    if let Some(ScaleEvent::CellExpanded { new_cell, .. }) = expanded {
        assert!((new_cell.x_min - 0.0).abs() < 1e-3);
        assert!((new_cell.x_max - 100.0).abs() < 1e-3);
    }
}

#[tokio::test]
async fn test_autoscaler_binary_split_produces_valid_cells() {
    use zeus_node::autoscaler::AutoScaler;
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 100.0, -50.0, 50.0, 0.0, 200.0);
    let positions: Vec<(f32, f32, f32)> = (0..40).map(|i| {
        (25.0 + (i as f32 % 10.0), (i as f32 % 20.0) - 10.0, 100.0)
    }).collect();

    let (keep, new, _axis, _pos) = AutoScaler::compute_binary_split(&cell, &positions);
    assert!(keep.volume() > 0.0);
    assert!(new.volume() > 0.0);
    let union = keep.union(&new);
    assert!((union.x_min - cell.x_min).abs() < 1e-3);
    assert!((union.x_max - cell.x_max).abs() < 1e-3);
    assert!((union.y_min - cell.y_min).abs() < 1e-3);
    assert!((union.y_max - cell.y_max).abs() < 1e-3);
    assert!((union.z_min - cell.z_min).abs() < 1e-3);
    assert!((union.z_max - cell.z_max).abs() < 1e-3);

    let keep_count = positions.iter().filter(|p| keep.contains(**p)).count();
    let new_count = positions.iter().filter(|p| new.contains(**p)).count();
    assert!(keep_count >= new_count, "keep_cell should have more entities");
}

#[tokio::test]
async fn test_gameloop_local_entity_positions() {
    use zeus_node::entity_manager::AuthorityState;

    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let world = TestWorld::new();
    let mut node = GameLoop::new(config, world).await.unwrap();

    node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (10.0, 20.0, 30.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 2, pos: (40.0, 50.0, 60.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Remote, verifying_key: None,
    });
    node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 3, pos: (70.0, 80.0, 90.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let positions = node.local_entity_positions();
    assert_eq!(positions.len(), 2);
    assert!(positions.iter().any(|(id, _)| *id == 1));
    assert!(positions.iter().any(|(id, _)| *id == 3));
    assert!(!positions.iter().any(|(id, _)| *id == 2));
}

#[tokio::test]
async fn test_entity_manager_remove_entity() {
    use zeus_node::entity_manager::{Entity, AuthorityState, EntityManager};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut em = EntityManager::new_3d(cell, 1.0);
    em.add_entity(Entity {
        id: 1, pos: (50.0, 50.0, 50.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    assert!(em.get_entity(1).is_some());
    let removed = em.remove_entity(1);
    assert!(removed.is_some());
    assert!(em.get_entity(1).is_none());
    let removed_again = em.remove_entity(1);
    assert!(removed_again.is_none());
}

#[tokio::test]
async fn test_force_exit_check_detects_entities_outside_cell() {
    use zeus_node::entity_manager::{Entity, AuthorityState, EntityManager};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 24.0, 0.0, 24.0, -12.0, 12.0);
    let mut em = EntityManager::new_3d(cell.clone(), 1.0);

    em.add_entity(Entity {
        id: 1, pos: (12.0, 12.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 2, pos: (20.0, 12.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 3, pos: (5.0, 20.0, -5.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let exits = em.force_exit_check();
    assert!(exits.is_empty(), "All entities are inside the full cell");

    em.set_cell(Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0));

    let exits = em.force_exit_check();
    assert_eq!(exits.len(), 1, "Entity 2 at x=20 should be outside [0,12]");
    assert_eq!(exits[0].0, 2);
}

#[tokio::test]
async fn test_force_exit_check_after_y_axis_split() {
    use zeus_node::entity_manager::{Entity, AuthorityState, EntityManager};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut em = EntityManager::new_3d(cell.clone(), 1.0);

    for i in 0..10 {
        em.add_entity(Entity {
            id: i + 1, pos: (12.0, i as f32 * 2.5, 0.0), vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let exits = em.force_exit_check();
    assert!(exits.is_empty(), "All entities inside original cell");

    em.set_cell(Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0));

    let exits = em.force_exit_check();
    let outside_count = exits.len();
    assert!(outside_count > 0, "Some entities should be above y=12");
    for (id, _) in &exits {
        let entity = em.get_entity(*id).unwrap();
        assert!(entity.pos.1 > 12.0 || entity.pos.1 < -1.0,
            "Exit entity {} at y={:.1} should be outside [-1,12]", id, entity.pos.1);
    }
}

#[tokio::test]
async fn test_force_exit_check_after_z_axis_split() {
    use zeus_node::entity_manager::{Entity, AuthorityState, EntityManager};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 24.0, 0.0, 24.0, -12.0, 12.0);
    let mut em = EntityManager::new_3d(cell.clone(), 1.0);

    em.add_entity(Entity {
        id: 1, pos: (12.0, 12.0, -8.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 2, pos: (12.0, 12.0, 8.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    em.set_cell(Cell::new(0.0, 24.0, 0.0, 24.0, -12.0, 0.0));

    let exits = em.force_exit_check();
    assert_eq!(exits.len(), 1, "Entity 2 at z=8 is outside [-12, 0]");
    assert_eq!(exits[0].0, 2);
}

#[tokio::test]
async fn test_force_exit_check_ignores_remote_entities() {
    use zeus_node::entity_manager::{Entity, AuthorityState, EntityManager};
    use zeus_node::cell::Cell;

    let cell = Cell::new(0.0, 24.0, 0.0, 24.0, -12.0, 12.0);
    let mut em = EntityManager::new_3d(cell.clone(), 1.0);

    em.add_entity(Entity {
        id: 1, pos: (30.0, 12.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Remote, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 2, pos: (30.0, 12.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let exits = em.force_exit_check();
    assert_eq!(exits.len(), 1, "Only local entities should be checked");
    assert_eq!(exits[0].0, 2);
}

#[tokio::test]
async fn test_cell_shrink_and_flush_consistency() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(zeus_node::cell::Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0)),
    };

    let mut world = TestWorld::new();
    world.local_ids.insert(1);
    world.states.insert(1, ((5.0, 5.0, 0.0), (0.0, 0.0, 0.0)));
    world.local_ids.insert(2);
    world.states.insert(2, ((5.0, 20.0, 0.0), (0.0, 0.0, 0.0)));
    world.local_ids.insert(3);
    world.states.insert(3, ((5.0, 22.0, 0.0), (0.0, 0.0, 0.0)));

    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    for id in [1u64, 2, 3] {
        let (pos, vel) = game_loop.world.states[&id];
        game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }

    game_loop.set_cell(zeus_node::cell::Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0));

    let exits = game_loop.engine.node.manager.force_exit_check();
    assert!(exits.len() >= 2, "Entities 2 and 3 (y=20,22) should be outside [-1,12]");

    let exit_ids: Vec<u64> = exits.iter().map(|(id, _)| *id).collect();
    assert!(exit_ids.contains(&2));
    assert!(exit_ids.contains(&3));
    assert!(!exit_ids.contains(&1));
}

#[tokio::test]
async fn test_split_flow_entity_conservation_two_nodes() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };

    let mut world_a = TestWorld::new();
    for id in 1..=20u64 {
        let y = (id as f32) * 2.5;
        world_a.spawn_local(id, (25.0, y, 25.0), (0.0, 0.0, 0.0));
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in 1..=20u64 {
        let (pos, vel) = node_a.world.states[&id];
        node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
    }

    let cell_a = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 50.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let world_b = TestWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node_a.set_cell(cell_a.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..80 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let a_local: Vec<u64> = node_a.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id).collect();
    let b_local: Vec<u64> = node_b.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id).collect();

    let total = a_local.len() + b_local.len();
    assert!(total >= 18, "Should conserve most entities across split. Got A={} B={} total={}", a_local.len(), b_local.len(), total);

    for id in &a_local {
        assert!(!b_local.contains(id), "Entity {} should not be Local on both nodes", id);
    }

    for id in &a_local {
        let e = node_a.engine.node.manager.get_entity(*id).unwrap();
        assert!(cell_a.contains(e.pos) || cell_a.contains_with_margin(e.pos, 1.0),
            "Entity {} at {:?} should be inside node A's cell", id, e.pos);
    }
    for id in &b_local {
        let e = node_b.engine.node.manager.get_entity(*id).unwrap();
        assert!(cell_b.contains(e.pos) || cell_b.contains_with_margin(e.pos, 1.0),
            "Entity {} at {:?} should be inside node B's cell", id, e.pos);
    }
}

#[tokio::test]
async fn test_split_no_entity_loss_with_velocity() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };

    let mut world_a = TestWorld::new();
    for id in 1..=10u64 {
        let y = 23.0 + (id as f32) * 0.4;
        let vy = 3.0;
        world_a.spawn_local(id, (25.0, y, 25.0), (0.0, vy, 0.0));
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in 1..=10u64 {
        let (pos, vel) = node_a.world.states[&id];
        node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
    }

    let cell_a = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 50.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let world_b = TestWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node_a.set_cell(cell_a.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..120 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let a_local: HashSet<u64> = node_a.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id).collect();
    let b_local: HashSet<u64> = node_b.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id).collect();

    let total = a_local.len() + b_local.len();
    assert_eq!(total, 10, "All 10 entities must be conserved. A={} B={}", a_local.len(), b_local.len());

    let overlap: Vec<u64> = a_local.intersection(&b_local).copied().collect();
    assert!(overlap.is_empty(), "No entity should be Local on both nodes: {:?}", overlap);
}

#[tokio::test]
async fn test_eviction_freezes_position_and_sets_handoff_out() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let keep_cell = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world = TestWorld::new();
    world.spawn_local(1, (50.0, 60.0, 50.0), (0.0, -200.0, 0.0));
    world.spawn_local(2, (50.0, 70.0, 50.0), (0.0, 30.0, 0.0));
    world.spawn_local(3, (50.0, 20.0, 50.0), (0.0, 0.0, 0.0));
    let mut gl = GameLoop::new(config, world).await.unwrap();

    for id in [1u64, 2, 3] {
        let (pos, vel) = gl.world.states[&id];
        gl.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    gl.set_cell(keep_cell.clone());
    gl.evict_out_of_cell_from_physics();

    let e1 = gl.engine.node.manager.get_entity(1).unwrap();
    assert_eq!(e1.state, AuthorityState::HandoffOut,
        "Entity 1 (y=60, outside keep_cell y_max=50) must be HandoffOut");
    assert!((e1.pos.1 - 60.0).abs() < 0.01,
        "Position must be frozen. y={}", e1.pos.1);
    assert!(!gl.world.local_ids.contains(&1),
        "Entity 1 must be removed from physics");

    let e2 = gl.engine.node.manager.get_entity(2).unwrap();
    assert_eq!(e2.state, AuthorityState::HandoffOut,
        "Entity 2 (y=70, outside keep_cell y_max=50) must be HandoffOut");

    let e3 = gl.engine.node.manager.get_entity(3).unwrap();
    assert_eq!(e3.state, AuthorityState::Local,
        "Entity 3 (y=20, inside keep_cell) must remain Local");
    assert!(gl.world.local_ids.contains(&3),
        "Entity 3 must remain in physics");

    gl.tick(0.016).await.unwrap();

    let e1_after = gl.engine.node.manager.get_entity(1).unwrap();
    assert_eq!(e1_after.state, AuthorityState::HandoffOut,
        "HandoffOut entity must NOT drift back to Local after tick");
    assert!((e1_after.pos.1 - 60.0).abs() < 0.01,
        "HandoffOut entity must NOT move. y={}", e1_after.pos.1);
}

#[tokio::test]
async fn test_split_entity_conservation_precise_tick_by_tick() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let keep_cell = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let new_cell = Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let total = 40u64;
    let mut world_a = TestWorld::new();
    let mut all_ids = Vec::new();
    for i in 0..total {
        let y = 5.0 + (i as f32) * 2.3;
        world_a.spawn_local(i + 1, (50.0, y, 50.0), (0.0, 0.0, 0.0));
        all_ids.push(i + 1);
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    for id in &all_ids {
        let (pos, vel) = node_a.world.states[id];
        node_a.engine.node.manager.add_entity(Entity {
            id: *id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let world_b = TestWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let pre_split_total: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(pre_split_total, total as usize,
        "Before split all entities should be Local on A");

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let after_evict_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let after_evict_ho: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::HandoffOut).count();
    assert_eq!(after_evict_local + after_evict_ho, total as usize,
        "Immediately after eviction: Local({}) + HandoffOut({}) must equal total({})",
        after_evict_local, after_evict_ho, total);

    let mut tick_violations = Vec::new();
    let mut max_in_flight = 0usize;

    for tick in 0..300 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let a_local: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let a_ho: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let b_local: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let b_hi: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();

        let in_flight = a_ho + b_hi;
        if in_flight > max_in_flight { max_in_flight = in_flight; }

        let accounted = a_local + a_ho + b_local + b_hi;
        if accounted < (total as usize) {
            tick_violations.push((tick, a_local, a_ho, b_local, b_hi, accounted));
        }

        let a_physics = node_a.world.local_ids.len();
        if (a_physics as i64 - a_local as i64).unsigned_abs() > 2 {
            tick_violations.push((tick, a_physics, 0, a_local, 0, 9999));
        }

        if in_flight == 0 && b_local > 0 && tick > 30 { break; }
    }

    assert!(tick_violations.is_empty(),
        "Entity conservation violated at {} ticks. First violations: {:?}",
        tick_violations.len(), &tick_violations[..tick_violations.len().min(5)]);

    let final_a: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let final_b: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(final_a + final_b, total as usize,
        "After split: A={} B={} sum={} (expected {})", final_a, final_b, final_a + final_b, total);
}

#[tokio::test]
async fn test_double_split_entity_conservation() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let total = 60u64;
    let mut world_a = TestWorld::new();
    for i in 0..total {
        let y = 5.0 + (i as f32) * 1.5;
        world_a.spawn_local(i + 1, (50.0, y, 50.0), (0.0, 0.0, 0.0));
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    for i in 0..total {
        let id = i + 1;
        let (pos, vel) = node_a.world.states[&id];
        node_a.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let cell_b = Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0);
    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let mut node_b = GameLoop::new(config_b, TestWorld::new()).await.unwrap();
    let addr_b = node_b.engine.endpoint.local_addr().unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let keep_a = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    node_a.set_cell(keep_a.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..200 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let pending_a: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let pending_b: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();
        if pending_a == 0 && pending_b == 0 { break; }
    }

    let s1_a: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let s1_b: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let s1_total = s1_a + s1_b;
    assert!(s1_total >= (total as usize - 2),
        "After split 1: A={} B={} total={} (expected ~{})", s1_a, s1_b, s1_total, total);

    let cell_c = Cell::new(0.0, 100.0, 50.0, 75.0, 0.0, 100.0);
    let cell_b2 = Cell::new(0.0, 100.0, 75.0, 100.0, 0.0, 100.0);

    let config_c = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a, addr_b],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 2,
        lower_boundary: 0.0,
        cell: Some(cell_c.clone()),
    };
    let mut node_c = GameLoop::new(config_c, TestWorld::new()).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_b.set_cell(cell_b2.clone());
    node_b.evict_out_of_cell_from_physics();

    for _ in 0..200 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let pa: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let pb: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let pc: usize = node_c.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();
        if pa + pb + pc == 0 { break; }
    }

    let f_a: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let f_b: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let f_c: usize = node_c.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let grand = f_a + f_b + f_c;
    assert!(grand >= (total as usize - 3),
        "After 2 splits across 3 nodes: A={} B={} C={} total={} (expected ~{})",
        f_a, f_b, f_c, grand, total);

    let a_ids: HashSet<u64> = node_a.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local).map(|(id, _)| *id).collect();
    let b_ids: HashSet<u64> = node_b.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local).map(|(id, _)| *id).collect();
    let c_ids: HashSet<u64> = node_c.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local).map(|(id, _)| *id).collect();
    let ab_overlap: Vec<u64> = a_ids.intersection(&b_ids).copied().collect();
    let bc_overlap: Vec<u64> = b_ids.intersection(&c_ids).copied().collect();
    let ac_overlap: Vec<u64> = a_ids.intersection(&c_ids).copied().collect();
    assert!(ab_overlap.is_empty(), "A/B dual ownership: {:?}", ab_overlap);
    assert!(bc_overlap.is_empty(), "B/C dual ownership: {:?}", bc_overlap);
    assert!(ac_overlap.is_empty(), "A/C dual ownership: {:?}", ac_overlap);
}

#[tokio::test]
async fn test_physics_ids_match_local_ids_throughout_split() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let keep_cell = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let new_cell = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world_a = TestWorld::new();
    for i in 0..20u64 {
        let y = 3.0 + (i as f32) * 2.3;
        world_a.spawn_local(i + 1, (25.0, y, 25.0), (0.0, 0.0, 0.0));
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    for i in 0..20u64 {
        let id = i + 1;
        let (pos, vel) = node_a.world.states[&id];
        node_a.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 50.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let mut node_b = GameLoop::new(config_b, TestWorld::new()).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut mismatches = Vec::new();

    for tick in 0..300 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let a_physics = node_a.world.local_ids.len();
        let a_local: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let b_physics = node_b.world.local_ids.len();
        let b_local: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();

        if a_physics != a_local {
            mismatches.push(format!("tick={} A: physics={} local={}", tick, a_physics, a_local));
        }
        if b_physics != b_local {
            mismatches.push(format!("tick={} B: physics={} local={}", tick, b_physics, b_local));
        }

        let pending: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count()
            + node_b.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::HandoffIn).count();
        if pending == 0 && b_local > 0 && tick > 30 { break; }
    }

    assert!(mismatches.len() <= 10,
        "Physics/local count mismatches: {} occurrences. Samples: {:?}",
        mismatches.len(), &mismatches[..mismatches.len().min(5)]);
}
