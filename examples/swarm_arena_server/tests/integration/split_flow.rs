use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
use zeus_node::cell::{Cell, Face};
use zeus_node::engine::ZeusConfig;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::{GameLoop, GameWorld};

#[tokio::test]
async fn test_multinode_entity_conservation() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 12.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell0),
    };
    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 12.0,
        cell: Some(cell1),
    };

    let mut world0 = TestDroneWorld::new();
    let mut world1 = TestDroneWorld::new();

    for i in 0..10 {
        world0.spawn_drone((2.0 + i as f32, 5.0, 0.0), (0.0, 0.0, 0.0));
    }
    for i in 0..10 {
        world1.spawn_drone((14.0 + i as f32, 5.0, 0.0), (0.0, 0.0, 0.0));
    }

    let mut node0 = GameLoop::new(config0, world0).await.unwrap();
    let mut node1 = GameLoop::new(config1, world1).await.unwrap();

    let total_before = node0.world.local_ids.len() + node1.world.local_ids.len();

    for id in node0.world.local_ids.clone() {
        let (pos, vel) = node0.world.drones[&id];
        node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }
    for id in node1.world.local_ids.clone() {
        let (pos, vel) = node1.world.drones[&id];
        node1.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }

    for _ in 0..5 {
        node0.tick(1.0 / 128.0).await.unwrap();
        node1.tick(1.0 / 128.0).await.unwrap();
    }

    let total_after = node0.world.local_ids.len() + node1.world.local_ids.len();
    assert_eq!(total_before, total_after, "Entity count must be conserved across nodes");
}

#[tokio::test]
async fn test_full_split_merge_cycle() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 10,
        warmup_threshold: 10,
        merge_threshold: 3,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();

    let positions: Vec<(f32, f32, f32)> = (0..15).map(|i| (i as f32, 12.0, 0.0)).collect();
    let events = scaler.evaluate(&full_cell, 15, &peers, &peer_cells, 1, &positions);
    let split = events.iter().find(|e| matches!(e, ScaleEvent::WarmupRecommended { .. }));
    assert!(split.is_some(), "Phase 1: should recommend split");

    if let Some(ScaleEvent::WarmupRecommended { projected_cell: keep_cell, projected_new_cell: new_cell, .. }) = split {
        let mut peer_set = HashSet::new();
        peer_set.insert(42);
        let mut pc = HashMap::new();
        pc.insert(42, new_cell.clone());

        let events2 = scaler.evaluate(keep_cell, 2, &peer_set, &pc, 2, &[]);
        assert!(events2.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
            "Phase 2: few entities should trigger merge");

        let events3 = scaler.evaluate(keep_cell, 2, &HashSet::new(), &HashMap::new(), 1, &[]);
        let expanded = events3.iter().find(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
        assert!(expanded.is_some(), "Phase 3: peer death should expand cell");
        if let Some(ScaleEvent::CellExpanded { new_cell: expanded_cell }) = expanded {
            assert!((expanded_cell.volume() - full_cell.volume()).abs() / full_cell.volume() < 0.01,
                "Expanded cell should approximate original full cell");
        }
    }
}

#[tokio::test]
async fn test_split_cell_union_preserves_original() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let positions: Vec<(f32, f32, f32)> = (0..50).map(|i| {
        ((i as f32 % 20.0) + 2.0, 12.0, (i as f32 / 20.0) * 8.0 - 4.0)
    }).collect();

    let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);
    let union = keep.union(&new);
    assert!((union.x_min - cell.x_min).abs() < 1e-3);
    assert!((union.x_max - cell.x_max).abs() < 1e-3);
    assert!((union.y_min - cell.y_min).abs() < 1e-3);
    assert!((union.y_max - cell.y_max).abs() < 1e-3);
    assert!((union.z_min - cell.z_min).abs() < 1e-3);
    assert!((union.z_max - cell.z_max).abs() < 1e-3);
}

#[tokio::test]
async fn test_multiple_splits_produce_valid_mosaic() {
    let mut cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut all_cells = Vec::new();

    for round in 0..3 {
        let positions: Vec<(f32, f32, f32)> = (0..20).map(|i| {
            let c = cell.center();
            (c.0 + (i as f32 - 10.0) * 0.5, c.1 + (round as f32) * 2.0, c.2)
        }).collect();

        let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);
        all_cells.push(new);
        cell = keep;
    }
    all_cells.push(cell);

    let mut bounding = all_cells[0].clone();
    for c in &all_cells[1..] {
        bounding = bounding.union(c);
    }
    assert!((bounding.x_min - 0.0).abs() < 1e-3);
    assert!((bounding.x_max - 100.0).abs() < 1e-3);
    assert!((bounding.y_min - 0.0).abs() < 1e-3);
    assert!((bounding.y_max - 100.0).abs() < 1e-3);
    assert!((bounding.z_min - 0.0).abs() < 1e-3);
    assert!((bounding.z_max - 100.0).abs() < 1e-3);
}

#[tokio::test]
async fn test_spawn_only_in_containing_cell() {
    let cell_a = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell_b = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let spawn_pos = (5.0, 12.0, 0.0);
    assert!(cell_a.contains(spawn_pos));
    assert!(!cell_b.contains(spawn_pos));

    let spawn_pos2 = (18.0, 12.0, 0.0);
    assert!(!cell_a.contains(spawn_pos2));
    assert!(cell_b.contains(spawn_pos2));
}

#[tokio::test]
async fn test_max_nodes_cap_prevents_over_splitting() {
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 5,
        warmup_threshold: 5,
        merge_threshold: 1,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 1024,
        max_nodes: 4,
        startup_grace_ticks: 0,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();
    let positions: Vec<(f32, f32, f32)> = (0..50).map(|i| (i as f32 * 2.0, 50.0, 50.0)).collect();

    let events = scaler.evaluate(&cell, 50, &peers, &peer_cells, 4, &positions);
    assert!(!events.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })),
        "At max_nodes=4, should not split");

    let events2 = scaler.evaluate(&cell, 50, &peers, &peer_cells, 3, &positions);
    assert!(events2.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })),
        "Below max_nodes, should allow split");
}

#[tokio::test]
async fn test_force_exit_after_cell_shrink_with_gameloop() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world = TestDroneWorld::new();
    for i in 0..10 {
        world.spawn_drone((12.0, 3.0 + i as f32 * 2.0, 0.0), (0.0, 0.0, 0.0));
    }
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    for id in game_loop.world.local_ids.clone() {
        let (pos, vel) = game_loop.world.drones[&id];
        game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }

    let exits_before = game_loop.engine.node.manager.force_exit_check();
    assert!(exits_before.is_empty());

    let keep_cell = Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0);
    game_loop.set_cell(keep_cell.clone());

    let exits_after = game_loop.engine.node.manager.force_exit_check();
    let exit_ids: Vec<u64> = exits_after.iter().map(|(id, _)| *id).collect();
    let above_split = game_loop.world.drones.iter()
        .filter(|(_, (pos, _))| pos.1 > 12.0)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for id in &above_split {
        assert!(exit_ids.contains(id), "Entity {} at y>{:.1} should be exit candidate", id, 12.0);
    }
}

#[tokio::test]
async fn test_split_distributes_entities_to_correct_cells() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut positions = Vec::new();
    for i in 0..50 {
        let x = 2.0 + (i as f32 % 10.0) * 2.0;
        let y = -0.5 + (i as f32 / 10.0).floor() * 5.0;
        positions.push((x, y, 0.0));
    }

    let (keep, new, _, _) = AutoScaler::compute_binary_split(&full_cell, &positions);
    let keep_entities: Vec<&(f32, f32, f32)> = positions.iter().filter(|p| keep.contains(**p)).collect();
    let new_entities: Vec<&(f32, f32, f32)> = positions.iter().filter(|p| new.contains(**p)).collect();

    assert!(keep_entities.len() >= 20, "Keep cell should have significant entities");
    assert!(keep_entities.len() + new_entities.len() >= 50,
        "All entities should be in one or both cells");

    for p in &keep_entities {
        assert!(keep.contains(**p));
    }
    for p in &new_entities {
        assert!(new.contains(**p));
    }
}

#[tokio::test]
async fn test_handoff_candidate_face_direction() {
    let cell = Cell::new(0.0, 12.0, 0.0, 12.0, -6.0, 6.0);
    let mut em = zeus_node::entity_manager::EntityManager::new_3d(cell, 1.0);

    em.add_entity(Entity {
        id: 1, pos: (15.0, 6.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 2, pos: (6.0, 15.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 3, pos: (6.0, 6.0, -10.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let exits = em.force_exit_check();
    assert_eq!(exits.len(), 3);

    let e1 = exits.iter().find(|(id, _)| *id == 1).unwrap();
    assert!(matches!(e1.1, Face::XPos), "Entity 1 should exit XPos, got {:?}", e1.1);

    let e2 = exits.iter().find(|(id, _)| *id == 2).unwrap();
    assert!(matches!(e2.1, Face::YPos), "Entity 2 should exit YPos, got {:?}", e2.1);

    let e3 = exits.iter().find(|(id, _)| *id == 3).unwrap();
    assert!(matches!(e3.1, Face::ZNeg), "Entity 3 should exit ZNeg, got {:?}", e3.1);
}

#[tokio::test]
async fn test_full_split_flow_tick_by_tick_entity_conservation() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world_a = TestDroneWorld::new();
    let mut spawned_ids: Vec<u64> = Vec::new();
    for i in 0..40u64 {
        let y = -0.5 + (i as f32) * 0.65;
        let id = world_a.spawn_drone((12.0, y, 0.0), (0.0, 0.0, 0.0));
        spawned_ids.push(id);
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in &spawned_ids {
        let (pos, vel) = node_a.world.drones[id];
        node_a.engine.node.manager.add_entity(Entity {
            id: *id, pos, vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let positions: Vec<(f32, f32, f32)> = spawned_ids.iter().map(|id| node_a.world.drones[id].0).collect();
    let local_count = positions.len();
    assert_eq!(local_count, 40);

    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        merge_threshold: 5,
        warmup_threshold: 30,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 1024,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });

    let peer_ids = HashSet::new();
    let peer_cells = HashMap::new();
    let events = scaler.evaluate(&full_cell, local_count, &peer_ids, &peer_cells, 1, &positions);
    let warmup = events.iter().find(|e| matches!(e, ScaleEvent::WarmupRecommended { .. }));
    assert!(warmup.is_some(), "Should emit WarmupRecommended at 40 entities with warmup_threshold=30");

    let (keep_cell, new_cell) = match warmup.unwrap() {
        ScaleEvent::WarmupRecommended { projected_cell, projected_new_cell, .. } =>
            (projected_cell.clone(), projected_new_cell.clone()),
        _ => unreachable!(),
    };

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let a_local_before: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(a_local_before, 40, "All 40 entities should still be Local on A before cell shrink");

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let a_physics_after_evict = node_a.world.local_ids.len();
    let _a_local_after_evict: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let a_total = node_a.engine.node.manager.entities.len();

    let entities_in_keep: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local && keep_cell.contains(e.pos))
        .count();
    let _entities_outside_keep: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local && !keep_cell.contains(e.pos))
        .count();

    assert_eq!(a_total, 40, "No entities lost yet, all still in manager (some outside cell)");
    assert!(a_physics_after_evict <= entities_in_keep + 2,
        "Physics should only contain entities inside keep_cell. physics={} in_keep={}",
        a_physics_after_evict, entities_in_keep);

    let mut max_ticks = 200;
    loop {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let a_local: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let a_handoff_out: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let a_remote: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Remote).count();
        let b_local: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let b_handoff_in: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();

        let total_authoritative = a_local + a_handoff_out + b_local + b_handoff_in;
        assert!(total_authoritative >= 38,
            "Entity conservation violated: a_local={} a_ho={} a_remote={} b_local={} b_hi={} total={}",
            a_local, a_handoff_out, a_remote, b_local, b_handoff_in, total_authoritative);

        for (id, e) in &node_a.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(eb) = node_b.engine.node.manager.get_entity(*id) {
                    assert_ne!(eb.state, AuthorityState::Local,
                        "Entity {} is Local on BOTH nodes! A={:?} B={:?}", id, e.state, eb.state);
                }
            }
        }

        if a_handoff_out == 0 && b_handoff_in == 0 && b_local > 0 {
            let final_a_local: usize = node_a.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::Local).count();
            let final_b_local: usize = node_b.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::Local).count();
            assert_eq!(final_a_local + final_b_local, 40,
                "All 40 entities must be accounted for. A={} B={}", final_a_local, final_b_local);

            for (id, e) in &node_a.engine.node.manager.entities {
                if e.state == AuthorityState::Local {
                    assert!(keep_cell.contains_with_margin(e.pos, 1.0),
                        "A's local entity {} at {:?} outside keep_cell", id, e.pos);
                }
            }
            for (id, e) in &node_b.engine.node.manager.entities {
                if e.state == AuthorityState::Local {
                    assert!(new_cell.contains_with_margin(e.pos, 1.0),
                        "B's local entity {} at {:?} outside new_cell", id, e.pos);
                }
            }

            let a_physics = node_a.world.local_ids.len();
            let b_physics = node_b.world.local_ids.len();
            assert_eq!(a_physics, final_a_local,
                "A physics count should match local count. physics={} local={}", a_physics, final_a_local);
            assert_eq!(b_physics, final_b_local,
                "B physics count should match local count. physics={} local={}", b_physics, final_b_local);
            break;
        }

        max_ticks -= 1;
        if max_ticks == 0 {
            let a_ho: usize = node_a.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::HandoffOut).count();
            let b_hi: usize = node_b.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::HandoffIn).count();
            panic!("Handoff did not complete in 200 ticks. a_ho={} b_hi={} b_local={}",
                a_ho, b_hi, node_b.engine.node.manager.entities.values()
                    .filter(|e| e.state == AuthorityState::Local).count());
        }
    }
}

#[tokio::test]
async fn test_split_entity_arrives_via_entity_arrived_not_update() {
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

    struct TrackedWorld {
        inner: TestDroneWorld,
        arrived_ids: Vec<u64>,
        update_ids: Vec<u64>,
    }

    impl GameWorld for TrackedWorld {
        fn step(&mut self, dt: f32) { self.inner.step(dt); }
        fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
            self.arrived_ids.push(id);
            self.inner.on_entity_arrived(id, pos, vel);
        }
        fn on_entity_departed(&mut self, id: u64) { self.inner.on_entity_departed(id); }
        fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
            self.update_ids.push(id);
            self.inner.on_entity_update(id, pos, vel);
        }
        fn locally_simulated_ids(&self) -> &std::collections::HashSet<u64> { self.inner.locally_simulated_ids() }
        fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> { self.inner.get_entity_state(id) }
    }

    let mut world_a = TestDroneWorld::new();
    let id1 = world_a.spawn_drone((25.0, 24.0, 25.0), (0.0, 5.0, 0.0));
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    let (pos, vel) = node_a.world.drones[&id1];
    node_a.engine.node.manager.add_entity(Entity {
        id: id1, pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

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
    let world_b = TrackedWorld {
        inner: TestDroneWorld::new(),
        arrived_ids: Vec::new(),
        update_ids: Vec::new(),
    };
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(cell_a.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..120 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let b_has_entity = node_b.engine.node.manager.get_entity(id1)
        .is_some_and(|e| e.state == AuthorityState::Local);

    if b_has_entity {
        assert!(node_b.world.arrived_ids.contains(&id1),
            "Entity {} must arrive via on_entity_arrived (dynamic body). arrived={:?} update={:?}",
            id1, node_b.world.arrived_ids, node_b.world.update_ids);
        assert!(node_b.world.inner.local_ids.contains(&id1),
            "Entity {} must be in locally_simulated_ids after arriving", id1);
    }
}

#[tokio::test]
async fn test_split_flow_no_dual_local_at_any_tick() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let keep_cell = Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0);
    let new_cell = Cell::new(0.0, 24.0, 12.0, 25.0, -12.0, 12.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world_a = TestDroneWorld::new();
    let mut ids = Vec::new();
    for i in 0..10u64 {
        let y = 11.0 + (i as f32) * 0.5;
        let id = world_a.spawn_drone((12.0, y, 0.0), (0.0, 2.0, 0.0));
        ids.push(id);
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in &ids {
        let (pos, vel) = node_a.world.drones[id];
        node_a.engine.node.manager.add_entity(Entity {
            id: *id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut dual_local_violations = 0;
    for tick in 0..200 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        for id in &ids {
            let a_local = node_a.engine.node.manager.get_entity(*id)
                .is_some_and(|e| e.state == AuthorityState::Local);
            let b_local = node_b.engine.node.manager.get_entity(*id)
                .is_some_and(|e| e.state == AuthorityState::Local);
            if a_local && b_local {
                dual_local_violations += 1;
            }
        }

        let a_ho: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let b_hi: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();
        if a_ho == 0 && b_hi == 0 && tick > 10 {
            break;
        }
    }

    assert_eq!(dual_local_violations, 0,
        "No entity should be Local on both nodes at any tick during split. Violations: {}", dual_local_violations);

    let a_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let b_local: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(a_local + b_local, 10,
        "All 10 entities must be conserved. A={} B={}", a_local, b_local);
}

#[tokio::test]
async fn test_split_handoff_completes_full_3way_handshake() {
    let keep_cell = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let new_cell = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0)),
    };
    let mut world_a = TestDroneWorld::new();
    let id = world_a.spawn_drone((25.0, 26.0, 25.0), (0.0, 0.0, 0.0));
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let (pos, vel) = node_a.world.drones[&id];
    node_a.engine.node.manager.add_entity(Entity {
        id, pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 50.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut saw_handoff_out = false;
    let mut saw_handoff_in = false;
    let mut saw_remote = false;
    let mut saw_b_local = false;

    for _ in 0..150 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        if let Some(ea) = node_a.engine.node.manager.get_entity(id) {
            if ea.state == AuthorityState::HandoffOut { saw_handoff_out = true; }
            if ea.state == AuthorityState::Remote { saw_remote = true; }
        }
        if let Some(eb) = node_b.engine.node.manager.get_entity(id) {
            if eb.state == AuthorityState::HandoffIn { saw_handoff_in = true; }
            if eb.state == AuthorityState::Local { saw_b_local = true; }
        }

        if saw_b_local { break; }
    }

    assert!(saw_handoff_out, "Entity should transition through HandoffOut on parent");
    assert!(saw_handoff_in, "Entity should transition through HandoffIn on receiver");
    assert!(saw_remote, "Entity should become Remote on parent after Ack");
    assert!(saw_b_local, "Entity should become Local on receiver after Commit");

    let a_state = node_a.engine.node.manager.get_entity(id).map(|e| e.state.clone());
    let b_state = node_b.engine.node.manager.get_entity(id).map(|e| e.state.clone());
    assert_eq!(a_state, Some(AuthorityState::Remote), "Final A state should be Remote");
    assert_eq!(b_state, Some(AuthorityState::Local), "Final B state should be Local");
    assert!(node_b.world.local_ids.contains(&id), "Entity should be in B's physics");
}

#[tokio::test]
async fn test_split_position_continuity_no_jumps() {
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
    let mut world_a = TestDroneWorld::new();
    let id = world_a.spawn_drone((25.0, 24.5, 25.0), (0.0, 2.0, 0.0));
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let (pos, vel) = node_a.world.drones[&id];
    node_a.engine.node.manager.add_entity(Entity {
        id, pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 50.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut last_pos: Option<(f32, f32, f32)> = None;
    let mut max_jump: f32 = 0.0;

    for _ in 0..120 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let current_pos = node_a.engine.node.manager.get_entity(id)
            .filter(|e| e.state == AuthorityState::Local || e.state == AuthorityState::HandoffOut)
            .map(|e| e.pos)
            .or_else(|| node_b.engine.node.manager.get_entity(id)
                .filter(|e| e.state == AuthorityState::Local)
                .map(|e| e.pos));

        if let (Some(prev), Some(curr)) = (last_pos, current_pos) {
            let dx = (curr.0 - prev.0).abs();
            let dy = (curr.1 - prev.1).abs();
            let dz = (curr.2 - prev.2).abs();
            let jump = (dx * dx + dy * dy + dz * dz).sqrt();
            if jump > max_jump { max_jump = jump; }
        }
        if current_pos.is_some() { last_pos = current_pos; }
    }

    assert!(max_jump < 2.0,
        "Position jump during handoff should be < 2.0 units. Max observed: {:.4}", max_jump);
}

#[tokio::test]
async fn test_eviction_sets_handoff_out_prevents_drift() {
    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let keep_cell = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);

    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world = TestDroneWorld::new();
    let id_outside = world.spawn_drone((25.0, 30.0, 25.0), (0.0, -20.0, 0.0));
    let id_inside = world.spawn_drone((25.0, 10.0, 25.0), (0.0, 0.0, 0.0));
    let mut gl = GameLoop::new(config, world).await.unwrap();

    gl.engine.node.manager.add_entity(Entity {
        id: id_outside, pos: (25.0, 30.0, 25.0), vel: (0.0, -20.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    gl.engine.node.manager.add_entity(Entity {
        id: id_inside, pos: (25.0, 10.0, 25.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    gl.set_cell(keep_cell.clone());
    gl.evict_out_of_cell_from_physics();

    let eo = gl.engine.node.manager.get_entity(id_outside).unwrap();
    assert_eq!(eo.state, AuthorityState::HandoffOut,
        "Evicted entity must be HandoffOut to freeze position. Got: {:?}", eo.state);
    assert!((eo.pos.1 - 30.0).abs() < 0.01,
        "Position must be frozen at eviction point. Got y={}", eo.pos.1);

    let ei = gl.engine.node.manager.get_entity(id_inside).unwrap();
    assert_eq!(ei.state, AuthorityState::Local,
        "Inside entity should remain Local. Got: {:?}", ei.state);
    assert!(gl.world.local_ids.contains(&id_inside),
        "Inside entity should stay in physics");
    assert!(!gl.world.local_ids.contains(&id_outside),
        "Outside entity should be evicted from physics");
}

#[tokio::test]
async fn test_evicted_entity_does_not_drift_back_into_cell() {
    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let keep_cell = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);

    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world = TestDroneWorld::new();
    let id = world.spawn_drone((25.0, 25.5, 25.0), (0.0, -100.0, 0.0));
    let mut gl = GameLoop::new(config, world).await.unwrap();

    gl.engine.node.manager.add_entity(Entity {
        id, pos: (25.0, 25.5, 25.0), vel: (0.0, -100.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    gl.set_cell(keep_cell.clone());
    gl.evict_out_of_cell_from_physics();

    let e = gl.engine.node.manager.get_entity(id).unwrap();
    assert_eq!(e.state, AuthorityState::HandoffOut,
        "Must be HandoffOut after eviction");

    for _ in 0..20 {
        gl.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let e = gl.engine.node.manager.get_entity(id);
        if let Some(entity) = e {
            assert_ne!(entity.state, AuthorityState::Local,
                "Entity should NOT drift back to Local. State: {:?} pos.y={}", entity.state, entity.pos.1);
            assert!((entity.pos.1 - 25.5).abs() < 0.1,
                "HandoffOut entity should NOT move. pos.y={} (expected ~25.5)", entity.pos.1);
        }
    }
}

#[tokio::test]
async fn test_multi_split_total_conservation_every_tick() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world_a = TestDroneWorld::new();
    let total_entities = 50u64;
    let mut all_ids: Vec<u64> = Vec::new();
    for i in 0..total_entities {
        let y = -0.5 + (i as f32) * 0.52;
        let vy = if i % 2 == 0 { 1.0 } else { -1.0 };
        let id = world_a.spawn_drone((12.0, y, 0.0), (0.0, vy, 0.0));
        all_ids.push(id);
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in &all_ids {
        let (pos, vel) = node_a.world.drones[id];
        node_a.engine.node.manager.add_entity(Entity {
            id: *id, pos, vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let cell_a = Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0);
    let cell_b = Cell::new(0.0, 24.0, 12.0, 25.0, -12.0, 12.0);

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();
    let addr_b = node_b.engine.endpoint.local_addr().unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(cell_a.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut conservation_violations = 0;
    let mut min_total = total_entities as usize;

    for tick in 0..200 {
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

        let authoritative = a_local + a_ho + b_local + b_hi;
        if authoritative < min_total { min_total = authoritative; }
        if authoritative < (total_entities as usize - 2) {
            conservation_violations += 1;
        }

        if a_ho == 0 && b_hi == 0 && b_local > 0 && tick > 20 {
            break;
        }
    }

    assert_eq!(conservation_violations, 0,
        "Entity conservation violated during first split. Min total seen: {}/{}", min_total, total_entities);

    let a_local_after_s1: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let b_local_after_s1: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let total_after_s1 = a_local_after_s1 + b_local_after_s1;
    assert!(total_after_s1 >= (total_entities as usize - 2),
        "After split 1: A={} B={} total={} (expected ~{})", a_local_after_s1, b_local_after_s1, total_after_s1, total_entities);

    let cell_b_keep = Cell::new(0.0, 12.0, 12.0, 25.0, -12.0, 12.0);
    let cell_c = Cell::new(12.0, 24.0, 12.0, 25.0, -12.0, 12.0);

    let config_c = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a, addr_b],
        boundary: 24.0,
        margin: 1.0,
        ordinal: 2,
        lower_boundary: 0.0,
        cell: Some(cell_c.clone()),
    };
    let world_c = TestDroneWorld::new();
    let mut node_c = GameLoop::new(config_c, world_c).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_b.set_cell(cell_b_keep.clone());
    node_b.evict_out_of_cell_from_physics();

    let mut s2_violations = 0;
    let mut s2_min = total_after_s1;
    for tick in 0..200 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let al: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let ah: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let bl: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let bh: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let cl: usize = node_c.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let ch: usize = node_c.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();

        let total = al + ah + bl + bh + cl + ch;
        if total < s2_min { s2_min = total; }
        if total < (total_after_s1 - 2) { s2_violations += 1; }

        let pending = ah + bh + ch +
            node_b.engine.node.manager.entities.values().filter(|e| e.state == AuthorityState::HandoffIn).count() +
            node_a.engine.node.manager.entities.values().filter(|e| e.state == AuthorityState::HandoffIn).count();
        if pending == 0 && cl > 0 && tick > 20 { break; }
    }

    assert_eq!(s2_violations, 0,
        "Entity conservation violated during second split. Min total: {}/{}", s2_min, total_after_s1);

    let final_a: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let final_b: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let final_c: usize = node_c.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let grand_total = final_a + final_b + final_c;
    assert!(grand_total >= (total_entities as usize - 3),
        "After 2 splits across 3 nodes: A={} B={} C={} total={} (expected ~{})",
        final_a, final_b, final_c, grand_total, total_entities);
}

#[tokio::test]
async fn test_eviction_with_inward_velocity_no_ghost_entity() {
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
    let mut world_a = TestDroneWorld::new();
    let id_out_inward = world_a.spawn_drone((25.0, 25.2, 25.0), (0.0, -50.0, 0.0));
    let id_out_outward = world_a.spawn_drone((25.0, 26.0, 25.0), (0.0, 10.0, 0.0));
    let id_inside = world_a.spawn_drone((25.0, 10.0, 25.0), (0.0, 0.0, 0.0));
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for &(id, pos, vel) in &[
        (id_out_inward, (25.0, 25.2, 25.0), (0.0, -50.0, 0.0)),
        (id_out_outward, (25.0, 26.0, 25.0), (0.0, 10.0, 0.0)),
        (id_inside, (25.0, 10.0, 25.0), (0.0, 0.0, 0.0)),
    ] {
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
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let e_inward = node_a.engine.node.manager.get_entity(id_out_inward).unwrap();
    assert_eq!(e_inward.state, AuthorityState::HandoffOut,
        "Entity with inward velocity must be HandoffOut after eviction, not Local. State: {:?}", e_inward.state);

    for tick in 0..150 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let a_physics = node_a.world.local_ids.len();
        let a_local: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();

        assert!(a_physics <= a_local + 2,
            "Tick {}: physics ({}) should match local ({}). Ghost entities detected!", tick, a_physics, a_local);
    }

    let final_a_local: usize = node_a.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let final_b_local: usize = node_b.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    let total = final_a_local + final_b_local;
    assert_eq!(total, 3, "All 3 entities must be conserved. A={} B={}", final_a_local, final_b_local);

    assert!(node_a.engine.node.manager.get_entity(id_inside)
        .is_some_and(|e| e.state == AuthorityState::Local),
        "Inside entity should remain Local on A");
}

#[tokio::test]
async fn test_physics_count_equals_local_count_throughout_split() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let keep_cell = Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0);
    let new_cell = Cell::new(0.0, 24.0, 12.0, 25.0, -12.0, 12.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world_a = TestDroneWorld::new();
    let mut ids = Vec::new();
    for i in 0..30u64 {
        let y = -0.5 + (i as f32) * 0.87;
        let vy = ((i as f32 * 7.3).sin()) * 5.0;
        let id = world_a.spawn_drone((12.0, y, 0.0), (0.0, vy, 0.0));
        ids.push(id);
    }
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in &ids {
        let (pos, vel) = node_a.world.drones[id];
        node_a.engine.node.manager.add_entity(Entity {
            id: *id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let world_b = TestDroneWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut physics_mismatch_count = 0;
    for tick in 0..200 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let a_physics = node_a.world.local_ids.len();
        let a_local: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let b_physics = node_b.world.local_ids.len();
        let b_local: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();

        if a_physics != a_local || b_physics != b_local {
            physics_mismatch_count += 1;
        }

        let a_ho: usize = node_a.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffOut).count();
        let b_hi: usize = node_b.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::HandoffIn).count();
        if a_ho == 0 && b_hi == 0 && b_local > 0 && tick > 20 { break; }
    }

    assert!(physics_mismatch_count <= 5,
        "Physics count should match local count at most ticks. Mismatches: {}/200", physics_mismatch_count);
}
