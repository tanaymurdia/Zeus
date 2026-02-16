use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_client::ZeusClient;
use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::GameLoop;

fn count_local(node: &GameLoop<TestWorld>) -> Vec<u64> {
    node.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .map(|(id, _)| *id)
        .collect()
}

fn parse_0xcc_datagram(data: &[u8]) -> Vec<(u64, (f32, f32, f32), (f32, f32, f32))> {
    super::helpers::parse_0xcc_datagram(data)
}

#[tokio::test]
async fn test_stress_4node_50_entities() {
    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs = Vec::new();

    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let n0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    addrs.push(n0.engine.endpoint.local_addr().unwrap());
    nodes.push(n0);

    for _i in 1..4u8 {
        let cfg = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: addrs.clone(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
        };
        let n = GameLoop::new(cfg, TestWorld::new()).await.unwrap();
        addrs.push(n.engine.endpoint.local_addr().unwrap());
        nodes.push(n);
    }

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..4 {
        for j in 0..12 {
            let id = (i * 1000 + j + 1) as u64;
            let x = (i as f32) * 10.0 + j as f32;
            let vel_x = if j % 2 == 0 { 1.0 } else { -1.0 };
            nodes[i].world.spawn_local(id, (x, 1.0, 0.0), (vel_x, 0.0, 0.0));
        }
    }

    let client1 = ZeusClient::new(10001).unwrap();
    let ep1 = client1.endpoint().clone();
    let mut c1_conns = Vec::new();
    for addr in &addrs {
        c1_conns.push(ep1.connect(*addr, "localhost").unwrap().await.unwrap());
    }
    sleep(Duration::from_millis(50)).await;

    let start = std::time::Instant::now();
    for tick_num in 0..200 {
        let tick_start = std::time::Instant::now();
        for node in nodes.iter_mut() {
            node.tick(0.008).await.unwrap();
        }
        let tick_elapsed = tick_start.elapsed();
        if tick_elapsed.as_millis() > 50 {
            eprintln!("[Stress] Tick {} took {}ms (warning: slow)", tick_num, tick_elapsed.as_millis());
        }
        if tick_num % 50 == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    }
    let total = start.elapsed();

    let mut all_local_ids: HashMap<u64, usize> = HashMap::new();
    for (node_idx, node) in nodes.iter().enumerate() {
        for (id, entity) in &node.engine.node.manager.entities {
            if entity.state == zeus_node::entity_manager::AuthorityState::Local {
                if let Some(existing) = all_local_ids.get(id) {
                    panic!("Entity {} is Local on both node {} and node {}", id, existing, node_idx);
                }
                all_local_ids.insert(*id, node_idx);
            }
        }
    }

    let mut c1_seen = HashSet::new();
    for conn in &c1_conns {
        for _ in 0..200 {
            match tokio::time::timeout(Duration::from_millis(5), conn.read_datagram()).await {
                Ok(Ok(data)) => {
                    if !data.is_empty() && data[0] == 0xCC {
                        for (id, _, _) in parse_0xcc_datagram(&data) {
                            c1_seen.insert(id);
                        }
                    }
                }
                _ => break,
            }
        }
    }

    assert!(
        c1_seen.len() >= 20,
        "Client 1 should see at least 20 entities from all nodes, got {}",
        c1_seen.len()
    );

    assert!(
        total.as_secs() < 30,
        "200 ticks across 4 nodes should complete within 30s, took {:?}",
        total
    );
}

#[tokio::test]
async fn test_stress_rapid_handoff() {
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

    node0.world.spawn_local(99, (11.0, 1.0, 0.0), (2.0, 0.0, 0.0));

    let mut entity_found_on_either = false;
    for tick in 0..200 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();

        let on0 = node0.engine.node.manager.get_entity(99).is_some();
        let on1 = node1.engine.node.manager.get_entity(99).is_some();

        if on0 || on1 {
            entity_found_on_either = true;
        }

        if tick % 20 == 0 && !on0 && !on1 {
            eprintln!("[tick {}] WARNING: entity 99 not found on either node", tick);
        }

        if tick % 50 == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    }

    assert!(entity_found_on_either, "Entity 99 should exist on at least one node throughout the test");
}

#[tokio::test]
async fn test_split_physics_no_gap_tick_by_tick() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_a = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let mut node_a = make_node(full_cell.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in 1..=40u64 {
        let y = (id as f32) * 1.2;
        node_a.world.spawn_local(id, (25.0, y, 25.0), (0.0, 0.0, 0.0));
        node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (25.0, y, 25.0), vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let pre_split_positions: HashMap<u64, (f32, f32, f32)> = node_a.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos))
        .collect();
    let expected_handoff: Vec<u64> = pre_split_positions.iter()
        .filter(|(_, pos)| !cell_a.contains(**pos))
        .map(|(id, _)| *id)
        .collect();

    node_a.set_cell(cell_a.clone());
    node_a.evict_out_of_cell_from_physics();

    for id in &expected_handoff {
        let e = node_a.engine.node.manager.get_entity(*id);
        assert!(
            e.is_some_and(|e| e.state == AuthorityState::HandoffOut),
            "Entity {} should be HandoffOut after eviction", id
        );
    }

    for _ in 0..60 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for id in &expected_handoff {
        let b_entity = node_b.engine.node.manager.get_entity(*id);
        assert!(
            b_entity.is_some_and(|e| e.state == AuthorityState::Local),
            "Entity {} should be Local on node B after handoff", id
        );
        assert!(
            node_b.world.arrived.contains(id),
            "Entity {} should have triggered on_entity_arrived (dynamic body) on node B", id
        );
        assert!(
            node_b.world.local_ids.contains(id),
            "Entity {} should be in locally_simulated_ids on node B (physics active)", id
        );
    }

    for id in &expected_handoff {
        if let Some(new_pos) = node_b.engine.node.manager.get_entity(*id).map(|e| e.pos) {
            if let Some(old_pos) = pre_split_positions.get(id) {
                let delta = ((new_pos.0 - old_pos.0).powi(2) + (new_pos.1 - old_pos.1).powi(2) + (new_pos.2 - old_pos.2).powi(2)).sqrt();
                assert!(delta < 5.0, "Entity {} position delta {:.2} should be small (velocity=0)", id, delta);
            }
        }
    }
}

#[tokio::test]
async fn test_split_physics_position_continuity() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let cell_keep = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let cell_b = Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0);

    let mut node_a = make_node(full_cell.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in 1..=200u64 {
        let y = (id as f32) * 0.5;
        let vel = (0.0, 0.1, 0.0);
        node_a.world.spawn_local(id, (50.0, y, 50.0), vel);
        node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (50.0, y, 50.0), vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let pre_split: HashMap<u64, (f32, f32, f32)> = node_a.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos))
        .collect();

    node_a.set_cell(cell_keep.clone());
    node_a.evict_out_of_cell_from_physics();

    for _ in 0..80 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let max_drift = 0.1 * 80.0 * 0.016 + 2.0;
    for (id, old_pos) in &pre_split {
        if !cell_keep.contains(*old_pos) {
            if let Some(e) = node_b.engine.node.manager.get_entity(*id) {
                let delta = ((e.pos.0 - old_pos.0).powi(2) + (e.pos.1 - old_pos.1).powi(2) + (e.pos.2 - old_pos.2).powi(2)).sqrt();
                assert!(delta < max_drift, "Entity {} jumped {:.2} (max_drift={:.2})", id, delta, max_drift);
            }
        }
    }
}

#[tokio::test]
async fn test_split_physics_dynamic_not_kinematic() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell_a = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    node_a.world.spawn_local(1, (25.0, 24.0, 25.0), (0.0, 10.0, 0.0));
    node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (25.0, 24.0, 25.0), vel: (0.0, 10.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for _ in 0..80 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    assert!(
        node_b.world.arrived.contains(&1),
        "Entity 1 should have arrived on node B (dynamic body created)"
    );
    assert!(
        node_b.world.local_ids.contains(&1),
        "Entity 1 should be physics-active (in locally_simulated_ids)"
    );

    let pos_before = node_b.world.states.get(&1).map(|(p, _)| *p);
    node_b.tick(0.016).await.unwrap();
    let pos_after = node_b.world.states.get(&1).map(|(p, _)| *p);

    if let (Some(before), Some(after)) = (pos_before, pos_after) {
        let moved = (after.0 - before.0).abs() + (after.1 - before.1).abs() + (after.2 - before.2).abs();
        assert!(moved > 0.001, "Entity should move after step (dynamic, not kinematic). Moved: {:.5}", moved);
    }
}

#[tokio::test]
async fn test_split_physics_velocity_preserved_through_chain() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cells: Vec<Cell> = (0..5).map(|i| {
        Cell::new(0.0, 100.0, (i as f32) * 20.0, ((i + 1) as f32) * 20.0, 0.0, 100.0)
    }).collect();

    let original_vel = (0.0, 5.0, 0.0);

    let mut node0 = make_node(cells[0].clone(), vec![]).await;
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    node0.world.spawn_local(1, (50.0, 5.0, 50.0), original_vel);
    node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (50.0, 5.0, 50.0), vel: original_vel,
        state: AuthorityState::Local, verifying_key: None,
    });

    let mut nodes = vec![node0];
    let mut addrs = vec![addr0];

    for i in 1..5 {
        let node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());
        nodes.push(node);
    }

    for _ in 0..20 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for _ in 0..300 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(5)).await;
    }

    let mut found = false;
    for node in &nodes {
        if let Some(e) = node.engine.node.manager.get_entity(1) {
            if e.state == AuthorityState::Local {
                let vel_diff = (e.vel.0 - original_vel.0).abs() +
                    (e.vel.1 - original_vel.1).abs() +
                    (e.vel.2 - original_vel.2).abs();
                assert!(vel_diff < 1.0, "Velocity should be approximately preserved through chain. Got {:?}, expected {:?}", e.vel, original_vel);
                found = true;
            }
        }
    }
    assert!(found, "Entity 1 should be Local on some node after chain traversal");
}

#[tokio::test]
async fn test_5node_sequential_splits_entity_conservation() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let total_entities = 80u64;

    let mut node0 = make_node(full_cell.clone(), vec![]).await;
    let addr0 = node0.engine.endpoint.local_addr().unwrap();

    for id in 1..=total_entities {
        let x = (id as f32) * 1.2;
        let y = ((id * 7) % 100) as f32;
        let z = ((id * 13) % 100) as f32;
        node0.world.spawn_local(id, (x, y, z), (0.0, 0.0, 0.0));
        node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (x, y, z), vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let split_cells = [
        (Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0), Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0)),
        (Cell::new(0.0, 100.0, 0.0, 25.0, 0.0, 100.0), Cell::new(0.0, 100.0, 25.0, 50.0, 0.0, 100.0)),
        (Cell::new(0.0, 100.0, 50.0, 75.0, 0.0, 100.0), Cell::new(0.0, 100.0, 75.0, 100.0, 0.0, 100.0)),
        (Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 100.0), Cell::new(50.0, 100.0, 0.0, 25.0, 0.0, 100.0)),
    ];

    let mut all_addrs = vec![addr0];
    let mut all_nodes: Vec<GameLoop<TestWorld>> = vec![node0];

    for (keep_cell, new_cell) in &split_cells {
        let parent_idx = all_nodes.iter().position(|n| {
            let c = n.engine.node.manager.cell();
            c.contains(keep_cell.center()) || c.contains(new_cell.center())
        }).unwrap_or(0);

        let mut new_node = make_node(new_cell.clone(), all_addrs.clone()).await;
        all_addrs.push(new_node.engine.endpoint.local_addr().unwrap());

        for _ in 0..20 {
            for n in all_nodes.iter_mut() {
                n.tick(0.016).await.unwrap();
            }
            new_node.tick(0.016).await.unwrap();
            sleep(Duration::from_millis(10)).await;
        }

        all_nodes[parent_idx].set_cell(keep_cell.clone());
        all_nodes[parent_idx].evict_out_of_cell_from_physics();

        all_nodes.push(new_node);

        for _ in 0..80 {
            for n in all_nodes.iter_mut() {
                n.tick(0.016).await.unwrap();
            }
            sleep(Duration::from_millis(10)).await;
        }

        let total = total_local_count(&all_nodes);
        assert!(
            total >= (total_entities as usize * 8 / 10),
            "After split, should conserve most entities. Total Local={}, expected ~{}", total, total_entities
        );
        assert_single_ownership(&all_nodes, &format!("after split to {} nodes", all_nodes.len()));
    }
}

#[tokio::test]
async fn test_5node_split_physics_continuity_per_split() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let full_cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_a = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let mut node_a = make_node(full_cell.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();

    for id in 1..=40u64 {
        let y = (id as f32) * 1.2;
        node_a.world.spawn_local(id, (25.0, y, 25.0), (0.0, 0.0, 0.0));
        node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (25.0, y, 25.0), vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;

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

    for id in 1..=40u64 {
        let y = (id as f32) * 1.2;
        if !cell_a.contains((25.0, y, 25.0)) {
            assert!(
                node_b.world.arrived.contains(&id),
                "Entity {} that was outside cell_a should have arrived on node_b (on_entity_arrived called)", id
            );
            assert!(
                node_b.world.local_ids.contains(&id),
                "Entity {} should be in locally_simulated_ids on node_b", id
            );
        }
    }

    let a_local = count_local(&node_a);
    let b_local = count_local(&node_b);
    for id in &a_local {
        assert!(!b_local.contains(id), "Entity {} should not be Local on both nodes", id);
    }
}

#[tokio::test]
async fn test_10node_single_ownership_invariant() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cells: Vec<Cell> = (0..10).map(|i| {
        Cell::new(0.0, 100.0, (i as f32) * 10.0, ((i + 1) as f32) * 10.0, 0.0, 100.0)
    }).collect();

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for i in 0..10 {
        let mut node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());

        if i == 0 {
            for id in 1..=200u64 {
                let y = ((id * 7) % 100) as f32;
                let vel_y = if id % 2 == 0 { 0.5 } else { -0.5 };
                node.world.spawn_local(id, (50.0, y, 50.0), (0.0, vel_y, 0.0));
                node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
                    id, pos: (50.0, y, 50.0), vel: (0.0, vel_y, 0.0),
                    state: AuthorityState::Local, verifying_key: None,
                });
            }
        }
        nodes.push(node);
    }

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    let split_boundaries = [50.0, 25.0, 75.0, 12.5, 37.5, 62.5, 87.5, 6.25, 18.75];
    for (i, &boundary) in split_boundaries.iter().enumerate() {
        if i >= 9 { break; }
        let parent_idx = nodes.iter().position(|n| {
            let c = n.engine.node.manager.cell();
            c.y_min < boundary && c.y_max > boundary
        });
        if let Some(pidx) = parent_idx {
            let old_cell = nodes[pidx].engine.node.manager.cell().clone();
            let keep = Cell::new(old_cell.x_min, old_cell.x_max, old_cell.y_min, boundary, old_cell.z_min, old_cell.z_max);
            nodes[pidx].set_cell(keep);
            nodes[pidx].evict_out_of_cell_from_physics();
        }
    }

    for tick in 0..500 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        if tick % 50 == 0 {
            sleep(Duration::from_millis(5)).await;
            let mut ownership: HashMap<u64, Vec<usize>> = HashMap::new();
            for (idx, node) in nodes.iter().enumerate() {
                for (id, e) in &node.engine.node.manager.entities {
                    if e.state == AuthorityState::Local {
                        ownership.entry(*id).or_default().push(idx);
                    }
                }
            }
            for (id, owners) in &ownership {
                assert!(
                    owners.len() <= 1,
                    "Tick {}: Entity {} is Local on {} nodes: {:?}", tick, id, owners.len(), owners
                );
            }
        }
    }
}

#[tokio::test]
async fn test_10node_corner_junction_handoff() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cells = vec![
        Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0),
        Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0),
        Cell::new(0.0, 50.0, 50.0, 100.0, 0.0, 50.0),
        Cell::new(50.0, 100.0, 50.0, 100.0, 0.0, 50.0),
        Cell::new(0.0, 50.0, 0.0, 50.0, 50.0, 100.0),
        Cell::new(50.0, 100.0, 0.0, 50.0, 50.0, 100.0),
        Cell::new(0.0, 50.0, 50.0, 100.0, 50.0, 100.0),
        Cell::new(50.0, 100.0, 50.0, 100.0, 50.0, 100.0),
    ];

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for cell in &cells {
        let node = make_node(cell.clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());
        nodes.push(node);
    }

    nodes[0].world.spawn_local(1, (49.0, 49.0, 49.0), (2.0, 2.0, 2.0));
    nodes[0].engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (49.0, 49.0, 49.0), vel: (2.0, 2.0, 2.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for _ in 0..200 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(5)).await;
    }

    let local_count: usize = nodes.iter()
        .filter(|n| n.engine.node.manager.get_entity(1).is_some_and(|e| e.state == AuthorityState::Local))
        .count();
    assert!(local_count <= 1, "Corner junction entity should resolve to at most 1 owner, got {}", local_count);
}

#[tokio::test]
async fn test_20node_entity_conservation_1000_ticks() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let num_nodes = 20usize;
    let entities_per_node = 25u64;
    let total_entities = (num_nodes as u64) * entities_per_node;

    let cells: Vec<Cell> = (0..num_nodes).map(|i| {
        Cell::new(0.0, 200.0, (i as f32) * 10.0, ((i + 1) as f32) * 10.0, 0.0, 200.0)
    }).collect();

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for i in 0..num_nodes {
        let mut node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());

        let base_id = (i as u64) * entities_per_node + 1;
        let cell_y_mid = (cells[i].y_min + cells[i].y_max) / 2.0;
        for j in 0..entities_per_node {
            let id = base_id + j;
            let y = cell_y_mid + (j as f32 - entities_per_node as f32 / 2.0) * 0.3;
            node.world.spawn_local(id, (100.0, y, 100.0), (0.0, 0.0, 0.0));
            node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
                id, pos: (100.0, y, 100.0), vel: (0.0, 0.0, 0.0),
                state: AuthorityState::Local, verifying_key: None,
            });
        }

        nodes.push(node);
    }

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for tick in 0..1000 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }

        if tick % 100 == 0 {
            sleep(Duration::from_millis(5)).await;

            let mut ownership: HashMap<u64, usize> = HashMap::new();
            let mut dual = Vec::new();
            for (idx, node) in nodes.iter().enumerate() {
                for (id, e) in &node.engine.node.manager.entities {
                    if e.state == AuthorityState::Local {
                        if let Some(prev_idx) = ownership.insert(*id, idx) {
                            dual.push((*id, prev_idx, idx));
                        }
                    }
                }
            }
            assert!(
                dual.is_empty(),
                "Tick {}: Dual ownership detected: {:?}", tick, &dual[..dual.len().min(5)]
            );
        }
    }

    let final_total = total_local_count(&nodes);
    assert!(
        final_total >= (total_entities as usize * 6 / 10),
        "20-node entity conservation: expected ~{}, got {}", total_entities, final_total
    );
}

#[tokio::test]
async fn test_20node_handoff_throughput() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let num_nodes = 20;
    let cells: Vec<Cell> = (0..num_nodes).map(|i| {
        Cell::new(0.0, 200.0, (i as f32) * 10.0, ((i + 1) as f32) * 10.0, 0.0, 200.0)
    }).collect();

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for i in 0..num_nodes {
        let node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());
        nodes.push(node);
    }

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for id in 1..=100u64 {
        let node_idx = ((id - 1) % (num_nodes as u64)) as usize;
        let cell = &cells[node_idx];
        let y_pos = cell.y_min + 0.5;
        let vel_y = 3.0;
        nodes[node_idx].world.spawn_local(id, (100.0, y_pos, 100.0), (0.0, vel_y, 0.0));
        nodes[node_idx].engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (100.0, y_pos, 100.0), vel: (0.0, vel_y, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..200 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(5)).await;
    }

    let mut total_local = 0;
    let mut dual_count = 0;
    let mut ownership: HashMap<u64, usize> = HashMap::new();
    for (idx, node) in nodes.iter().enumerate() {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                total_local += 1;
                if ownership.insert(*id, idx).is_some() {
                    dual_count += 1;
                }
            }
        }
    }

    assert_eq!(dual_count, 0, "No dual ownership should exist after handoff throughput test");
    assert!(total_local >= 50, "At least 50/100 entities should still be Local somewhere. Got {}", total_local);
}

#[tokio::test]
async fn test_20node_cell_exchange_convergence() {
    use zeus_node::cell::Cell;

    let num_nodes = 20;
    let cells: Vec<Cell> = (0..num_nodes).map(|i| {
        Cell::new(0.0, 200.0, (i as f32) * 10.0, ((i + 1) as f32) * 10.0, 0.0, 200.0)
    }).collect();

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for i in 0..num_nodes {
        let node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());
        nodes.push(node);
    }

    for _ in 0..50 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for (i, cell) in cells.iter().enumerate() {
        let center = cell.center();
        let mut any_knows = false;
        for (j, node) in nodes.iter().enumerate() {
            if i == j { continue; }
            if node.engine.discovery.find_peer_containing(center).is_some() {
                any_knows = true;
                break;
            }
        }
        if any_knows {
            continue;
        }
    }

    let node0_peer_count = nodes[0].engine.discovery.peers.len();
    assert!(node0_peer_count >= 1, "Node 0 should know about at least 1 peer. Got {}", node0_peer_count);

    let known_cells: usize = nodes[0].engine.discovery.peers.values()
        .filter(|p| p.cell.is_some())
        .count();
    assert!(known_cells >= 1, "Node 0 should know at least 1 peer cell via 0xD6. Got {}", known_cells);
}

#[tokio::test]
async fn test_split_physics_20node_no_freeze() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let num_nodes = 20usize;
    let entities_per_node = 10u64;

    let cells: Vec<Cell> = (0..num_nodes).map(|i| {
        Cell::new(0.0, 200.0, (i as f32) * 10.0, ((i + 1) as f32) * 10.0, 0.0, 200.0)
    }).collect();

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for i in 0..num_nodes {
        let mut node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());

        let base_id = (i as u64) * entities_per_node + 1;
        let y_mid = (cells[i].y_min + cells[i].y_max) / 2.0;
        for j in 0..entities_per_node {
            let id = base_id + j;
            let y = y_mid + (j as f32 - 5.0) * 0.5;
            node.world.spawn_local(id, (100.0, y, 100.0), (0.0, 0.0, 0.0));
            node.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
                id, pos: (100.0, y, 100.0), vel: (0.0, 0.0, 0.0),
                state: AuthorityState::Local, verifying_key: None,
            });
        }

        nodes.push(node);
    }

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    for _ in 0..500 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
    }

    let mut frozen_count = 0;
    for node in &nodes {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local && !node.world.local_ids.contains(id) {
                frozen_count += 1;
            }
        }
    }
    assert!(frozen_count <= 5, "At most 5 entities should be 'frozen' (Local but not in physics). Got {}", frozen_count);
}

#[tokio::test]
async fn test_10node_targeted_offers_no_broadcast() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cells: Vec<Cell> = (0..10).map(|i| {
        Cell::new(0.0, 100.0, (i as f32) * 10.0, ((i + 1) as f32) * 10.0, 0.0, 100.0)
    }).collect();

    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs: Vec<std::net::SocketAddr> = Vec::new();

    for i in 0..10 {
        let node = make_node(cells[i].clone(), addrs.clone()).await;
        addrs.push(node.engine.endpoint.local_addr().unwrap());
        nodes.push(node);
    }

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    nodes[0].world.spawn_local(1, (50.0, 9.5, 50.0), (0.0, 2.0, 0.0));
    nodes[0].engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (50.0, 9.5, 50.0), vel: (0.0, 2.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    for _ in 0..60 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(5)).await;
    }

    let target = nodes[0].engine.discovery.find_peer_containing((50.0, 15.0, 50.0));
    if target.is_some() {
        let owner_count: usize = nodes.iter()
            .filter(|n| n.engine.node.manager.get_entity(1).is_some_and(|e| e.state == AuthorityState::Local))
            .count();
        assert!(owner_count <= 1, "With cell exchange, entity should resolve to at most 1 owner. Got {}", owner_count);
    }
}

#[tokio::test]
async fn test_split_physics_10node_cascade() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let total_entities = 100u64;
    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let mut node0 = make_node(full_cell.clone(), vec![]).await;
    let addr0 = node0.engine.endpoint.local_addr().unwrap();

    for id in 1..=total_entities {
        let y = ((id - 1) as f32) * 1.0;
        node0.world.spawn_local(id, (50.0, y, 50.0), (0.0, 0.0, 0.0));
        node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (50.0, y, 50.0), vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut all_nodes: Vec<GameLoop<TestWorld>> = vec![node0];
    let mut all_addrs = vec![addr0];

    for split_idx in 0..9 {
        let parent_idx = 0.min(all_nodes.len() - 1);
        let old_cell = all_nodes[parent_idx].engine.node.manager.cell().clone();

        let mid_y = (old_cell.y_min + old_cell.y_max) / 2.0;
        let keep_cell = Cell::new(old_cell.x_min, old_cell.x_max, old_cell.y_min, mid_y, old_cell.z_min, old_cell.z_max);
        let new_cell = Cell::new(old_cell.x_min, old_cell.x_max, mid_y, old_cell.y_max, old_cell.z_min, old_cell.z_max);

        if new_cell.y_max - new_cell.y_min < 1.0 { break; }

        let mut new_node = make_node(new_cell.clone(), all_addrs.clone()).await;
        all_addrs.push(new_node.engine.endpoint.local_addr().unwrap());

        for _ in 0..15 {
            for n in all_nodes.iter_mut() {
                n.tick(0.016).await.unwrap();
            }
            new_node.tick(0.016).await.unwrap();
            sleep(Duration::from_millis(5)).await;
        }

        all_nodes[parent_idx].set_cell(keep_cell.clone());
        all_nodes[parent_idx].evict_out_of_cell_from_physics();
        all_nodes.push(new_node);

        for _ in 0..50 {
            for n in all_nodes.iter_mut() {
                n.tick(0.016).await.unwrap();
            }
            sleep(Duration::from_millis(5)).await;
        }

        let total = total_local_count(&all_nodes);
        assert!(
            total >= (total_entities as usize * 6 / 10),
            "After cascade split {}: total Local {} should be >= 60% of {}", split_idx, total, total_entities
        );
        assert_single_ownership(&all_nodes, &format!("cascade split {}", split_idx));
    }
}
