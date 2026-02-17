use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::autoscaler::AutoScaler;
use zeus_node::cell::Cell;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::GameLoop;

#[tokio::test]
async fn test_spawn_10_split_spawn_10_more_entity_conservation() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 10, (6.0, 12.0, 0.0), 1);
    let initial_count = 10;
    nodes.push(node0);

    let positions = local_positions_for(&nodes[0]);
    let cell0 = nodes[0].engine.node.manager.cell().clone();
    let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell0, &positions);

    let addr0 = nodes[0].engine.endpoint.local_addr().unwrap();
    nodes.push(GameLoop::new(make_config(new, vec![addr0]), new_world()).await.unwrap());
    for _ in 0..60 { for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); } sleep(Duration::from_millis(3)).await; }
    nodes[0].set_cell(keep);
    nodes[0].evict_out_of_cell_from_physics();
    tick_all(&mut nodes, 300).await;

    let count_after_split1 = total_local(&nodes);
    assert!(
        count_after_split1 >= initial_count - 1,
        "After first split: expected ~{} entities, got {} | {}",
        initial_count, count_after_split1, entity_state_summary(&nodes)
    );

    spawn_stationary(&mut nodes[0], 10, (4.0, 12.0, 0.0), 100);
    let total_expected = count_after_split1 + 10;
    tick_all(&mut nodes, 100).await;

    let final_count = total_local(&nodes);
    assert!(
        final_count >= total_expected - 1,
        "After second batch: expected ~{}, got {} | {}",
        total_expected, final_count, entity_state_summary(&nodes)
    );
    assert_single_ownership(&nodes, "spawn-split-spawn");
}

#[tokio::test]
async fn test_scale_up_to_4_nodes_entity_conservation() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 40, (12.0, 12.0, 0.0), 1);
    let total_spawned = 40usize;
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    assert_eq!(total_local(&nodes), total_spawned);
    assert_single_ownership(&nodes, "baseline");

    do_split(&mut nodes, 0).await;
    let after_split1 = total_local(&nodes);
    assert!(after_split1 >= total_spawned - 2, "Split 1: ≥{}, got {} | {}", total_spawned - 2, after_split1, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "split1");

    let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
    do_split(&mut nodes, biggest).await;
    let after_split2 = total_local(&nodes);
    assert!(after_split2 >= total_spawned - 4, "Split 2: ≥{}, got {} | {}", total_spawned - 4, after_split2, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "split2");

    let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
    do_split(&mut nodes, biggest).await;
    let final_count = total_local(&nodes);
    assert!(final_count >= total_spawned - 6, "4-node: ≥{}, got {} | {}", total_spawned - 6, final_count, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "4-node");

    for (i, node) in nodes.iter().enumerate() {
        let local = local_count_for(node);
        let physics = node.world.local_ids.len();
        assert!((local as i64 - physics as i64).abs() <= 2, "Node {}: mgr={} vs phys={}", i, local, physics);
    }
}

#[tokio::test]
async fn test_many_node_scale_up_entity_conservation() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 60, (12.0, 12.0, 0.0), 1);
    let total_spawned = 60usize;
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    for split_round in 0..5 {
        let biggest_idx = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
        if local_count_for(&nodes[biggest_idx]) < 6 { break; }
        do_split(&mut nodes, biggest_idx).await;
        let count = total_local(&nodes);
        assert!(count >= total_spawned - (split_round + 1) * 3, "Round {}: ≥{}, got {} | {}", split_round, total_spawned - (split_round + 1) * 3, count, entity_state_summary(&nodes));
        assert_single_ownership(&nodes, &format!("round-{}", split_round));
    }

    assert!(nodes.len() >= 4);
    let final_count = total_local(&nodes);
    assert!(final_count >= total_spawned - 15, "Final: ≥{}, got {} | {}", total_spawned - 15, final_count, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "many-node-final");
}

#[tokio::test]
async fn test_no_dual_ownership_during_split_cascade() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 40, (12.0, 12.0, 0.0), 1);
    nodes.push(node0);
    tick_all(&mut nodes, 5).await;

    for split_round in 0..3 {
        let biggest_idx = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
        if local_count_for(&nodes[biggest_idx]) < 6 { break; }

        let cell = nodes[biggest_idx].engine.node.manager.cell().clone();
        let positions = local_positions_for(&nodes[biggest_idx]);
        let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);

        let addrs: Vec<_> = nodes.iter().map(|n| n.engine.endpoint.local_addr().unwrap()).collect();
        nodes.push(GameLoop::new(make_config(new, addrs), new_world()).await.unwrap());
        for _ in 0..60 { for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); } sleep(Duration::from_millis(3)).await; }
        nodes[biggest_idx].set_cell(keep);
        nodes[biggest_idx].evict_out_of_cell_from_physics();

        for tick in 0..200 {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(2)).await;
            if tick % 50 == 0 {
                assert_single_ownership(&nodes, &format!("split{}-tick{}", split_round, tick));
            }
        }
    }

    assert_single_ownership(&nodes, "final");
    let total = total_local(&nodes);
    assert!(total >= 35, "After 3 splits of 40: ≥35, got {}", total);
}

#[tokio::test]
async fn test_moving_entities_conserved_through_split() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_batch(&mut node0, 30, (12.0, 12.0, 0.0), 1, 2.0);
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    let total_spawned = 30usize;
    let pre_positions: HashMap<u64, (f32, f32, f32)> = nodes[0].engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos)).collect();

    do_split(&mut nodes, 0).await;

    let count_after = total_local(&nodes);
    assert!(count_after >= total_spawned - 3, "Moving: ≥{}, got {} | {}", total_spawned - 3, count_after, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "moving-split");

    let mut frozen_count = 0;
    for node in &nodes {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(pre_pos) = pre_positions.get(id) {
                    if pos_distance(e.pos, *pre_pos) < 0.001 { frozen_count += 1; }
                }
            }
        }
    }
    assert!(frozen_count <= 2, "{} entities frozen (expected ≤ 2)", frozen_count);
}

#[tokio::test]
async fn test_handoff_in_entities_clear_after_settle() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..15 {
        let x = 11.0 + (i as f32) * 0.3;
        let id = i as u64 + 1;
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), (2.0, 0.5, 0.0));
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel: (2.0, 0.5, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    tick_all(&mut nodes, 300).await;

    let hi_total: usize = nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::HandoffIn).count();
    assert!(hi_total <= 2, "HandoffIn stuck: {}", hi_total);
}

#[tokio::test]
async fn test_entities_cross_boundary_not_stuck() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..5 {
        let id = i + 1;
        node0.world.spawn_drone_at(id, (6.0 + i as f32, 12.0, 0.0), (3.0, 0.0, 0.0));
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (6.0 + i as f32, 12.0, 0.0), vel: (3.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let mut node1_received = HashSet::new();
    for _ in 0..400 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(2)).await;
        for (id, e) in &nodes[1].engine.node.manager.entities {
            if e.state == AuthorityState::Local { node1_received.insert(*id); }
        }
    }

    assert!(!node1_received.is_empty(), "Entities with +x velocity should cross to node1");
}

#[tokio::test]
async fn test_physics_world_consistency_after_many_handoffs() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..10 {
        let id = i + 1;
        let x = 10.0 + (i as f32) * 0.5;
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), (2.0, 0.0, 0.0));
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel: (2.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    tick_all(&mut nodes, 400).await;

    for (i, node) in nodes.iter().enumerate() {
        let mgr_local: HashSet<u64> = node.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .map(|(id, _)| *id).collect();
        let phys_local: HashSet<u64> = node.world.local_ids.clone();
        let in_mgr_not_phys: Vec<_> = mgr_local.difference(&phys_local).collect();
        let in_phys_not_mgr: Vec<_> = phys_local.difference(&mgr_local).collect();
        assert!(in_mgr_not_phys.is_empty(), "Node {}: in mgr not phys: {:?}", i, in_mgr_not_phys);
        assert!(in_phys_not_mgr.is_empty(), "Node {}: in phys not mgr: {:?}", i, in_phys_not_mgr);
    }
}
