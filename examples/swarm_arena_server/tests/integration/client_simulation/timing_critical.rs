use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::autoscaler::AutoScaler;
use zeus_node::cell::Cell;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::GameLoop;

const TICKS_5MS: usize = 1;
const TICKS_10MS: usize = 2;
const TICKS_50MS: usize = 7;

fn assert_no_dual_local(nodes: &[GameLoop<BoundedPhysicsWorld>], label: &str) {
    let mut seen: HashMap<u64, usize> = HashMap::new();
    for (ni, node) in nodes.iter().enumerate() {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(prev) = seen.insert(*id, ni) {
                    panic!("[{}] Entity {} Local on node {} AND node {}", label, id, prev, ni);
                }
            }
        }
    }
}

fn assert_physics_sync(nodes: &[GameLoop<BoundedPhysicsWorld>], label: &str) {
    for (i, node) in nodes.iter().enumerate() {
        let mgr: HashSet<u64> = node.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .map(|(id, _)| *id).collect();
        let phys: HashSet<u64> = node.world.local_ids.clone();
        let only_mgr: Vec<_> = mgr.difference(&phys).copied().collect();
        let only_phys: Vec<_> = phys.difference(&mgr).copied().collect();
        assert!(
            only_mgr.is_empty() && only_phys.is_empty(),
            "[{}] N{}: in-mgr-not-phys={:?}, in-phys-not-mgr={:?}",
            label, i, only_mgr, only_phys
        );
    }
}

#[tokio::test]
async fn test_per_tick_during_split_no_dual_ownership() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 30, (12.0, 12.0, 0.0), 1);
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    let cell = nodes[0].engine.node.manager.cell().clone();
    let positions = local_positions_for(&nodes[0]);
    let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);

    let addrs: Vec<_> = nodes.iter().map(|n| n.engine.endpoint.local_addr().unwrap()).collect();
    nodes.push(GameLoop::new(make_config(new, addrs), new_world()).await.unwrap());

    for t in 0..80 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(3)).await;
        assert_no_dual_local(&nodes, &format!("warmup-t{}", t));
    }

    nodes[0].set_cell(keep);
    nodes[0].evict_out_of_cell_from_physics();

    for t in 0..512 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;
        assert_no_dual_local(&nodes, &format!("split-t{}", t));
    }
}

#[tokio::test]
async fn test_per_tick_during_split_physics_sync() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 20, (12.0, 12.0, 0.0), 1);
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    assert_physics_sync(&nodes, "pre-split");

    let cell = nodes[0].engine.node.manager.cell().clone();
    let positions = local_positions_for(&nodes[0]);
    let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);

    let addrs: Vec<_> = nodes.iter().map(|n| n.engine.endpoint.local_addr().unwrap()).collect();
    nodes.push(GameLoop::new(make_config(new, addrs), new_world()).await.unwrap());

    for _ in 0..80 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(3)).await;
    }

    nodes[0].set_cell(keep);
    nodes[0].evict_out_of_cell_from_physics();

    let mut desync_ticks = 0;
    for _t in 0..400 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        for (_i, node) in nodes.iter().enumerate() {
            let mgr = node.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::Local).count();
            let phys = node.world.local_ids.len();
            if mgr != phys {
                desync_ticks += 1;
            }
        }
    }

    assert!(
        desync_ticks <= 10,
        "Physics/manager desync on {} of 400 ticks (expected ≤ 10)",
        desync_ticks
    );
}

#[tokio::test]
async fn test_every_10ms_entity_count_during_handoff() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..10 {
        let id = i + 1;
        let x = 10.0 + (i as f32) * 0.4;
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), (2.5, 0.0, 0.0));
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel: (2.5, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let total_spawned = 10;
    let mut min_total_seen = total_spawned;

    for checkpoint in 0..50 {
        for _ in 0..TICKS_10MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }

        let total = total_local(&nodes);
        min_total_seen = min_total_seen.min(total);

        let in_flight: usize = nodes.iter()
            .flat_map(|n| n.engine.node.manager.entities.values())
            .filter(|e| e.state == AuthorityState::HandoffOut || e.state == AuthorityState::HandoffIn)
            .count();

        assert!(
            total + in_flight >= total_spawned - 1,
            "10ms #{} (t={}ms): local={} + inflight={} = {} < {} (lost entities)",
            checkpoint, (checkpoint + 1) * 10, total, in_flight, total + in_flight, total_spawned - 1
        );

        assert_no_dual_local(&nodes, &format!("10ms-{}", checkpoint));
    }

    assert!(
        min_total_seen >= total_spawned - 5,
        "Min total local across all 10ms checkpoints: {} (expected ≥ {})",
        min_total_seen, total_spawned - 5
    );
}

#[tokio::test]
async fn test_every_50ms_during_split_cascade() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 40, (12.0, 12.0, 0.0), 1);
    let total_spawned = 40usize;
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    let mut checkpoints: Vec<(u64, usize, usize)> = Vec::new();
    let mut cumulative_ms = 0u64;

    for split_round in 0..3 {
        let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
        if local_count_for(&nodes[biggest]) < 6 { break; }

        let cell = nodes[biggest].engine.node.manager.cell().clone();
        let positions = local_positions_for(&nodes[biggest]);
        let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);

        let addrs: Vec<_> = nodes.iter().map(|n| n.engine.endpoint.local_addr().unwrap()).collect();
        nodes.push(GameLoop::new(make_config(new, addrs), new_world()).await.unwrap());

        for _ in 0..80 {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(2)).await;
        }
        cumulative_ms += (80.0 * DT * 1000.0) as u64;

        nodes[biggest].set_cell(keep);
        nodes[biggest].evict_out_of_cell_from_physics();

        for checkpoint in 0..8 {
            for _ in 0..TICKS_50MS {
                for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
                sleep(Duration::from_millis(1)).await;
            }
            cumulative_ms += 50;

            let total = total_local(&nodes);
            assert_no_dual_local(&nodes, &format!("split{}-50ms{}", split_round, checkpoint));
            checkpoints.push((cumulative_ms, nodes.len(), total));
        }

        tick_all(&mut nodes, 200).await;
        cumulative_ms += (200.0 * DT * 1000.0) as u64;
    }

    for (ms, node_count, total) in &checkpoints {
        assert!(
            *total >= total_spawned / 2,
            "At t={}ms ({} nodes): total={}, expected ≥ {} (catastrophic loss)",
            ms, node_count, total, total_spawned / 2
        );
    }
}

#[tokio::test]
async fn test_every_5ms_spawn_delete_single_node() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();

    let mut expected = 0usize;
    let mut next_id: u64 = 1;

    let schedule: Vec<(&str, usize)> = vec![
        ("spawn", 10), ("tick", 0), ("tick", 0),
        ("spawn", 10), ("tick", 0),
        ("spawn", 10), ("tick", 0), ("tick", 0),
        ("delete", 8), ("tick", 0),
        ("spawn", 5),  ("tick", 0), ("tick", 0),
        ("delete", 12), ("tick", 0),
        ("spawn", 15), ("tick", 0), ("tick", 0), ("tick", 0),
        ("delete", 5), ("tick", 0),
    ];

    for (step, (action, count)) in schedule.iter().enumerate() {
        match *action {
            "spawn" => {
                spawn_batch(&mut node0, *count, (12.0, 12.0, 0.0), next_id, 1.5);
                next_id += *count as u64;
                expected += count;
            }
            "delete" => {
                let remove_n = (*count).min(expected);
                remove_entities(&mut node0, remove_n);
                expected -= remove_n;
            }
            _ => {}
        }

        for _ in 0..TICKS_5MS {
            node0.tick(DT).await.unwrap();
        }

        let mgr = node0.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let phys = node0.world.local_ids.len();

        assert_eq!(mgr, expected, "Step {} '{}({})' ~{}ms: mgr={} expected={}", step, action, count, step * 5, mgr, expected);
        assert_eq!(phys, expected, "Step {} '{}({})' ~{}ms: phys={} expected={}", step, action, count, step * 5, phys, expected);
    }
}

#[tokio::test]
async fn test_per_tick_position_continuity_during_split() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_batch(&mut node0, 15, (12.0, 12.0, 0.0), 1, 2.0);
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    let cell = nodes[0].engine.node.manager.cell().clone();
    let positions = local_positions_for(&nodes[0]);
    let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);
    let addrs: Vec<_> = nodes.iter().map(|n| n.engine.endpoint.local_addr().unwrap()).collect();
    nodes.push(GameLoop::new(make_config(new, addrs), new_world()).await.unwrap());

    for _ in 0..80 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(2)).await;
    }

    nodes[0].set_cell(keep);
    nodes[0].evict_out_of_cell_from_physics();

    let mut prev = collect_all_local(&nodes);
    let max_step = 3.0 * DT * 2.5;
    let mut teleport_count = 0;

    for _t in 0..256 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        let current = collect_all_local(&nodes);
        for (id, (cur_pos, _)) in &current {
            if let Some((prev_pos, _)) = prev.get(id) {
                let d = pos_distance(*cur_pos, *prev_pos);
                if d > max_step {
                    teleport_count += 1;
                }
            }
        }
        prev = current;
    }

    assert!(
        teleport_count <= 5,
        "{} teleport events during 256 post-split ticks (expected ≤ 5)",
        teleport_count
    );
}

#[tokio::test]
async fn test_every_10ms_spawn_split_delete_interleaved() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    nodes.push(node0);

    let mut expected = 0usize;
    let mut next_id: u64 = 1;
    let batch = 10;

    for _ in 0..3 {
        spawn_stationary(&mut nodes[0], batch, (12.0, 12.0, 0.0), next_id);
        next_id += batch as u64;
        expected += batch;

        for _ in 0..TICKS_10MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(total_local(&nodes), expected, "Post-spawn: {} vs {}", total_local(&nodes), expected);
    }

    do_split(&mut nodes, 0).await;
    let post_split = total_local(&nodes);
    assert!(post_split >= expected - 2, "Post-split: ≥{}, got {}", expected - 2, post_split);
    expected = post_split;

    for del_round in 0..2 {
        let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
        let remove_n = 5.min(local_count_for(&nodes[biggest]));
        remove_entities(&mut nodes[biggest], remove_n);
        expected -= remove_n;

        for cp in 0..5 {
            for _ in 0..TICKS_10MS {
                for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
                sleep(Duration::from_millis(1)).await;
            }

            let total = total_local(&nodes);
            assert_eq!(
                total, expected,
                "Del {} 10ms#{}: expected {}, got {} | {}",
                del_round, cp, expected, total, entity_state_summary(&nodes)
            );
            assert_no_dual_local(&nodes, &format!("del{}-10ms{}", del_round, cp));
        }
    }
}

#[tokio::test]
async fn test_every_50ms_velocity_stable_through_handoff() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    let original_vel = (2.5, 0.3, -0.1);
    for i in 0..6 {
        let id = i + 1;
        let x = 9.0 + (i as f32) * 0.6;
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), original_vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel: original_vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let original_mag = vel_magnitude(original_vel);
    let mut nodes = vec![node0, node1];
    let mut vel_anomalies = 0;

    for checkpoint in 0..20 {
        for _ in 0..TICKS_50MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }

        let all = collect_all_local(&nodes);
        for (_, (_, vel)) in &all {
            let mag = vel_magnitude(*vel);
            let ratio = mag / original_mag;
            if ratio < 0.3 || ratio > 3.0 {
                vel_anomalies += 1;
            }
        }

        assert_no_dual_local(&nodes, &format!("50ms-vel-{}", checkpoint));
    }

    assert!(
        vel_anomalies <= 2,
        "{} velocity anomalies across 20 50ms checkpoints (expected ≤ 2)",
        vel_anomalies
    );
}

#[tokio::test]
async fn test_per_tick_no_entity_loss_during_two_node_deletion() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    spawn_stationary(&mut node0, 10, (6.0, 12.0, 0.0), 1);
    let mut nodes = vec![node0, node1];

    spawn_stationary(&mut nodes[1], 10, (18.0, 12.0, 0.0), 100);

    tick_all(&mut nodes, 30).await;
    let baseline = total_local(&nodes);
    assert_eq!(baseline, 20);

    remove_entities(&mut nodes[0], 3);
    let mut expected = baseline - 3;

    for t in 0..64 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        let total = total_local(&nodes);
        assert_eq!(total, expected, "Post-delete tick {}: {} vs {}", t, total, expected);
        assert_no_dual_local(&nodes, &format!("del-tick{}", t));
    }

    remove_entities(&mut nodes[1], 5);
    expected -= 5;

    for t in 0..64 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        let total = total_local(&nodes);
        assert_eq!(total, expected, "Post-delete2 tick {}: {} vs {}", t, total, expected);
    }
}

#[tokio::test]
async fn test_every_10ms_full_session_spawn5_delete2_split_scenario() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    nodes.push(node0);

    let batch = 10;
    let mut expected = 0usize;
    let mut next_id: u64 = 1;
    let mut ms = 0u64;

    macro_rules! advance_10ms {
        ($nodes:expr, $ms:expr, $label:expr) => {{
            for _ in 0..TICKS_10MS {
                for n in $nodes.iter_mut() { n.tick(DT).await.unwrap(); }
                sleep(Duration::from_millis(1)).await;
            }
            $ms += 10;
            let total = total_local(&$nodes);
            assert_no_dual_local(&$nodes, &format!("{}@{}ms", $label, $ms));
            total
        }};
    }

    for wave in 0..5 {
        spawn_stationary(&mut nodes[0], batch, (12.0, 12.0, (wave as f32) * 2.0 - 4.0), next_id);
        next_id += batch as u64;
        expected += batch;

        let total = advance_10ms!(nodes, ms, "spawn");
        assert_eq!(total, expected, "Spawn wave {} at {}ms: {} vs {}", wave, ms, total, expected);
    }

    assert_eq!(expected, 50);

    do_split(&mut nodes, 0).await;
    ms += 3750;
    let post_split = total_local(&nodes);
    assert!(post_split >= expected - 2);
    expected = post_split;

    for wave in 0..3 {
        let total = advance_10ms!(nodes, ms, "settle");
        assert_eq!(total, expected, "Settle {} at {}ms: {} vs {}", wave, ms, total, expected);
    }

    for del_wave in 0..2 {
        let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
        let remove_n = batch.min(local_count_for(&nodes[biggest]));
        remove_entities(&mut nodes[biggest], remove_n);
        expected -= remove_n;

        let total = advance_10ms!(nodes, ms, "delete");
        assert_eq!(total, expected, "Delete {} at {}ms: {} vs {}", del_wave, ms, total, expected);
    }
}
