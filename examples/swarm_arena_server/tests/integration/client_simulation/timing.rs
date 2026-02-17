use super::helpers::*;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::entity_manager::AuthorityState;
use zeus_node::game_loop::GameLoop;

struct Checkpoint {
    time_ms: u64,
    expected_count: usize,
    actual_count: usize,
}

fn collect_velocities(nodes: &[GameLoop<BoundedPhysicsWorld>]) -> HashMap<u64, (f32, f32, f32)> {
    let mut vels = HashMap::new();
    for node in nodes {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                vels.insert(*id, e.vel);
            }
        }
    }
    vels
}

#[tokio::test]
async fn test_spawn_5_batches_delete_2_timed_checkpoints() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    nodes.push(node0);

    let batch_size = 10;
    let mut expected_count = 0usize;
    let mut next_id: u64 = 1;
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    let mut prev_snapshot = collect_all_local(&nodes);
    let mut prev_vels = collect_velocities(&nodes);
    let mut cumulative_ticks = 0u64;

    for wave in 0..7 {
        let is_despawn = wave == 5 || wave == 6;

        if is_despawn {
            let remove_n = batch_size.min(expected_count);
            remove_entities(&mut nodes[0], remove_n);
            expected_count -= remove_n;
        } else {
            let center_z = (wave as f32) * 2.0 - 5.0;
            spawn_batch(&mut nodes[0], batch_size, (12.0, 12.0, center_z), next_id, 2.0);
            next_id += batch_size as u64;
            expected_count += batch_size;
        }

        for _ in 0..TICKS_PER_100MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }
        cumulative_ticks += TICKS_PER_100MS as u64;

        let actual = total_local(&nodes);
        let current_snapshot = collect_all_local(&nodes);
        let current_vels = collect_velocities(&nodes);

        let mut max_vel_delta: f32 = 0.0;
        for (id, cur_vel) in &current_vels {
            if let Some(prev_vel) = prev_vels.get(id) {
                let dv = vel_magnitude((cur_vel.0 - prev_vel.0, cur_vel.1 - prev_vel.1, cur_vel.2 - prev_vel.2));
                max_vel_delta = max_vel_delta.max(dv);
            }
        }

        let mut max_pos_jump: f32 = 0.0;
        let elapsed_s = TICKS_PER_100MS as f32 * DT;
        for (id, (cur_pos, _)) in &current_snapshot {
            if let Some((prev_pos, prev_vel)) = prev_snapshot.get(id) {
                let expected_displacement = vel_magnitude(*prev_vel) * elapsed_s;
                let actual_displacement = pos_distance(*cur_pos, *prev_pos);
                let overshoot = (actual_displacement - expected_displacement * 1.5).max(0.0);
                max_pos_jump = max_pos_jump.max(overshoot);
            }
        }

        assert!(
            max_vel_delta < 10.0,
            "Wave {}: velocity delta {:.2} too large (entity changing direction unexpectedly)",
            wave, max_vel_delta
        );
        assert!(
            max_pos_jump < 2.0,
            "Wave {}: position overshoot {:.2} too large (teleportation detected)",
            wave, max_pos_jump
        );

        checkpoints.push(Checkpoint {
            time_ms: cumulative_ticks * (1000 / 128),
            expected_count,
            actual_count: actual,
        });

        prev_snapshot = current_snapshot;
        prev_vels = current_vels;
    }

    for (i, cp) in checkpoints.iter().enumerate() {
        assert_eq!(
            cp.actual_count, cp.expected_count,
            "Checkpoint {} (t={}ms): expected {} entities, got {}",
            i, cp.time_ms, cp.expected_count, cp.actual_count
        );
    }
}

#[tokio::test]
async fn test_spawn_5_delete_2_with_split_timed() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    nodes.push(node0);

    let batch_size = 10;
    let mut next_id: u64 = 1;
    let mut expected_count = 0usize;

    for wave in 0..5 {
        let center_z = (wave as f32) * 2.0 - 4.0;
        spawn_stationary(&mut nodes[0], batch_size, (12.0, 12.0, center_z), next_id);
        next_id += batch_size as u64;
        expected_count += batch_size;
        tick_all(&mut nodes, TICKS_PER_100MS).await;

        let actual = total_local(&nodes);
        assert_eq!(actual, expected_count, "Wave {} (t={}ms): expected {}, got {}", wave, (wave + 1) * 100, expected_count, actual);
    }

    assert_eq!(expected_count, 50);

    do_split(&mut nodes, 0).await;
    let post_split = total_local(&nodes);
    assert!(post_split >= expected_count - 2, "Post-split: ≥{}, got {} | {}", expected_count - 2, post_split, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "post-split");
    expected_count = post_split;

    for del_wave in 0..2 {
        let remove_n = batch_size.min(expected_count);
        let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
        remove_entities(&mut nodes[biggest], remove_n);
        expected_count -= remove_n;
        tick_all(&mut nodes, TICKS_PER_100MS).await;

        let actual = total_local(&nodes);
        assert_eq!(actual, expected_count, "Delete wave {} (t={}ms): expected {}, got {}", del_wave, 600 + (del_wave + 1) * 100, expected_count, actual);
    }

    assert_single_ownership(&nodes, "final");
}

#[tokio::test]
async fn test_entity_positions_advance_at_100ms_intervals() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();

    let speed = 2.0;
    spawn_batch(&mut node0, 10, (12.0, 12.0, 0.0), 1, speed);

    let initial_snapshot = collect_all_local(&std::slice::from_ref(&node0));
    let mut prev_snapshot = initial_snapshot.clone();

    for interval in 0..10 {
        for _ in 0..TICKS_PER_100MS {
            node0.tick(DT).await.unwrap();
            sleep(Duration::from_millis(1)).await;
        }

        let current = collect_all_local(&std::slice::from_ref(&node0));
        let elapsed_s = TICKS_PER_100MS as f32 * DT;

        for (id, (cur_pos, _)) in &current {
            if let Some((prev_pos, prev_vel)) = prev_snapshot.get(id) {
                let expected_d = vel_magnitude(*prev_vel) * elapsed_s;
                let actual_d = pos_distance(*cur_pos, *prev_pos);
                assert!(
                    actual_d < expected_d * 2.5 + 0.5,
                    "Interval {}: entity {} moved {} but expected ~{} (vel mag {})",
                    interval, id, actual_d, expected_d, vel_magnitude(*prev_vel)
                );
            }
        }

        assert_eq!(
            current.len(), initial_snapshot.len(),
            "Interval {}: entity count changed from {} to {}",
            interval, initial_snapshot.len(), current.len()
        );

        prev_snapshot = current;
    }
}

#[tokio::test]
async fn test_velocity_magnitude_stable_over_1_second() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();
    let speed = 3.0;
    spawn_batch(&mut node0, 15, (12.0, 12.0, 0.0), 1, speed);

    let initial_vels = collect_velocities(&std::slice::from_ref(&node0));
    let initial_mags: HashMap<u64, f32> = initial_vels.iter()
        .map(|(id, v)| (*id, vel_magnitude(*v)))
        .collect();

    for interval in 0..10 {
        for _ in 0..TICKS_PER_100MS {
            node0.tick(DT).await.unwrap();
            sleep(Duration::from_millis(1)).await;
        }

        let current_vels = collect_velocities(&std::slice::from_ref(&node0));
        for (id, cur_vel) in &current_vels {
            let cur_mag = vel_magnitude(*cur_vel);
            if let Some(init_mag) = initial_mags.get(id) {
                let diff = (cur_mag - init_mag).abs();
                assert!(
                    diff < init_mag * 0.5 + 0.5,
                    "Interval {} (t={}ms): entity {} vel mag {:.2} vs initial {:.2} (delta {:.2})",
                    interval, (interval + 1) * 100, id, cur_mag, init_mag, diff
                );
            }
        }
    }
}

#[tokio::test]
async fn test_spawn_5_delete_2_split_spawn_3_delete_1_complex_pattern() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    nodes.push(node0);

    let batch = 10;
    let mut next_id: u64 = 1;
    let mut expected = 0usize;

    for _ in 0..5 {
        spawn_stationary(&mut nodes[0], batch, (12.0, 12.0, 0.0), next_id);
        next_id += batch as u64;
        expected += batch;
    }
    tick_all(&mut nodes, TICKS_PER_100MS).await;
    assert_eq!(total_local(&nodes), expected, "After 5 spawns: {} vs {}", total_local(&nodes), expected);

    for _ in 0..2 {
        let remove_n = batch.min(expected);
        remove_entities(&mut nodes[0], remove_n);
        expected -= remove_n;
    }
    tick_all(&mut nodes, TICKS_PER_100MS).await;
    assert_eq!(total_local(&nodes), expected, "After 2 deletes: {} vs {}", total_local(&nodes), expected);

    do_split(&mut nodes, 0).await;
    let post_split = total_local(&nodes);
    assert!(post_split >= expected - 2, "Post-split: ≥{}, got {}", expected - 2, post_split);
    expected = post_split;
    assert_single_ownership(&nodes, "mid-pattern-split");

    for wave in 0..3 {
        let target_node = wave % nodes.len();
        let center = if target_node == 0 { (6.0, 12.0, 0.0) } else { (18.0, 12.0, 0.0) };
        spawn_stationary(&mut nodes[target_node], batch, center, next_id);
        next_id += batch as u64;
        expected += batch;
    }
    tick_all(&mut nodes, TICKS_PER_100MS * 2).await;
    let post_spawn3 = total_local(&nodes);
    assert!(post_spawn3 >= expected - 2, "After +3 batches: ≥{}, got {} | {}", expected - 2, post_spawn3, entity_state_summary(&nodes));

    let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
    let remove_n = batch.min(local_count_for(&nodes[biggest]));
    remove_entities(&mut nodes[biggest], remove_n);
    expected = post_spawn3 - remove_n;
    tick_all(&mut nodes, TICKS_PER_100MS).await;
    let final_count = total_local(&nodes);
    assert_eq!(final_count, expected, "Final: {} vs {}", final_count, expected);
    assert_single_ownership(&nodes, "complex-final");
}

#[tokio::test]
async fn test_100ms_checkpoint_count_exact_single_node() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();

    let mut expected = 0usize;
    let mut next_id: u64 = 1;

    let actions: Vec<(&str, usize)> = vec![
        ("spawn", 10),
        ("spawn", 10),
        ("spawn", 10),
        ("spawn", 10),
        ("spawn", 10),
        ("delete", 10),
        ("delete", 10),
        ("spawn", 5),
        ("delete", 15),
        ("spawn", 20),
    ];

    for (i, (action, count)) in actions.iter().enumerate() {
        match *action {
            "spawn" => {
                spawn_stationary(&mut node0, *count, (12.0, 12.0, 0.0), next_id);
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

        for _ in 0..TICKS_PER_100MS {
            node0.tick(DT).await.unwrap();
        }

        let actual = node0.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let phys = node0.world.local_ids.len();

        assert_eq!(actual, expected, "Step {} '{}({})' at t={}ms: mgr {} vs expected {}", i, action, count, (i + 1) * 100, actual, expected);
        assert_eq!(phys, expected, "Step {} '{}({})' at t={}ms: phys {} vs expected {}", i, action, count, (i + 1) * 100, phys, expected);
    }
}

#[tokio::test]
async fn test_two_node_handoff_settles_within_500ms() {
    let cell0 = zeus_node::cell::Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = zeus_node::cell::Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..10 {
        let id = i + 1;
        let x = 10.5 + (i as f32) * 0.3;
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), (3.0, 0.0, 0.0));
        node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos: (x, 12.0, 0.0), vel: (3.0, 0.0, 0.0),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let ticks_500ms = TICKS_PER_100MS * 5;

    for _ in 0..ticks_500ms {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(2)).await;
    }

    let ho_total: usize = nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::HandoffOut || e.state == AuthorityState::HandoffIn)
        .count();
    assert!(ho_total <= 2, "After 500ms, {} entities still in handoff (expected ≤ 2)", ho_total);

    let total = total_local(&nodes);
    assert!(total >= 8, "After 500ms handoff: ≥8 of 10 entities local, got {}", total);
    assert_single_ownership(&nodes, "500ms-handoff");
}
