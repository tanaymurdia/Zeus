use super::helpers::*;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::GameLoop;

#[tokio::test]
async fn test_no_position_teleport_single_node_per_tick() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();
    let speed = 3.0;
    spawn_batch(&mut node0, 20, (12.0, 12.0, 0.0), 1, speed);

    let max_step = speed * DT * 2.5;
    let mut prev: HashMap<u64, (f32, f32, f32)> = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos)).collect();
    let mut max_jump_seen: f32 = 0.0;

    for tick in 0..128 {
        node0.tick(DT).await.unwrap();
        sleep(Duration::from_millis(1)).await;

        for (id, e) in &node0.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(prev_pos) = prev.get(id) {
                    let d = pos_distance(e.pos, *prev_pos);
                    max_jump_seen = max_jump_seen.max(d);
                    assert!(
                        d < max_step,
                        "Tick {}: entity {} teleported {:.4} (max {:.4}), prev={:?} cur={:?}",
                        tick, id, d, max_step, prev_pos, e.pos
                    );
                }
            }
        }

        prev = node0.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .map(|(id, e)| (*id, e.pos)).collect();
    }
}

#[tokio::test]
async fn test_no_velocity_sign_flip_single_node() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();

    for i in 0..10 {
        let id = i + 1;
        let vel = (2.0, 1.0, 0.5);
        let pos = (4.0 + i as f32, 12.0, 0.0);
        node0.world.spawn_drone_at(id, pos, vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut prev_vels: HashMap<u64, (f32, f32, f32)> = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.vel)).collect();

    let mut unexpected_flips = 0;
    for _tick in 0..64 {
        node0.tick(DT).await.unwrap();
        sleep(Duration::from_millis(1)).await;

        for (id, e) in &node0.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(pv) = prev_vels.get(id) {
                    let flip_x = pv.0.signum() != e.vel.0.signum() && pv.0.abs() > 0.5 && e.vel.0.abs() > 0.5;
                    let flip_y = pv.1.signum() != e.vel.1.signum() && pv.1.abs() > 0.5 && e.vel.1.abs() > 0.5;
                    let flip_z = pv.2.signum() != e.vel.2.signum() && pv.2.abs() > 0.5 && e.vel.2.abs() > 0.5;
                    if flip_x || flip_y || flip_z {
                        let at_boundary =
                            e.pos.0 <= WORLD.x_min + 1.0 || e.pos.0 >= WORLD.x_max - 1.0 ||
                            e.pos.1 <= WORLD.y_min + 1.0 || e.pos.1 >= WORLD.y_max - 1.0 ||
                            e.pos.2 <= WORLD.z_min + 1.0 || e.pos.2 >= WORLD.z_max - 1.0;
                        if !at_boundary {
                            unexpected_flips += 1;
                        }
                    }
                }
            }
        }

        prev_vels = node0.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .map(|(id, e)| (*id, e.vel)).collect();
    }

    assert_eq!(unexpected_flips, 0, "{} unexpected velocity sign flips (not at boundary)", unexpected_flips);
}

#[tokio::test]
async fn test_no_teleport_during_handoff_two_nodes() {
    let cell0 = zeus_node::cell::Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = zeus_node::cell::Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..8 {
        let id = i + 1;
        let x = 9.0 + (i as f32) * 0.5;
        let vel = (2.5, 0.3, 0.0);
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let mut prev_positions: HashMap<u64, (f32, f32, f32)> = collect_all_local(&nodes)
        .into_iter().map(|(id, (pos, _))| (id, pos)).collect();

    let max_step = 3.0 * DT * 3.0;
    let mut teleport_events = Vec::new();

    for tick in 0..256 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(2)).await;

        let current = collect_all_local(&nodes);
        for (id, (cur_pos, _)) in &current {
            if let Some(prev_pos) = prev_positions.get(id) {
                let d = pos_distance(*cur_pos, *prev_pos);
                if d > max_step {
                    teleport_events.push((tick, *id, d, *prev_pos, *cur_pos));
                }
            }
        }

        prev_positions = current.into_iter().map(|(id, (pos, _))| (id, pos)).collect();
    }

    assert!(
        teleport_events.len() <= 3,
        "Too many teleport events ({}): first few = {:?}",
        teleport_events.len(),
        &teleport_events[..teleport_events.len().min(5)]
    );
}

#[tokio::test]
async fn test_smooth_position_curve_over_1_second() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();
    let speed = 2.0;
    spawn_batch(&mut node0, 5, (12.0, 12.0, 0.0), 1, speed);

    let mut trajectories: HashMap<u64, Vec<(f32, f32, f32)>> = HashMap::new();
    for (id, e) in &node0.engine.node.manager.entities {
        if e.state == AuthorityState::Local {
            trajectories.insert(*id, vec![e.pos]);
        }
    }

    for _ in 0..128 {
        node0.tick(DT).await.unwrap();
        sleep(Duration::from_millis(1)).await;

        for (id, e) in &node0.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(traj) = trajectories.get_mut(id) {
                    traj.push(e.pos);
                }
            }
        }
    }

    for (id, traj) in &trajectories {
        if traj.len() < 3 { continue; }

        let mut max_accel: f32 = 0.0;
        for i in 2..traj.len() {
            let v_prev = (
                (traj[i-1].0 - traj[i-2].0) / DT,
                (traj[i-1].1 - traj[i-2].1) / DT,
                (traj[i-1].2 - traj[i-2].2) / DT,
            );
            let v_cur = (
                (traj[i].0 - traj[i-1].0) / DT,
                (traj[i].1 - traj[i-1].1) / DT,
                (traj[i].2 - traj[i-1].2) / DT,
            );
            let accel_mag = vel_magnitude((
                v_cur.0 - v_prev.0,
                v_cur.1 - v_prev.1,
                v_cur.2 - v_prev.2,
            ));
            max_accel = max_accel.max(accel_mag);
        }

        assert!(
            max_accel < speed * 128.0 * 2.0,
            "Entity {}: max acceleration {:.2} exceeds bound (smooth curve violated)",
            id, max_accel
        );
    }
}

#[tokio::test]
async fn test_velocity_direction_preserved_through_handoff() {
    let cell0 = zeus_node::cell::Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = zeus_node::cell::Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    let vel = (3.0, 0.5, -0.2);
    for i in 0..5 {
        let id = i + 1;
        node0.world.spawn_drone_at(id, (10.0 + i as f32 * 0.5, 12.0, 0.0), vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (10.0 + i as f32 * 0.5, 12.0, 0.0), vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let mut arrival_vels: HashMap<u64, (f32, f32, f32)> = HashMap::new();

    for _ in 0..400 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(2)).await;

        for (id, e) in &nodes[1].engine.node.manager.entities {
            if e.state == AuthorityState::Local && !arrival_vels.contains_key(id) {
                arrival_vels.insert(*id, e.vel);
            }
        }
    }

    assert!(!arrival_vels.is_empty(), "Some entities should arrive at node1");

    for (id, av) in &arrival_vels {
        assert!(av.0 > 0.0, "Entity {}: x velocity should stay positive, got {:.2}", id, av.0);
        assert!(av.1 > 0.0, "Entity {}: y velocity should stay positive, got {:.2}", id, av.1);
        assert!(av.2 < 0.0, "Entity {}: z velocity should stay negative, got {:.2}", id, av.2);

        let original_mag = vel_magnitude(vel);
        let arrival_mag = vel_magnitude(*av);
        let ratio = arrival_mag / original_mag;
        assert!(
            ratio > 0.5 && ratio < 2.0,
            "Entity {}: velocity magnitude ratio {:.2} out of bounds (original {:.2}, arrival {:.2})",
            id, ratio, original_mag, arrival_mag
        );
    }
}

#[tokio::test]
async fn test_entity_count_stable_per_tick_during_steady_state() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell, vec![]), new_world()).await.unwrap();
    spawn_batch(&mut node0, 30, (12.0, 12.0, 0.0), 1, 2.0);

    let expected = 30;
    let mut count_deviations = 0;

    for tick in 0..256 {
        node0.tick(DT).await.unwrap();

        let mgr = node0.engine.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local).count();
        let phys = node0.world.local_ids.len();

        if mgr != expected || phys != expected {
            count_deviations += 1;
        }

        assert_eq!(mgr, phys, "Tick {}: mgr={} phys={}", tick, mgr, phys);
    }

    assert_eq!(count_deviations, 0, "{} ticks had count deviations from {}", count_deviations, expected);
}

#[tokio::test]
async fn test_physics_manager_sync_through_spawn_despawn_split() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_batch(&mut node0, 20, (12.0, 12.0, 0.0), 1, 1.5);
    nodes.push(node0);

    for _ in 0..50 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;
        for (i, node) in nodes.iter().enumerate() {
            let mgr = node.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::Local).count();
            let phys = node.world.local_ids.len();
            assert_eq!(mgr, phys, "Pre-split N{}: mgr={} phys={}", i, mgr, phys);
        }
    }

    spawn_batch(&mut nodes[0], 10, (6.0, 12.0, 0.0), 100, 1.0);

    for tick in 0..50 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;
        for (i, node) in nodes.iter().enumerate() {
            let mgr = node.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::Local).count();
            let phys = node.world.local_ids.len();
            assert_eq!(mgr, phys, "Post-spawn tick {} N{}: mgr={} phys={}", tick, i, mgr, phys);
        }
    }

    remove_entities(&mut nodes[0], 5);

    for tick in 0..50 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;
        for (i, node) in nodes.iter().enumerate() {
            let mgr = node.engine.node.manager.entities.values()
                .filter(|e| e.state == AuthorityState::Local).count();
            let phys = node.world.local_ids.len();
            assert_eq!(mgr, phys, "Post-remove tick {} N{}: mgr={} phys={}", tick, i, mgr, phys);
        }
    }
}
