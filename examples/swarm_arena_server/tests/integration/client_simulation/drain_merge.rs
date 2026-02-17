use super::helpers::*;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::cell::Cell;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::{GameLoop, GameWorld};

const TICKS_5MS: usize = 1;
const TICKS_10MS: usize = 2;
const TICKS_50MS: usize = 7;

fn simulate_drain_pair(
    draining: &mut GameLoop<BoundedPhysicsWorld>,
    absorber: &mut GameLoop<BoundedPhysicsWorld>,
) {
    let local_ids: Vec<(u64, (f32, f32, f32), (f32, f32, f32))> = draining
        .engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos, e.vel))
        .collect();
    for (id, pos, vel) in &local_ids {
        absorber.world.on_entity_arrived(*id, *pos, *vel);
        absorber.engine.node.manager.add_entity(Entity {
            id: *id, pos: *pos, vel: *vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
        absorber.engine.node.manager.mark_arrived(*id);
        draining.world.on_entity_departed(*id);
        draining.engine.node.manager.remove_entity(*id);
    }
}

fn simulate_drain_idx(
    nodes: &mut [GameLoop<BoundedPhysicsWorld>],
    draining_idx: usize,
    absorber_idx: usize,
) {
    let local_ids: Vec<(u64, (f32, f32, f32), (f32, f32, f32))> = nodes[draining_idx]
        .engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos, e.vel))
        .collect();
    for (id, pos, vel) in &local_ids {
        nodes[absorber_idx].world.on_entity_arrived(*id, *pos, *vel);
        nodes[absorber_idx].engine.node.manager.add_entity(Entity {
            id: *id, pos: *pos, vel: *vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
        nodes[absorber_idx].engine.node.manager.mark_arrived(*id);
        nodes[draining_idx].world.on_entity_departed(*id);
        nodes[draining_idx].engine.node.manager.remove_entity(*id);
    }
}

#[tokio::test]
async fn test_drain_preserves_exact_positions_no_clamping() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    let positions = vec![
        (3.0, 18.0, 5.0),
        (6.0, 20.0, -8.0),
        (1.5, 5.0, 0.0),
        (11.0, 24.0, 11.5),
        (0.5, 0.0, -11.0),
    ];
    for (i, &pos) in positions.iter().enumerate() {
        let id = (i + 1) as u64;
        let vel = (0.5, -0.3, 0.2);
        node0.world.spawn_drone_at(id, pos, vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let pre_drain: HashMap<u64, (f32, f32, f32)> = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos))
        .collect();

    let expanded_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    node1.set_cell(expanded_cell);
    node1.world.bounds = WORLD.clone();

    simulate_drain_pair(&mut node0, &mut node1);

    for (id, original_pos) in &pre_drain {
        let entity = node1.engine.node.manager.get_entity(*id)
            .unwrap_or_else(|| panic!("Entity {} should exist after drain", id));
        let d = pos_distance(entity.pos, *original_pos);
        assert!(
            d < 0.001,
            "Entity {} position changed during drain: original={:?} received={:?} delta={:.4}",
            id, original_pos, entity.pos, d
        );
    }

    let node0_local = node0.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(node0_local, 0, "Draining node should have 0 local entities");
    let node1_local = node1.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count();
    assert_eq!(node1_local, positions.len(), "Absorber should have all entities");
}

#[tokio::test]
async fn test_drain_merge_no_teleport_per_tick() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let cell2 = Cell::new(0.0, 24.0, -1.0, 12.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();
    let addr1 = node1.engine.endpoint.local_addr().unwrap();
    let node2 = GameLoop::new(make_config(cell2, vec![addr0, addr1]), new_world()).await.unwrap();

    spawn_batch(&mut node0, 10, (6.0, 12.0, 0.0), 1, 2.0);
    spawn_batch(&mut node1, 10, (18.0, 12.0, 0.0), 100, 2.0);

    let mut nodes = vec![node0, node1, node2];
    tick_all(&mut nodes, 30).await;

    let mut prev_positions: HashMap<u64, (f32, f32, f32)> = collect_all_local(&nodes)
        .into_iter().map(|(id, (pos, _))| (id, pos)).collect();

    let expanded = nodes[0].engine.node.manager.cell().union(
        nodes[2].engine.node.manager.cell()
    );
    nodes[0].set_cell(expanded.clone());
    nodes[0].world.bounds = WORLD.clone();

    simulate_drain_idx(&mut nodes, 2, 0);

    let max_step = 3.0 * DT * 3.0;
    let mut teleport_events = Vec::new();

    for tick in 0..128 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

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
        teleport_events.is_empty(),
        "{} teleport events after drain: {:?}",
        teleport_events.len(),
        &teleport_events[..teleport_events.len().min(5)]
    );
}

#[tokio::test]
async fn test_drain_entity_conservation_every_10ms() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    spawn_batch(&mut node0, 15, (6.0, 12.0, 0.0), 1, 1.5);
    let mut nodes = vec![node0, node1];
    spawn_batch(&mut nodes[1], 15, (18.0, 12.0, 0.0), 100, 1.5);
    let total_spawned = 30;

    tick_all(&mut nodes, 20).await;

    let expanded = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    nodes[1].set_cell(expanded);
    nodes[1].world.bounds = WORLD.clone();
    simulate_drain_idx(&mut nodes, 0, 1);

    for checkpoint in 0..30 {
        for _ in 0..TICKS_10MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }

        let total = total_local(&nodes);
        let in_flight: usize = nodes.iter()
            .flat_map(|n| n.engine.node.manager.entities.values())
            .filter(|e| e.state == AuthorityState::HandoffOut || e.state == AuthorityState::HandoffIn)
            .count();
        assert!(
            total + in_flight >= total_spawned - 1,
            "10ms #{}: local={} + inflight={} < {} (entities lost after drain)",
            checkpoint, total, in_flight, total_spawned - 1
        );
    }
}

#[tokio::test]
async fn test_drain_far_entity_not_clamped() {
    let small_cell = Cell::new(12.0, 24.0, 12.0, 25.0, -12.0, 12.0);
    let mut absorber = GameLoop::new(make_config(small_cell.clone(), vec![]), new_world()).await.unwrap();

    let far_pos = (3.0, 2.0, -10.0);
    let far_vel = (1.0, 0.5, 0.3);
    absorber.world.on_entity_arrived(42, far_pos, far_vel);
    absorber.engine.node.manager.add_entity(Entity {
        id: 42, pos: far_pos, vel: far_vel,
        state: AuthorityState::Local, verifying_key: None,
    });

    let entity = absorber.engine.node.manager.get_entity(42).unwrap();
    assert!(
        (entity.pos.0 - far_pos.0).abs() < 0.001
        && (entity.pos.1 - far_pos.1).abs() < 0.001
        && (entity.pos.2 - far_pos.2).abs() < 0.001,
        "Entity position should NOT be clamped: expected {:?}, got {:?}",
        far_pos, entity.pos
    );
}

#[tokio::test]
async fn test_drain_velocity_preserved_every_5ms() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    let original_vels: Vec<(f32, f32, f32)> = vec![
        (2.0, 1.0, -0.5),
        (-1.5, 2.5, 0.3),
        (0.0, -3.0, 1.0),
        (3.0, 0.0, -2.0),
    ];
    for (i, vel) in original_vels.iter().enumerate() {
        let id = (i + 1) as u64;
        let pos = (3.0 + i as f32 * 2.0, 12.0, 0.0);
        node0.world.spawn_drone_at(id, pos, *vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos, vel: *vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    let pre_drain_vels: HashMap<u64, (f32, f32, f32)> = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.vel))
        .collect();

    let expanded = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    node1.set_cell(expanded);
    node1.world.bounds = WORLD.clone();
    simulate_drain_pair(&mut node0, &mut node1);

    for (id, original_vel) in &pre_drain_vels {
        let entity = node1.engine.node.manager.get_entity(*id).unwrap();
        let vel_diff = vel_magnitude((
            entity.vel.0 - original_vel.0,
            entity.vel.1 - original_vel.1,
            entity.vel.2 - original_vel.2,
        ));
        assert!(
            vel_diff < 0.001,
            "Entity {} velocity changed during drain: original={:?} received={:?}",
            id, original_vel, entity.vel
        );
    }

    let mut nodes = vec![node0, node1];
    let mut prev_vels: HashMap<u64, (f32, f32, f32)> = collect_all_local(&nodes)
        .into_iter().map(|(id, (_, vel))| (id, vel)).collect();
    let mut anomalies = 0;

    for _cp in 0..20 {
        for _ in 0..TICKS_5MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }

        let current = collect_all_local(&nodes);
        for (id, (_, vel)) in &current {
            if let Some(prev_vel) = prev_vels.get(id) {
                let delta = vel_magnitude((
                    vel.0 - prev_vel.0,
                    vel.1 - prev_vel.1,
                    vel.2 - prev_vel.2,
                ));
                if delta > 5.0 {
                    anomalies += 1;
                }
            }
        }
        prev_vels = current.into_iter().map(|(id, (_, vel))| (id, vel)).collect();
    }

    assert!(anomalies == 0, "{} velocity anomalies after drain (expected 0)", anomalies);
}

#[tokio::test]
async fn test_merge_expand_then_drain_ordering() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![addr0]), new_world()).await.unwrap();

    spawn_batch(&mut node0, 5, (6.0, 12.0, 0.0), 1, 2.0);
    spawn_batch(&mut node1, 5, (18.0, 12.0, 0.0), 100, 2.0);

    let mut nodes = vec![node0, node1];
    tick_all(&mut nodes, 20).await;

    let pre_drain: HashMap<u64, ((f32, f32, f32), (f32, f32, f32))> = collect_all_local(&nodes);
    let total_pre = pre_drain.len();

    let expanded = cell0.union(&cell1);
    nodes[0].set_cell(expanded);
    nodes[0].world.bounds = WORLD.clone();

    simulate_drain_idx(&mut nodes, 1, 0);

    let post_drain = collect_all_local(&nodes);
    assert_eq!(
        post_drain.len(), total_pre,
        "Entity count should be conserved: pre={} post={}",
        total_pre, post_drain.len()
    );

    for (id, (pre_pos, pre_vel)) in &pre_drain {
        if let Some((post_pos, post_vel)) = post_drain.get(id) {
            let pos_d = pos_distance(*pre_pos, *post_pos);
            assert!(
                pos_d < 0.001,
                "Entity {} position shifted during merge: pre={:?} post={:?} delta={:.4}",
                id, pre_pos, post_pos, pos_d
            );
            let vel_d = vel_magnitude((
                pre_vel.0 - post_vel.0,
                pre_vel.1 - post_vel.1,
                pre_vel.2 - post_vel.2,
            ));
            assert!(
                vel_d < 0.001,
                "Entity {} velocity shifted during merge: pre={:?} post={:?}",
                id, pre_vel, post_vel
            );
        }
    }
}

#[tokio::test]
async fn test_drain_merge_full_session_every_50ms() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_batch(&mut node0, 30, (12.0, 12.0, 0.0), 1, 1.5);
    nodes.push(node0);

    tick_all(&mut nodes, 10).await;
    let initial_count = total_local(&nodes);

    do_split(&mut nodes, 0).await;
    let post_split = total_local(&nodes);
    assert!(post_split >= initial_count - 2);

    for cp in 0..5 {
        for _ in 0..TICKS_50MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }
        let total = total_local(&nodes);
        assert!(
            total >= post_split - 2,
            "50ms #{}: {} (expected >= {})",
            cp, total, post_split - 2
        );
    }

    let expanded = nodes[0].engine.node.manager.cell().union(
        nodes[1].engine.node.manager.cell()
    );
    nodes[0].set_cell(expanded);
    nodes[0].world.bounds = WORLD.clone();

    let pre_merge_count = total_local(&nodes);

    simulate_drain_idx(&mut nodes, 1, 0);

    for cp in 0..10 {
        for _ in 0..TICKS_50MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }

        let total = total_local(&nodes);
        assert!(
            total >= pre_merge_count - 2,
            "Post-merge 50ms #{}: {} (expected >= {})",
            cp, total, pre_merge_count - 2
        );
    }
}

#[tokio::test]
async fn test_drain_position_max_delta_under_threshold() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![addr0]), new_world()).await.unwrap();

    let extreme_positions = vec![
        (0.5, -0.5, -11.5),
        (11.5, 24.5, 11.5),
        (6.0, 12.0, 0.0),
        (1.0, 1.0, -11.0),
        (11.0, 24.0, 10.0),
    ];
    for (i, &pos) in extreme_positions.iter().enumerate() {
        let id = (i + 1) as u64;
        node0.world.spawn_drone_at(id, pos, (1.0, 0.5, -0.3));
        node0.engine.node.manager.add_entity(Entity {
            id, pos, vel: (1.0, 0.5, -0.3),
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let pre_positions: HashMap<u64, (f32, f32, f32)> = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, e)| (*id, e.pos))
        .collect();

    let expanded = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    node1.set_cell(expanded);
    node1.world.bounds = WORLD.clone();
    simulate_drain_pair(&mut node0, &mut node1);

    for (id, pre_pos) in &pre_positions {
        let entity = node1.engine.node.manager.get_entity(*id).unwrap();
        let d = pos_distance(entity.pos, *pre_pos);
        assert!(
            d < 0.01,
            "Entity {} at extreme position {:?} was shifted by {:.4} during drain (max allowed 0.01)",
            id, pre_pos, d
        );
    }
}
