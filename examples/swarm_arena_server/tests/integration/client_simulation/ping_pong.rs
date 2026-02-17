use super::helpers::*;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::cell::Cell;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::{GameLoop, GameWorld};

const TICKS_10MS: usize = 2;

#[tokio::test]
async fn test_grace_period_prevents_immediate_bounce_back() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    node1.world.on_entity_arrived(42, (12.2, 12.0, 0.0), (-2.0, 0.5, 0.1));
    node1.engine.node.manager.add_entity(Entity {
        id: 42, pos: (12.2, 12.0, 0.0), vel: (-2.0, 0.5, 0.1),
        state: AuthorityState::Local, verifying_key: None,
    });
    node1.engine.node.manager.mark_arrived(42);

    let mut nodes = vec![node0, node1];
    let mut entity_on_node1_count = 0;

    for _tick in 0..32 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        if let Some(e) = nodes[1].engine.node.manager.get_entity(42) {
            if e.state == AuthorityState::Local {
                entity_on_node1_count += 1;
            }
        }
    }

    assert_eq!(
        entity_on_node1_count, 32,
        "Entity should stay on node1 for all 32 grace ticks, was there for {}",
        entity_on_node1_count
    );
}

#[tokio::test]
async fn test_no_rapid_ownership_flip_at_boundary() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..5 {
        let id = (i + 1) as u64;
        let x = 11.0 + (i as f32) * 0.3;
        let vel = (1.5, 0.0, 0.0);
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let mut ownership_log: HashMap<u64, Vec<usize>> = HashMap::new();
    for i in 1..=5u64 { ownership_log.insert(i, Vec::new()); }

    for _tick in 0..512 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        for id in 1..=5u64 {
            for (ni, node) in nodes.iter().enumerate() {
                if let Some(e) = node.engine.node.manager.get_entity(id) {
                    if e.state == AuthorityState::Local {
                        if let Some(log) = ownership_log.get_mut(&id) {
                            log.push(ni);
                        }
                    }
                }
            }
        }
    }

    for (id, log) in &ownership_log {
        if log.is_empty() { continue; }
        let mut transitions = 0;
        for i in 1..log.len() {
            if log[i] != log[i - 1] {
                transitions += 1;
            }
        }
        assert!(
            transitions <= 6,
            "Entity {} had {} ownership transitions in 512 ticks (expected ≤ 6, ping-pong detected)",
            id, transitions
        );
    }
}

#[tokio::test]
async fn test_boundary_drone_settles_within_grace_window() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node0 = GameLoop::new(make_config(cell, vec![]), new_world()).await.unwrap();

    node0.world.spawn_drone_at(1, (12.0, 12.0, 0.0), (0.1, 0.0, 0.0));
    node0.engine.node.manager.add_entity(Entity {
        id: 1, pos: (12.0, 12.0, 0.0), vel: (0.1, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    node0.engine.node.manager.mark_arrived(1);

    let mut exits_during_grace = 0;
    for _tick in 0..32 {
        node0.tick(DT).await.unwrap();
        sleep(Duration::from_millis(1)).await;

        let candidates = node0.engine.node.manager.update(DT);
        if candidates.iter().any(|(id, _)| *id == 1) {
            exits_during_grace += 1;
        }
    }

    assert_eq!(exits_during_grace, 0, "Entity should not be exit candidate during 32-tick grace period");
}

#[tokio::test]
async fn test_per_tick_handoff_count_bounded_two_nodes() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..10 {
        let id = (i + 1) as u64;
        let x = 9.0 + (i as f32) * 0.5;
        let vel = (2.0, 0.0, 0.0);
        node0.world.spawn_drone_at(id, (x, 12.0, 0.0), vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0, 0.0), vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];
    let mut per_entity_handoffs: HashMap<u64, usize> = HashMap::new();
    let mut prev_owners: HashMap<u64, usize> = HashMap::new();

    for id in 1..=10u64 {
        per_entity_handoffs.insert(id, 0);
        prev_owners.insert(id, 0);
    }

    for _tick in 0..512 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;

        for id in 1..=10u64 {
            for (ni, node) in nodes.iter().enumerate() {
                if let Some(e) = node.engine.node.manager.get_entity(id) {
                    if e.state == AuthorityState::Local {
                        if let Some(prev) = prev_owners.get(&id) {
                            if *prev != ni {
                                *per_entity_handoffs.entry(id).or_insert(0) += 1;
                            }
                        }
                        prev_owners.insert(id, ni);
                    }
                }
            }
        }
    }

    for (id, count) in &per_entity_handoffs {
        assert!(
            *count <= 4,
            "Entity {} was handed off {} times in 512 ticks (ping-pong: expected ≤ 4)",
            id, count
        );
    }
}

#[tokio::test]
async fn test_every_10ms_no_ownership_oscillation_after_handoff() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    for i in 0..4 {
        let id = (i + 1) as u64;
        let x = 11.0;
        let vel = (3.0, 0.0, 0.0);
        node0.world.spawn_drone_at(id, (x, 12.0 + i as f32, 0.0), vel);
        node0.engine.node.manager.add_entity(Entity {
            id, pos: (x, 12.0 + i as f32, 0.0), vel,
            state: AuthorityState::Local, verifying_key: None,
        });
    }

    let mut nodes = vec![node0, node1];

    for _ in 0..200 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;
    }

    let mut node1_arrivals: Vec<u64> = Vec::new();
    for (id, e) in &nodes[1].engine.node.manager.entities {
        if e.state == AuthorityState::Local && *id <= 4 {
            node1_arrivals.push(*id);
        }
    }

    let mut ownership_after: HashMap<u64, Vec<usize>> = HashMap::new();
    for &id in &node1_arrivals { ownership_after.insert(id, Vec::new()); }

    for _cp in 0..20 {
        for _ in 0..TICKS_10MS {
            for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
            sleep(Duration::from_millis(1)).await;
        }

        for &id in &node1_arrivals {
            for (ni, node) in nodes.iter().enumerate() {
                if let Some(e) = node.engine.node.manager.get_entity(id) {
                    if e.state == AuthorityState::Local {
                        if let Some(log) = ownership_after.get_mut(&id) {
                            log.push(ni);
                        }
                    }
                }
            }
        }
    }

    for (id, log) in &ownership_after {
        if log.is_empty() { continue; }
        let transitions: usize = log.windows(2).filter(|w| w[0] != w[1]).count();
        assert!(
            transitions <= 2,
            "Entity {} oscillated {} times across 20 10ms checkpoints after arriving on node1",
            id, transitions
        );
    }
}

#[tokio::test]
async fn test_grace_period_32_ticks_exact() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut mgr = zeus_node::entity_manager::EntityManager::new_3d(cell, 1.0);

    mgr.add_entity(Entity {
        id: 1, pos: (25.5, 12.0, 0.0), vel: (2.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });
    mgr.mark_arrived(1);

    for tick in 0..31 {
        mgr.tick_grace();
        let candidates = mgr.update(DT);
        assert!(
            candidates.is_empty(),
            "Tick {}: entity should be protected by grace period (31 ticks remain)",
            tick
        );
    }

    mgr.tick_grace();
    let candidates = mgr.update(DT);
    assert!(
        !candidates.is_empty(),
        "After 32 grace ticks, entity should be an exit candidate"
    );
}

#[tokio::test]
async fn test_velocity_direction_check_prevents_wrong_handoff() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0, vec![]), new_world()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let node1 = GameLoop::new(make_config(cell1, vec![addr0]), new_world()).await.unwrap();

    node0.world.spawn_drone_at(1, (12.5, 12.0, 0.0), (-3.0, 0.0, 0.0));
    node0.engine.node.manager.add_entity(Entity {
        id: 1, pos: (12.5, 12.0, 0.0), vel: (-3.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let mut nodes = vec![node0, node1];

    for _tick in 0..64 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(1)).await;
    }

    let on_node0 = nodes[0].engine.node.manager.get_entity(1)
        .is_some_and(|e| e.state == AuthorityState::Local);
    assert!(
        on_node0,
        "Entity at x=12.5 moving -x should stay on node0 (moving back into cell)"
    );
}
