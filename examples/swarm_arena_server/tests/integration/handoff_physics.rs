use super::helpers::PhysicsTestDroneWorld;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::cell::Cell;
use zeus_node::engine::ZeusConfig;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::GameLoop;

fn make_config(cell: Cell, peers: Vec<std::net::SocketAddr>) -> ZeusConfig {
    ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: peers,
        boundary: cell.x_max,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: cell.x_min,
        cell: Some(cell),
    }
}

fn dist(a: (f32, f32, f32), b: (f32, f32, f32)) -> f32 {
    ((a.0 - b.0).powi(2) + (a.1 - b.1).powi(2) + (a.2 - b.2).powi(2)).sqrt()
}

fn vel_mag(v: (f32, f32, f32)) -> f32 {
    (v.0 * v.0 + v.1 * v.1 + v.2 * v.2).sqrt()
}

fn local_entity_node(nodes: &[GameLoop<PhysicsTestDroneWorld>], eid: u64) -> Option<usize> {
    nodes.iter().position(|n| {
        n.engine.node.manager.get_entity(eid)
            .is_some_and(|e| e.state == AuthorityState::Local)
    })
}

#[tokio::test]
async fn test_handoff_preserves_velocity_magnitude() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (11.5, 12.0, 0.0);
    let start_vel = (3.0, 0.0, 0.5);
    node0.engine.node.manager.add_entity(Entity {
        id: 42,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(42, start_pos, start_vel);

    let original_speed = vel_mag(start_vel);
    let mut pre_handoff_vel = start_vel;
    let mut post_handoff_vel: Option<(f32, f32, f32)> = None;
    let mut handoff_happened = false;

    for tick in 0..300 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        if !handoff_happened {
            if let Some(e) = node0.engine.node.manager.get_entity(42) {
                if e.state == AuthorityState::Local {
                    pre_handoff_vel = e.vel;
                }
            }
        }

        if let Some(e) = node1.engine.node.manager.get_entity(42) {
            if e.state == AuthorityState::Local && !handoff_happened {
                handoff_happened = true;
                post_handoff_vel = Some(e.vel);
                break;
            }
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    assert!(handoff_happened, "Entity 42 should have been handed off to node1");

    let post_vel = post_handoff_vel.unwrap();
    let pre_speed = vel_mag(pre_handoff_vel);
    let post_speed = vel_mag(post_vel);
    let speed_delta = (post_speed - pre_speed).abs();
    assert!(
        speed_delta < 1.5,
        "Velocity magnitude should be approximately preserved. pre={:.2} post={:.2} delta={:.2}",
        pre_speed, post_speed, speed_delta
    );
    assert!(
        post_vel.0 > 0.0,
        "X velocity should remain positive (entity was moving +X). got vx={:.2}",
        post_vel.0
    );
}

#[tokio::test]
async fn test_handoff_position_continuity() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (11.0, 12.0, 0.0);
    let start_vel = (2.0, 0.0, 0.0);
    node0.engine.node.manager.add_entity(Entity {
        id: 100,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(100, start_pos, start_vel);

    let mut last_pos_on_source: Option<(f32, f32, f32)> = None;
    let mut first_pos_on_target: Option<(f32, f32, f32)> = None;
    let mut handoff_tick: Option<u32> = None;

    for tick in 0..400 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        if handoff_tick.is_none() {
            if let Some(e) = node0.engine.node.manager.get_entity(100) {
                if e.state == AuthorityState::Local || e.state == AuthorityState::HandoffOut {
                    last_pos_on_source = Some(e.pos);
                }
            }
            if let Some(e) = node1.engine.node.manager.get_entity(100) {
                if e.state == AuthorityState::Local {
                    first_pos_on_target = Some(e.pos);
                    handoff_tick = Some(tick);
                }
            }
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    assert!(handoff_tick.is_some(), "Handoff should have completed");
    let src_pos = last_pos_on_source.unwrap();
    let tgt_pos = first_pos_on_target.unwrap();
    let gap = dist(src_pos, tgt_pos);
    assert!(
        gap < 2.0,
        "Position gap during handoff should be small. source=({:.2},{:.2},{:.2}) target=({:.2},{:.2},{:.2}) gap={:.2}",
        src_pos.0, src_pos.1, src_pos.2, tgt_pos.0, tgt_pos.1, tgt_pos.2, gap
    );
}

#[tokio::test]
async fn test_entity_not_lost_during_handoff() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let num_entities = 5;
    for i in 0..num_entities {
        let id = 200 + i;
        let x = 11.0 + (i as f32) * 0.2;
        let pos = (x, 12.0, 0.0);
        let vel = (2.5, 0.0, ((i as f32) - 2.0) * 0.3);
        node0.engine.node.manager.add_entity(Entity {
            id,
            pos,
            vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
        node0.world.spawn_drone_at(id, pos, vel);
    }

    for tick in 0..500 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    let mut found = 0u64;
    for i in 0..num_entities {
        let id = 200 + i;
        let on_0 = node0.engine.node.manager.get_entity(id)
            .is_some_and(|e| e.state == AuthorityState::Local);
        let on_1 = node1.engine.node.manager.get_entity(id)
            .is_some_and(|e| e.state == AuthorityState::Local);
        if on_0 || on_1 { found += 1; }
    }

    assert_eq!(
        found, num_entities,
        "All {} entities should be Local on exactly one node, found {}",
        num_entities, found
    );
}

#[tokio::test]
async fn test_handoff_completes_within_time_budget() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (11.8, 12.0, 0.0);
    let start_vel = (3.0, 0.0, 0.0);
    node0.engine.node.manager.add_entity(Entity {
        id: 500,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(500, start_pos, start_vel);

    let start_time = std::time::Instant::now();
    let mut handoff_time: Option<Duration> = None;

    for tick in 0..300 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        if let Some(e) = node1.engine.node.manager.get_entity(500) {
            if e.state == AuthorityState::Local {
                handoff_time = Some(start_time.elapsed());
                break;
            }
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    let elapsed = handoff_time.expect("Handoff should complete");
    assert!(
        elapsed < Duration::from_millis(500),
        "Handoff should complete within 500ms, took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_velocity_direction_preserved_across_handoff() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (11.5, 12.0, 0.0);
    let start_vel = (2.0, 1.0, -0.5);
    node0.engine.node.manager.add_entity(Entity {
        id: 600,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(600, start_pos, start_vel);

    let mut pre_vel = start_vel;
    let mut post_vel: Option<(f32, f32, f32)> = None;

    for tick in 0..300 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        if post_vel.is_none() {
            if let Some(e) = node0.engine.node.manager.get_entity(600) {
                if e.state == AuthorityState::Local {
                    pre_vel = e.vel;
                }
            }
            if let Some(e) = node1.engine.node.manager.get_entity(600) {
                if e.state == AuthorityState::Local {
                    post_vel = Some(e.vel);
                    break;
                }
            }
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    let pv = post_vel.expect("Handoff should complete");
    let dot = pre_vel.0 * pv.0 + pre_vel.1 * pv.1 + pre_vel.2 * pv.2;
    let pre_mag = vel_mag(pre_vel);
    let post_mag = vel_mag(pv);

    if pre_mag > 0.01 && post_mag > 0.01 {
        let cos_angle = dot / (pre_mag * post_mag);
        assert!(
            cos_angle > 0.5,
            "Velocity direction should be roughly preserved. cos_angle={:.3} pre=({:.2},{:.2},{:.2}) post=({:.2},{:.2},{:.2})",
            cos_angle, pre_vel.0, pre_vel.1, pre_vel.2, pv.0, pv.1, pv.2
        );
    }
}

#[tokio::test]
async fn test_multiple_simultaneous_handoffs_no_loss() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let count = 5u64;
    for i in 0..count {
        let id = 700 + i;
        let x = 11.5 + (i as f32) * 0.1;
        let z = ((i as f32) - 2.0) * 0.3;
        let pos = (x, 12.0, z);
        let vel = (1.0, 0.0, 0.0);
        node0.engine.node.manager.add_entity(Entity {
            id,
            pos,
            vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
        node0.world.spawn_drone_at(id, pos, vel);
    }

    let mut handoff_done = false;
    for tick in 0..400 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        if !handoff_done {
            let arrived = (0..count).filter(|i| {
                node1.engine.node.manager.get_entity(700 + i)
                    .is_some_and(|e| e.state == AuthorityState::Local)
            }).count();
            if arrived == count as usize {
                handoff_done = true;
            }
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    assert!(handoff_done, "All entities should have been handed off within 400 ticks");

    let mut on_node0 = 0u64;
    let mut on_node1 = 0u64;
    for i in 0..count {
        let id = 700 + i;
        let n0_local = node0.engine.node.manager.get_entity(id)
            .is_some_and(|e| e.state == AuthorityState::Local);
        let n1_local = node1.engine.node.manager.get_entity(id)
            .is_some_and(|e| e.state == AuthorityState::Local);
        if n0_local { on_node0 += 1; }
        if n1_local { on_node1 += 1; }
    }

    let total = on_node0 + on_node1;
    assert_eq!(
        total, count,
        "All {} entities should be Local on exactly one node. on_node0={} on_node1={}",
        count, on_node0, on_node1
    );
    assert!(
        on_node1 > 0,
        "At least some entities should have migrated to node1"
    );
}

#[tokio::test]
async fn test_commit_carries_latest_position() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (11.5, 12.0, 0.0);
    let start_vel = (3.0, 0.5, -0.3);
    node0.engine.node.manager.add_entity(Entity {
        id: 900,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(900, start_pos, start_vel);

    let mut source_pos_at_departure: Option<(f32, f32, f32)> = None;
    let mut target_pos_at_arrival: Option<(f32, f32, f32)> = None;
    let mut source_vel_at_departure: Option<(f32, f32, f32)> = None;
    let mut target_vel_at_arrival: Option<(f32, f32, f32)> = None;

    for tick in 0..400 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        if let Some(e) = node0.engine.node.manager.get_entity(900) {
            if e.state == AuthorityState::HandoffOut || e.state == AuthorityState::Remote {
                source_pos_at_departure = Some(e.pos);
                source_vel_at_departure = Some(e.vel);
            }
        }

        if target_pos_at_arrival.is_none() {
            if let Some(e) = node1.engine.node.manager.get_entity(900) {
                if e.state == AuthorityState::Local {
                    target_pos_at_arrival = Some(e.pos);
                    target_vel_at_arrival = Some(e.vel);
                    break;
                }
            }
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    let src_pos = source_pos_at_departure.expect("Source should have had entity in transit");
    let tgt_pos = target_pos_at_arrival.expect("Target should have received entity as Local");
    let src_vel = source_vel_at_departure.unwrap();
    let tgt_vel = target_vel_at_arrival.unwrap();

    let pos_gap = dist(src_pos, tgt_pos);
    assert!(
        pos_gap < 0.5,
        "Commit should carry latest position. source=({:.3},{:.3},{:.3}) target=({:.3},{:.3},{:.3}) gap={:.3}",
        src_pos.0, src_pos.1, src_pos.2, tgt_pos.0, tgt_pos.1, tgt_pos.2, pos_gap
    );

    let vel_gap = dist(src_vel, tgt_vel);
    assert!(
        vel_gap < 0.5,
        "Commit should carry latest velocity. source=({:.3},{:.3},{:.3}) target=({:.3},{:.3},{:.3}) gap={:.3}",
        src_vel.0, src_vel.1, src_vel.2, tgt_vel.0, tgt_vel.1, tgt_vel.2, vel_gap
    );
}

#[tokio::test]
async fn test_client_perspective_no_backward_jumps() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (10.0, 12.0, 0.0);
    let start_vel = (2.5, 0.0, 0.0);
    node0.engine.node.manager.add_entity(Entity {
        id: 950,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(950, start_pos, start_vel);

    let mut client_positions: Vec<f32> = Vec::new();
    let mut handoff_done = false;

    for tick in 0..400 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        let mut best_x: Option<f32> = None;

        if let Some(e) = node0.engine.node.manager.get_entity(950) {
            if e.state == AuthorityState::Local || e.state == AuthorityState::HandoffOut {
                best_x = Some(e.pos.0);
            }
        }

        if best_x.is_none() {
            if let Some(e) = node1.engine.node.manager.get_entity(950) {
                if e.state == AuthorityState::Local {
                    best_x = Some(e.pos.0);
                    handoff_done = true;
                }
            }
        }

        if let Some(x) = best_x {
            client_positions.push(x);
        }

        if handoff_done && client_positions.len() > 30 {
            break;
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    assert!(handoff_done, "Handoff should complete");

    let mut large_backward_jumps = 0;
    for w in client_positions.windows(2) {
        let delta = w[1] - w[0];
        if delta < -0.3 {
            large_backward_jumps += 1;
        }
    }
    assert!(
        large_backward_jumps == 0,
        "Client should see no large backward jumps (>0.3). jumps={} first_positions={:?}",
        large_backward_jumps, &client_positions[..client_positions.len().min(30)]
    );
}

#[tokio::test]
async fn test_velocity_sign_preserved_through_commit() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let test_cases: Vec<(u64, (f32, f32, f32), (f32, f32, f32))> = vec![
        (1001, (11.5, 12.0, 0.0), (2.0, 1.5, -1.0)),
        (1002, (11.5, 12.0, 3.0), (3.0, -0.5, 0.8)),
        (1003, (11.5, 8.0, -2.0), (1.5, 0.3, 0.0)),
    ];

    for (id, pos, vel) in &test_cases {
        node0.engine.node.manager.add_entity(Entity {
            id: *id,
            pos: *pos,
            vel: *vel,
            state: AuthorityState::Local,
            verifying_key: None,
        });
        node0.world.spawn_drone_at(*id, *pos, *vel);
    }

    let mut arrival_vels: HashMap<u64, (f32, f32, f32)> = HashMap::new();

    for tick in 0..400 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        for (id, _, _) in &test_cases {
            if !arrival_vels.contains_key(id) {
                if let Some(e) = node1.engine.node.manager.get_entity(*id) {
                    if e.state == AuthorityState::Local {
                        arrival_vels.insert(*id, e.vel);
                    }
                }
            }
        }

        if arrival_vels.len() == test_cases.len() {
            break;
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    for (id, _, start_vel) in &test_cases {
        let arrival_vel = arrival_vels.get(id).unwrap_or_else(|| panic!("Entity {} should have arrived", id));
        if start_vel.0.abs() > 0.5 {
            assert!(
                arrival_vel.0.signum() == start_vel.0.signum(),
                "Entity {} vx sign mismatch: start={:.2} arrival={:.2}", id, start_vel.0, arrival_vel.0
            );
        }
        if start_vel.1.abs() > 0.5 {
            assert!(
                arrival_vel.1.signum() == start_vel.1.signum(),
                "Entity {} vy sign mismatch: start={:.2} arrival={:.2}", id, start_vel.1, arrival_vel.1
            );
        }
        if start_vel.2.abs() > 0.5 {
            assert!(
                arrival_vel.2.signum() == start_vel.2.signum(),
                "Entity {} vz sign mismatch: start={:.2} arrival={:.2}", id, start_vel.2, arrival_vel.2
            );
        }
    }
}

#[tokio::test]
async fn test_entity_position_advances_monotonically_during_handoff() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;
        sleep(Duration::from_millis(2)).await;
    }

    let start_pos = (10.0, 12.0, 0.0);
    let start_vel = (2.0, 0.0, 0.0);
    node0.engine.node.manager.add_entity(Entity {
        id: 800,
        pos: start_pos,
        vel: start_vel,
        state: AuthorityState::Local,
        verifying_key: None,
    });
    node0.world.spawn_drone_at(800, start_pos, start_vel);

    let mut positions: Vec<f32> = Vec::new();
    let mut handoff_done = false;

    for tick in 0..400 {
        let _ = node0.tick(1.0 / 128.0).await;
        let _ = node1.tick(1.0 / 128.0).await;

        let x = if let Some(e) = node0.engine.node.manager.get_entity(800) {
            if e.state == AuthorityState::Local || e.state == AuthorityState::HandoffOut {
                Some(e.pos.0)
            } else { None }
        } else { None };

        let x = x.or_else(|| {
            node1.engine.node.manager.get_entity(800)
                .and_then(|e| if e.state == AuthorityState::Local { Some(e.pos.0) } else { None })
        });

        if let Some(xv) = x {
            positions.push(xv);
        }

        if !handoff_done {
            if let Some(e) = node1.engine.node.manager.get_entity(800) {
                if e.state == AuthorityState::Local {
                    handoff_done = true;
                }
            }
        }

        if handoff_done && positions.len() > 20 {
            break;
        }

        if tick % 4 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    assert!(handoff_done, "Handoff should complete");
    assert!(positions.len() > 10, "Should have tracked multiple positions");

    let mut reversals = 0;
    for w in positions.windows(2) {
        if w[1] < w[0] - 0.5 {
            reversals += 1;
        }
    }
    assert!(
        reversals <= 2,
        "X position should advance mostly monotonically for +X velocity. reversals={} positions={:?}",
        reversals, &positions[..positions.len().min(20)]
    );
}
