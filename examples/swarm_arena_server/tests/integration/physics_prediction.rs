use super::helpers::PhysicsTestDroneWorld;
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

fn predict_pos(pos: (f32, f32, f32), vel: (f32, f32, f32), dt: f32, ticks: u32) -> (f32, f32, f32) {
    let t = dt * ticks as f32;
    (pos.0 + vel.0 * t, pos.1 + vel.1 * t, pos.2 + vel.2 * t)
}

fn local_entity_node(nodes: &[GameLoop<PhysicsTestDroneWorld>], eid: u64) -> Option<usize> {
    nodes.iter().position(|n| {
        n.engine.node.manager.get_entity(eid)
            .is_some_and(|e| e.state == AuthorityState::Local)
    })
}

fn total_local(nodes: &[GameLoop<PhysicsTestDroneWorld>]) -> usize {
    nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::Local)
        .count()
}

#[tokio::test]
async fn test_entity_velocity_predicts_target_node() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let eid = 42u64;
    let start_pos = (11.0, 5.0, 0.0);
    let vel = (2.0, 0.0, 0.0);
    let dt = 1.0 / 128.0;

    node0.world.spawn_drone_at(eid, start_pos, vel);
    node0.engine.node.manager.add_entity(Entity {
        id: eid, pos: start_pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let ticks_to_cross = ((12.0 - 11.0) / (vel.0 * dt)).ceil() as u32 + 20;

    let mut nodes = vec![];
    for tick in 0..ticks_to_cross + 50 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();

        if tick % 10 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }

    nodes.push(node0);
    nodes.push(node1);

    let predicted = predict_pos(start_pos, vel, dt, ticks_to_cross + 50);
    assert!(predicted.0 > 12.0, "Predicted position should be past cell boundary");

    let owner = local_entity_node(&nodes, eid);
    assert_eq!(owner, Some(1), "Entity should have migrated to node1 (cell1)");

    if let Some(e) = nodes[1].engine.node.manager.get_entity(eid) {
        let pos_diff = (e.pos.0 - predicted.0).abs();
        assert!(pos_diff < 3.0, "Entity position should be near predicted. Actual: {:?}, Predicted: {:?}", e.pos, predicted);
    }
}

#[tokio::test]
async fn test_entity_stays_in_correct_node_3d() {
    let cell0 = Cell::new(0.0, 24.0, 0.0, 12.0, -12.0, 12.0);
    let cell1 = Cell::new(0.0, 24.0, 12.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![node0_addr]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let dt = 1.0 / 128.0;

    let e_stay = 10u64;
    let stay_pos = (5.0, 3.0, 0.0);
    let stay_vel = (0.0, 0.0, 0.0);
    node0.world.spawn_drone_at(e_stay, stay_pos, stay_vel);
    node0.engine.node.manager.add_entity(Entity {
        id: e_stay, pos: stay_pos, vel: stay_vel, state: AuthorityState::Local, verifying_key: None,
    });

    let e_cross = 11u64;
    let cross_pos = (5.0, 11.5, 0.0);
    let cross_vel = (0.0, 2.0, 0.0);
    node0.world.spawn_drone_at(e_cross, cross_pos, cross_vel);
    node0.engine.node.manager.add_entity(Entity {
        id: e_cross, pos: cross_pos, vel: cross_vel, state: AuthorityState::Local, verifying_key: None,
    });

    for tick in 0..200 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();
        if tick % 20 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }

    let nodes = vec![node0, node1];

    let stay_owner = local_entity_node(&nodes, e_stay);
    assert_eq!(stay_owner, Some(0), "Stationary entity should remain on node0");

    let cross_owner = local_entity_node(&nodes, e_cross);
    assert_eq!(cross_owner, Some(1), "Crossing entity should have migrated to node1 (Y>12)");
}

#[tokio::test]
async fn test_fast_entity_crosses_multiple_cells() {
    let cell0 = Cell::new(0.0, 6.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(6.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell2 = Cell::new(12.0, 18.0, 0.0, 24.0, -12.0, 12.0);
    let cell3 = Cell::new(18.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let a0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![a0]), PhysicsTestDroneWorld::new()).await.unwrap();
    let a1 = node1.engine.endpoint.local_addr().unwrap();
    let mut node2 = GameLoop::new(make_config(cell2.clone(), vec![a0, a1]), PhysicsTestDroneWorld::new()).await.unwrap();
    let a2 = node2.engine.endpoint.local_addr().unwrap();
    let mut node3 = GameLoop::new(make_config(cell3.clone(), vec![a0, a1, a2]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(200)).await;
    for _ in 0..30 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        node2.tick(0.008).await.unwrap();
        node3.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let dt = 1.0 / 128.0;
    let eid = 99u64;
    let start_pos = (1.0, 12.0, 0.0);
    let vel = (6.0, 0.0, 0.0);
    node0.world.spawn_drone_at(eid, start_pos, vel);
    node0.engine.node.manager.add_entity(Entity {
        id: eid, pos: start_pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let mut nodes_arr = [node0, node1, node2, node3];
    let initial_total = total_local(&nodes_arr);

    for tick in 0..400 {
        for n in nodes_arr.iter_mut() {
            n.tick(dt).await.unwrap();
        }
        if tick % 20 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
        let tl = total_local(&nodes_arr);
        assert!(tl >= initial_total - 1, "tick {}: lost entities ({} < {})", tick, tl, initial_total - 1);
    }

    let final_owner = local_entity_node(&nodes_arr, eid);
    assert!(final_owner.is_some(), "Fast entity should still exist somewhere");
    assert!(final_owner.unwrap() > 0, "Fast entity should have migrated past cell0");
}

#[tokio::test]
async fn test_entity_conservation_across_split() {
    let full_cell = Cell::new(0.0, 24.0, 0.0, 24.0, -12.0, 12.0);
    let half0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let half1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let a0 = node0.engine.endpoint.local_addr().unwrap();

    let dt = 1.0 / 128.0;
    let entity_count = 50;
    for i in 0..entity_count {
        let x = 1.0 + (i as f32) * (22.0 / entity_count as f32);
        let pos = (x, 12.0, 0.0);
        let vel = (0.3, 0.0, 0.0);
        let eid = (i + 1) as u64;
        node0.world.spawn_drone_at(eid, pos, vel);
        node0.engine.node.manager.add_entity(Entity {
            id: eid, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..10 {
        node0.tick(dt).await.unwrap();
    }
    let before_nodes = [&node0];
    let before_count: usize = before_nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::Local)
        .count();
    assert_eq!(before_count, entity_count, "Should start with {} entities", entity_count);

    node0.engine.node.manager.set_cell(half0.clone());
    let mut node1 = GameLoop::new(make_config(half1.clone(), vec![a0]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let mut worst_loss = 0usize;
    for tick in 0..200 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();

        let n0_local = node0.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .count();
        let n1_local = node1.engine.node.manager.entities.iter()
            .filter(|(_, e)| e.state == AuthorityState::Local)
            .count();
        let total = n0_local + n1_local;
        if entity_count > total {
            let loss = entity_count - total;
            if loss > worst_loss {
                worst_loss = loss;
            }
        }

        if tick % 20 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }

    assert!(
        worst_loss <= 2,
        "Entity conservation violated: worst loss was {} (from {})",
        worst_loss, entity_count
    );
}

#[tokio::test]
async fn test_boundary_entity_with_zero_velocity_stays_put() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let a0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![a0]), PhysicsTestDroneWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let dt = 1.0 / 128.0;
    let eid = 77u64;
    let boundary_pos = (11.9, 12.0, 0.0);
    let zero_vel = (0.0, 0.0, 0.0);
    node0.world.spawn_drone_at(eid, boundary_pos, zero_vel);
    node0.engine.node.manager.add_entity(Entity {
        id: eid, pos: boundary_pos, vel: zero_vel, state: AuthorityState::Local, verifying_key: None,
    });

    let mut lost = false;
    for tick in 0..200 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();

        let n0_has = node0.engine.node.manager.get_entity(eid)
            .is_some_and(|e| e.state == AuthorityState::Local);
        let n1_has = node1.engine.node.manager.get_entity(eid)
            .is_some_and(|e| e.state == AuthorityState::Local);

        if !n0_has && !n1_has {
            let n0_any = node0.engine.node.manager.get_entity(eid).is_some();
            let n1_any = node1.engine.node.manager.get_entity(eid).is_some();
            if !n0_any && !n1_any {
                lost = true;
                break;
            }
        }

        if tick % 20 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }
    assert!(!lost, "Boundary entity with zero velocity was lost");
}

#[tokio::test]
async fn test_returning_entity_handoff_round_trip() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let a0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![a0]), PhysicsTestDroneWorld::new()).await.unwrap();
    node1.engine.node.manager.set_cell(cell1.clone());

    sleep(Duration::from_millis(150)).await;
    for _ in 0..30 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let dt = 1.0 / 128.0;
    let eid = 55u64;
    let start_pos = (10.0, 12.0, 0.0);
    let forward_vel = (3.0, 0.0, 0.0);
    node0.world.spawn_drone_at(eid, start_pos, forward_vel);
    node0.engine.node.manager.add_entity(Entity {
        id: eid, pos: start_pos, vel: forward_vel, state: AuthorityState::Local, verifying_key: None,
    });

    let mut migrated_to_1 = false;
    for tick in 0..300 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();

        if node1.engine.node.manager.get_entity(eid)
            .is_some_and(|e| e.state == AuthorityState::Local)
        {
            migrated_to_1 = true;
            break;
        }

        if tick % 10 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }
    assert!(migrated_to_1, "Entity should have migrated to node1");

    if let Some(e) = node1.engine.node.manager.get_entity_mut(eid) {
        e.vel = (-3.0, 0.0, 0.0);
    }
    if let Some(state) = node1.world.drones.get_mut(&eid) {
        state.1 = (-3.0, 0.0, 0.0);
    }

    for _ in 0..10 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();
        sleep(Duration::from_millis(3)).await;
    }

    let mut returned_to_0 = false;
    for tick in 0..400 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();

        if node0.engine.node.manager.get_entity(eid)
            .is_some_and(|e| e.state == AuthorityState::Local)
        {
            returned_to_0 = true;
            break;
        }

        if tick % 10 == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    }
    assert!(returned_to_0, "Entity should have returned to node0");
}
