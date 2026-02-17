use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use zeus_node::cell::Cell;
use zeus_node::engine::{decode_compact_client, ZeusConfig};
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::GameLoop;

use super::helpers::PhysicsTestDroneWorld;

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

#[allow(dead_code)]
fn build_spawn_at(count: u16, pos: (f32, f32, f32)) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.push(0xDD);
    buf.push(0x02);
    buf.push((count >> 8) as u8);
    buf.push((count & 0xFF) as u8);
    buf.extend_from_slice(&pos.0.to_le_bytes());
    buf.extend_from_slice(&pos.1.to_le_bytes());
    buf.extend_from_slice(&pos.2.to_le_bytes());
    buf
}

#[allow(dead_code)]
fn build_despawn(count: u16) -> Vec<u8> {
    vec![0xDE, (count >> 8) as u8, (count & 0xFF) as u8]
}

fn parse_cells(data: &[u8]) -> Vec<Cell> {
    let mut cells = Vec::new();
    if data.len() < 2 || data[0] != 0xEE { return cells; }
    let count = data[1] as usize;
    let mut off = 2;
    for _ in 0..count {
        if off + 24 > data.len() { break; }
        let x_min = f32::from_le_bytes(data[off..off+4].try_into().unwrap());
        let x_max = f32::from_le_bytes(data[off+4..off+8].try_into().unwrap());
        let y_min = f32::from_le_bytes(data[off+8..off+12].try_into().unwrap());
        let y_max = f32::from_le_bytes(data[off+12..off+16].try_into().unwrap());
        let z_min = f32::from_le_bytes(data[off+16..off+20].try_into().unwrap());
        let z_max = f32::from_le_bytes(data[off+20..off+24].try_into().unwrap());
        cells.push(Cell::new(x_min, x_max, y_min, y_max, z_min, z_max));
        off += 24;
    }
    cells
}

fn parse_status(data: &[u8]) -> Option<(u16, u8)> {
    if data.len() < 4 || data[0] != 0xAA { return None; }
    let ec = ((data[1] as u16) << 8) | (data[2] as u16);
    let nodes = data[3];
    Some((ec, nodes))
}

fn parse_removals(data: &[u8]) -> Vec<u64> {
    let mut ids = Vec::new();
    if data.len() < 3 || data[0] != 0xDF { return ids; }
    let count = ((data[1] as u16) << 8) | (data[2] as u16);
    let mut off = 3;
    for _ in 0..count {
        if off + 8 > data.len() { break; }
        ids.push(u64::from_le_bytes(data[off..off+8].try_into().unwrap()));
        off += 8;
    }
    ids
}

async fn make_client_conn(server_addr: std::net::SocketAddr) -> quinn::Connection {
    let (endpoint, _) = zeus_transport::make_promiscuous_endpoint(
        "127.0.0.1:0".parse().unwrap()
    ).unwrap();
    endpoint.connect(server_addr, "localhost").unwrap().await.unwrap()
}

struct ClientState {
    entities: HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>,
    cells: Vec<Cell>,
    entity_count_status: u16,
    node_count_status: u8,
    removed_ids: Vec<u64>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            entities: HashMap::new(),
            cells: Vec::new(),
            entity_count_status: 0,
            node_count_status: 0,
            removed_ids: Vec::new(),
        }
    }

    fn process_datagram(&mut self, data: &[u8]) {
        if data.is_empty() { return; }
        match data[0] {
            0xCC => {
                for (id, pos, vel) in decode_compact_client(data) {
                    self.entities.insert(id, (pos, vel));
                }
            }
            0xEE => {
                self.cells = parse_cells(data);
            }
            0xAA => {
                if let Some((ec, nodes)) = parse_status(data) {
                    self.entity_count_status = ec;
                    self.node_count_status = nodes;
                }
            }
            0xDF => {
                let ids = parse_removals(data);
                for id in &ids {
                    self.entities.remove(id);
                }
                self.removed_ids.extend(ids);
            }
            _ => {}
        }
    }
}

#[allow(dead_code)]
async fn drain_client(conn: &quinn::Connection, state: &mut ClientState) {
    loop {
        match conn.read_datagram().await {
            Ok(dg) => state.process_datagram(&dg),
            Err(_) => break,
        }
    }
}

async fn drain_client_timeout(conn: &quinn::Connection, state: &mut ClientState, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() { break; }
        tokio::select! {
            res = conn.read_datagram() => {
                match res {
                    Ok(dg) => state.process_datagram(&dg),
                    Err(_) => break,
                }
            }
            _ = sleep(remaining) => break,
        }
    }
}

#[tokio::test]
async fn test_client_receives_wireframe_on_connect() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node = GameLoop::new(make_config(cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr = node.engine.endpoint.local_addr().unwrap();

    for _ in 0..5 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let client_conn = make_client_conn(addr).await;
    let mut state = ClientState::new();

    for _ in 0..20 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(100)).await;

    assert!(!state.cells.is_empty(), "Client should receive cell wireframe immediately on connect");
    let c = &state.cells[0];
    assert!((c.x_min - cell.x_min).abs() < 0.1, "Cell x_min mismatch");
    assert!((c.x_max - cell.x_max).abs() < 0.1, "Cell x_max mismatch");
}

#[tokio::test]
async fn test_client_receives_status_on_connect() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node = GameLoop::new(make_config(cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr = node.engine.endpoint.local_addr().unwrap();

    for _ in 0..5 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let client_conn = make_client_conn(addr).await;
    let mut state = ClientState::new();

    for _ in 0..20 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(100)).await;

    assert!(state.node_count_status >= 1, "Should report at least 1 active node");
}

#[tokio::test]
async fn test_client_spawn_entities_and_receive_positions() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node = GameLoop::new(make_config(cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr = node.engine.endpoint.local_addr().unwrap();

    for _ in 0..10 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let client_conn = make_client_conn(addr).await;
    let mut state = ClientState::new();

    for _ in 0..5 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for i in 1..=20u64 {
        let pos = (5.0 + i as f32 * 0.5, 12.0, 0.0);
        let vel = (0.1, 0.0, 0.0);
        node.world.spawn_drone_at(i, pos, vel);
        node.engine.node.manager.add_entity(Entity {
            id: i, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..50 {
        node.tick(1.0 / 128.0).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }

    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(200)).await;

    assert!(
        state.entities.len() >= 15,
        "Client should see most entities: got {}",
        state.entities.len()
    );
}

#[tokio::test]
async fn test_client_entity_positions_change_over_time() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node = GameLoop::new(make_config(cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr = node.engine.endpoint.local_addr().unwrap();

    let client_conn = make_client_conn(addr).await;

    for _ in 0..10 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let eid = 42u64;
    let start_pos = (5.0, 12.0, 0.0);
    let vel = (2.0, 0.0, 0.0);
    node.world.spawn_drone_at(eid, start_pos, vel);
    node.engine.node.manager.add_entity(Entity {
        id: eid, pos: start_pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let mut state1 = ClientState::new();
    for _ in 0..30 {
        node.tick(1.0 / 128.0).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    drain_client_timeout(&client_conn, &mut state1, Duration::from_millis(100)).await;

    let pos_snap1 = state1.entities.get(&eid).map(|(p, _)| *p);

    let mut state2 = ClientState::new();
    state2.entities = state1.entities;
    for _ in 0..100 {
        node.tick(1.0 / 128.0).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    drain_client_timeout(&client_conn, &mut state2, Duration::from_millis(100)).await;

    let pos_snap2 = state2.entities.get(&eid).map(|(p, _)| *p);

    assert!(pos_snap1.is_some(), "Entity should appear in first snapshot");
    assert!(pos_snap2.is_some(), "Entity should appear in second snapshot");

    let p1 = pos_snap1.unwrap();
    let p2 = pos_snap2.unwrap();
    let dx = (p2.0 - p1.0).abs();
    assert!(
        dx > 0.01,
        "Entity position should change over time: snap1={:?} snap2={:?}",
        p1, p2
    );
}

#[tokio::test]
async fn test_client_sees_wireframe_update_after_split() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let keep = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let new = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();

    let client_conn = make_client_conn(addr0).await;
    let mut state = ClientState::new();

    for _ in 0..10 {
        node0.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(100)).await;
    assert!(!state.cells.is_empty(), "Should have initial wireframe");

    node0.engine.node.manager.set_cell(keep.clone());
    node0.set_cell(keep.clone());
    node0.broadcast_cells(&[keep.clone()]);

    let mut node1 = GameLoop::new(make_config(new.clone(), vec![addr0]), PhysicsTestDroneWorld::new()).await.unwrap();

    for _ in 0..30 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(200)).await;

    assert!(
        state.cells.iter().any(|c| (c.x_max - 12.0).abs() < 1.0),
        "Client should see updated (shrunk) wireframe after split. Got: {:?}",
        state.cells
    );
}

#[tokio::test]
async fn test_client_despawn_removes_entities() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node = GameLoop::new(make_config(cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr = node.engine.endpoint.local_addr().unwrap();

    let client_conn = make_client_conn(addr).await;
    let mut state = ClientState::new();

    for _ in 0..10 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for i in 1..=10u64 {
        let pos = (5.0 + i as f32, 12.0, 0.0);
        let vel = (0.0, 0.0, 0.0);
        node.world.spawn_drone_at(i, pos, vel);
        node.engine.node.manager.add_entity(Entity {
            id: i, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..40 {
        node.tick(1.0 / 128.0).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(100)).await;

    let count_before = state.entities.len();
    assert!(count_before >= 5, "Client should see entities before despawn: got {}", count_before);

    let mut to_remove = Vec::new();
    for i in 1..=5u64 {
        node.engine.node.manager.remove_entity(i);
        node.world.drones.remove(&i);
        node.world.local_ids.remove(&i);
        to_remove.push(i);
    }
    node.broadcast_entity_removals(&to_remove);
    node.broadcast_status();

    for _ in 0..20 {
        node.tick(1.0 / 128.0).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    drain_client_timeout(&client_conn, &mut state, Duration::from_millis(100)).await;

    assert!(
        state.removed_ids.len() >= 5,
        "Client should receive removal for 5 entities: got {}",
        state.removed_ids.len()
    );
    for id in &to_remove {
        assert!(
            !state.entities.contains_key(id),
            "Entity {} should be removed from client state",
            id
        );
    }
}

#[tokio::test]
async fn test_two_node_client_sees_entities_from_both() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![addr0]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr1 = node1.engine.endpoint.local_addr().unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for i in 1..=5u64 {
        let pos = (3.0 + i as f32, 12.0, 0.0);
        node0.world.spawn_drone_at(i, pos, (0.0, 0.0, 0.0));
        node0.engine.node.manager.add_entity(Entity {
            id: i, pos, vel: (0.0, 0.0, 0.0), state: AuthorityState::Local, verifying_key: None,
        });
    }
    for i in 101..=105u64 {
        let pos = (15.0 + (i - 100) as f32, 12.0, 0.0);
        node1.world.spawn_drone_at(i, pos, (0.0, 0.0, 0.0));
        node1.engine.node.manager.add_entity(Entity {
            id: i, pos, vel: (0.0, 0.0, 0.0), state: AuthorityState::Local, verifying_key: None,
        });
    }

    let client_conn0 = make_client_conn(addr0).await;
    let client_conn1 = make_client_conn(addr1).await;
    let mut state0 = ClientState::new();
    let mut state1 = ClientState::new();

    for _ in 0..50 {
        node0.tick(1.0 / 128.0).await.unwrap();
        node1.tick(1.0 / 128.0).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }

    drain_client_timeout(&client_conn0, &mut state0, Duration::from_millis(200)).await;
    drain_client_timeout(&client_conn1, &mut state1, Duration::from_millis(200)).await;

    let has_node0_entities = (1..=5u64).any(|id| state0.entities.contains_key(&id));
    let has_node1_entities = (101..=105u64).any(|id| state1.entities.contains_key(&id));

    assert!(has_node0_entities, "Client on node0 should see node0's entities");
    assert!(has_node1_entities, "Client on node1 should see node1's entities");

    assert!(!state0.cells.is_empty(), "Client on node0 should have wireframe");
    assert!(!state1.cells.is_empty(), "Client on node1 should have wireframe");
}

#[tokio::test]
async fn test_handoff_entity_visible_to_client_on_new_node() {
    let cell0 = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(cell0.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();
    let mut node1 = GameLoop::new(make_config(cell1.clone(), vec![addr0]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr1 = node1.engine.endpoint.local_addr().unwrap();

    sleep(Duration::from_millis(100)).await;
    for _ in 0..20 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let eid = 77u64;
    let start_pos = (11.5, 12.0, 0.0);
    let vel = (3.0, 0.0, 0.0);
    node0.world.spawn_drone_at(eid, start_pos, vel);
    node0.engine.node.manager.add_entity(Entity {
        id: eid, pos: start_pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let client_conn1 = make_client_conn(addr1).await;
    let mut client_state = ClientState::new();

    let dt = 1.0 / 128.0;
    let mut found_on_client = false;
    for tick in 0..300 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();

        if tick % 20 == 0 {
            drain_client_timeout(&client_conn1, &mut client_state, Duration::from_millis(10)).await;
            if client_state.entities.contains_key(&eid) {
                found_on_client = true;
                break;
            }
            sleep(Duration::from_millis(2)).await;
        }
    }

    assert!(
        found_on_client,
        "Entity {} should appear on node1's client after handoff",
        eid
    );
}

#[tokio::test]
async fn test_entity_physics_timing_server_to_client() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut node = GameLoop::new(make_config(cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr = node.engine.endpoint.local_addr().unwrap();

    let client_conn = make_client_conn(addr).await;
    let mut state = ClientState::new();

    for _ in 0..10 {
        node.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let eid = 99u64;
    let start_pos = (5.0, 12.0, 0.0);
    let vel = (4.0, 0.0, 0.0);
    node.world.spawn_drone_at(eid, start_pos, vel);
    node.engine.node.manager.add_entity(Entity {
        id: eid, pos: start_pos, vel, state: AuthorityState::Local, verifying_key: None,
    });

    let dt = 1.0 / 128.0;
    let start = Instant::now();
    let mut positions_over_time: Vec<(Duration, f32)> = Vec::new();

    for tick in 0..200 {
        node.tick(dt).await.unwrap();

        if tick % 10 == 0 {
            drain_client_timeout(&client_conn, &mut state, Duration::from_millis(5)).await;
            if let Some((pos, _)) = state.entities.get(&eid) {
                positions_over_time.push((start.elapsed(), pos.0));
            }
        }
        sleep(Duration::from_millis(1)).await;
    }

    assert!(
        positions_over_time.len() >= 5,
        "Should have at least 5 position samples, got {}",
        positions_over_time.len()
    );

    let mut increasing = 0;
    for w in positions_over_time.windows(2) {
        if w[1].1 > w[0].1 + 0.01 {
            increasing += 1;
        }
    }
    assert!(
        increasing >= positions_over_time.len() / 2,
        "Most position samples should be increasing (entity moving right): increasing={}/{}",
        increasing, positions_over_time.len()
    );
}

#[tokio::test]
async fn test_spawn_split_despawn_merge_full_cycle_timed() {
    let full_cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let keep = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let new_cell = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);

    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr0 = node0.engine.endpoint.local_addr().unwrap();

    let client0 = make_client_conn(addr0).await;
    let mut cs0 = ClientState::new();

    for _ in 0..10 {
        node0.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }
    drain_client_timeout(&client0, &mut cs0, Duration::from_millis(50)).await;
    assert!(cs0.node_count_status >= 1, "Phase 0: initial status");

    let dt = 1.0 / 128.0;
    for i in 1..=50u64 {
        let x = 1.0 + (i as f32) * (22.0 / 50.0);
        let pos = (x, 12.0, 0.0);
        let vel = (0.1 * (i as f32 % 3.0 - 1.0), 0.0, 0.0);
        node0.world.spawn_drone_at(i, pos, vel);
        node0.engine.node.manager.add_entity(Entity {
            id: i, pos, vel, state: AuthorityState::Local, verifying_key: None,
        });
    }

    for _ in 0..40 {
        node0.tick(dt).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    drain_client_timeout(&client0, &mut cs0, Duration::from_millis(100)).await;
    let spawn_count = cs0.entities.len();
    assert!(spawn_count >= 30, "Phase 1: client sees spawned entities: {}", spawn_count);

    node0.engine.node.manager.set_cell(keep.clone());
    node0.set_cell(keep.clone());
    node0.broadcast_cells(&[keep.clone()]);

    let mut node1 = GameLoop::new(make_config(new_cell.clone(), vec![addr0]), PhysicsTestDroneWorld::new()).await.unwrap();
    let addr1 = node1.engine.endpoint.local_addr().unwrap();

    let split_start = Instant::now();
    sleep(Duration::from_millis(100)).await;

    for _ in 0..100 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    let split_elapsed = split_start.elapsed();

    drain_client_timeout(&client0, &mut cs0, Duration::from_millis(100)).await;
    assert!(
        cs0.cells.iter().any(|c| c.x_max < 15.0),
        "Phase 2: client wireframe should show shrunk cell"
    );

    let client1 = make_client_conn(addr1).await;
    let mut cs1 = ClientState::new();
    for _ in 0..30 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }
    drain_client_timeout(&client1, &mut cs1, Duration::from_millis(100)).await;

    let n0_local = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .count();
    let n1_local = node1.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .count();

    assert!(n0_local > 0 && n1_local > 0, "Phase 2: both nodes have entities (n0={} n1={})", n0_local, n1_local);
    assert!(
        n0_local + n1_local >= 45,
        "Phase 2: entity conservation (n0={} n1={} total={})",
        n0_local, n1_local, n0_local + n1_local
    );

    let n0_ids: Vec<u64> = node0.engine.node.manager.entities.iter()
        .filter(|(eid, e)| e.state == AuthorityState::Local && **eid < 1_000_000)
        .map(|(id, _)| *id)
        .collect();
    for rid in &n0_ids {
        node0.engine.node.manager.remove_entity(*rid);
        node0.world.drones.remove(rid);
        node0.world.local_ids.remove(rid);
    }
    node0.broadcast_entity_removals(&n0_ids);
    node0.broadcast_status();

    let n1_ids: Vec<u64> = node1.engine.node.manager.entities.iter()
        .filter(|(eid, e)| e.state == AuthorityState::Local && **eid < 1_000_000)
        .map(|(id, _)| *id)
        .collect();
    for rid in &n1_ids {
        node1.engine.node.manager.remove_entity(*rid);
        node1.world.drones.remove(rid);
        node1.world.local_ids.remove(rid);
    }
    node1.broadcast_entity_removals(&n1_ids);
    node1.broadcast_status();

    for _ in 0..20 {
        node0.tick(dt).await.unwrap();
        node1.tick(dt).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }

    drain_client_timeout(&client0, &mut cs0, Duration::from_millis(100)).await;
    drain_client_timeout(&client1, &mut cs1, Duration::from_millis(100)).await;

    assert!(
        cs0.removed_ids.len() >= n0_ids.len() / 2,
        "Phase 3: client0 should see removals (got {} expected ~{})",
        cs0.removed_ids.len(), n0_ids.len()
    );

    let remaining_server = node0.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .count()
        + node1.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .count();
    assert_eq!(remaining_server, 0, "Phase 3: all entities removed from server");

    eprintln!(
        "[TIMING] Split transition completed in {:?}, n0_local={}, n1_local={}, total_removed={}",
        split_elapsed, n0_local, n1_local, n0_ids.len() + n1_ids.len()
    );
}
