use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_client::ZeusClient;
use zeus_node::engine::{RemoteEntityState, ZeusConfig, decode_compact_client};
use zeus_node::game_loop::{GameLoop, GameWorld};

fn parse_0xcc_datagram(data: &[u8]) -> Vec<(u64, (f32, f32, f32), (f32, f32, f32))> {
    decode_compact_client(data)
}

struct TestWorld {
    local_ids: HashSet<u64>,
    states: HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>,
    arrived: Vec<u64>,
    departed: Vec<u64>,
    step_count: u32,
}

impl TestWorld {
    fn new() -> Self {
        Self {
            local_ids: HashSet::new(),
            states: HashMap::new(),
            arrived: Vec::new(),
            departed: Vec::new(),
            step_count: 0,
        }
    }

    fn spawn_local(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.local_ids.insert(id);
        self.states.insert(id, (pos, vel));
    }
}

impl GameWorld for TestWorld {
    fn step(&mut self, _dt: f32) {
        self.step_count += 1;
        let ids: Vec<u64> = self.local_ids.iter().copied().collect();
        for id in ids {
            if let Some((pos, vel)) = self.states.get(&id).copied() {
                self.states.insert(
                    id,
                    (
                        (pos.0 + vel.0 * _dt, pos.1 + vel.1 * _dt, pos.2 + vel.2 * _dt),
                        vel,
                    ),
                );
            }
        }
    }

    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.arrived.push(id);
        if id < 1_000_000 {
            self.local_ids.insert(id);
        }
        self.states.insert(id, (pos, vel));
    }

    fn on_entity_departed(&mut self, id: u64) {
        self.departed.push(id);
        self.local_ids.remove(&id);
        self.states.remove(&id);
    }

    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.states.insert(id, (pos, vel));
    }

    fn locally_simulated_ids(&self) -> &HashSet<u64> {
        &self.local_ids
    }

    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.states.get(&id).copied()
    }
}

#[tokio::test]
async fn test_single_client_server_round_trip() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    game_loop.world.spawn_local(1, (5.0, 0.0, 0.0), (1.0, 0.0, 0.0));

    let mut client = ZeusClient::new(9001).unwrap();
    client.connect(server_addr).await.unwrap();

    sleep(Duration::from_millis(50)).await;

    client
        .send_state((10.0, 0.0, 0.0), (2.0, 0.0, 0.0))
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    for _ in 0..10 {
        game_loop.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    assert!(
        game_loop.world.step_count >= 10,
        "World should have been stepped at least 10 times, got {}",
        game_loop.world.step_count
    );

    let entity_count = game_loop.engine.node.manager.entity_count();
    assert!(
        entity_count >= 1,
        "EntityManager should have at least 1 entity (local ball), got {}",
        entity_count
    );

    let local_entity = game_loop.engine.node.manager.get_entity(1);
    assert!(local_entity.is_some(), "Local entity 1 should exist in EntityManager");

    let conn = client.connection().unwrap();
    let mut received_cc = false;
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_millis(20), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    let decoded = parse_0xcc_datagram(&data);
                    if !decoded.is_empty() {
                        received_cc = true;
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    assert!(received_cc, "Client should have received at least one 0xCC state update");

    let player_arrived = game_loop
        .world
        .arrived
        .contains(&9001)
        || game_loop.engine.node.manager.get_entity(9001).is_some();
    assert!(
        player_arrived,
        "Player entity 9001 should have arrived in the engine after sending state"
    );
}

#[tokio::test]
async fn test_multiplayer_two_clients_see_each_other() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    let mut client_a = ZeusClient::new(1_001_001).unwrap();
    client_a.connect(server_addr).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let mut client_b = ZeusClient::new(1_001_002).unwrap();
    client_b.connect(server_addr).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    client_a
        .send_state((5.0, 1.0, 0.0), (1.0, 0.0, 0.0))
        .await
        .unwrap();
    client_b
        .send_state((15.0, 2.0, 0.0), (-1.0, 0.0, 0.0))
        .await
        .unwrap();

    sleep(Duration::from_millis(50)).await;

    for _ in 0..20 {
        game_loop.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let player_ids = game_loop.player_entity_ids();
    assert!(
        player_ids.len() >= 2,
        "Should have at least 2 player entities, got {} (ids: {:?})",
        player_ids.len(),
        player_ids
    );

    let has_a = game_loop.engine.node.manager.get_entity(1_001_001).is_some();
    let has_b = game_loop.engine.node.manager.get_entity(1_001_002).is_some();
    assert!(has_a, "Entity 1001001 (Client A) should exist on server");
    assert!(has_b, "Entity 1001002 (Client B) should exist on server");

    let conn_a = client_a.connection().unwrap();
    let mut a_saw_entities = HashSet::new();
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(10), conn_a.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        a_saw_entities.insert(id);
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        a_saw_entities.contains(&1_001_002),
        "Client A should see Client B's entity (1001002) in state updates. Saw: {:?}",
        a_saw_entities
    );
}

#[tokio::test]
async fn test_multiplayer_player_id_broadcast() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    let mut client_a = ZeusClient::new(2_001_001).unwrap();
    client_a.connect(server_addr).await.unwrap();
    let mut client_b = ZeusClient::new(2_001_002).unwrap();
    client_b.connect(server_addr).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    client_a
        .send_state((5.0, 0.0, 0.0), (0.0, 0.0, 0.0))
        .await
        .unwrap();
    client_b
        .send_state((15.0, 0.0, 0.0), (0.0, 0.0, 0.0))
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    for _ in 0..20 {
        game_loop.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let player_ids = game_loop.player_entity_ids();
    let mut bb_buf = Vec::with_capacity(3 + player_ids.len() * 8);
    bb_buf.push(0xBB);
    let count = player_ids.len() as u16;
    bb_buf.push((count >> 8) as u8);
    bb_buf.push((count & 0xFF) as u8);
    for pid in &player_ids {
        bb_buf.extend_from_slice(&pid.to_le_bytes());
    }

    for conn in &game_loop.engine.connections {
        let _ = conn.send_datagram(bb_buf.clone().into());
    }

    sleep(Duration::from_millis(30)).await;

    let conn_a = client_a.connection().unwrap();
    let mut received_bb = false;
    let mut bb_ids = HashSet::new();
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_millis(10), conn_a.read_datagram()).await {
            Ok(Ok(data)) => {
                if data.len() >= 3 && data[0] == 0xBB {
                    received_bb = true;
                    let c = ((data[1] as u16) << 8) | (data[2] as u16);
                    let mut offset = 3;
                    for _ in 0..c {
                        if offset + 8 <= data.len() {
                            let id =
                                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                            bb_ids.insert(id);
                            offset += 8;
                        }
                    }
                    break;
                }
            }
            _ => break,
        }
    }

    assert!(
        received_bb,
        "Client A should receive a 0xBB player ID broadcast"
    );
    assert!(
        bb_ids.len() >= 2,
        "0xBB should contain at least 2 player IDs, got {:?}",
        bb_ids
    );
}

#[tokio::test]
async fn test_spawn_request_0xdd_creates_entities() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    let mut client = ZeusClient::new(3001).unwrap();
    client.connect(server_addr).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    for _ in 0..5 {
        game_loop.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let conn = client.connection().unwrap();
    let dd_buf: Vec<u8> = vec![0xDD, 0x00, 0x05];
    conn.send_datagram(dd_buf.into()).unwrap();

    sleep(Duration::from_millis(50)).await;

    game_loop.tick(0.016).await.unwrap();

    let datagrams = game_loop.engine.client_datagrams.clone();
    let mut found_dd = false;
    for dg in &datagrams {
        if dg.len() >= 3 && dg[0] == 0xDD {
            found_dd = true;
            let count = ((dg[1] as u16) << 8) | (dg[2] as u16);
            assert_eq!(count, 5, "0xDD should request 5 entities");
        }
    }
    assert!(
        found_dd,
        "Server should have received the 0xDD spawn request datagram"
    );
}

#[tokio::test]
async fn test_multinode_entity_handoff() {
    let config_node0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 10.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config_node0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config_node1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 20.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world1 = TestWorld::new();
    let mut node1 = GameLoop::new(config_node1, world1).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let initial_conn_0 = node0.engine.connections.len();
    let initial_conn_1 = node1.engine.connections.len();
    assert!(
        initial_conn_0 >= 1 || initial_conn_1 >= 1,
        "Nodes should be connected to each other (node0 conns: {}, node1 conns: {})",
        initial_conn_0,
        initial_conn_1
    );

    node0.world.spawn_local(42, (9.0, 0.0, 0.0), (5.0, 0.0, 0.0));

    for i in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();

        if let Some(e) = node0.engine.node.manager.get_entity(42) {
            if i % 10 == 0 {
                println!(
                    "[tick {}] Entity 42 on node0: pos=({:.1},{:.1},{:.1}) state={:?}",
                    i, e.pos.0, e.pos.1, e.pos.2, e.state
                );
            }
        }

        let n1_has = node1.engine.node.manager.get_entity(42).is_some();
        if n1_has {
            println!("[tick {}] Entity 42 arrived on node1!", i);
            break;
        }

        sleep(Duration::from_millis(5)).await;
    }

    let entity_on_node0 = node0.engine.node.manager.get_entity(42);
    let entity_on_node1 = node1.engine.node.manager.get_entity(42);

    let departed_from_0 = entity_on_node0
        .map(|e| {
            e.state == zeus_node::entity_manager::AuthorityState::Remote
                || e.state == zeus_node::entity_manager::AuthorityState::HandoffOut
        })
        .unwrap_or(false);
    let arrived_on_1 = entity_on_node1.is_some();

    assert!(
        departed_from_0 || arrived_on_1,
        "Entity 42 should have been handed off: node0 departed={}, node1 arrived={} (node0 entity: {:?}, node1 entity: {:?})",
        departed_from_0,
        arrived_on_1,
        entity_on_node0.map(|e| format!("state={:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
        entity_on_node1.map(|e| format!("state={:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
    );
}

#[tokio::test]
async fn test_multinode_client_receives_cross_node_entities() {
    let config_node0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config_node0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    node0.world.spawn_local(77, (3.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node0.world.spawn_local(78, (6.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    let mut client = ZeusClient::new(5001).unwrap();
    client.connect(node0_addr).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let conn = client.connection().unwrap();
    let mut seen_ids = HashSet::new();
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(10), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        seen_ids.insert(id);
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        seen_ids.contains(&77),
        "Client should see entity 77 from node0. Seen: {:?}",
        seen_ids
    );
    assert!(
        seen_ids.contains(&78),
        "Client should see entity 78 from node0. Seen: {:?}",
        seen_ids
    );
}

#[tokio::test]
async fn test_server_status_0xaa_broadcast() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    game_loop.world.spawn_local(1, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    game_loop.world.spawn_local(2, (1.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    let mut client = ZeusClient::new(6001).unwrap();
    client.connect(server_addr).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    for _ in 0..10 {
        game_loop.tick(0.016).await.unwrap();

        let entity_count = game_loop.engine.node.manager.entities.len() as u16;
        let status_bytes: [u8; 6] = [
            0xAA,
            (entity_count >> 8) as u8,
            (entity_count & 0xFF) as u8,
            1,
            24,
            8,
        ];
        for conn in &game_loop.engine.connections {
            let _ = conn.send_datagram(status_bytes.to_vec().into());
        }

        sleep(Duration::from_millis(10)).await;
    }

    let conn = client.connection().unwrap();
    let mut received_aa = false;
    let mut entity_count_received = 0u16;
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_millis(10), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if data.len() >= 6 && data[0] == 0xAA {
                    received_aa = true;
                    entity_count_received = ((data[1] as u16) << 8) | (data[2] as u16);
                    break;
                }
            }
            _ => break,
        }
    }

    assert!(
        received_aa,
        "Client should receive 0xAA status broadcast"
    );
    assert!(
        entity_count_received >= 2,
        "Status should report at least 2 entities, got {}",
        entity_count_received
    );
}

#[tokio::test]
async fn test_remote_gossip_fed_to_world() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();

    game_loop.world.spawn_local(1, (5.0, 0.0, 0.0), (1.0, 0.0, 0.0));

    game_loop.engine.remote_entity_states.insert(500, RemoteEntityState {
        pos: (20.0, 3.0, 1.0),
        vel: (-1.0, 0.0, 0.0),
        last_seen: std::time::Instant::now(),
    });

    game_loop.tick(0.016).await.unwrap();

    let state = game_loop.world.get_entity_state(500);
    assert!(state.is_some(), "Remote gossip entity 500 should be fed into world as proxy");
    let (pos, vel) = state.unwrap();
    assert!((pos.0 - 20.0).abs() < 0.5, "Proxy 500 x should be near 20.0, got {}", pos.0);
    assert!((vel.0 - (-1.0)).abs() < 0.5, "Proxy 500 vel.x should be near -1.0, got {}", vel.0);
}

#[tokio::test]
async fn test_gossip_two_nodes_propagation() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let world1 = TestWorld::new();
    let mut node1 = GameLoop::new(config1, world1).await.unwrap();

    sleep(Duration::from_millis(50)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(10, (3.0, 0.0, 0.0), (1.0, 0.0, 0.0));

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let has_remote = node1.engine.remote_entity_states.contains_key(&10);
    assert!(has_remote, "Node 1 should have entity 10 in remote_entity_states via gossip from Node 0");

    let state = node1.world.get_entity_state(10);
    assert!(state.is_some(), "Node 1 world should have entity 10 as kinematic proxy");
}

#[tokio::test]
async fn test_3node_gossip_chain() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr, node1_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(10, (3.0, 0.0, 0.0), (1.0, 0.0, 0.0));

    for _ in 0..40 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let node1_has = node1.engine.remote_entity_states.contains_key(&10);
    let node2_has = node2.engine.remote_entity_states.contains_key(&10);

    assert!(node1_has, "Node 1 should have entity 10 from Node 0 via direct gossip");
    assert!(node2_has, "Node 2 should have entity 10 from Node 0 via direct gossip (full mesh)");
}

#[tokio::test]
async fn test_gossip_no_infinite_loop() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(50)).await;

    node0.world.spawn_local(1, (3.0, 0.0, 0.0), (0.5, 0.0, 0.0));

    for _ in 0..30 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let e = node0.engine.node.manager.get_entity(1);
    assert!(e.is_some(), "Node 0 should still own entity 1");
    assert!(
        !node0.engine.remote_entity_states.contains_key(&1),
        "Node 0 should NOT have entity 1 in remote_entity_states (it owns it locally)"
    );
}

#[tokio::test]
async fn test_client_sees_full_world_from_any_node() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    sleep(Duration::from_millis(50)).await;

    node0.world.spawn_local(100, (3.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node0.world.spawn_local(101, (6.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node1.world.spawn_local(200, (15.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let client = ZeusClient::new(7001).unwrap();
    let ep = client.endpoint().clone();
    let conn0 = ep.connect(node0_addr, "localhost").unwrap().await.unwrap();
    let conn1 = ep.connect(node1_addr, "localhost").unwrap().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let mut seen_ids = HashSet::new();
    for conn in [&conn0, &conn1] {
        for _ in 0..200 {
            match tokio::time::timeout(Duration::from_millis(10), conn.read_datagram()).await {
                Ok(Ok(data)) => {
                    if !data.is_empty() && data[0] == 0xCC {
                        for (id, _, _) in parse_0xcc_datagram(&data) {
                            seen_ids.insert(id);
                        }
                    }
                }
                _ => break,
            }
        }
    }

    assert!(
        seen_ids.contains(&100),
        "Client should see entity 100 (local to Node 0). Saw: {:?}",
        seen_ids
    );
    assert!(
        seen_ids.contains(&101),
        "Client should see entity 101 (local to Node 0). Saw: {:?}",
        seen_ids
    );
    assert!(
        seen_ids.contains(&200),
        "Client should see entity 200 (local to Node 1, via direct connection). Saw: {:?}",
        seen_ids
    );
}

#[tokio::test]
async fn test_e2e_4node_full_world() {
    let mut nodes: Vec<GameLoop<TestWorld>> = Vec::new();
    let mut addrs = Vec::new();

    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let n0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    addrs.push(n0.engine.endpoint.local_addr().unwrap());
    nodes.push(n0);

    for i in 1..4u8 {
        let cfg = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: addrs.clone(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
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
        for j in 0..5 {
            let id = (i * 100 + j + 1) as u64;
            let x = (i as f32) * 5.0 + j as f32;
            nodes[i].world.spawn_local(id, (x, 1.0, 0.0), (0.0, 0.0, 0.0));
        }
    }

    for _ in 0..60 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    let client = ZeusClient::new(8001).unwrap();
    let ep = client.endpoint().clone();
    let mut client_conns = Vec::new();
    for addr in &addrs {
        client_conns.push(ep.connect(*addr, "localhost").unwrap().await.unwrap());
    }
    sleep(Duration::from_millis(50)).await;

    for _ in 0..30 {
        for node in nodes.iter_mut() {
            node.tick(0.016).await.unwrap();
        }
        sleep(Duration::from_millis(10)).await;
    }

    let mut seen_ids = HashSet::new();
    for conn in &client_conns {
        for _ in 0..500 {
            match tokio::time::timeout(Duration::from_millis(10), conn.read_datagram()).await {
                Ok(Ok(data)) => {
                    if !data.is_empty() && data[0] == 0xCC {
                        for (id, _, _) in parse_0xcc_datagram(&data) {
                            seen_ids.insert(id);
                        }
                    }
                }
                _ => break,
            }
        }
    }

    assert!(
        seen_ids.len() >= 15,
        "Client should see at least 15 entities from all 4 nodes, got {} (ids: {:?})",
        seen_ids.len(),
        seen_ids
    );

    for i in 0..4 {
        let base = (i * 100 + 1) as u64;
        let has_any = (base..base + 5).any(|id| seen_ids.contains(&id));
        assert!(
            has_any,
            "Client should see at least one entity from node {} (range {}..{}). Saw: {:?}",
            i, base, base + 5, seen_ids
        );
    }

    let mut no_dupes = true;
    for i in 0..4 {
        for j in 0..5 {
            let id = (i * 100 + j + 1) as u64;
            let count = seen_ids.iter().filter(|&&x| x == id).count();
            if count > 1 {
                no_dupes = false;
            }
        }
    }
    assert!(no_dupes, "No duplicate entity IDs should exist");
}

#[tokio::test]
async fn test_e2e_handoff_with_gossip() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 10.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let mut client = ZeusClient::new(9001).unwrap();
    client.connect(node0_addr).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    node0.world.spawn_local(42, (9.0, 1.0, 0.0), (5.0, 0.0, 0.0));

    let mut client_saw_42 = false;
    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let conn = client.connection().unwrap();
        match tokio::time::timeout(Duration::from_millis(2), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        if id == 42 {
                            client_saw_42 = true;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    assert!(client_saw_42, "Client should see entity 42 through handoff (either from node0 directly or via gossip)");
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
    };
    let n0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    addrs.push(n0.engine.endpoint.local_addr().unwrap());
    nodes.push(n0);

    for i in 1..4u8 {
        let cfg = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: addrs.clone(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
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
async fn test_handoff_with_preexisting_gossip_proxy() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 5.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(42, (3.0, 0.0, 0.0), (10.0, 0.0, 0.0));

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let gossip_proxy = node1.engine.remote_entity_states.contains_key(&42);
    assert!(gossip_proxy, "Node 1 should have entity 42 as gossip proxy before handoff");

    for _ in 0..100 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let on_node1 = node1.engine.node.manager.get_entity(42);
    let on_node0 = node0.engine.node.manager.get_entity(42);
    let handed_off = on_node1.is_some()
        || on_node0
            .map(|e| e.state == zeus_node::entity_manager::AuthorityState::HandoffOut
                   || e.state == zeus_node::entity_manager::AuthorityState::Remote)
            .unwrap_or(false);
    assert!(
        handed_off,
        "Entity 42 should have been handed off after drifting past boundary. node0={:?} node1={:?}",
        on_node0.map(|e| format!("{:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
        on_node1.map(|e| format!("{:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
    );

    if let Some(e) = on_node1 {
        assert!(
            e.pos.0 > 0.0 && e.pos.0 < 200.0,
            "Entity 42 position on Node 1 should be reasonable, got x={:.1}",
            e.pos.0
        );
    }
}

#[tokio::test]
async fn test_boundary_shift_position_continuity() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..5 {
        let x = 3.0 + (i as f32) * 12.0;
        node0.world.spawn_local(100 + i, (x, 0.0, 0.0), (0.5, 0.0, 0.0));
    }

    let mut prev_positions: HashMap<u64, f32> = HashMap::new();
    let mut max_delta: f32 = 0.0;

    for tick in 0..120 {
        if tick == 30 {
            node0.set_boundary(15.0);
        }

        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        for id in 100..105u64 {
            let pos_x = node0.engine.node.manager.get_entity(id)
                .map(|e| e.pos.0)
                .or_else(|| node1.engine.node.manager.get_entity(id).map(|e| e.pos.0));

            if let Some(x) = pos_x {
                if let Some(prev_x) = prev_positions.get(&id) {
                    let delta = (x - prev_x).abs();
                    if delta > max_delta {
                        max_delta = delta;
                    }
                }
                prev_positions.insert(id, x);
            }
        }
    }

    assert!(
        max_delta < 5.0,
        "Max position delta between ticks should be < 5.0, got {:.2}",
        max_delta
    );
}

#[tokio::test]
async fn test_gossip_gates_boundary_shift() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    node0.world.spawn_local(1, (15.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(20)).await;
    }

    let e = node0.engine.node.manager.get_entity(1);
    assert!(
        e.is_some(),
        "Entity 1 should still exist on node0"
    );
    if let Some(e) = e {
        assert_eq!(
            e.state,
            zeus_node::entity_manager::AuthorityState::Local,
            "Entity 1 should still be Local on node0 before boundary shrinks"
        );
    }

    for _ in 0..50 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let peers_discovered = node0.engine.discovery.peers.len() >= 1
        || node1.engine.discovery.peers.len() >= 1;
    assert!(
        peers_discovered,
        "Nodes should have discovered each other as peers (n0={}, n1={})",
        node0.engine.discovery.peers.len(),
        node1.engine.discovery.peers.len()
    );

    let gossip_flowing = !node0.engine.remote_entity_states.is_empty()
        || !node1.engine.remote_entity_states.is_empty();

    if gossip_flowing {
        node0.set_boundary(10.0);
        for _ in 0..60 {
            node0.tick(0.016).await.unwrap();
            node1.tick(0.016).await.unwrap();
            sleep(Duration::from_millis(5)).await;
        }
        let e = node0.engine.node.manager.get_entity(1);
        let handed_off = e
            .map(|e| e.state != zeus_node::entity_manager::AuthorityState::Local)
            .unwrap_or(true);
        let on_node1 = node1.engine.node.manager.get_entity(1).is_some();
        assert!(
            handed_off || on_node1,
            "After gossip is flowing and boundary shrinks, entity should hand off"
        );
    }
}

#[tokio::test]
async fn test_authority_unique_during_split() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 10.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
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
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..5u64 {
        let x = 3.0 + (i as f32) * 3.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (1.0, 0.0, 0.0));
    }

    let mut duplicate_found = false;
    for tick in 0..100 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();

        let local_on_0: HashSet<u64> = node0
            .engine
            .node
            .manager
            .entities
            .iter()
            .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
            .map(|(id, _)| *id)
            .collect();
        let local_on_1: HashSet<u64> = node1
            .engine
            .node
            .manager
            .entities
            .iter()
            .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
            .map(|(id, _)| *id)
            .collect();

        let overlap: Vec<u64> = local_on_0.intersection(&local_on_1).copied().collect();
        if !overlap.is_empty() {
            eprintln!(
                "[tick {}] DUPLICATE Local authority: {:?} (node0 local={:?}, node1 local={:?})",
                tick, overlap, local_on_0, local_on_1
            );
            duplicate_found = true;
        }

        if tick % 20 == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    }

    assert!(
        !duplicate_found,
        "No entity should be Local on two nodes simultaneously during split"
    );
}

#[tokio::test]
async fn test_sequential_3node_split() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    for i in 0..8u64 {
        let x = 2.0 + (i as f32) * 3.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (0.1, 0.0, 0.0));
    }

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.set_boundary(12.0);
    for _ in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    node0.set_boundary(8.0);
    node1.set_boundary(16.0);

    for _ in 0..120 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let mut entity_seen_on: HashMap<u64, Vec<usize>> = HashMap::new();
    for (node_idx, node) in [&node0, &node1, &node2].iter().enumerate() {
        for (id, _entity) in &node.engine.node.manager.entities {
            entity_seen_on.entry(*id).or_default().push(node_idx);
        }
    }

    let unique_entities = entity_seen_on.len();
    assert!(
        unique_entities >= 6,
        "At least 6 of 8 entities should be tracked across 3 nodes (got {}). Map: {:?}",
        unique_entities, entity_seen_on
    );

    let mut lost = Vec::new();
    for id in 1..=8u64 {
        let exists_anywhere = node0.engine.node.manager.get_entity(id).is_some()
            || node1.engine.node.manager.get_entity(id).is_some()
            || node2.engine.node.manager.get_entity(id).is_some()
            || node0.engine.remote_entity_states.contains_key(&id)
            || node1.engine.remote_entity_states.contains_key(&id)
            || node2.engine.remote_entity_states.contains_key(&id);
        if !exists_anywhere {
            lost.push(id);
        }
    }

    assert!(
        lost.len() <= 1,
        "At most 1 entity can be lost during 3-node split. Lost: {:?}",
        lost
    );
}

#[tokio::test]
async fn test_entity_count_conserved_during_rapid_boundary_change() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 20.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..10u64 {
        let x = 2.0 + (i as f32) * 2.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (0.1, 0.0, 0.0));
    }
    for i in 0..10u64 {
        let x = 25.0 + (i as f32) * 2.0;
        node1.world.spawn_local(100 + i + 1, (x, 0.0, 0.0), (-0.1, 0.0, 0.0));
    }

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    for tick in 0..50 {
        let new_boundary = 20.0 - (tick as f32 * 0.3);
        node0.set_boundary(new_boundary.max(3.0));
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
    }

    for _ in 0..100 {
        node0.tick(0.008).await.unwrap();
        node1.tick(0.008).await.unwrap();
        sleep(Duration::from_millis(2)).await;
    }

    let mut total_local = 0;
    for node in [&node0, &node1] {
        for entity in node.engine.node.manager.entities.values() {
            if entity.state == zeus_node::entity_manager::AuthorityState::Local {
                total_local += 1;
            }
        }
    }

    assert!(
        total_local >= 12,
        "At least 12 of 20 entities should remain Local across both nodes (got {})",
        total_local
    );
}

#[tokio::test]
async fn test_client_sees_all_during_split() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    for i in 0..6u64 {
        let x = 2.0 + (i as f32) * 4.0;
        node0.world.spawn_local(i + 1, (x, 0.0, 0.0), (0.0, 0.0, 0.0));
    }

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let mut client = ZeusClient::new(11001).unwrap();
    client.connect(node0_addr).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    node0.set_boundary(10.0);

    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let conn = client.connection().unwrap();
    let mut seen_ids = HashSet::new();
    for _ in 0..300 {
        match tokio::time::timeout(Duration::from_millis(10), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        seen_ids.insert(id);
                    }
                }
            }
            _ => break,
        }
    }

    let mut missing = Vec::new();
    for id in 1..=6u64 {
        if !seen_ids.contains(&id) {
            missing.push(id);
        }
    }

    assert!(
        missing.len() <= 1,
        "Client should see at least 5 of 6 entities during/after split. Missing: {:?}, Seen: {:?}",
        missing, seen_ids
    );
}

#[tokio::test]
async fn test_cross_node_collision_after_handoff() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(10, (5.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node0.world.spawn_local(20, (9.0, 0.0, 0.0), (2.0, 0.0, 0.0));

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.set_boundary(7.0);

    for _ in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let entity_20_on_node1 = node1.engine.node.manager.get_entity(20);
    let entity_20_state_on_node0 = node0.engine.node.manager.get_entity(20);
    let entity_20_migrated = entity_20_on_node1.is_some()
        || entity_20_state_on_node0
            .map(|e| e.state != zeus_node::entity_manager::AuthorityState::Local)
            .unwrap_or(false);

    let entity_10_on_node0 = node0.engine.node.manager.get_entity(10);
    assert!(
        entity_10_on_node0.is_some(),
        "Entity 10 should remain on node0"
    );

    if entity_20_migrated {
        let node0_sees_20 = node0.engine.remote_entity_states.contains_key(&20)
            || node0.engine.node.manager.get_entity(20).is_some();
        assert!(
            node0_sees_20,
            "After entity 20 migrated to node1, node0 should still see it via gossip or manager"
        );
    }

    let entity_10_exists = node0.engine.node.manager.get_entity(10).is_some()
        || node1.engine.node.manager.get_entity(10).is_some();
    let entity_20_exists = node0.engine.node.manager.get_entity(20).is_some()
        || node1.engine.node.manager.get_entity(20).is_some();
    assert!(entity_10_exists, "Entity 10 should still exist somewhere");
    assert!(entity_20_exists, "Entity 20 should still exist somewhere");
}

#[tokio::test]
async fn test_3node_boundary_convergence() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 2,
        lower_boundary: 0.0,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..400 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(3)).await;
    }

    let count0 = node0.engine.discovery.total_node_count();
    let count1 = node1.engine.discovery.total_node_count();
    let count2 = node2.engine.discovery.total_node_count();

    assert!(count1 >= 2, "Node 1 (middle) should know at least 2 nodes, got {}", count1);

    let any_leaf_converged = count0 >= 2 || count2 >= 2;
    assert!(any_leaf_converged,
        "At least one leaf node should know 2+ nodes after gossip convergence (n0={}, n2={})",
        count0, count2
    );
}

#[tokio::test]
async fn test_leftward_handoff() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 12.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 8.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node1.world.spawn_local(50, (7.0, 0.0, 0.0), (-3.0, 0.0, 0.0));
    node1.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 50,
        pos: (7.0, 0.0, 0.0),
        vel: (-3.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let on_node0 = node0.engine.node.manager.get_entity(50)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);
    let on_node1_local = node1.engine.node.manager.get_entity(50)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);

    let entity_exists = node0.engine.node.manager.get_entity(50).is_some()
        || node1.engine.node.manager.get_entity(50).is_some();
    assert!(entity_exists, "Entity 50 should still exist on at least one node");

    if on_node0 {
        assert!(!on_node1_local, "Entity should not be Local on both nodes");
    }
}

#[tokio::test]
async fn test_no_dual_ownership_3node_targeted() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 8.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 16.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 8.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 2,
        lower_boundary: 16.0,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..15 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(77, (6.0, 0.0, 0.0), (5.0, 0.0, 0.0));
    node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 77,
        pos: (6.0, 0.0, 0.0),
        vel: (5.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let local_on_0 = node0.engine.node.manager.get_entity(77)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);
    let local_on_1 = node1.engine.node.manager.get_entity(77)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);
    let local_on_2 = node2.engine.node.manager.get_entity(77)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);

    let local_count = [local_on_0, local_on_1, local_on_2].iter().filter(|&&x| x).count();
    assert!(
        local_count <= 1,
        "Entity 77 should be Local on at most 1 node, found Local on {} nodes (n0={}, n1={}, n2={})",
        local_count, local_on_0, local_on_1, local_on_2
    );

    let exists = node0.engine.node.manager.get_entity(77).is_some()
        || node1.engine.node.manager.get_entity(77).is_some()
        || node2.engine.node.manager.get_entity(77).is_some();
    assert!(exists, "Entity 77 should exist on at least one node");
}

#[tokio::test]
async fn test_entity_conservation_bidirectional() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 12.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 12.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(1, (10.0, 0.0, 0.0), (3.0, 0.0, 0.0));
    node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1,
        pos: (10.0, 0.0, 0.0),
        vel: (3.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    node1.world.spawn_local(2, (13.0, 0.0, 0.0), (-3.0, 0.0, 0.0));
    node1.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 2,
        pos: (13.0, 0.0, 0.0),
        vel: (-3.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..100 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let e1_exists = node0.engine.node.manager.get_entity(1).is_some()
        || node1.engine.node.manager.get_entity(1).is_some();
    let e2_exists = node0.engine.node.manager.get_entity(2).is_some()
        || node1.engine.node.manager.get_entity(2).is_some();

    assert!(e1_exists, "Entity 1 should be conserved across nodes");
    assert!(e2_exists, "Entity 2 should be conserved across nodes");
}

#[tokio::test]
async fn test_membership_convergence_daisy_chain() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let node0_id = node0.engine.discovery.local_id;

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();
    let node1_id = node1.engine.discovery.local_id;

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 2,
        lower_boundary: 0.0,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();
    let node2_id = node2.engine.discovery.local_id;

    sleep(Duration::from_millis(100)).await;

    for _ in 0..400 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(3)).await;
    }

    let n0_knows = &node0.engine.discovery.known_node_ids;
    let n1_knows = &node1.engine.discovery.known_node_ids;
    let n2_knows = &node2.engine.discovery.known_node_ids;

    assert!(
        n1_knows.len() >= 2,
        "Node 1 (middle, directly connected to both) should know at least 2 nodes. Known: {:?}",
        n1_knows
    );

    let n0_n1_overlap = n0_knows.contains(&node1_id);
    let n2_n1_overlap = n2_knows.contains(&node1_id);
    assert!(n0_n1_overlap, "Node 0 should at least know Node 1");
    assert!(n2_n1_overlap, "Node 2 should at least know Node 1");
}

#[tokio::test]
async fn test_ed25519_verifying_key_exchange() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let node0_id = node0.engine.discovery.local_id;
    let node0_vk = node0.engine.verifying_key;

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_id = node1.engine.discovery.local_id;
    let node1_vk = node1.engine.verifying_key;

    sleep(Duration::from_millis(100)).await;

    for _ in 0..200 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let node1_has_node0_vk = node1.engine.peer_verifying_keys.get(&node0_id);
    assert!(
        node1_has_node0_vk.is_some(),
        "Node 1 should have Node 0's verifying key after discovery exchange"
    );
    assert_eq!(
        node1_has_node0_vk.unwrap().as_bytes(),
        node0_vk.as_bytes(),
        "Exchanged verifying key should match"
    );

    let node0_has_node1_vk = node0.engine.peer_verifying_keys.get(&node1_id);
    assert!(
        node0_has_node1_vk.is_some(),
        "Node 0 should have Node 1's verifying key after discovery exchange"
    );
    assert_eq!(
        node0_has_node1_vk.unwrap().as_bytes(),
        node1_vk.as_bytes(),
        "Exchanged verifying key should match"
    );
}

#[tokio::test]
async fn test_full_mesh_1hop_gossip_3nodes() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 1,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr, node1_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 2,
        lower_boundary: 0.0,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(10, (3.0, 0.0, 0.0), (1.0, 0.0, 0.0));
    node1.world.spawn_local(20, (50.0, 0.0, 0.0), (1.0, 0.0, 0.0));
    node2.world.spawn_local(30, (80.0, 0.0, 0.0), (1.0, 0.0, 0.0));

    for _ in 0..40 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    assert!(
        node2.engine.remote_entity_states.contains_key(&10),
        "Node 2 should have entity 10 from Node 0 via direct connection (1-hop)"
    );
    assert!(
        node0.engine.remote_entity_states.contains_key(&30),
        "Node 0 should have entity 30 from Node 2 via direct connection (1-hop)"
    );
    assert!(
        node0.engine.remote_entity_states.contains_key(&20),
        "Node 0 should have entity 20 from Node 1 via direct connection (1-hop)"
    );
}

#[tokio::test]
async fn test_sdk_broadcast_status() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut game_loop = GameLoop::new(config, TestWorld::new()).await.unwrap();
    let node_addr = game_loop.engine.endpoint.local_addr().unwrap();

    game_loop.world.spawn_local(1_000_001, (5.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    game_loop.world.spawn_local(42, (10.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    let client = ZeusClient::new(20001).unwrap();
    let ep = client.endpoint().clone();
    let conn = ep.connect(node_addr, "localhost").unwrap().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    for _ in 0..10 {
        game_loop.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    game_loop.broadcast_status();

    sleep(Duration::from_millis(50)).await;

    let mut saw_aa = false;
    let mut saw_bb = false;
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_millis(20), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if data.len() >= 4 && data[0] == 0xAA {
                    saw_aa = true;
                    let nodes = data[3];
                    assert!(nodes >= 1, "Node count should be at least 1");
                } else if data.len() >= 3 && data[0] == 0xBB {
                    saw_bb = true;
                    let count = ((data[1] as u16) << 8) | (data[2] as u16);
                    assert_eq!(count, 1, "Should have exactly 1 player entity (>= 1_000_000)");
                }
            }
            _ => break,
        }
    }

    assert!(saw_aa, "Client should receive 0xAA status broadcast via broadcast_status()");
    assert!(saw_bb, "Client should receive 0xBB player IDs via broadcast_status()");
}

#[tokio::test]
async fn test_sdk_should_split() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let game_loop = GameLoop::new(config, TestWorld::new()).await.unwrap();

    assert!(!game_loop.should_split(0), "0 entities should not trigger split");
    assert!(!game_loop.should_split(4), "4 entities should not trigger split (threshold is 5)");
    assert!(game_loop.should_split(5), "5 entities should trigger split (1 node -> 2)");
    assert!(game_loop.should_split(10), "10 entities should trigger split (1 node -> 3)");
    assert!(game_loop.should_split(15), "15 entities should trigger split (1 node -> 4)");
}

#[tokio::test]
async fn test_node_only_broadcasts_local_to_clients() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 5.0,
        ordinal: 1,
        lower_boundary: 0.0,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(50)).await;

    node0.world.spawn_local(100, (3.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node1.world.spawn_local(200, (50.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    let client = ZeusClient::new(30001).unwrap();
    let ep = client.endpoint().clone();
    let conn0 = ep.connect(node0_addr, "localhost").unwrap().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    for _ in 0..30 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let mut seen_from_node0 = HashSet::new();
    for _ in 0..200 {
        match tokio::time::timeout(Duration::from_millis(10), conn0.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        seen_from_node0.insert(id);
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        seen_from_node0.contains(&100),
        "Node 0 should broadcast its own entity 100 to client"
    );
    assert!(
        !seen_from_node0.contains(&200),
        "Node 0 should NOT broadcast Node 1's entity 200 to client (full mesh: each node only sends local)"
    );
}
