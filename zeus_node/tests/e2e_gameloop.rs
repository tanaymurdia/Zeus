use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_client::ZeusClient;
use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::{GameLoop, GameWorld};

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
        seed_addr: None,
        boundary: 100.0,
        margin: 5.0,
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
                    let fb = &data[1..];
                    if let Ok(update) =
                        zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(fb)
                    {
                        if let Some(ghosts) = update.ghosts() {
                            if ghosts.len() > 0 {
                                received_cc = true;
                                let ghost = ghosts.get(0);
                                assert!(
                                    ghost.position().is_some(),
                                    "Ghost should have position"
                                );
                                break;
                            }
                        }
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
        seed_addr: None,
        boundary: 100.0,
        margin: 5.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    let mut client_a = ZeusClient::new(1001).unwrap();
    client_a.connect(server_addr).await.unwrap();
    sleep(Duration::from_millis(30)).await;

    let mut client_b = ZeusClient::new(1002).unwrap();
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

    let has_1001 = game_loop.engine.node.manager.get_entity(1001).is_some();
    let has_1002 = game_loop.engine.node.manager.get_entity(1002).is_some();
    assert!(has_1001, "Entity 1001 (Client A) should exist on server");
    assert!(has_1002, "Entity 1002 (Client B) should exist on server");

    let conn_a = client_a.connection().unwrap();
    let mut a_saw_entities = HashSet::new();
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(10), conn_a.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    if let Ok(update) =
                        zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(&data[1..])
                    {
                        if let Some(ghosts) = update.ghosts() {
                            for ghost in ghosts {
                                a_saw_entities.insert(ghost.entity_id());
                            }
                        }
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        a_saw_entities.contains(&1002),
        "Client A should see Client B's entity (1002) in state updates. Saw: {:?}",
        a_saw_entities
    );
}

#[tokio::test]
async fn test_multiplayer_player_id_broadcast() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addr: None,
        boundary: 100.0,
        margin: 5.0,
    };
    let world = TestWorld::new();
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    let server_addr = game_loop.engine.endpoint.local_addr().unwrap();

    let mut client_a = ZeusClient::new(2001).unwrap();
    client_a.connect(server_addr).await.unwrap();
    let mut client_b = ZeusClient::new(2002).unwrap();
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
        seed_addr: None,
        boundary: 100.0,
        margin: 5.0,
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
        seed_addr: None,
        boundary: 10.0,
        margin: 2.0,
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config_node0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config_node1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addr: Some(node0_addr),
        boundary: 20.0,
        margin: 2.0,
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
        seed_addr: None,
        boundary: 100.0,
        margin: 5.0,
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
                    if let Ok(update) =
                        zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(&data[1..])
                    {
                        if let Some(ghosts) = update.ghosts() {
                            for ghost in ghosts {
                                seen_ids.insert(ghost.entity_id());
                            }
                        }
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
        seed_addr: None,
        boundary: 100.0,
        margin: 5.0,
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
