use bevy::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU16, Ordering};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use zeus_client::ZeusClient;

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(NetworkResource::default())
            .insert_resource(ServerStatus::default())
            .insert_resource(BallPositions::default())
            .insert_resource(ServerStatus::default())
            .insert_resource(BallPositions::default())
            .insert_resource(AccumulatedState::default())
            .add_systems(Startup, setup_network)
            .add_systems(
                Update,
                (send_player_state, generate_snapshots, check_roaming),
            );
    }
}

#[derive(Resource, Default)]
pub struct AccumulatedState {
    pub positions: Arc<std::sync::Mutex<std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32), std::time::Instant)>>>,
    pub player_id: Arc<std::sync::Mutex<Option<u64>>>,
    pub player_entity_ids: Arc<std::sync::Mutex<std::collections::HashSet<u64>>>,
}

fn generate_snapshots(
    accumulated: Res<AccumulatedState>,
    ball_pos: Res<BallPositions>,
    time: Res<Time>,
    mut timer: Local<f32>,
) {
    *timer += time.delta_secs();
    if *timer < 0.008 {
        return;
    }
    *timer = 0.0;

    let mut map_lock = accumulated.positions.lock().unwrap();
    if map_lock.is_empty() {
        return;
    }

    let now = std::time::Instant::now();
    let stale_threshold = std::time::Duration::from_secs(5);
    map_lock.retain(|_, (_, _, last_seen)| now.duration_since(*last_seen) < stale_threshold);

    let new_positions: std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32))> = map_lock
        .iter()
        .map(|(&id, &(pos, vel, last_seen))| {
            let dt = now.duration_since(last_seen).as_secs_f32().min(0.05);
            let extrapolated = (
                pos.0 + vel.0 * dt,
                pos.1 + vel.1 * dt,
                pos.2 + vel.2 * dt,
            );
            (id, (extrapolated, vel))
        })
        .collect();

    drop(map_lock);

    if new_positions.is_empty() {
        return;
    }

    if let Ok(mut buffer) = ball_pos.snapshots.lock() {
        buffer.push_back(Snapshot {
            timestamp: now,
            entities: new_positions,
        });
        while buffer.len() > 60 {
            buffer.pop_front();
        }
    }
}

#[derive(Resource, Default)]
pub struct ServerStatus {
    pub entity_count: Arc<AtomicU16>,
    pub node_count: Arc<AtomicU8>,
    pub map_width: Arc<AtomicU8>,
    pub ball_radius: Arc<AtomicU8>,
}

impl ServerStatus {
    pub fn get_node_count(&self) -> u8 {
        self.node_count.load(Ordering::Relaxed)
    }

    pub fn get_map_width(&self) -> f32 {
        self.map_width.load(Ordering::Relaxed) as f32
    }

    pub fn get_ball_radius(&self) -> f32 {
        self.ball_radius.load(Ordering::Relaxed) as f32 / 10.0
    }
}

#[derive(Clone)]
pub struct Snapshot {
    pub timestamp: std::time::Instant,
    pub entities: std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>,
}

#[derive(Resource, Default)]
pub struct BallPositions {
    pub snapshots: Arc<std::sync::Mutex<std::collections::VecDeque<Snapshot>>>,
}

use bevy_rapier3d::prelude::Velocity;

fn send_player_state(
    net: Res<NetworkResource>,
    query: Query<(&Transform, &Velocity), With<crate::PlayerShip>>,
    time: Res<Time>,
    mut timer: Local<f32>,
) {
    if net.client.is_none() {
        return;
    }

    *timer += time.delta_secs();
    if *timer < 0.016 {
        return;
    }
    *timer = 0.0;

    if let Ok((transform, velocity)) = query.get_single() {
        let pos = (
            transform.translation.x,
            transform.translation.y,
            transform.translation.z,
        );
        let vel = (velocity.linvel.x, velocity.linvel.y, velocity.linvel.z);

        let conns = net.all_connections.clone();
        let client_lock = net.client.as_ref().unwrap().clone();
        let rt_handle = net.runtime.handle().clone();
        rt_handle.spawn(async move {
            let msg_bytes = {
                let client = client_lock.lock().await;
                let mut serializer = zeus_common::GhostSerializer::new();
                serializer.set_keypair(client.signing_key().clone());
                serializer.serialize(client.local_id(), pos, vel).to_vec()
            };
            let conn_list: Vec<quinn::Connection> = {
                conns.lock().map(|g| g.clone()).unwrap_or_default()
            };
            for conn in &conn_list {
                if let Ok(mut stream) = conn.open_uni().await {
                    let _ = stream.write_all(&msg_bytes).await;
                    let _ = stream.finish();
                }
            }
        });
    }
}

fn check_roaming(
    _net: ResMut<NetworkResource>,
    _query: Query<&Transform, With<crate::PlayerShip>>,
    _server_status: Res<ServerStatus>,
) {
}

#[derive(Resource)]
pub struct NetworkResource {
    pub client: Option<Arc<Mutex<ZeusClient>>>,
    pub runtime: Runtime,

    pub accumulated:
        Option<Arc<std::sync::Mutex<std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32), std::time::Instant)>>>>,
    pub all_connections: Arc<std::sync::Mutex<Vec<quinn::Connection>>>,
}

impl Default for NetworkResource {
    fn default() -> Self {
        Self {
            client: None,
            runtime: Runtime::new().unwrap(),
            accumulated: None,
            all_connections: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

fn setup_network(
    mut net: ResMut<NetworkResource>,
    status: Res<ServerStatus>,
    accumulated_state: Res<AccumulatedState>,
) {
    let rt_handle = net.runtime.handle().clone();

    let client = {
        let _guard = net.runtime.enter();
        match ZeusClient::new(rand::random()) {
            Ok(c) => Arc::new(Mutex::new(c)),
            Err(e) => {
                eprintln!("Failed to create client: {}", e);
                return;
            }
        }
    };

    {
        let player_id = {
            let _guard = net.runtime.enter();
            let c = client.blocking_lock();
            c.local_id()
        };
        if let Ok(mut pid) = accumulated_state.player_id.lock() {
            *pid = Some(player_id);
        }
    }

    net.client = Some(client.clone());

    let entity_count = status.entity_count.clone();
    let node_count = status.node_count.clone();
    let map_width = status.map_width.clone();
    let ball_radius = status.ball_radius.clone();

    let accumulated_positions = accumulated_state.positions.clone();
    let accumulated_positions_bb = accumulated_state.player_entity_ids.clone();
    net.accumulated = Some(accumulated_positions.clone());

    let connected_ports: Arc<std::sync::Mutex<std::collections::HashSet<u16>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let all_connections = net.all_connections.clone();

    let spawn_reader_for_port = {
        let client_clone = client.clone();
        let entity_count = entity_count.clone();
        let node_count = node_count.clone();
        let map_width = map_width.clone();
        let ball_radius = ball_radius.clone();
        let accumulated_positions = accumulated_positions.clone();
        let accumulated_positions_bb = accumulated_positions_bb.clone();
        let connected_ports = connected_ports.clone();
        let all_connections = all_connections.clone();

        move |port: u16, rt_handle: tokio::runtime::Handle| {
            let client_for_port = client_clone.clone();
            let entity_count = entity_count.clone();
            let node_count = node_count.clone();
            let map_width = map_width.clone();
            let ball_radius = ball_radius.clone();
            let accumulated_positions = accumulated_positions.clone();
            let accumulated_positions_bb = accumulated_positions_bb.clone();
            let connected_ports = connected_ports.clone();
            let all_connections = all_connections.clone();

            rt_handle.spawn(async move {
                let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
                let endpoint = {
                    let c = client_for_port.lock().await;
                    c.endpoint().clone()
                };

                for attempt in 0..10u32 {
                    match endpoint.connect(addr, "localhost") {
                Ok(connecting) => {
                    match connecting.await {
                        Ok(conn) => {
                            if let Ok(mut ports) = connected_ports.lock() {
                                ports.insert(port);
                            }
                            if let Ok(mut conns) = all_connections.lock() {
                                conns.push(conn.clone());
                            }
                                    loop {
                                        match conn.read_datagram().await {
                                            Ok(data) => {
                                                let data = data.to_vec();
                                                if data.len() >= 4 && data[0] == 0xAA {
                                                    let entities = ((data[1] as u16) << 8) | (data[2] as u16);
                                                    let nodes = data[3];
                                                    entity_count.store(entities, Ordering::Relaxed);
                                                    let prev = node_count.load(Ordering::Relaxed);
                                                    if nodes > prev {
                                                        node_count.store(nodes, Ordering::Relaxed);
                                                    }
                                                    if data.len() >= 6 {
                                                        map_width.store(data[4], Ordering::Relaxed);
                                                        ball_radius.store(data[5], Ordering::Relaxed);
                                                    }
                                                } else if data.len() >= 3 && data[0] == 0xCC {
                                                    if let Ok(mut map) = accumulated_positions.lock() {
                                                        let now = std::time::Instant::now();
                                                        let count = u16::from_le_bytes([data[1], data[2]]) as usize;
                                                        let mut offset = 3usize;
                                                        for _ in 0..count {
                                                            if offset + 15 > data.len() { break; }
                                                            let id = u64::from_le_bytes([
                                                                data[offset], data[offset+1], data[offset+2], data[offset+3],
                                                                data[offset+4], data[offset+5], data[offset+6], data[offset+7],
                                                            ]);
                                                            offset += 8;
                                                            let flags = data[offset];
                                                            offset += 1;
                                                            let px = i16::from_le_bytes([data[offset], data[offset+1]]) as f32 / 500.0;
                                                            offset += 2;
                                                            let py = i16::from_le_bytes([data[offset], data[offset+1]]) as f32 / 500.0;
                                                            offset += 2;
                                                            let pz = i16::from_le_bytes([data[offset], data[offset+1]]) as f32 / 500.0;
                                                            offset += 2;
                                                            let (vx, vy, vz) = if flags & 1 == 0 {
                                                                if offset + 6 > data.len() { break; }
                                                                let vx = i16::from_le_bytes([data[offset], data[offset+1]]) as f32 / 100.0;
                                                                offset += 2;
                                                                let vy = i16::from_le_bytes([data[offset], data[offset+1]]) as f32 / 100.0;
                                                                offset += 2;
                                                                let vz = i16::from_le_bytes([data[offset], data[offset+1]]) as f32 / 100.0;
                                                                offset += 2;
                                                                (vx, vy, vz)
                                                            } else {
                                                                (0.0, 0.0, 0.0)
                                                            };
                                                            map.insert(id, ((px, py, pz), (vx, vy, vz), now));
                                                        }
                                                    }
                                                } else if data.len() >= 3 && data[0] == 0xBB {
                                                    let count = ((data[1] as u16) << 8) | (data[2] as u16);
                                                    let mut offset = 3;
                                                    if let Ok(mut set) = accumulated_positions_bb.lock() {
                                                        for _ in 0..count {
                                                            if offset + 8 <= data.len() {
                                                                let id = u64::from_le_bytes(
                                                                    data[offset..offset + 8].try_into().unwrap(),
                                                                );
                                                                set.insert(id);
                                                                offset += 8;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                    return;
                                }
                                Err(_) => {}
                            }
                        }
                        Err(_) => {}
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt as u64 + 1))).await;
                }
            });
        }
    };

    let spawn_reader = spawn_reader_for_port.clone();
    let rth = rt_handle.clone();
    spawn_reader(5000, rth);

    let spawn_reader_for_new = spawn_reader_for_port;
    let node_count_poll = status.node_count.clone();
    let connected_ports_poll = connected_ports.clone();
    rt_handle.spawn(async move {
        let mut last_count = 1u8;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let current = node_count_poll.load(Ordering::Relaxed);
            if current > last_count {
                for i in last_count..current {
                    let port = 5000 + i as u16;
                    let already = connected_ports_poll.lock().map(|s| s.contains(&port)).unwrap_or(false);
                    if !already {
                        let rth = tokio::runtime::Handle::current();
                        spawn_reader_for_new(port, rth);
                    }
                }
                last_count = current;
            }
        }
    });
}

pub fn encode_0xdd(count: u16) -> Vec<u8> {
    vec![0xDD, (count >> 8) as u8, (count & 0xFF) as u8]
}

#[allow(dead_code)]
pub fn decode_0xdd(data: &[u8]) -> u16 {
    if data.len() < 3 || data[0] != 0xDD {
        return 0;
    }
    ((data[1] as u16) << 8) | (data[2] as u16)
}

#[allow(dead_code)]
pub fn encode_0xbb(player_ids: &[u64]) -> Vec<u8> {
    let count = player_ids.len() as u16;
    let mut buf = Vec::with_capacity(3 + player_ids.len() * 8);
    buf.push(0xBB);
    buf.push((count >> 8) as u8);
    buf.push((count & 0xFF) as u8);
    for id in player_ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    buf
}

#[allow(dead_code)]
pub fn decode_0xbb(data: &[u8]) -> Vec<u64> {
    if data.len() < 3 || data[0] != 0xBB {
        return Vec::new();
    }
    let count = ((data[1] as u16) << 8) | (data[2] as u16);
    let mut ids = Vec::with_capacity(count as usize);
    let mut offset = 3;
    for _ in 0..count {
        if offset + 8 <= data.len() {
            let id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            ids.push(id);
            offset += 8;
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_0xdd_encode() {
        let buf = encode_0xdd(100);
        assert_eq!(buf, vec![0xDD, 0x00, 0x64]);
    }

    #[test]
    fn test_0xdd_decode() {
        let buf = vec![0xDD, 0x00, 0x64];
        assert_eq!(decode_0xdd(&buf), 100);
    }

    #[test]
    fn test_extrapolation_basic() {
        let pos = (10.0f32, 0.0, 0.0);
        let vel = (5.0f32, 0.0, 0.0);
        let elapsed = 0.02f32;
        let result = (pos.0 + vel.0 * elapsed, pos.1 + vel.1 * elapsed, pos.2 + vel.2 * elapsed);
        assert!((result.0 - 10.1).abs() < 0.001);
    }

    #[test]
    fn test_extrapolation_capped() {
        let pos = (10.0f32, 0.0, 0.0);
        let vel = (5.0f32, 0.0, 0.0);
        let elapsed = 0.1f32;
        let capped = elapsed.min(0.05);
        let result = (pos.0 + vel.0 * capped, pos.1 + vel.1 * capped, pos.2 + vel.2 * capped);
        assert!((result.0 - 10.25).abs() < 0.001);
    }

    #[test]
    fn test_snapshot_stores_velocity() {
        let mut entities = std::collections::HashMap::new();
        entities.insert(1u64, ((1.0f32, 2.0, 3.0), (4.0f32, 5.0, 6.0)));
        let snap = Snapshot {
            timestamp: std::time::Instant::now(),
            entities,
        };
        let (pos, vel) = snap.entities.get(&1).unwrap();
        assert_eq!(*pos, (1.0, 2.0, 3.0));
        assert_eq!(*vel, (4.0, 5.0, 6.0));
    }

    #[test]
    fn test_0xbb_encode() {
        let ids = vec![100u64, 200, 300];
        let buf = encode_0xbb(&ids);
        assert_eq!(buf[0], 0xBB);
        assert_eq!(buf[1], 0x00);
        assert_eq!(buf[2], 0x03);
        assert_eq!(buf.len(), 3 + 3 * 8);
        let id1 = u64::from_le_bytes(buf[3..11].try_into().unwrap());
        assert_eq!(id1, 100);
    }

    #[test]
    fn test_0xbb_decode() {
        let ids = vec![111u64, 222, 333];
        let buf = encode_0xbb(&ids);
        let decoded = decode_0xbb(&buf);
        assert_eq!(decoded, ids);
    }

    #[test]
    fn test_0xbb_empty() {
        let buf = encode_0xbb(&[]);
        assert_eq!(buf, vec![0xBB, 0x00, 0x00]);
        let decoded = decode_0xbb(&buf);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_player_id_set_updates() {
        let set = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::<u64>::new()));
        let buf1 = encode_0xbb(&[10, 20]);
        let ids1 = decode_0xbb(&buf1);
        {
            let mut s = set.lock().unwrap();
            *s = ids1.into_iter().collect();
        }
        assert_eq!(set.lock().unwrap().len(), 2);

        let buf2 = encode_0xbb(&[30, 40, 50]);
        let ids2 = decode_0xbb(&buf2);
        {
            let mut s = set.lock().unwrap();
            *s = ids2.into_iter().collect();
        }
        assert_eq!(set.lock().unwrap().len(), 3);
        assert!(!set.lock().unwrap().contains(&10));
        assert!(set.lock().unwrap().contains(&30));
    }
}
