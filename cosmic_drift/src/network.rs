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
    let stale_threshold = std::time::Duration::from_millis(300);
    map_lock.retain(|_, (_, _, last_seen)| now.duration_since(*last_seen) < stale_threshold);

    let new_positions: std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32))> = map_lock
        .iter()
        .map(|(&id, &(pos, vel, _))| (id, (pos, vel)))
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
    pub fn get_entity_count(&self) -> u16 {
        self.entity_count.load(Ordering::Relaxed)
    }

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

        let client_lock = net.client.as_ref().unwrap().clone();
        let rt_handle = net.runtime.handle().clone();
        rt_handle.spawn(async move {
            let client = client_lock.lock().await;
            let _ = client.send_state(pos, vel).await;
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
        Option<Arc<std::sync::Mutex<std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>>>>,
    pub current_zone: usize,
}

impl Default for NetworkResource {
    fn default() -> Self {
        Self {
            client: None,
            runtime: Runtime::new().unwrap(),
            accumulated: None,
            current_zone: 0,
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

    let client_clone = client.clone();
    rt_handle.spawn(async move {
        let accumulated_positions_bb = accumulated_positions_bb;
        let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
        {
            let mut locked_client = client_clone.lock().await;
            if let Err(e) = locked_client.connect(addr).await {
                eprintln!("[Net] Connection failed: {}", e);
                return;
            }
        }

        loop {
            let conn = {
                let client = client_clone.lock().await;
                client.connection()
            };

            if let Some(conn) = conn {
                match conn.read_datagram().await {
                    Ok(data) => {
                        let data = data.to_vec();
                        if data.len() >= 4 && data[0] == 0xAA {
                            let entities = ((data[1] as u16) << 8) | (data[2] as u16);
                            let nodes = data[3];
                            entity_count.store(entities, Ordering::Relaxed);
                            node_count.store(nodes, Ordering::Relaxed);

                            if data.len() >= 6 {
                                map_width.store(data[4], Ordering::Relaxed);
                                ball_radius.store(data[5], Ordering::Relaxed);
                            }
                        } else if data.len() >= 1 && data[0] == 0xCC {
                            let bytes = &data[1..];
                            if let Ok(update) =
                                zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(bytes)
                            {
                                if let Some(ghosts) = update.ghosts() {
                                    if let Ok(mut map) = accumulated_positions.lock() {
                                        let now = std::time::Instant::now();
                                        for ghost in ghosts {
                                            let id = ghost.entity_id();
                                            let pos = ghost
                                                .position()
                                                .map(|p| (p.x(), p.y(), p.z()))
                                                .unwrap_or((0.0, 0.0, 0.0));
                                            let vel = ghost
                                                .velocity()
                                                .map(|v| (v.x(), v.y(), v.z()))
                                                .unwrap_or((0.0, 0.0, 0.0));
                                            map.insert(id, (pos, vel, now));
                                        }
                                    }
                                }
                            }
                        } else if data.len() >= 3 && data[0] == 0xBB {
                            let count =
                                ((data[1] as u16) << 8) | (data[2] as u16);
                            let mut ids = std::collections::HashSet::new();
                            let mut offset = 3;
                            for _ in 0..count {
                                if offset + 8 <= data.len() {
                                    let id = u64::from_le_bytes(
                                        data[offset..offset + 8].try_into().unwrap(),
                                    );
                                    ids.insert(id);
                                    offset += 8;
                                }
                            }
                            if let Ok(mut set) =
                                accumulated_positions_bb.lock()
                            {
                                *set = ids;
                            }
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
