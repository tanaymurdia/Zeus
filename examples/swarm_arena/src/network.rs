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
            .insert_resource(DronePositions::default())
            .insert_resource(AccumulatedState::default())
            .insert_resource(OctreeCells::default())
            .add_systems(Startup, setup_network)
            .add_systems(
                Update,
                (send_player_state, generate_snapshots),
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
    drone_pos: Res<DronePositions>,
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

    if let Ok(mut buffer) = drone_pos.snapshots.lock() {
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
    pub per_node_counts: Arc<std::sync::Mutex<std::collections::HashMap<u16, u16>>>,
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
pub struct DronePositions {
    pub snapshots: Arc<std::sync::Mutex<std::collections::VecDeque<Snapshot>>>,
}

#[derive(Clone, Debug)]
pub struct CellBounds {
    pub x_min: f32,
    pub x_max: f32,
    pub y_min: f32,
    pub y_max: f32,
    pub z_min: f32,
    pub z_max: f32,
}

#[derive(Resource, Default)]
pub struct OctreeCells {
    pub cells: Arc<std::sync::Mutex<Vec<CellBounds>>>,
    pub per_port: Arc<std::sync::Mutex<std::collections::HashMap<u16, Vec<CellBounds>>>>,
}

use bevy_rapier3d::prelude::Velocity;

fn send_player_state(
    net: Res<NetworkResource>,
    query: Query<(&Transform, &Velocity), With<crate::PlayerShip>>,
    gravity_mode: Res<crate::GravityMode>,
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

        let mode = gravity_mode.mode;
        let conns = net.all_connections.clone();
        let client_lock = net.client.as_ref().unwrap().clone();
        let rt_handle = net.runtime.handle().clone();
        rt_handle.spawn(async move {
            let (msg_bytes, local_id) = {
                let client = client_lock.lock().await;
                let mut serializer = zeus_common::GhostSerializer::new();
                serializer.set_keypair(client.signing_key().clone());
                let bytes = serializer.serialize(client.local_id(), pos, vel).to_vec();
                (bytes, client.local_id())
            };
            let conn_list: Vec<quinn::Connection> = {
                conns.lock().map(|g| g.clone()).unwrap_or_default()
            };

            let mut gravity_buf = Vec::with_capacity(12);
            gravity_buf.push(0xDD);
            gravity_buf.push(mode);
            gravity_buf.push(0);
            gravity_buf.push(0);
            gravity_buf.extend_from_slice(&local_id.to_le_bytes());

            for conn in &conn_list {
                if let Ok(mut stream) = conn.open_uni().await {
                    let _ = stream.write_all(&msg_bytes).await;
                    let _ = stream.finish();
                }
                let _ = conn.send_datagram(bytes::Bytes::from(gravity_buf.clone()));
            }
        });
    }
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
    octree_cells: Res<OctreeCells>,
) {
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
    let octree_cells_shared = octree_cells.cells.clone();
    let octree_per_port = octree_cells.per_port.clone();
    net.accumulated = Some(accumulated_positions.clone());

    let connected_ports: Arc<std::sync::Mutex<std::collections::HashSet<u16>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let all_connections = net.all_connections.clone();
    let rt_handle = net.runtime.handle().clone();

    let per_node_counts = status.per_node_counts.clone();

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
        let octree_cells_shared = octree_cells_shared.clone();
        let octree_per_port = octree_per_port.clone();
        let per_node_counts = per_node_counts.clone();

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
            let octree_cells_shared = octree_cells_shared.clone();
            let octree_per_port = octree_per_port.clone();
            let per_node_counts = per_node_counts.clone();

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
                                                    if let Ok(mut counts) = per_node_counts.lock() {
                                                        counts.insert(port, entities);
                                                        let total: u16 = counts.values().sum();
                                                        entity_count.store(total, Ordering::Relaxed);
                                                    }
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
                                                            if let Some(&((ox, oy, oz), _, ref old_t)) = map.get(&id) {
                                                                let dx = px - ox;
                                                                let dy = py - oy;
                                                                let dz = pz - oz;
                                                                let dist = (dx*dx + dy*dy + dz*dz).sqrt();
                                                                let dt_ms = now.duration_since(*old_t).as_millis();
                                                                if dist > 1.0 && dt_ms < 200 && id < 1_000_000 {
                                                                    eprintln!(
                                                                        "[CLIENT JUMP] port={} id={} dist={:.2} dt={}ms old=({:.2},{:.2},{:.2}) new=({:.2},{:.2},{:.2})",
                                                                        port, id, dist, dt_ms, ox, oy, oz, px, py, pz
                                                                    );
                                                                }
                                                            }
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
                                                } else if data.len() >= 2 && data[0] == 0xEE {
                                                    let cell_count = data[1] as usize;
                                                    let mut new_cells = Vec::with_capacity(cell_count);
                                                    let mut offset = 2usize;
                                                    for _ in 0..cell_count {
                                                        if offset + 24 > data.len() { break; }
                                                        let x_min = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                                                        offset += 4;
                                                        let x_max = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                                                        offset += 4;
                                                        let y_min = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                                                        offset += 4;
                                                        let y_max = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                                                        offset += 4;
                                                        let z_min = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                                                        offset += 4;
                                                        let z_max = f32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
                                                        offset += 4;
                                                        new_cells.push(CellBounds { x_min, x_max, y_min, y_max, z_min, z_max });
                                                    }
                                                    if let Ok(mut pp) = octree_per_port.lock() {
                                                        pp.insert(port, new_cells);
                                                        let merged: Vec<CellBounds> = pp.values().flatten().cloned().collect();
                                                        if let Ok(mut c) = octree_cells_shared.lock() {
                                                            *c = merged;
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
    spawn_reader(9000, rth);

    let spawn_reader_for_new = spawn_reader_for_port;
    let node_count_poll = status.node_count.clone();
    let connected_ports_poll = connected_ports.clone();
    rt_handle.spawn(async move {
        let mut last_count = 1u8;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let current = node_count_poll.load(Ordering::Relaxed);
            if current > last_count {
                for i in last_count..current {
                    let port = 9000 + i as u16;
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
