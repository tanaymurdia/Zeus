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
            .add_systems(Update, (send_player_state, generate_snapshots));
    }
}

/// Buffer for incoming entity updates (since packet batching splits them up)
#[derive(Resource, Default)]
pub struct AccumulatedState {
    pub positions: Arc<std::sync::Mutex<std::collections::HashMap<u64, (f32, f32, f32)>>>,
    /// The local player's entity ID, used to filter self from NPC rendering
    pub player_id: Arc<std::sync::Mutex<Option<u64>>>,
}

fn generate_snapshots(
    accumulated: Res<AccumulatedState>,
    ball_pos: Res<BallPositions>,
    time: Res<Time>,
    mut timer: Local<f32>,
) {
    // Generate snapshot at 60Hz independent of packet arrival
    *timer += time.delta_secs();
    if *timer < 0.016 {
        return;
    }
    *timer = 0.0;

    let map_lock = accumulated.positions.lock().unwrap();
    if map_lock.is_empty() {
        return;
    }

    let mut new_positions = std::collections::HashMap::with_capacity(map_lock.len());
    for (&id, &pos) in map_lock.iter() {
        new_positions.insert(id, pos);
    }

    if let Ok(mut buffer) = ball_pos.snapshots.lock() {
        buffer.push_back(Snapshot {
            timestamp: std::time::Instant::now(),
            entities: new_positions,
        });
        while buffer.len() > 20 {
            buffer.pop_front();
        }
    }
}

/// Real server status received from backend
#[derive(Resource, Default)]
pub struct ServerStatus {
    // Using atomics for lock-free access from game thread
    pub entity_count: Arc<AtomicU16>,
    pub node_count: Arc<AtomicU8>,
}

impl ServerStatus {
    pub fn get_entity_count(&self) -> u16 {
        self.entity_count.load(Ordering::Relaxed)
    }

    pub fn get_node_count(&self) -> u8 {
        self.node_count.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct Snapshot {
    pub timestamp: std::time::Instant,
    pub entities: std::collections::HashMap<u64, (f32, f32, f32)>,
}

/// Ball positions history for interpolation
#[derive(Resource, Default)]
pub struct BallPositions {
    // Store last 10 snapshots
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

    // Rate Limit: 60Hz (0.016s) for responsive collisions
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

        // Send via Client
        let client_lock = net.client.as_ref().unwrap().clone();
        let rt_handle = net.runtime.handle().clone();
        // Fire and forget send task
        rt_handle.spawn(async move {
            let client = client_lock.lock().await;
            let _ = client.send_state(pos, vel).await;
        });
    }
}

#[derive(Resource)]
pub struct NetworkResource {
    pub client: Option<Arc<Mutex<ZeusClient>>>,
    pub runtime: Runtime,
    // Shared accumulated state for net thread to write to
    pub accumulated: Option<Arc<std::sync::Mutex<std::collections::HashMap<u64, (f32, f32, f32)>>>>,
}

impl Default for NetworkResource {
    fn default() -> Self {
        Self {
            client: None,
            runtime: Runtime::new().unwrap(),
            accumulated: None,
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

    // Store the player's entity ID so we can filter it from NPC rendering
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

    // Clone resources for background task
    let entity_count = status.entity_count.clone();
    let node_count = status.node_count.clone();

    // Use the global resource's accumulator so the systems can read it!
    let accumulated_positions = accumulated_state.positions.clone();
    net.accumulated = Some(accumulated_positions.clone()); // Optional: keep it in net resource too if needed, but redundant now

    // Spawn connect and status reader task
    let client_clone = client.clone();
    rt_handle.spawn(async move {
        let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
        println!("Attempting to connect to Mesh Node at {}...", addr);

        {
            let mut locked_client = client_clone.lock().await;
            match locked_client.connect(addr).await {
                Ok(_) => println!("✅ Connected to Mesh!"),
                Err(e) => {
                    eprintln!("❌ Connection failed: {}", e);
                    return;
                }
            }
        }

        // Background loop: read all datagrams
        loop {
            // Get connection handle so we don't hold the client lock while waiting
            let conn = {
                let client = client_clone.lock().await;
                client.connection()
            };

            if let Some(conn) = conn {
                // Now we can await without holding the lock!
                // println!("[Net] Waiting for datagram...");
                match conn.read_datagram().await {
                    Ok(data) => {
                        let data = data.to_vec(); // Copy data from Bytes
                        // println!("[Net] Read datagram {} bytes. Byte[0]={:02X?}", data.len(), data.get(0));

                        if data.len() >= 4 && data[0] == 0xAA {
                            // Status message
                            let entities = ((data[1] as u16) << 8) | (data[2] as u16);
                            let nodes = data[3];
                            entity_count.store(entities, Ordering::Relaxed);
                            node_count.store(nodes, Ordering::Relaxed);
                        } else if data.len() >= 1 && data[0] == 0xCC {
                            // StateUpdate (standardized)
                            let bytes = &data[1..];
                            // println!("[Net] Received StateUpdate ({} bytes)", bytes.len());
                            if let Ok(update) =
                                zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(bytes)
                            {
                                if let Some(ghosts) = update.ghosts() {
                                    if let Ok(mut map) = accumulated_positions.lock() {
                                        for ghost in ghosts {
                                            if let Some(pos) = ghost.position() {
                                                let id = ghost.entity_id();
                                                map.insert(id, (pos.x(), pos.y(), pos.z()));
                                            }
                                        }
                                        // Log sample positions every ~60 updates
                                        static COUNTER: std::sync::atomic::AtomicU32 =
                                            std::sync::atomic::AtomicU32::new(0);
                                        let c = COUNTER.fetch_add(1, Ordering::Relaxed);
                                        if c % 60 == 0 {
                                            println!(
                                                "[Net] === NPC Positions ({} total) ===",
                                                map.len()
                                            );
                                            for (id, (x, y, z)) in map.iter().take(5) {
                                                println!(
                                                    "[Net]   Ball {}: x={:.2}, y={:.2}, z={:.2}",
                                                    id, x, y, z
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        } else if data.len() >= 3 && data[0] == 0xBB {
                            // Legacy Position message
                        }
                    }
                    Err(e) => {
                        eprintln!("[Net] Read Error (Connection lost?): {}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            } else {
                // Not connected yet, wait a bit
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    });
}
