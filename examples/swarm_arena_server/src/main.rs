use clap::Parser;
use rapier3d::prelude::*;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use zeus_node::cell::Cell;
use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::{GameLoop, GameWorld};

fn parse_cell_arg(s: &str) -> Option<Cell> {
    let parts: Vec<f32> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    if parts.len() == 6 {
        Some(Cell::new(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]))
    } else {
        None
    }
}

#[derive(Parser)]
#[command(name = "swarm_arena_server")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, default_value = "127.0.0.1:9000")]
    bind: SocketAddr,
}

#[derive(clap::Subcommand)]
enum Commands {
    Orchestrator {
        #[arg(short, long, default_value = "9000")]
        start_port: u16,
    },
    RunNode {
        #[arg(short, long)]
        bind: SocketAddr,
        #[arg(short, long)]
        id: u8,
        #[arg(long)]
        peer: Option<SocketAddr>,
        #[arg(long)]
        peers: Option<String>,
        #[arg(long)]
        cell: Option<String>,
    },
}

const WORLD_SIZE: f32 = 24.0;
const DRONE_RADIUS: f32 = 0.3;
const DRONE_DENSITY: f32 = 1.0;
const DRONE_DAMPING: f32 = 3.0;
const MAX_DRONE_SPEED: f32 = 4.0;
const WANDER_FORCE: f32 = 0.8;
const WALL_REPEL_DIST: f32 = 4.0;
const WALL_REPEL_FORCE: f32 = 4.0;
const ATTRACT_STRENGTH: f32 = 30.0;
const REPEL_STRENGTH: f32 = 50.0;
const WELL_RADIUS: f32 = 30.0;

struct Drone {
    rigid_body_handle: RigidBodyHandle,
    wander_seed: u32,
}

#[derive(Clone, Copy)]
struct GravityWell {
    pos: (f32, f32, f32),
    mode: u8,
}

struct DroneWorld {
    rigid_body_set: RigidBodySet,
    collider_set: ColliderSet,
    integration_parameters: IntegrationParameters,
    physics_pipeline: PhysicsPipeline,
    island_manager: IslandManager,
    broad_phase: DefaultBroadPhase,
    narrow_phase: NarrowPhase,
    impulse_joint_set: ImpulseJointSet,
    multibody_joint_set: MultibodyJointSet,
    ccd_solver: CCDSolver,
    query_pipeline: QueryPipeline,

    drones: HashMap<u64, Drone>,
    drone_ids: HashSet<u64>,
    next_drone_id: u64,
    player_wells: HashMap<u64, GravityWell>,
    tick_counter: u64,
    world_min: (f32, f32, f32),
    world_max: (f32, f32, f32),
}

impl DroneWorld {
    #[allow(dead_code)]
    fn new() -> Self {
        Self::with_bounds(0.0, WORLD_SIZE, -1.0, WORLD_SIZE + 1.0, -WORLD_SIZE / 2.0, WORLD_SIZE / 2.0)
    }

    fn with_bounds(x_min: f32, x_max: f32, y_min: f32, y_max: f32, z_min: f32, z_max: f32) -> Self {
        let rigid_body_set = RigidBodySet::new();
        let collider_set = ColliderSet::new();

        Self {
            rigid_body_set,
            collider_set,
            integration_parameters: {
                let mut p = IntegrationParameters::default();
                p.dt = 1.0 / 128.0;
                p.num_solver_iterations = std::num::NonZeroUsize::new(4).unwrap();
                p
            },
            physics_pipeline: PhysicsPipeline::new(),
            island_manager: IslandManager::new(),
            broad_phase: DefaultBroadPhase::new(),
            narrow_phase: NarrowPhase::new(),
            impulse_joint_set: ImpulseJointSet::new(),
            multibody_joint_set: MultibodyJointSet::new(),
            ccd_solver: CCDSolver::new(),
            query_pipeline: QueryPipeline::new(),
            drones: HashMap::new(),
            drone_ids: HashSet::new(),
            next_drone_id: 1,
            player_wells: HashMap::new(),
            tick_counter: 0,
            world_min: (x_min, y_min, z_min),
            world_max: (x_max, y_max, z_max),
        }
    }

    #[allow(dead_code)]
    fn spawn_drone(&mut self) -> Option<u64> {
        while self.drones.contains_key(&self.next_drone_id) || self.next_drone_id >= 999_000 {
            self.next_drone_id += 1;
            if self.next_drone_id >= 999_000 {
                return None;
            }
        }
        let id = self.next_drone_id;
        self.next_drone_id += 1;

        let hash = id.wrapping_mul(2654435761);
        let rx = (self.world_max.0 - self.world_min.0) * 0.8;
        let ry = (self.world_max.1 - self.world_min.1) * 0.8;
        let rz = (self.world_max.2 - self.world_min.2) * 0.8;
        let x = self.world_min.0 + rx * 0.1 + ((hash % 1000) as f32 / 1000.0) * rx;
        let y = self.world_min.1 + ry * 0.1 + (((hash / 1000) % 1000) as f32 / 1000.0) * ry;
        let z = self.world_min.2 + rz * 0.1 + (((hash / 1000000) % 1000) as f32 / 1000.0) * rz;

        let vx = ((hash % 7) as f32 - 3.0) * 0.5;
        let vy = (((hash / 7) % 7) as f32 - 3.0) * 0.3;
        let vz = (((hash / 49) % 7) as f32 - 3.0) * 0.5;

        let rigid_body = RigidBodyBuilder::dynamic()
            .translation(vector![x, y, z])
            .linvel(vector![vx, vy, vz])
            .linear_damping(DRONE_DAMPING)
            .angular_damping(1.0)
            .gravity_scale(0.0)
            .ccd_enabled(true)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(DRONE_RADIUS)
            .restitution(0.8)
            .friction(0.1)
            .density(DRONE_DENSITY)
            .build();
        self.collider_set.insert_with_parent(collider, handle, &mut self.rigid_body_set);

        self.drones.insert(id, Drone {
            rigid_body_handle: handle,
            wander_seed: hash as u32,
        });
        self.drone_ids.insert(id);
        Some(id)
    }

    fn spawn_drone_at(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if let Some(drone) = self.drones.get(&id) {
            if let Some(rb) = self.rigid_body_set.get(drone.rigid_body_handle) {
                if rb.is_kinematic() {
                    self.remove_drone(id);
                } else {
                    return;
                }
            } else {
                return;
            }
        }
        let rigid_body = RigidBodyBuilder::dynamic()
            .translation(vector![pos.0, pos.1, pos.2])
            .linvel(vector![vel.0, vel.1, vel.2])
            .linear_damping(DRONE_DAMPING)
            .angular_damping(1.0)
            .gravity_scale(0.0)
            .ccd_enabled(true)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);
        let collider = ColliderBuilder::ball(DRONE_RADIUS)
            .restitution(0.8)
            .friction(0.1)
            .density(DRONE_DENSITY)
            .build();
        self.collider_set.insert_with_parent(collider, handle, &mut self.rigid_body_set);
        self.drones.insert(id, Drone {
            rigid_body_handle: handle,
            wander_seed: id as u32,
        });
        self.drone_ids.insert(id);
    }

    fn spawn_remote_drone(&mut self, id: u64, pos: (f32, f32, f32), _vel: (f32, f32, f32)) {
        if self.drones.contains_key(&id) {
            return;
        }
        let rigid_body = RigidBodyBuilder::kinematic_position_based()
            .translation(vector![pos.0, pos.1, pos.2])
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);
        let collider = ColliderBuilder::ball(DRONE_RADIUS)
            .restitution(0.8)
            .friction(0.1)
            .density(DRONE_DENSITY)
            .build();
        self.collider_set.insert_with_parent(collider, handle, &mut self.rigid_body_set);
        self.drones.insert(id, Drone {
            rigid_body_handle: handle,
            wander_seed: id as u32,
        });
    }

    fn update_drone(&mut self, id: u64, pos: (f32, f32, f32), _vel: (f32, f32, f32)) {
        if let Some(drone) = self.drones.get(&id) {
            if let Some(rb) = self.rigid_body_set.get_mut(drone.rigid_body_handle) {
                if rb.is_kinematic() {
                    rb.set_next_kinematic_position(Isometry::translation(pos.0, pos.1, pos.2));
                }
            }
        }
    }

    fn remove_drone(&mut self, id: u64) {
        if let Some(drone) = self.drones.remove(&id) {
            self.rigid_body_set.remove(
                drone.rigid_body_handle,
                &mut self.island_manager,
                &mut self.collider_set,
                &mut self.impulse_joint_set,
                &mut self.multibody_joint_set,
                true,
            );
        }
        self.drone_ids.remove(&id);
    }

    fn apply_wander_and_walls(&mut self) {
        self.tick_counter += 1;
        let tick = self.tick_counter;
        let wmin = self.world_min;
        let wmax = self.world_max;

        for drone in self.drones.values() {
            if let Some(rb) = self.rigid_body_set.get_mut(drone.rigid_body_handle) {
                if !rb.is_dynamic() { continue; }
                let pos = *rb.translation();
                let inside = pos.x >= wmin.0 && pos.x <= wmax.0
                    && pos.y >= wmin.1 && pos.y <= wmax.1
                    && pos.z >= wmin.2 && pos.z <= wmax.2;
                if !inside { continue; }

                let s = drone.wander_seed as f32;
                let t = tick as f32 / 128.0;
                let freq_x = 0.3 + (s % 7.0) * 0.1;
                let freq_y = 0.2 + (s % 11.0) * 0.08;
                let freq_z = 0.35 + (s % 13.0) * 0.09;
                let phase_x = s * 0.73;
                let phase_y = s * 1.17;
                let phase_z = s * 0.91;
                let wx = (t * freq_x + phase_x).sin() * WANDER_FORCE;
                let wy = (t * freq_y + phase_y).sin() * WANDER_FORCE * 0.7;
                let wz = (t * freq_z + phase_z).sin() * WANDER_FORCE;
                let mut force = vector![wx, wy, wz];

                let margin = WALL_REPEL_DIST;
                if pos.x < wmin.0 + margin { force.x += WALL_REPEL_FORCE * ((1.0 - (pos.x - wmin.0) / margin).clamp(0.0, 2.0)); }
                if pos.x > wmax.0 - margin { force.x -= WALL_REPEL_FORCE * ((1.0 - (wmax.0 - pos.x) / margin).clamp(0.0, 2.0)); }
                if pos.y < wmin.1 + margin { force.y += WALL_REPEL_FORCE * ((1.0 - (pos.y - wmin.1) / margin).clamp(0.0, 2.0)); }
                if pos.y > wmax.1 - margin { force.y -= WALL_REPEL_FORCE * ((1.0 - (wmax.1 - pos.y) / margin).clamp(0.0, 2.0)); }
                if pos.z < wmin.2 + margin { force.z += WALL_REPEL_FORCE * ((1.0 - (pos.z - wmin.2) / margin).clamp(0.0, 2.0)); }
                if pos.z > wmax.2 - margin { force.z -= WALL_REPEL_FORCE * ((1.0 - (wmax.2 - pos.z) / margin).clamp(0.0, 2.0)); }

                rb.add_force(force, true);
            }
        }
    }

    fn apply_gravity_wells(&mut self) {
        let wells: Vec<GravityWell> = self.player_wells.values().copied().collect();
        for drone in self.drones.values() {
            if let Some(rb) = self.rigid_body_set.get_mut(drone.rigid_body_handle) {
                if !rb.is_dynamic() { continue; }
                let pos = *rb.translation();
                for well in &wells {
                    if well.mode == 0 { continue; }
                    let dx = well.pos.0 - pos.x;
                    let dy = well.pos.1 - pos.y;
                    let dz = well.pos.2 - pos.z;
                    let dist_sq = dx * dx + dy * dy + dz * dz;
                    if dist_sq > WELL_RADIUS * WELL_RADIUS || dist_sq < 0.5 { continue; }
                    let dist = dist_sq.sqrt();
                    let strength = if well.mode == 1 { ATTRACT_STRENGTH } else { -REPEL_STRENGTH };
                    let f_mag = strength / dist_sq.max(1.0);
                    let nx = dx / dist;
                    let ny = dy / dist;
                    let nz = dz / dist;
                    rb.add_force(vector![nx * f_mag, ny * f_mag, nz * f_mag], true);
                }
            }
        }
    }

    fn cap_speeds(&mut self) {
        let wmin = self.world_min;
        let wmax = self.world_max;
        for drone in self.drones.values() {
            if let Some(rb) = self.rigid_body_set.get_mut(drone.rigid_body_handle) {
                if !rb.is_dynamic() { continue; }
                let vel = *rb.linvel();
                let speed = vel.norm();
                if speed > MAX_DRONE_SPEED {
                    rb.set_linvel(vel * (MAX_DRONE_SPEED / speed), true);
                }
                let pos = *rb.translation();
                let inside = pos.x >= wmin.0 && pos.x <= wmax.0
                    && pos.y >= wmin.1 && pos.y <= wmax.1
                    && pos.z >= wmin.2 && pos.z <= wmax.2;
                if inside {
                    let clamped = vector![
                        pos.x.clamp(wmin.0 + 0.1, wmax.0 - 0.1),
                        pos.y.clamp(wmin.1 + 0.1, wmax.1 - 0.1),
                        pos.z.clamp(wmin.2 + 0.1, wmax.2 - 0.1)
                    ];
                    if clamped != pos {
                        rb.set_translation(clamped, true);
                    }
                }
            }
        }
    }

    fn run_physics(&mut self) {
        let gravity = vector![0.0, 0.0, 0.0];
        self.physics_pipeline.step(
            &gravity,
            &self.integration_parameters,
            &mut self.island_manager,
            &mut self.broad_phase,
            &mut self.narrow_phase,
            &mut self.rigid_body_set,
            &mut self.collider_set,
            &mut self.impulse_joint_set,
            &mut self.multibody_joint_set,
            &mut self.ccd_solver,
            Some(&mut self.query_pipeline),
            &(),
            &(),
        );
    }

    fn get_drone_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.drones.get(&id).and_then(|drone| {
            self.rigid_body_set.get(drone.rigid_body_handle).map(|rb| {
                let pos = rb.translation();
                let vel = rb.linvel();
                ((pos.x, pos.y, pos.z), (vel.x, vel.y, vel.z))
            })
        })
    }

    fn drone_count(&self) -> usize {
        self.drone_ids.len()
    }

    fn spawn_drone_near(&mut self, center: (f32, f32, f32), count: usize) -> Vec<u64> {
        let mut spawned = Vec::new();
        for i in 0..count {
            while self.drones.contains_key(&self.next_drone_id) {
                self.next_drone_id += 1;
            }
            let id = self.next_drone_id;
            self.next_drone_id += 1;
            let hash = id.wrapping_mul(2654435761);
            let offset_x = ((hash % 100) as f32 / 50.0 - 1.0) * 3.0;
            let offset_y = (((hash / 100) % 100) as f32 / 50.0 - 1.0) * 3.0;
            let offset_z = (((hash / 10000) % 100) as f32 / 50.0 - 1.0) * 3.0;
            let x = (center.0 + offset_x).clamp(self.world_min.0 + 0.5, self.world_max.0 - 0.5);
            let y = (center.1 + offset_y).clamp(self.world_min.1 + 0.5, self.world_max.1 - 0.5);
            let z = (center.2 + offset_z).clamp(self.world_min.2 + 0.5, self.world_max.2 - 0.5);
            let vx = ((hash % 7) as f32 - 3.0) * 0.3;
            let vy = (((hash / 7) % 7) as f32 - 3.0) * 0.2;
            let vz = (((hash / 49) % 7) as f32 - 3.0) * 0.3;
            let rigid_body = RigidBodyBuilder::dynamic()
                .translation(vector![x, y, z])
                .linvel(vector![vx, vy, vz])
                .linear_damping(DRONE_DAMPING)
                .angular_damping(1.0)
                .gravity_scale(0.0)
                .ccd_enabled(true)
                .build();
            let handle = self.rigid_body_set.insert(rigid_body);
            let collider = ColliderBuilder::ball(DRONE_RADIUS)
                .restitution(0.8)
                .friction(0.1)
                .density(DRONE_DENSITY)
                .build();
            self.collider_set.insert_with_parent(collider, handle, &mut self.rigid_body_set);
            self.drones.insert(id, Drone {
                rigid_body_handle: handle,
                wander_seed: (hash as u32).wrapping_add(i as u32),
            });
            self.drone_ids.insert(id);
            spawned.push(id);
        }
        spawned
    }

    fn remove_n_drones(&mut self, count: usize) -> Vec<u64> {
        let ids_to_remove: Vec<u64> = self.drone_ids.iter().copied().take(count).collect();
        for &id in &ids_to_remove {
            self.remove_drone(id);
        }
        ids_to_remove
    }
}

impl GameWorld for DroneWorld {
    fn step(&mut self, _dt: f32) {
        self.apply_wander_and_walls();
        self.apply_gravity_wells();
        self.run_physics();
        self.cap_speeds();
    }

    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if id < 1_000_000 {
            self.spawn_drone_at(id, pos, vel);
        } else {
            self.spawn_remote_drone(id, pos, vel);
        }
    }

    fn on_entity_departed(&mut self, id: u64) {
        self.remove_drone(id);
        self.drone_ids.remove(&id);
    }

    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.spawn_remote_drone(id, pos, vel);
        self.update_drone(id, pos, vel);
    }

    fn locally_simulated_ids(&self) -> &HashSet<u64> {
        &self.drone_ids
    }

    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.get_drone_state(id)
    }

    fn status_payload(&self) -> (u16, u8, u8) {
        (0, WORLD_SIZE as u8, (DRONE_RADIUS * 10.0) as u8)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Orchestrator { start_port }) => run_orchestrator(start_port).await,
        Some(Commands::RunNode { bind, id, peer, peers, cell }) => {
            let mut seed_addrs: Vec<SocketAddr> = Vec::new();
            if let Some(peers_str) = peers {
                for addr_str in peers_str.split(',') {
                    if let Ok(addr) = addr_str.trim().parse::<SocketAddr>() {
                        seed_addrs.push(addr);
                    }
                }
            }
            if let Some(p) = peer {
                if !seed_addrs.contains(&p) {
                    seed_addrs.push(p);
                }
            }
            let parsed_cell = cell.and_then(|s| parse_cell_arg(&s));
            run_node(bind, id, seed_addrs, parsed_cell).await
        }
        None => run_orchestrator(9000).await,
    }
}

enum OrchestratorMsg {
    Warmup { _node_id: u8, new_cell: Option<String> },
    Split { _node_id: u8, new_cell: Option<String> },
    Merge { node_id: u8 },
}

async fn run_orchestrator(start_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};

    for port in start_port..start_port + 16 {
        let output = std::process::Command::new("lsof")
            .args(["-ti", &format!(":{}", port)])
            .output();
        if let Ok(out) = output {
            let pids = String::from_utf8_lossy(&out.stdout);
            for pid_str in pids.split_whitespace() {
                if let Ok(pid) = pid_str.trim().parse::<i32>() {
                    let _ = std::process::Command::new("kill").arg("-9").arg(pid.to_string()).output();
                }
            }
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    println!("[Swarm Arena Orchestrator] Starting on port {}", start_port);

    let mut nodes: Vec<(u8, tokio::process::Child)> = Vec::new();
    let mut next_id = 1u8;
    let mut active_ports: Vec<u16> = Vec::new();

    let spawn_node = |id: u8, port: u16, peer_ports: &[u16], cell: Option<&str>| {
        let mut cmd = tokio::process::Command::new(std::env::current_exe().unwrap());
        cmd.arg("run-node")
            .arg("--bind").arg(format!("127.0.0.1:{}", port))
            .arg("--id").arg(id.to_string());
        if !peer_ports.is_empty() {
            let peers_str: String = peer_ports.iter()
                .map(|p| format!("127.0.0.1:{}", p))
                .collect::<Vec<_>>().join(",");
            cmd.arg("--peers").arg(peers_str);
        }
        if let Some(cell_str) = cell {
            cmd.arg("--cell").arg(cell_str);
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        cmd
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel::<OrchestratorMsg>(32);

    let mut cmd0 = spawn_node(0, start_port, &[], None);
    let mut child0 = cmd0.spawn()?;
    if let Some(stdout) = child0.stdout.take() {
        let tx_c = tx.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                println!("[Node 0] {}", line);
                if line.contains("REQUEST_WARMUP") {
                    let nc = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
                    let _ = tx_c.send(OrchestratorMsg::Warmup { _node_id: 0, new_cell: nc }).await;
                } else if line.contains("REQUEST_SPLIT") {
                    let new_cell = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
                    let _ = tx_c.send(OrchestratorMsg::Split { _node_id: 0, new_cell }).await;
                } else if line.contains("REQUEST_MERGE") {
                    let _ = tx_c.send(OrchestratorMsg::Merge { node_id: 0 }).await;
                }
            }
        });
    }
    if let Some(stderr) = child0.stderr.take() {
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                eprintln!("[Node 0 ERR] {}", line);
            }
        });
    }
    nodes.push((0, child0));
    active_ports.push(start_port);

    let mut last_spawn = std::time::Instant::now();
    let mut last_merge = std::time::Instant::now();
    let split_cooldown = std::time::Duration::from_secs(3);
    let merge_cooldown = std::time::Duration::from_secs(5);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            Some(msg) = rx.recv() => {
                match msg {
                    OrchestratorMsg::Warmup { _node_id: _, new_cell } => {
                        if next_id < 16 && last_spawn.elapsed() >= split_cooldown {
                            let port = start_port + next_id as u16;
                            let cell_str = new_cell.as_deref();
                            let mut cmd = spawn_node(next_id, port, &active_ports, cell_str);
                            if let Ok(mut child) = cmd.spawn() {
                                let nid = next_id;
                                if let Some(stdout) = child.stdout.take() {
                                    let tx_c = tx.clone();
                                    tokio::spawn(async move {
                                        let mut reader = BufReader::new(stdout).lines();
                                        while let Ok(Some(line)) = reader.next_line().await {
                                            println!("[Node {}] {}", nid, line);
                                            if line.contains("REQUEST_WARMUP") {
                                                let nc = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
                                                let _ = tx_c.send(OrchestratorMsg::Warmup { _node_id: nid, new_cell: nc }).await;
                                            } else if line.contains("REQUEST_SPLIT") {
                                                let nc = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
                                                let _ = tx_c.send(OrchestratorMsg::Split { _node_id: nid, new_cell: nc }).await;
                                            } else if line.contains("REQUEST_MERGE") {
                                                let _ = tx_c.send(OrchestratorMsg::Merge { node_id: nid }).await;
                                            }
                                        }
                                    });
                                }
                                if let Some(stderr) = child.stderr.take() {
                                    tokio::spawn(async move {
                                        let mut reader = BufReader::new(stderr).lines();
                                        while let Ok(Some(line)) = reader.next_line().await {
                                            eprintln!("[Node {} ERR] {}", nid, line);
                                        }
                                    });
                                }
                                println!("[Orchestrator] Pre-warmed Node {} on port {} cell={:?}", nid, port, cell_str);
                                nodes.push((nid, child));
                                active_ports.push(port);
                                next_id += 1;
                                last_spawn = std::time::Instant::now();
                            }
                        }
                    }
                    OrchestratorMsg::Split { _node_id: _, new_cell } => {
                        if next_id < 16 && last_spawn.elapsed() >= split_cooldown {
                            let port = start_port + next_id as u16;
                            let cell_str = new_cell.as_deref();
                            let mut cmd = spawn_node(next_id, port, &active_ports, cell_str);
                            if let Ok(mut child) = cmd.spawn() {
                                let nid = next_id;
                                if let Some(stdout) = child.stdout.take() {
                                    let tx_c = tx.clone();
                                    tokio::spawn(async move {
                                        let mut reader = BufReader::new(stdout).lines();
                                        while let Ok(Some(line)) = reader.next_line().await {
                                            println!("[Node {}] {}", nid, line);
                                            if line.contains("REQUEST_WARMUP") {
                                                let nc = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
                                                let _ = tx_c.send(OrchestratorMsg::Warmup { _node_id: nid, new_cell: nc }).await;
                                            } else if line.contains("REQUEST_SPLIT") {
                                                let nc = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
                                                let _ = tx_c.send(OrchestratorMsg::Split { _node_id: nid, new_cell: nc }).await;
                                            } else if line.contains("REQUEST_MERGE") {
                                                let _ = tx_c.send(OrchestratorMsg::Merge { node_id: nid }).await;
                                            }
                                        }
                                    });
                                }
                                if let Some(stderr) = child.stderr.take() {
                                    tokio::spawn(async move {
                                        let mut reader = BufReader::new(stderr).lines();
                                        while let Ok(Some(line)) = reader.next_line().await {
                                            eprintln!("[Node {} ERR] {}", nid, line);
                                        }
                                    });
                                }
                                println!("[Orchestrator] Spawned Node {} on port {} cell={:?}", nid, port, cell_str);
                                nodes.push((nid, child));
                                active_ports.push(port);
                                next_id += 1;
                                last_spawn = std::time::Instant::now();
                            }
                        }
                    }
                    OrchestratorMsg::Merge { node_id } => {
                        if nodes.len() > 1 && last_merge.elapsed() >= merge_cooldown {
                            if let Some(idx) = nodes.iter().position(|(nid, _)| *nid == node_id) {
                                let (killed_id, mut child) = nodes.remove(idx);
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                let _ = child.kill().await;
                                if let Some(port_idx) = active_ports.iter().position(|p| *p == start_port + killed_id as u16) {
                                    active_ports.remove(port_idx);
                                }
                                println!("[Orchestrator] Killed Node {} (merge)", killed_id);
                                last_merge = std::time::Instant::now();
                            }
                        }
                    }
                }
            }
        }
    }

    for (_, mut node) in nodes { let _ = node.kill().await; }
    Ok(())
}

async fn run_node(
    bind: SocketAddr,
    id: u8,
    seed_addrs: Vec<SocketAddr>,
    cell_override: Option<Cell>,
) -> Result<(), Box<dyn std::error::Error>> {
    use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};

    eprintln!("[Node {}] bind={} peers={:?} cell={:?}", id, bind, seed_addrs, cell_override);

    let initial_cell = cell_override.clone().unwrap_or_else(|| {
        Cell::new(0.0, WORLD_SIZE, -1.0, WORLD_SIZE + 1.0, -(WORLD_SIZE / 2.0), WORLD_SIZE / 2.0)
    });

    let config = ZeusConfig {
        bind_addr: bind,
        seed_addrs,
        boundary: WORLD_SIZE,
        margin: 1.0,
        ordinal: id as u32,
        lower_boundary: 0.0,
        cell: Some(initial_cell.clone()),
    };

    let mut physics = DroneWorld::with_bounds(
        initial_cell.x_min, initial_cell.x_max,
        initial_cell.y_min, initial_cell.y_max,
        initial_cell.z_min, initial_cell.z_max,
    );
    physics.next_drone_id = (id as u64) * 50_000 + 1;

    let mut game_loop = GameLoop::new(config, physics).await?;
    let mut autoscaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        merge_threshold: 5,
        warmup_threshold: 30,
        split_cooldown_ticks: 512,
        merge_cooldown_ticks: 1024,
        max_nodes: 16,
        startup_grace_ticks: 2048,
    });

    let tick_duration = std::time::Duration::from_micros(7812);
    let dt = 1.0 / 128.0;
    let mut diag_counter: u32 = 0;
    let mut status_counter: u32 = 0;
    let mut cell_broadcast_counter: u32 = 0;
    let mut my_cell = initial_cell;
    let mut pending_split: Option<(Cell, Cell)> = None;
    let mut split_debug_ticks: i32 = -1;
    let mut tracked_ids: Vec<u64> = Vec::new();
    let mut draining = false;
    let mut drain_ticks: u32 = 0;
    let mut drain_merge_requested = false;

    loop {
        let loop_start = std::time::Instant::now();

        let _events = game_loop.tick(dt).await?;

        for dg in &game_loop.engine.client_datagrams.clone() {
            if dg.len() >= 16 && dg[0] == 0xDD && dg[1] == 0x02 {
                let count = ((dg[2] as u16) << 8) | (dg[3] as u16);
                let x = f32::from_le_bytes(dg[4..8].try_into().unwrap_or([0; 4]));
                let y = f32::from_le_bytes(dg[8..12].try_into().unwrap_or([0; 4]));
                let z = f32::from_le_bytes(dg[12..16].try_into().unwrap_or([0; 4]));
                let spawn_pos = (x, y, z);
                if my_cell.contains(spawn_pos) {
                    let spawned = game_loop.world.spawn_drone_near(spawn_pos, count as usize);
                    for did in spawned {
                        if let Some((pos, vel)) = game_loop.world.get_drone_state(did) {
                            game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
                                id: did,
                                pos,
                                vel,
                                state: zeus_node::entity_manager::AuthorityState::Local,
                                verifying_key: None,
                            });
                        }
                    }
                }
            } else if dg.len() >= 3 && dg[0] == 0xDE {
                let count = ((dg[1] as u16) << 8) | (dg[2] as u16);
                let removed = game_loop.world.remove_n_drones(count as usize);
                for rid in removed {
                    game_loop.engine.node.manager.remove_entity(rid);
                }
            } else if dg.len() >= 4 && dg[0] == 0xDD {
                let mode = dg[1];
                let player_id_bytes = if dg.len() >= 12 { Some(&dg[4..12]) } else { None };
                if let Some(id_bytes) = player_id_bytes {
                    let pid = u64::from_le_bytes(id_bytes.try_into().unwrap_or([0u8; 8]));
                    if let Some(entity) = game_loop.engine.node.manager.get_entity(pid) {
                        game_loop.world.player_wells.insert(pid, GravityWell {
                            pos: entity.pos,
                            mode,
                        });
                    }
                }
            } else if dg.len() >= 3 && dg[0] == 0xDD {
                let count = ((dg[1] as u16) << 8) | (dg[2] as u16);
                let spawn_pos = my_cell.center();
                if my_cell.contains(spawn_pos) {
                    let spawned = game_loop.world.spawn_drone_near(spawn_pos, count as usize);
                    for did in spawned {
                        if let Some((pos, vel)) = game_loop.world.get_drone_state(did) {
                            game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
                                id: did,
                                pos,
                                vel,
                                state: zeus_node::entity_manager::AuthorityState::Local,
                                verifying_key: None,
                            });
                        }
                    }
                }
            }
        }

        let local_positions: Vec<(f32, f32, f32)> = game_loop.local_entity_positions()
            .iter().map(|(_, pos)| *pos).collect();
        let local_count = local_positions.len();
        let peer_ids = game_loop.engine.discovery.peer_ids();
        let peer_cells = game_loop.engine.discovery.peer_cells();
        let total_nodes = game_loop.engine.discovery.total_node_count().max(1);

        let scale_events = autoscaler.evaluate(
            &my_cell,
            local_count,
            &peer_ids,
            &peer_cells,
            total_nodes,
            &local_positions,
        );

        for se in &scale_events {
            match se {
                ScaleEvent::WarmupRecommended { projected_cell, projected_new_cell, .. } => {
                    pending_split = Some((projected_cell.clone(), projected_new_cell.clone()));
                    println!(
                        "REQUEST_SPLIT new_cell={},{},{},{},{},{}",
                        projected_new_cell.x_min, projected_new_cell.x_max,
                        projected_new_cell.y_min, projected_new_cell.y_max,
                        projected_new_cell.z_min, projected_new_cell.z_max,
                    );
                }
                ScaleEvent::SplitRecommended { keep_cell, new_cell, .. } => {
                    pending_split = Some((keep_cell.clone(), new_cell.clone()));
                    println!(
                        "REQUEST_SPLIT new_cell={},{},{},{},{},{}",
                        new_cell.x_min, new_cell.x_max,
                        new_cell.y_min, new_cell.y_max,
                        new_cell.z_min, new_cell.z_max,
                    );
                }
                ScaleEvent::MergeRecommended => {
                    if !draining && !drain_merge_requested {
                        draining = true;
                        drain_ticks = 0;
                        eprintln!("[Node {}] Entering drain mode for merge", id);
                    }
                }
                ScaleEvent::CellExpanded { new_cell } => {
                    let alive_peer_cells = game_loop.engine.discovery.peer_cells();
                    let overlaps = alive_peer_cells.values().any(|pc| {
                        new_cell.x_min < pc.x_max && new_cell.x_max > pc.x_min
                            && new_cell.y_min < pc.y_max && new_cell.y_max > pc.y_min
                            && new_cell.z_min < pc.z_max && new_cell.z_max > pc.z_min
                    });
                    if overlaps {
                        eprintln!(
                            "[Node {}] CellExpanded REJECTED (would overlap with alive peers): {:?}",
                            id, new_cell
                        );
                    } else {
                        my_cell = new_cell.clone();
                        game_loop.set_cell(my_cell.clone());
                    game_loop.world.world_min = (my_cell.x_min, my_cell.y_min, my_cell.z_min);
                    game_loop.world.world_max = (my_cell.x_max, my_cell.y_max, my_cell.z_max);
                    eprintln!("[Node {}] Cell expanded to {:?}", id, my_cell);
                    }
                }
                ScaleEvent::PeerJoined { id: pid } => {
                    eprintln!("[Node {}] Peer {} joined", id, pid);
                    if let Some((keep_cell, new_cell)) = pending_split.take() {
                        let peer_cells = game_loop.engine.discovery.peer_cells();
                        let peer_cell = peer_cells.get(&pid);
                        let cell_matches = peer_cell.map_or(false, |pc| {
                            (pc.x_min - new_cell.x_min).abs() < 1.0
                            && (pc.x_max - new_cell.x_max).abs() < 1.0
                            && (pc.y_min - new_cell.y_min).abs() < 1.0
                            && (pc.y_max - new_cell.y_max).abs() < 1.0
                            && (pc.z_min - new_cell.z_min).abs() < 1.0
                            && (pc.z_max - new_cell.z_max).abs() < 1.0
                        });
                        if !cell_matches {
                            eprintln!(
                                "[Node {}] Peer {} cell {:?} doesn't match expected {:?}, re-queuing split",
                                id, pid, peer_cell, new_cell
                            );
                            pending_split = Some((keep_cell, new_cell));
                        } else {
                        let pre_local: Vec<_> = game_loop.engine.node.manager.entities.iter()
                            .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
                            .map(|(eid, e)| (*eid, e.pos, e.vel))
                            .collect();
                        eprintln!("[Node {}] PRE-EVICT local={} sample:", id, pre_local.len());
                        for (eid, p, v) in pre_local.iter().take(5) {
                            eprintln!("  id={} pos=({:.2},{:.2},{:.2}) vel=({:.2},{:.2},{:.2})", eid, p.0, p.1, p.2, v.0, v.1, v.2);
                        }
                        my_cell = keep_cell;
                        game_loop.set_cell(my_cell.clone());
                        game_loop.world.world_min = (my_cell.x_min, my_cell.y_min, my_cell.z_min);
                        game_loop.world.world_max = (my_cell.x_max, my_cell.y_max, my_cell.z_max);
                        game_loop.evict_out_of_cell_from_physics();
                        let post_states: Vec<_> = game_loop.engine.node.manager.entities.iter()
                            .map(|(eid, e)| (*eid, e.state.clone(), e.pos, e.vel))
                            .collect();
                        let ho_count = post_states.iter().filter(|(_, s, _, _)| *s == zeus_node::entity_manager::AuthorityState::HandoffOut).count();
                        let lo_count = post_states.iter().filter(|(_, s, _, _)| *s == zeus_node::entity_manager::AuthorityState::Local).count();
                        eprintln!("[Node {}] POST-EVICT local={} handoffout={} total={}", id, lo_count, ho_count, post_states.len());
                        tracked_ids = post_states.iter()
                            .filter(|(_, s, _, _)| *s == zeus_node::entity_manager::AuthorityState::HandoffOut)
                            .take(3)
                            .map(|(eid, _, _, _)| *eid)
                            .collect();
                        eprintln!("[Node {}] TRACKING IDs: {:?}", id, tracked_ids);
                        for (eid, st, p, v) in post_states.iter().filter(|(eid, _, _, _)| tracked_ids.contains(eid)) {
                            eprintln!("  id={} state={:?} pos=({:.2},{:.2},{:.2}) vel=({:.2},{:.2},{:.2})", eid, st, p.0, p.1, p.2, v.0, v.1, v.2);
                        }
                        eprintln!("[Node {}] Cell shrunk to {:?} (peer {} ready)", id, my_cell, pid);
                        split_debug_ticks = 0;
                        game_loop.broadcast_status();
                        game_loop.broadcast_cells(&[my_cell.clone()]);
                    }}
                }
                ScaleEvent::PeerLeft { id: pid, .. } => {
                    eprintln!("[Node {}] Peer {} left", id, pid);
                }
            }
        }

        if draining && !drain_merge_requested {
            drain_ticks += 1;
            let player_ids: Vec<u64> = game_loop.engine.node.manager.entities.iter()
                .filter(|(_, e)| e.state == zeus_node::entity_manager::AuthorityState::Local)
                .filter(|(eid, _)| !game_loop.world.drone_ids.contains(eid))
                .map(|(eid, _)| *eid)
                .collect();
            let local_drone_count = game_loop.engine.node.manager.entities.iter()
                .filter(|(eid, e)| e.state == zeus_node::entity_manager::AuthorityState::Local && game_loop.world.drone_ids.contains(eid))
                .count();
            let ho_drone_count = game_loop.engine.node.manager.entities.iter()
                .filter(|(eid, e)| e.state == zeus_node::entity_manager::AuthorityState::HandoffOut && game_loop.world.drone_ids.contains(eid))
                .count();
            if local_drone_count == 0 && ho_drone_count == 0 {
                eprintln!("[Node {}] Drain complete after {} ticks, requesting merge", id, drain_ticks);
                println!("REQUEST_MERGE");
                drain_merge_requested = true;
            } else {
                let _ = game_loop.engine.drain_local_entities(&player_ids).await;
                if drain_ticks % 64 == 0 {
                    eprintln!("[Node {}] Draining: local_drones={} ho_drones={} ticks={}", id, local_drone_count, ho_drone_count, drain_ticks);
                }
                if drain_ticks > 512 {
                    eprintln!("[Node {}] Drain timeout, forcing merge with {} remaining", id, local_drone_count + ho_drone_count);
                    println!("REQUEST_MERGE");
                    drain_merge_requested = true;
                }
            }
        }

        if split_debug_ticks >= 0 && split_debug_ticks < 128 {
            if split_debug_ticks % 4 == 0 {
                let em = &game_loop.engine.node.manager;
                let lo = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::Local).count();
                let ho = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::HandoffOut).count();
                let rd = game_loop.engine.recently_departed.len();
                eprintln!(
                    "[Node {}] T+{}: L={} HO={} dep={}",
                    id, split_debug_ticks, lo, ho, rd
                );
                for tid in &tracked_ids {
                    let in_em = em.entities.get(tid);
                    let in_dep = game_loop.engine.recently_departed.get(tid);
                    let in_remote = game_loop.engine.remote_entity_states.get(tid);
                    if let Some(e) = in_em {
                        eprintln!(
                            "  [SWARM] id={} src=EM({:?}) pos=({:.3},{:.3},{:.3}) vel=({:.3},{:.3},{:.3})",
                            tid, e.state, e.pos.0, e.pos.1, e.pos.2, e.vel.0, e.vel.1, e.vel.2
                        );
                    }
                    if let Some((pos, vel, ticks)) = in_dep {
                        let dt = 1.0_f32 / 128.0;
                        let ep = (
                            pos.0 + vel.0 * (*ticks as f32) * dt,
                            pos.1 + vel.1 * (*ticks as f32) * dt,
                            pos.2 + vel.2 * (*ticks as f32) * dt,
                        );
                        eprintln!(
                            "  [SWARM] id={} src=RELAY(t={}) base=({:.3},{:.3},{:.3}) broadcast=({:.3},{:.3},{:.3})",
                            tid, ticks, pos.0, pos.1, pos.2, ep.0, ep.1, ep.2
                        );
                    }
                    if let Some(rs) = in_remote {
                        eprintln!(
                            "  [SWARM] id={} src=GOSSIP pos=({:.3},{:.3},{:.3}) vel=({:.3},{:.3},{:.3})",
                            tid, rs.pos.0, rs.pos.1, rs.pos.2, rs.vel.0, rs.vel.1, rs.vel.2
                        );
                    }
                    if in_em.is_none() && in_dep.is_none() && in_remote.is_none() {
                        eprintln!("  [SWARM] id={} NOWHERE", tid);
                    }
                }
            }
            split_debug_ticks += 1;
        }

        cell_broadcast_counter += 1;
        if cell_broadcast_counter % 16 == 0 {
            game_loop.broadcast_cells(&[my_cell.clone()]);
        }

        status_counter += 1;
        if status_counter % 16 == 0 {
            game_loop.broadcast_status();
        }

        diag_counter += 1;
        if diag_counter % 512 == 0 {
            let em = &game_loop.engine.node.manager;
            let lc = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::Local).count();
            let physics_drones = game_loop.world.drone_count();
            let tn = game_loop.engine.discovery.total_node_count();
            eprintln!(
                "[Node {}] entities:{} local:{} physics:{} nodes:{} cell=[{:.1},{:.1},{:.1},{:.1},{:.1},{:.1}]",
                id, em.entities.len(), lc, physics_drones, tn,
                my_cell.x_min, my_cell.x_max, my_cell.y_min, my_cell.y_max, my_cell.z_min, my_cell.z_max,
            );
        }

        let elapsed = loop_start.elapsed();
        if elapsed < tick_duration {
            tokio::time::sleep(tick_duration - elapsed).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drone_spawn_and_state() {
        let mut world = DroneWorld::new();
        let id = world.spawn_drone().unwrap();
        assert!(world.drone_ids.contains(&id));
        let state = world.get_drone_state(id);
        assert!(state.is_some());
    }

    #[test]
    fn test_drone_removal() {
        let mut world = DroneWorld::new();
        let id = world.spawn_drone().unwrap();
        world.remove_drone(id);
        assert!(!world.drones.contains_key(&id));
        assert!(!world.drone_ids.contains(&id));
    }

    #[test]
    fn test_gravity_well_attract() {
        let mut world = DroneWorld::new();
        world.spawn_drone_at(1, (10.0, 10.0, 0.0), (0.0, 0.0, 0.0));
        world.player_wells.insert(999, GravityWell {
            pos: (12.0, 10.0, 0.0),
            mode: 1,
        });
        for _ in 0..20 {
            world.step(1.0 / 128.0);
        }
        let (pos, _) = world.get_drone_state(1).unwrap();
        assert!(pos.0 > 10.0, "Drone should move toward well, got x={:.2}", pos.0);
    }

    #[test]
    fn test_gravity_well_repel() {
        let mut world = DroneWorld::new();
        world.spawn_drone_at(1, (10.0, 10.0, 0.0), (0.0, 0.0, 0.0));
        world.player_wells.insert(999, GravityWell {
            pos: (12.0, 10.0, 0.0),
            mode: 2,
        });
        for _ in 0..20 {
            world.step(1.0 / 128.0);
        }
        let (pos, _) = world.get_drone_state(1).unwrap();
        assert!(pos.0 < 10.0, "Drone should move away from well, got x={:.2}", pos.0);
    }

    #[test]
    fn test_speed_cap() {
        let mut world = DroneWorld::new();
        world.spawn_drone_at(1, (12.0, 12.0, 0.0), (100.0, 100.0, 100.0));
        world.step(1.0 / 128.0);
        let (_, vel) = world.get_drone_state(1).unwrap();
        let speed = (vel.0 * vel.0 + vel.1 * vel.1 + vel.2 * vel.2).sqrt();
        assert!(speed <= MAX_DRONE_SPEED + 1.0, "Speed should be capped, got {:.2}", speed);
    }

    #[test]
    fn test_zero_gravity() {
        let mut world = DroneWorld::new();
        world.spawn_drone_at(1, (12.0, 12.0, 0.0), (0.0, 0.0, 0.0));
        let (pos_before, _) = world.get_drone_state(1).unwrap();
        for _ in 0..10 {
            world.run_physics();
        }
        let (pos_after, _) = world.get_drone_state(1).unwrap();
        assert!((pos_after.1 - pos_before.1).abs() < 0.5, "No gravity: y should stay roughly same");
    }

    #[test]
    fn test_game_world_trait() {
        let mut world = DroneWorld::new();
        world.on_entity_arrived(42, (5.0, 5.0, 0.0), (1.0, 0.0, 0.0));
        assert!(world.drones.contains_key(&42));
        world.on_entity_departed(42);
        assert!(!world.drones.contains_key(&42));
    }

    #[test]
    fn test_with_bounds() {
        let world = DroneWorld::with_bounds(-100.0, 100.0, -100.0, 100.0, -100.0, 100.0);
        assert_eq!(world.world_min, (-100.0, -100.0, -100.0));
        assert_eq!(world.world_max, (100.0, 100.0, 100.0));
    }

    #[test]
    fn test_remote_drone_kinematic() {
        let mut world = DroneWorld::new();
        world.spawn_remote_drone(500, (5.0, 5.0, 0.0), (0.0, 0.0, 0.0));
        assert!(world.drones.contains_key(&500));
        assert!(!world.drone_ids.contains(&500));
        let rb = world.rigid_body_set.get(world.drones[&500].rigid_body_handle).unwrap();
        assert!(rb.is_kinematic());
    }

    #[test]
    fn test_spawn_drone_near() {
        let mut world = DroneWorld::new();
        let center = (12.0, 12.0, 0.0);
        let spawned = world.spawn_drone_near(center, 5);
        assert_eq!(spawned.len(), 5);
        for &id in &spawned {
            assert!(world.drone_ids.contains(&id));
            let (pos, _) = world.get_drone_state(id).unwrap();
            assert!((pos.0 - center.0).abs() < 5.0);
            assert!((pos.1 - center.1).abs() < 5.0);
            assert!((pos.2 - center.2).abs() < 5.0);
        }
    }

    #[test]
    fn test_spawn_drone_near_respects_bounds() {
        let mut world = DroneWorld::with_bounds(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let spawned = world.spawn_drone_near((0.0, 0.0, 0.0), 10);
        for &id in &spawned {
            let (pos, _) = world.get_drone_state(id).unwrap();
            assert!(pos.0 >= -0.5 && pos.0 <= 10.5);
            assert!(pos.1 >= -0.5 && pos.1 <= 10.5);
            assert!(pos.2 >= -0.5 && pos.2 <= 10.5);
        }
    }

    #[test]
    fn test_remove_n_drones() {
        let mut world = DroneWorld::new();
        for _ in 0..10 {
            world.spawn_drone();
        }
        assert_eq!(world.drone_count(), 10);
        let removed = world.remove_n_drones(3);
        assert_eq!(removed.len(), 3);
        assert_eq!(world.drone_count(), 7);
    }

    #[test]
    fn test_remove_n_drones_more_than_available() {
        let mut world = DroneWorld::new();
        for _ in 0..3 {
            world.spawn_drone();
        }
        let removed = world.remove_n_drones(10);
        assert_eq!(removed.len(), 3);
        assert_eq!(world.drone_count(), 0);
    }

    #[test]
    fn test_spawn_at_position_cell_check() {
        let cell = Cell::new(0.0, 12.0, 0.0, 12.0, -6.0, 6.0);
        let inside = cell.contains((6.0, 6.0, 0.0));
        let outside = cell.contains((20.0, 20.0, 20.0));
        assert!(inside);
        assert!(!outside);
    }
}
