use clap::Parser;
use rapier3d::prelude::*;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::{GameLoop, GameWorld};

#[derive(Parser)]
#[command(name = "cosmic_drift_server")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, default_value = "127.0.0.1:5000")]
    bind: SocketAddr,

    #[arg(short, long, default_value = "5000")]
    max_balls: usize,
}

#[derive(clap::Subcommand)]
enum Commands {
    Orchestrator {
        #[arg(short, long, default_value = "5000")]
        start_port: u16,
    },

    RunNode {
        #[arg(short, long)]
        bind: SocketAddr,

        #[arg(short, long)]
        id: u8,

        #[arg(long, default_value = "0.0")]
        boundary: f32,

        #[arg(long)]
        peer: Option<SocketAddr>,
    },
}

struct Ball {
    rigid_body_handle: RigidBodyHandle,
}

struct PhysicsWorld {
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

    balls: HashMap<u64, Ball>,
    server_ball_ids: HashSet<u64>,
    next_ball_id: u64,
}

impl PhysicsWorld {
    fn new() -> Self {
        let mut rigid_body_set = RigidBodySet::new();
        let mut collider_set = ColliderSet::new();

        let ground = RigidBodyBuilder::fixed()
            .translation(vector![0.0, -1.0, 0.0])
            .build();
        let ground_handle = rigid_body_set.insert(ground);
        let ground_collider = ColliderBuilder::cuboid(500.0, 0.1, 500.0) // Thickness 0.1 -> surface at -0.9
            .restitution(0.5)
            .friction(0.0)
            .build();
        collider_set.insert_with_parent(ground_collider, ground_handle, &mut rigid_body_set);

        // Add enclosure walls (Front/Back)
        let wall_front = RigidBodyBuilder::fixed()
            .translation(vector![0.0, 5.0, 12.0])
            .build();
        let wall_back = RigidBodyBuilder::fixed()
            .translation(vector![0.0, 5.0, -12.0])
            .build();
        let h_front = rigid_body_set.insert(wall_front);
        let h_back = rigid_body_set.insert(wall_back);

        let wall_col = ColliderBuilder::cuboid(500.0, 10.0, 2.0).build();
        collider_set.insert_with_parent(wall_col.clone(), h_front, &mut rigid_body_set);
        collider_set.insert_with_parent(wall_col, h_back, &mut rigid_body_set);

        // Add enclosure walls (Left/Right)
        let wall_left = RigidBodyBuilder::fixed()
            .translation(vector![0.0, 5.0, 0.0])
            .rotation(vector![0.0, 1.5708, 0.0])
            .build();
        let wall_right = RigidBodyBuilder::fixed()
            .translation(vector![24.0, 5.0, 0.0])
            .rotation(vector![0.0, 1.5708, 0.0])
            .build(); // 4 nodes * 6 width = 24
        let h_left = rigid_body_set.insert(wall_left);
        let h_right = rigid_body_set.insert(wall_right);

        let wall_side_col = ColliderBuilder::cuboid(500.0, 10.0, 2.0).build();
        collider_set.insert_with_parent(wall_side_col.clone(), h_left, &mut rigid_body_set);
        collider_set.insert_with_parent(wall_side_col, h_right, &mut rigid_body_set);

        Self {
            rigid_body_set,
            collider_set,
            integration_parameters: {
                let mut p = IntegrationParameters::default();
                p.dt = 1.0 / 128.0;
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
            balls: HashMap::new(),
            server_ball_ids: HashSet::new(),
            next_ball_id: 1,
        }
    }

    fn spawn_ball(&mut self) -> Option<u64> {
        let id = self.next_ball_id;
        self.next_ball_id += 1;

        let x = (id % 6 + 1) as f32; // X in [1..7] (tight pack)
        let y = 3.0;
        let z = ((id * 17) % 20) as f32 - 10.0; // Z in [-10..10]

        let rigid_body = RigidBodyBuilder::dynamic()
            .translation(vector![x, y, z])
            .linvel(vector![5.0, 0.0, 0.0])
            .linear_damping(1.5)
            .angular_damping(1.0)
            .ccd_enabled(true)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.6)
            .friction(0.3)
            .density(8.0)
            .build();
        self.collider_set
            .insert_with_parent(collider, handle, &mut self.rigid_body_set);

        self.balls.insert(
            id,
            Ball {
                rigid_body_handle: handle,
            },
        );
        self.server_ball_ids.insert(id);
        Some(id)
    }

    fn spawn_ball_at(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if let Some(ball) = self.balls.get(&id) {
            if let Some(rb) = self.rigid_body_set.get(ball.rigid_body_handle) {
                if rb.is_kinematic() {
                    self.remove_ball(id);
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
            .linear_damping(1.5)
            .angular_damping(1.0)
            .ccd_enabled(true)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);
        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.6)
            .friction(0.3)
            .density(8.0)
            .build();
        self.collider_set
            .insert_with_parent(collider, handle, &mut self.rigid_body_set);
        self.balls.insert(id, Ball { rigid_body_handle: handle });
        self.server_ball_ids.insert(id);
    }

    fn spawn_remote_ball(&mut self, id: u64, pos: (f32, f32, f32), _vel: (f32, f32, f32)) {
        if self.balls.contains_key(&id) {
            return;
        }

        let rigid_body = RigidBodyBuilder::kinematic_position_based()
            .translation(vector![pos.0, pos.1, pos.2])
            .linear_damping(0.0)
            .angular_damping(0.0)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.6)
            .friction(0.3)
            .density(8.0)
            .build();
        self.collider_set
            .insert_with_parent(collider, handle, &mut self.rigid_body_set);

        self.balls.insert(
            id,
            Ball {
                rigid_body_handle: handle,
            },
        );
    }

    fn update_ball(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if let Some(ball) = self.balls.get(&id) {
            if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
                if rb.is_kinematic() {
                    rb.set_next_kinematic_position(rapier3d::prelude::Isometry::translation(
                        pos.0, pos.1, pos.2,
                    ));
                    rb.set_linvel(rapier3d::na::vector![vel.0, vel.1, vel.2], true);
                }
            }
        }
    }

    fn remove_ball(&mut self, id: u64) {
        if let Some(ball) = self.balls.remove(&id) {
            self.rigid_body_set.remove(
                ball.rigid_body_handle,
                &mut self.island_manager,
                &mut self.collider_set,
                &mut self.impulse_joint_set,
                &mut self.multibody_joint_set,
                true,
            );
        }
    }

    fn step(&mut self) {
        let gravity = vector![0.0, -9.81, 0.0];

        for ball in self.balls.values() {
            if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
                rb.wake_up(true);
            }
        }

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

        for ball in self.balls.values() {
            if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
                if !rb.is_dynamic() {
                    continue;
                }
                let pos = *rb.translation();
                let vel = *rb.linvel();
                let max_speed = 30.0;
                let clamped_vel = if vel.norm() > max_speed {
                    vel.normalize() * max_speed
                } else {
                    vel
                };
                if pos.x < -1.0 || pos.x > 25.0 || pos.y < -5.0 || pos.y > 20.0 || pos.z < -14.0 || pos.z > 14.0 {
                    let cx = pos.x.clamp(1.0, 23.0);
                    let cy = 1.0;
                    let cz = pos.z.clamp(-10.0, 10.0);
                    rb.set_translation(vector![cx, cy, cz], true);
                    rb.set_linvel(vector![0.0, 0.0, 0.0], true);
                } else if clamped_vel != vel {
                    rb.set_linvel(clamped_vel, true);
                }
            }
        }
    }

    fn get_ball_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.balls.get(&id).and_then(|ball| {
            self.rigid_body_set.get(ball.rigid_body_handle).map(|rb| {
                let pos = rb.translation();
                let vel = rb.linvel();
                ((pos.x, pos.y, pos.z), (vel.x, vel.y, vel.z))
            })
        })
    }

    fn ball_count(&self) -> usize {
        self.balls.len()
    }
}

impl GameWorld for PhysicsWorld {
    fn step(&mut self, _dt: f32) {
        self.step();
    }

    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if id < 1_000_000 {
            self.spawn_ball_at(id, pos, vel);
        } else {
            self.spawn_remote_ball(id, pos, vel);
        }
    }

    fn on_entity_departed(&mut self, id: u64) {
        self.remove_ball(id);
        self.server_ball_ids.remove(&id);
    }

    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.spawn_remote_ball(id, pos, vel);
        self.update_ball(id, pos, vel);
    }

    fn locally_simulated_ids(&self) -> &HashSet<u64> {
        &self.server_ball_ids
    }

    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.get_ball_state(id)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Orchestrator { start_port }) => run_orchestrator(start_port).await,
        Some(Commands::RunNode {
            bind,
            id,
            boundary,
            peer,
        }) => run_physics_node(bind, id, boundary, peer, cli.max_balls).await,
        None => run_physics_node(cli.bind, 0, 20.0, None, cli.max_balls).await,
    }
}

async fn run_orchestrator(start_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::ChildStdout;

    println!("╔════════════════════════════════════════════════════════╗");
    println!("║       COSMIC DRIFT PHYSICS ORCHESTRATOR                ║");
    println!("║  Protocol-Driven Autoscaling Demo                      ║");
    println!("╚════════════════════════════════════════════════════════╝");

    let mut nodes = Vec::new();
    let mut next_id = 1;

    let spawn_node = |id: u8, port: u16, boundary: f32, peer_port: Option<u16>| {
        println!(
            "[Orchestrator] Spawning Node {} (Bound={}) on port {} -> Peer {:?}",
            id, boundary, port, peer_port
        );
        let mut cmd = tokio::process::Command::new(std::env::current_exe().unwrap());
        cmd.arg("run-node")
            .arg("--bind")
            .arg(format!("127.0.0.1:{}", port))
            .arg("--id")
            .arg(id.to_string())
            .arg("--boundary")
            .arg(boundary.to_string());

        if let Some(pp) = peer_port {
            cmd.arg("--peer").arg(format!("127.0.0.1:{}", pp));
        }

        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        cmd
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(10);

    let monitor_node = |id: u8,
                        stdout: Option<ChildStdout>,
                        stderr: Option<tokio::process::ChildStderr>,
                        tx: tokio::sync::mpsc::Sender<()>| {
        if let Some(stdout) = stdout {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    println!("[Node {}] {}", id, line);
                    if line.contains("REQUEST_SPLIT") {
                        let _ = tx.send(()).await;
                    }
                }
            });
        }
        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    eprintln!("[Node {} ERR] {}", id, line);
                }
            });
        }
    };

    let mut cmd0 = spawn_node(0, start_port, 24.0, None);
    let mut child0 = cmd0.spawn()?;
    monitor_node(0, child0.stdout.take(), child0.stderr.take(), tx.clone());
    nodes.push(child0);

    println!("[Orchestrator] Chain Active: Node 0");
    println!("[Orchestrator] Autoscaling enabled. Press Ctrl+C to stop.");

    let mut last_spawn = std::time::Instant::now();
    let spawn_cooldown = std::time::Duration::from_secs(3);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break;
            }
            Some(_) = rx.recv() => {
                if next_id < 4 && last_spawn.elapsed() >= spawn_cooldown {
                    println!("[Orchestrator] ⚠️ SPLIT REQUEST from Node 0! Spawning Node {}...", next_id);
                    let port = start_port + next_id as u16;
                    let boundary = 24.0;
                    let peer = port - 1;

                    let mut cmd = spawn_node(next_id, port, boundary, Some(peer));
                    if let Ok(mut child) = cmd.spawn() {
                        monitor_node(next_id, child.stdout.take(), child.stderr.take(), tx.clone());
                        nodes.push(child);
                        next_id += 1;
                        last_spawn = std::time::Instant::now();
                    }
                }
            }
        }
    }

    for mut node in nodes {
        let _ = node.kill().await;
    }
    Ok(())
}

async fn run_physics_node(
    bind: SocketAddr,
    id: u8,
    boundary: f32,
    peer: Option<SocketAddr>,
    _max_balls: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!(
        "[Node {}] bind={} boundary={} peer={:?}",
        id, bind, boundary, peer
    );

    let config = ZeusConfig {
        bind_addr: bind,
        seed_addr: peer,
        boundary,
        margin: 1.0,
        ordinal: id as u32,
        lower_boundary: 0.0,
    };

    let physics = PhysicsWorld::new();
    let mut game_loop = GameLoop::new(config, physics).await?;

    let tick_duration = std::time::Duration::from_micros(7812);
    let dt = 1.0 / 128.0;
    let mut spawn_counter: u32 = 0;
    let mut diag_counter: u32 = 0;
    let mut last_split_request: usize = 0;

    loop {
        let loop_start = std::time::Instant::now();

        if id == 0 && game_loop.world.ball_count() < 50 {
            spawn_counter += 1;
            if spawn_counter % 256 == 0 {
                game_loop.world.spawn_ball();
            }
        }

        let total_nodes = game_loop.engine.discovery.total_node_count().max(1) as f32;
        let should_recompute = if id == 0 {
            true
        } else {
            total_nodes > 1.0
        };
        if should_recompute {
            let zone_width = 24.0 / total_nodes;
            let lower = id as f32 * zone_width;
            let upper = (id as f32 + 1.0) * zone_width;
            game_loop.set_lower_boundary(lower);
            game_loop.set_boundary(upper);
        }

        let events = game_loop.tick(dt).await?;

        if id == 0 {
            let total_balls = game_loop.engine.node.manager.entities.keys().filter(|id| **id < 1_000_000).count();
            let current_nodes = game_loop.engine.discovery.total_node_count();
            let desired_nodes = if total_balls >= 15 {
                4
            } else if total_balls >= 10 {
                3
            } else if total_balls >= 5 {
                2
            } else {
                1
            };
            if desired_nodes > current_nodes && desired_nodes > last_split_request {
                last_split_request = desired_nodes;
                println!("REQUEST_SPLIT");
            }
        }

        diag_counter += 1;
        if diag_counter % 256 == 0 {
            let em = &game_loop.engine.node.manager;
            let local_count = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::Local).count();
            let handoff_count = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::HandoffOut).count();
            let remote_count = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::Remote).count();
            let physics_balls = game_loop.world.ball_count();
            let conns = game_loop.engine.connections.len();
            let boundary = game_loop.engine.node.manager.boundary();
            let lower_b = game_loop.engine.node.manager.lower_boundary();
            let tn = game_loop.engine.discovery.total_node_count();
            eprintln!(
                "[Node {}] entities: {} (L:{} H:{} R:{}) physics:{} conns:{} events:{} zone=[{:.1},{:.1}] nodes={}",
                id, em.entities.len(), local_count, handoff_count, remote_count,
                physics_balls, conns, events.len(), lower_b, boundary, tn
            );
            for e in em.entities.values().take(3) {
                let in_physics = game_loop.world.get_entity_state(e.id).is_some();
                let is_local_sim = game_loop.world.locally_simulated_ids().contains(&e.id);
                eprintln!(
                    "  ent {} state={:?} pos=({:.1},{:.1},{:.1}) in_rapier={} local_sim={}",
                    e.id, e.state, e.pos.0, e.pos.1, e.pos.2, in_physics, is_local_sim
                );
            }
        }

        for dg in &game_loop.engine.client_datagrams.clone() {
            if dg.len() >= 3 && dg[0] == 0xDD {
                let count = ((dg[1] as u16) << 8) | (dg[2] as u16);
                for _ in 0..count {
                    game_loop.world.spawn_ball();
                }
                eprintln!("[Server] +{} balls (total: {})", count, game_loop.world.ball_count());
            }
        }

        let entity_count = game_loop.engine.node.manager.entities.len() as u16;
        let active_nodes = game_loop.engine.discovery.total_node_count().max(1) as u8;
        let status_bytes: [u8; 6] = [
            0xAA,
            (entity_count >> 8) as u8,
            (entity_count & 0xFF) as u8,
            active_nodes,
            24,
            8,
        ];

        let player_ids = game_loop.player_entity_ids();
        let count = player_ids.len() as u16;
        let mut bb_buf = Vec::with_capacity(3 + count as usize * 8);
        bb_buf.push(0xBB);
        bb_buf.push((count >> 8) as u8);
        bb_buf.push((count & 0xFF) as u8);
        for pid in &player_ids {
            bb_buf.extend_from_slice(&pid.to_le_bytes());
        }

        for conn in &game_loop.engine.connections {
            let _ = conn.send_datagram(status_bytes.to_vec().into());
            let _ = conn.send_datagram(bb_buf.clone().into());
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
    fn test_physics_world_spawn_tracks_ids() {
        let mut world = PhysicsWorld::new();
        let mut ids = Vec::new();
        for _ in 0..5 {
            ids.push(world.spawn_ball().unwrap());
        }
        assert_eq!(world.server_ball_ids.len(), 5);
        assert_eq!(world.locally_simulated_ids().len(), 5);
        for id in &ids {
            assert!(world.server_ball_ids.contains(id));
        }
    }

    #[test]
    fn test_physics_world_entity_arrived() {
        let mut world = PhysicsWorld::new();
        let pos = (5.0, 3.0, -2.0);
        let vel = (1.0, 0.0, 0.0);
        world.on_entity_arrived(999, pos, vel);
        assert!(world.balls.contains_key(&999));
        let state = world.get_entity_state(999);
        assert!(state.is_some());
        let (p, _v) = state.unwrap();
        assert!((p.0 - 5.0).abs() < 0.01);
        assert!((p.1 - 3.0).abs() < 0.01);
        assert!((p.2 - (-2.0)).abs() < 0.01);
    }

    #[test]
    fn test_physics_world_entity_update_kinematic() {
        let mut world = PhysicsWorld::new();
        world.on_entity_arrived(2_000_000, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0));
        world.on_entity_update(2_000_000, (10.0, 5.0, 3.0), (1.0, 2.0, 3.0));
        world.step();
        let state = world.get_entity_state(2_000_000).unwrap();
        assert!((state.0 .0 - 10.0).abs() < 0.5);
        assert!((state.0 .1 - 5.0).abs() < 0.5);
        world.on_entity_update(2_000_000, (15.0, 5.0, 3.0), (0.0, 0.0, 0.0));
        world.step();
        let state2 = world.get_entity_state(2_000_000).unwrap();
        assert!((state2.0 .0 - 15.0).abs() < 0.5);
    }

    #[test]
    fn test_spawn_handler_respects_count() {
        let mut world = PhysicsWorld::new();
        let before = world.ball_count();
        let count = 50;
        for _ in 0..count {
            world.spawn_ball();
        }
        assert_eq!(world.ball_count(), before + count);
        assert_eq!(world.server_ball_ids.len(), before + count);
    }

    #[test]
    fn test_physics_dt_matches_tick_rate() {
        let world = PhysicsWorld::new();
        let expected = 1.0 / 128.0_f32;
        assert!(
            (world.integration_parameters.dt - expected).abs() < 1e-6,
            "dt should be 1/128, got {}",
            world.integration_parameters.dt
        );
    }

    #[test]
    fn test_balls_stay_inside_walls() {
        let mut world = PhysicsWorld::new();
        for i in 0..20 {
            let id = world.next_ball_id;
            world.next_ball_id += 1;
            let x = (i % 10 + 2) as f32;
            let z = ((i as f32 / 20.0) * 16.0) - 8.0;
            let vx = if i % 2 == 0 { 15.0 } else { -15.0 };
            let vz = if i % 3 == 0 { 10.0 } else { -10.0 };
            let rb = RigidBodyBuilder::dynamic()
                .translation(vector![x, 3.0, z])
                .linvel(vector![vx, 0.0, vz])
                .ccd_enabled(true)
                .build();
            let h = world.rigid_body_set.insert(rb);
            let col = ColliderBuilder::ball(0.8).restitution(0.6).friction(0.3).density(8.0).build();
            world.collider_set.insert_with_parent(col, h, &mut world.rigid_body_set);
            world.balls.insert(id, Ball { rigid_body_handle: h });
            world.server_ball_ids.insert(id);
        }
        for _ in 0..1000 {
            world.step();
        }
        for ball in world.balls.values() {
            if let Some(rb) = world.rigid_body_set.get(ball.rigid_body_handle) {
                let pos = rb.translation();
                assert!(pos.x > -5.0 && pos.x < 30.0, "Ball escaped x: {}", pos.x);
                assert!(pos.z > -16.0 && pos.z < 16.0, "Ball escaped z: {}", pos.z);
                assert!(pos.y > -3.0, "Ball fell through ground: y={}", pos.y);
            }
        }
    }

    #[test]
    fn test_dynamic_body_not_teleported() {
        let mut world = PhysicsWorld::new();
        world.spawn_ball();
        let orig = world.get_ball_state(1).unwrap().0;
        world.update_ball(1, (100.0, 100.0, 100.0), (0.0, 0.0, 0.0));
        let after = world.get_ball_state(1).unwrap().0;
        assert!(
            (after.0 - orig.0).abs() < 1.0,
            "Dynamic body should NOT be teleported, was at {:.1} now {:.1}",
            orig.0,
            after.0
        );
    }

    #[test]
    fn test_cross_node_collision_via_proxy() {
        let mut world = PhysicsWorld::new();
        world.spawn_ball_at(1, (5.0, 1.0, 0.0), (10.0, 0.0, 0.0));

        world.on_entity_update(2_000_000, (8.0, 1.0, 0.0), (0.0, 0.0, 0.0));

        let orig_vel = world.get_ball_state(1).unwrap().1;

        for _ in 0..50 {
            world.on_entity_update(2_000_000, (8.0, 1.0, 0.0), (0.0, 0.0, 0.0));
            world.step();
        }

        let (pos, vel) = world.get_ball_state(1).unwrap();
        let deflected = (vel.0 - orig_vel.0).abs() > 0.1
            || pos.0 < 7.0;
        assert!(
            deflected,
            "Dynamic ball should be deflected by kinematic proxy. pos=({:.2},{:.2},{:.2}) vel=({:.2},{:.2},{:.2})",
            pos.0, pos.1, pos.2, vel.0, vel.1, vel.2
        );
    }

    #[test]
    fn test_kinematic_proxy_does_not_move_on_collision() {
        let mut world = PhysicsWorld::new();
        world.spawn_ball_at(1, (5.0, 1.0, 0.0), (10.0, 0.0, 0.0));
        world.on_entity_update(2_000_000, (8.0, 1.0, 0.0), (0.0, 0.0, 0.0));

        for _ in 0..50 {
            world.on_entity_update(2_000_000, (8.0, 1.0, 0.0), (0.0, 0.0, 0.0));
            world.step();
        }

        let (proxy_pos, _) = world.get_ball_state(2_000_000).unwrap();
        assert!(
            (proxy_pos.0 - 8.0).abs() < 0.5,
            "Kinematic proxy should stay near (8,1,0), got ({:.2},{:.2},{:.2})",
            proxy_pos.0, proxy_pos.1, proxy_pos.2
        );
    }

    #[test]
    fn test_proxy_created_and_updated() {
        let mut world = PhysicsWorld::new();
        assert!(world.get_ball_state(3_000_000).is_none());

        world.on_entity_update(3_000_000, (5.0, 3.0, 2.0), (1.0, 0.0, 0.0));
        world.step();

        let state = world.get_ball_state(3_000_000);
        assert!(state.is_some(), "Proxy should exist after on_entity_update");
        let (pos, _) = state.unwrap();
        assert!((pos.0 - 5.0).abs() < 1.0, "Proxy x should be near 5.0, got {}", pos.0);

        world.on_entity_update(3_000_000, (15.0, 3.0, 2.0), (0.0, 0.0, 0.0));
        world.step();
        let (pos2, _) = world.get_ball_state(3_000_000).unwrap();
        assert!((pos2.0 - 15.0).abs() < 1.0, "Proxy x should move to 15.0 after update, got {}", pos2.0);
    }

    #[test]
    fn test_physics_world_entity_departed() {
        let mut world = PhysicsWorld::new();
        world.on_entity_arrived(55, (1.0, 2.0, 3.0), (0.0, 0.0, 0.0));
        assert!(world.balls.contains_key(&55));
        world.on_entity_departed(55);
        assert!(!world.balls.contains_key(&55));
        assert!(world.get_entity_state(55).is_none());
    }

    #[test]
    fn test_kinematic_to_dynamic_on_arrival() {
        let mut world = PhysicsWorld::new();
        world.spawn_remote_ball(42, (10.0, 1.0, 0.0), (0.0, 0.0, 0.0));
        assert!(world.balls.contains_key(&42));
        assert!(!world.server_ball_ids.contains(&42));
        let rb = world.rigid_body_set.get(world.balls[&42].rigid_body_handle).unwrap();
        assert!(rb.is_kinematic(), "Should start as kinematic proxy");

        world.on_entity_arrived(42, (10.0, 1.0, 0.0), (1.0, 0.0, 0.0));
        assert!(world.balls.contains_key(&42));
        assert!(world.server_ball_ids.contains(&42));
        let rb = world.rigid_body_set.get(world.balls[&42].rigid_body_handle).unwrap();
        assert!(rb.is_dynamic(), "Ball should be dynamic after handoff arrival");
    }

    #[test]
    fn test_dynamic_stays_dynamic_on_double_arrival() {
        let mut world = PhysicsWorld::new();
        world.spawn_ball_at(10, (5.0, 1.0, 0.0), (1.0, 0.0, 0.0));
        let orig_handle = world.balls[&10].rigid_body_handle;
        let orig_pos = world.get_ball_state(10).unwrap().0;

        world.on_entity_arrived(10, (5.0, 1.0, 0.0), (1.0, 0.0, 0.0));
        let rb = world.rigid_body_set.get(world.balls[&10].rigid_body_handle).unwrap();
        assert!(rb.is_dynamic(), "Should remain dynamic");
        assert_eq!(world.balls[&10].rigid_body_handle, orig_handle, "Handle should not change");
        let pos = world.get_ball_state(10).unwrap().0;
        assert!((pos.0 - orig_pos.0).abs() < 0.01, "Position should be unchanged");
    }

    #[test]
    fn test_collision_works_after_kinematic_to_dynamic_conversion() {
        let mut world = PhysicsWorld::new();
        world.spawn_remote_ball(42, (10.0, 1.0, 0.0), (0.0, 0.0, 0.0));
        world.on_entity_arrived(42, (10.0, 1.0, 0.0), (0.0, 0.0, 0.0));
        let rb = world.rigid_body_set.get(world.balls[&42].rigid_body_handle).unwrap();
        assert!(rb.is_dynamic(), "Ball 42 should be dynamic after conversion");

        world.spawn_ball_at(43, (5.0, 1.0, 0.0), (10.0, 0.0, 0.0));
        let orig_vel = world.get_ball_state(43).unwrap().1;

        for _ in 0..50 {
            world.step();
        }

        let (pos, vel) = world.get_ball_state(43).unwrap();
        let deflected = (vel.0 - orig_vel.0).abs() > 0.1 || pos.0 < 9.0;
        assert!(
            deflected,
            "Ball 43 should collide with converted-dynamic ball 42. pos=({:.2},{:.2},{:.2}) vel=({:.2},{:.2},{:.2})",
            pos.0, pos.1, pos.2, vel.0, vel.1, vel.2
        );
    }

    #[test]
    fn test_split_thresholds_by_ball_count() {
        fn desired_nodes(total_balls: usize) -> usize {
            if total_balls >= 15 { 4 }
            else if total_balls >= 10 { 3 }
            else if total_balls >= 5 { 2 }
            else { 1 }
        }
        assert_eq!(desired_nodes(0), 1);
        assert_eq!(desired_nodes(4), 1);
        assert_eq!(desired_nodes(5), 2);
        assert_eq!(desired_nodes(9), 2);
        assert_eq!(desired_nodes(10), 3);
        assert_eq!(desired_nodes(14), 3);
        assert_eq!(desired_nodes(15), 4);
        assert_eq!(desired_nodes(100), 4);
    }

    #[test]
    fn test_position_stable_across_conversion() {
        let mut world = PhysicsWorld::new();
        world.spawn_remote_ball(42, (10.0, 1.0, 0.0), (0.0, 0.0, 0.0));
        world.step();

        world.on_entity_arrived(42, (10.0, 1.0, 0.0), (0.0, 0.0, 0.0));
        world.step();
        let (pos, _) = world.get_ball_state(42).unwrap();
        assert!(
            (pos.0 - 10.0).abs() < 1.5,
            "Position should be stable after conversion, got x={:.2}",
            pos.0
        );
        assert!(
            (pos.1 - 1.0).abs() < 1.5,
            "Position y should be stable after conversion, got y={:.2}",
            pos.1
        );
    }

    #[test]
    fn test_departed_entity_cleaned_from_all_tracking() {
        let mut world = PhysicsWorld::new();
        let id = world.spawn_ball().unwrap();
        assert!(world.balls.contains_key(&id));
        assert!(world.server_ball_ids.contains(&id));
        let handle = world.balls[&id].rigid_body_handle;
        assert!(world.rigid_body_set.get(handle).is_some());

        world.on_entity_departed(id);
        assert!(!world.balls.contains_key(&id));
        assert!(!world.server_ball_ids.contains(&id));
        assert!(world.rigid_body_set.get(handle).is_none());
    }
}
