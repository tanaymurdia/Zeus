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
    split_requested: bool,
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

        let wall_col = ColliderBuilder::cuboid(500.0, 10.0, 1.0).build();
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

        let wall_side_col = ColliderBuilder::cuboid(500.0, 10.0, 1.0).build();
        collider_set.insert_with_parent(wall_side_col.clone(), h_left, &mut rigid_body_set);
        collider_set.insert_with_parent(wall_side_col, h_right, &mut rigid_body_set);

        Self {
            rigid_body_set,
            collider_set,
            integration_parameters: IntegrationParameters::default(),
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
            split_requested: false,
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
            .linear_damping(0.0)
            .angular_damping(0.0)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.8)
            .friction(0.05)
            .density(1.0)
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
        if self.balls.contains_key(&id) {
            return;
        }
        let rigid_body = RigidBodyBuilder::dynamic()
            .translation(vector![pos.0, pos.1, pos.2])
            .linvel(vector![vel.0, vel.1, vel.2])
            .linear_damping(0.0)
            .angular_damping(0.0)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);
        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.8)
            .friction(0.05)
            .density(1.0)
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
            .restitution(0.8)
            .friction(0.05)
            .density(1.0)
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

    fn update_ball(&mut self, id: u64, pos: (f32, f32, f32), _vel: (f32, f32, f32)) {
        if let Some(ball) = self.balls.get(&id) {
            if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
                rb.set_next_kinematic_position(rapier3d::prelude::Isometry::translation(
                    pos.0, pos.1, pos.2,
                ));
                rb.set_linvel(rapier3d::na::vector![_vel.0, _vel.1, _vel.2], true);
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

        if self.balls.len() > 10 && !self.split_requested {
            self.split_requested = true;
            println!("REQUEST_SPLIT");
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

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break;
            }
            Some(_) = rx.recv() => {
                if next_id < 4 {
                    println!("[Orchestrator] ⚠️ SPLIT REQUEST RECEIVED! Spawning Node {}...", next_id);
                    let port = start_port + next_id as u16;
                    let boundary = 6.0 * (next_id as f32 + 1.0);
                    let peer = port - 1;

                    let mut cmd = spawn_node(next_id, port, boundary, Some(peer));
                    if let Ok(mut child) = cmd.spawn() {
                        monitor_node(next_id, child.stdout.take(), child.stderr.take(), tx.clone());
                        nodes.push(child);
                        next_id += 1;
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
        margin: 5.0,
    };

    let physics = PhysicsWorld::new();
    let mut game_loop = GameLoop::new(config, physics).await?;

    let tick_duration = std::time::Duration::from_micros(7812);
    let dt = 1.0 / 128.0;
    let mut spawn_counter: u32 = 0;
    let mut diag_counter: u32 = 0;

    loop {
        let loop_start = std::time::Instant::now();

        if id == 0 && game_loop.world.ball_count() < 50 {
            spawn_counter += 1;
            if spawn_counter % 120 == 0 {
                game_loop.world.spawn_ball();
            }
        }

        if id == 0 {
            let has_peers = !game_loop.engine.discovery.peers.is_empty();
            if game_loop.world.split_requested && has_peers {
                game_loop.set_boundary(6.0);
            } else {
                game_loop.set_boundary(24.0);
            }
        }

        let events = game_loop.tick(dt).await?;

        diag_counter += 1;
        if diag_counter % 256 == 0 {
            let em = &game_loop.engine.node.manager;
            let local_count = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::Local).count();
            let handoff_count = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::HandoffOut).count();
            let remote_count = em.entities.values().filter(|e| e.state == zeus_node::entity_manager::AuthorityState::Remote).count();
            let physics_balls = game_loop.world.ball_count();
            let conns = game_loop.engine.connections.len();
            eprintln!(
                "[Node {}] entities: {} (L:{} H:{} R:{}) physics:{} conns:{} events:{}",
                id, em.entities.len(), local_count, handoff_count, remote_count,
                physics_balls, conns, events.len()
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
        let peer_node_count = game_loop.engine.discovery.peers.len() as u8;
        let active_nodes = (peer_node_count + 1).max(1);
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
    fn test_physics_world_entity_update() {
        let mut world = PhysicsWorld::new();
        world.on_entity_arrived(42, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0));
        world.on_entity_update(42, (10.0, 20.0, 30.0), (1.0, 2.0, 3.0));
        world.step();
        let state = world.get_entity_state(42).unwrap();
        assert!((state.0 .0 - 10.0).abs() < 0.5);
        assert!((state.0 .1 - 20.0).abs() < 0.5);
        world.on_entity_update(42, (99.0, 88.0, 77.0), (0.0, 0.0, 0.0));
        world.step();
        let state2 = world.get_entity_state(42).unwrap();
        assert!((state2.0 .0 - 99.0).abs() < 0.5);
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
    fn test_physics_world_entity_departed() {
        let mut world = PhysicsWorld::new();
        world.on_entity_arrived(55, (1.0, 2.0, 3.0), (0.0, 0.0, 0.0));
        assert!(world.balls.contains_key(&55));
        world.on_entity_departed(55);
        assert!(!world.balls.contains_key(&55));
        assert!(world.get_entity_state(55).is_none());
    }
}
