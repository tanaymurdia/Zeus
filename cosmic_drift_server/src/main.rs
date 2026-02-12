//! Cosmic Drift Server - Physics Backend Demo
//!
//! This server demonstrates Zeus auto-scaling with server-side physics.
//! It simulates physics for all balls and broadcasts positions to clients.
//!
//! Modes:
//! - Orchestrator: Spawns and manages physics nodes
//! - Node: Runs physics simulation for a region (or simple load balancing)

use clap::Parser;
use rapier3d::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
// use tokio::fs;
use tokio::sync::RwLock;
use zeus_node::engine::{ZeusConfig, ZeusEngine, ZeusEvent};

#[derive(Parser)]
#[command(name = "cosmic_drift_server")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Bind address (for default run)
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    bind: SocketAddr,

    /// Maximum number of balls to simulate
    #[arg(short, long, default_value = "5000")]
    max_balls: usize,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Run the auto-scaling orchestrator
    Orchestrator {
        /// Starting port for nodes
        #[arg(short, long, default_value = "5000")]
        start_port: u16,
    },
    /// Run a single physics node
    RunNode {
        /// Bind address
        #[arg(short, long)]
        bind: SocketAddr,

        /// Node ID
        #[arg(short, long)]
        id: u8,

        /// Boundary (X-axis) for handoff
        #[arg(long, default_value = "0.0")]
        boundary: f32,

        /// Peer address (Right Neighbor)
        #[arg(long)]
        peer: Option<SocketAddr>,
    },
}

/// Ball entity tracked by physics
struct Ball {
    rigid_body_handle: RigidBodyHandle,
}

/// Shared physics world state
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
    next_ball_id: u64,
}

impl PhysicsWorld {
    fn new() -> Self {
        let mut rigid_body_set = RigidBodySet::new();
        let mut collider_set = ColliderSet::new();

        // Create ground plane
        let ground = RigidBodyBuilder::fixed()
            .translation(vector![0.0, -1.0, 0.0])
            .build();
        let ground_handle = rigid_body_set.insert(ground);
        let ground_collider = ColliderBuilder::cuboid(500.0, 0.5, 500.0)
            .restitution(0.5)
            .friction(0.3)
            .build();
        collider_set.insert_with_parent(ground_collider, ground_handle, &mut rigid_body_set);

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
            next_ball_id: 1,
        }
    }

    fn spawn_ball(&mut self) -> Option<u64> {
        let id = self.next_ball_id;
        self.next_ball_id += 1;

        // Place on a grid to avoid interpenetration
        // Grid: 5 columns in X (spacing 3.0), rows in Z (spacing 3.0)
        let grid_idx = (id - 1) as f32;
        let col = (grid_idx % 5.0) as f32;
        let row = (grid_idx / 5.0).floor();
        let x = col * 3.0; // X in [0, 12]
        let y = 2.0; // Drop from just above ground
        let z = row * 3.0 - 6.0; // Z centered around 0

        let rigid_body = RigidBodyBuilder::dynamic()
            .translation(vector![x, y, z])
            .linvel(vector![0.0, 0.0, 0.0])
            .linear_damping(1.0)
            .angular_damping(1.0)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(0.5)
            .restitution(0.2)
            .friction(1.0)
            .density(2.0)
            .build();
        self.collider_set
            .insert_with_parent(collider, handle, &mut self.rigid_body_set);

        self.balls.insert(
            id,
            Ball {
                rigid_body_handle: handle,
            },
        );
        Some(id)
    }

    fn spawn_remote_ball(&mut self, id: u64, pos: (f32, f32, f32), _vel: (f32, f32, f32)) {
        if self.balls.contains_key(&id) {
            return;
        }

        // Use kinematic body for player-controlled entities so Rapier
        // generates proper collision responses that push NPC balls away
        let rigid_body = RigidBodyBuilder::kinematic_position_based()
            .translation(vector![pos.0, pos.1, pos.2])
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(0.5)
            .restitution(0.8)
            .friction(0.5)
            .density(2.0)
            .build();
        self.collider_set
            .insert_with_parent(collider, handle, &mut self.rigid_body_set);

        self.balls.insert(
            id,
            Ball {
                rigid_body_handle: handle,
            },
        );
        // println!("[Physics] Remote Entity {} Arrived.", id);
    }

    fn update_ball(&mut self, id: u64, pos: (f32, f32, f32), _vel: (f32, f32, f32)) {
        if let Some(ball) = self.balls.get(&id) {
            if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
                // Use kinematic target so Rapier moves the body smoothly
                // and generates contact forces against dynamic NPC balls
                rb.set_next_kinematic_position(rapier3d::prelude::Isometry::translation(
                    pos.0, pos.1, pos.2,
                ));
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
            // println!("[Physics] Entity {} Departed (Handoff Complete).", id);
        }
    }

    fn step(&mut self) {
        let gravity = vector![0.0, -9.81, 0.0];

        // "Cosmic Wind" disabled — it pushes balls off-screen before they can settle
        // for ball in self.balls.values() {
        //     if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
        //         rb.add_force(vector![20.0, 0.0, 0.0], true);
        //     }
        // }

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

    /// Get all ball positions for broadcasting
    fn get_positions(&self) -> Vec<(u64, f32, f32, f32)> {
        let mut positions: Vec<(u64, f32, f32, f32)> = self
            .balls
            .iter()
            .filter_map(|(&id, ball)| {
                self.rigid_body_set.get(ball.rigid_body_handle).map(|rb| {
                    let pos = rb.translation();
                    (id, pos.x, pos.y, pos.z)
                })
            })
            .collect();
        positions.sort_by_key(|(id, _, _, _)| *id);
        positions
    }

    // Get individual ball state
    fn _get_ball_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
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
        None => {
            // Default: Run as node 0
            run_physics_node(cli.bind, 0, 20.0, None, cli.max_balls).await
        }
    }
}

async fn run_orchestrator(start_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::ChildStdout;

    println!("╔════════════════════════════════════════════════════════╗");
    println!("║       COSMIC DRIFT PHYSICS ORCHESTRATOR                ║");
    println!("║  Protocol-Driven Linear Handoff Chain                  ║");
    println!("╚════════════════════════════════════════════════════════╝");

    let mut nodes = Vec::new();
    let mut _next_id = 0;

    // Topology:
    // Node 0: Bound 20, Peer -> Node 1 (if exists?)
    // Actually, Orchestrator spawns them sequentially.
    // Node 0 needs to know Node 1 address.
    // But Node 1 isn't spawned yet!
    // Solution: WE know the ports.
    // Node 0 (5000) -> Peer 127.0.0.1:5001
    // Node 1 (5001) -> Peer 127.0.0.1:5002

    // Pre-spawn strict chain.

    // Function to spawn a node
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

    // Monitor Output (Stdout + Stderr)
    let monitor_node =
        |id: u8, stdout: Option<ChildStdout>, stderr: Option<tokio::process::ChildStderr>| {
            if let Some(stdout) = stdout {
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = reader.next_line().await {
                        println!("[Node {}] {}", id, line);
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

    // Initial Chain: Node 0
    // We will spawn Node 1 when Node 0 requests split OR just pre-spawn 2 nodes to demo handoff immediately.
    // Let's pre-spawn 2 nodes.

    // Node 0 -> Node 1
    let mut cmd0 = spawn_node(0, start_port, 20.0, Some(start_port + 1));
    let mut child0 = cmd0.spawn()?;
    monitor_node(0, child0.stdout.take(), child0.stderr.take());
    nodes.push(child0);

    // Node 1 -> None (Sink)
    let mut cmd1 = spawn_node(1, start_port + 1, 40.0, None);
    let mut child1 = cmd1.spawn()?;
    monitor_node(1, child1.stdout.take(), child1.stderr.take());
    nodes.push(child1);

    _next_id = 2;

    println!("[Orchestrator] Chain Active: Node 0 -> Node 1");
    println!("[Orchestrator] Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;

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
    println!(
        "[Node {}] Initializing Zeus Engine on {}... (Bound: {}, Peer: {:?})",
        id, bind, boundary, peer
    );

    // 1. Initialize Zeus Engine
    let config = ZeusConfig {
        bind_addr: bind,
        seed_addr: peer, // For Discovery bootstrapping (simplification: Use peer as seed)
        boundary,
        margin: 5.0, // Hysteresis margin
    };

    let mut engine = ZeusEngine::new(config).await?;

    // We also need to manually connect to Peer for Handoff if we are Source.
    // ZeusEngine handles "Seed" connection, but Handoff requires knowing *who* to send to.
    // In this simple demo, we assume the Seed IS the neighbor we hand off to.

    // 2. Physics World
    let physics = Arc::new(RwLock::new(PhysicsWorld::new()));

    // 3. Loop
    let tick_duration = std::time::Duration::from_millis(16); // 60 Hz

    loop {
        let loop_start = std::time::Instant::now();

        // A. Step Physics
        {
            let mut world = physics.write().await;
            world.step();

            // Spawn balls (Only Node 0)
            if id == 0 && world.ball_count() < 20 {
                // Spawn 1 ball every ~30 ticks (0.5 sec) to avoid pile-up
                static mut SPAWN_CTR: u32 = 0;
                unsafe {
                    SPAWN_CTR += 1;
                }
                if unsafe { SPAWN_CTR } % 30 == 0 {
                    world.spawn_ball();
                }
            }
        }

        // B. Tick Zeus (Networking) - BEFORE physics sync
        // This runs node.update() which applies crude integration,
        // but we overwrite with correct positions right after.
        let events = engine.tick(0.016).await?;

        // C. Handle Zeus Events
        {
            let mut world = physics.write().await;
            for event in events {
                match event {
                    ZeusEvent::EntityArrived { id, pos, vel } => {
                        world.spawn_remote_ball(id, pos, vel);
                    }
                    ZeusEvent::EntityDeparted { id } => {
                        world.remove_ball(id);
                    }
                    ZeusEvent::RemoteUpdate { id, pos, vel } => {
                        // Client Authority Update - Teleport physics body
                        world.spawn_remote_ball(id, pos, vel); // Spawns if new
                        world.update_ball(id, pos, vel);
                    }
                }
            }
        }

        // D. Sync Physics -> Zeus (AFTER tick so EntityManager has correct positions)
        // This overwrites the entity manager's crude integration with accurate rapier positions.
        {
            let world = physics.read().await;
            static mut SYNC_CTR: u32 = 0;
            unsafe {
                SYNC_CTR += 1;
            }
            let mut sample_count = 0;
            for (ball_id, ball) in &world.balls {
                if let Some(rb) = world.rigid_body_set.get(ball.rigid_body_handle) {
                    let pos = rb.translation();
                    let vel = rb.linvel();
                    engine.update_entity(*ball_id, (pos.x, pos.y, pos.z), (vel.x, vel.y, vel.z));

                    // Log first 3 balls every 60 ticks
                    if unsafe { SYNC_CTR } % 60 == 0 && sample_count < 3 {
                        let em_pos = engine.node.manager.get_entity(*ball_id).map(|e| e.pos);
                        println!(
                            "[Node {}] Rapier Ball {}: ({:.1}, {:.1}, {:.1}) EM: {:?}",
                            id, ball_id, pos.x, pos.y, pos.z, em_pos
                        );
                        sample_count += 1;
                    }
                }
            }
        }
        // E. Broadcast entity state to clients (with correct physics positions)
        engine.broadcast_state_to_clients().await;

        // F. Broadcast Visuals (Custom Datagrams via Zeus Endpoint)
        // We use engine.connections or engine.endpoint
        // ZeusEngine exposes endpoint & connections now.
        static mut TICK_COUNTER: u32 = 0;
        unsafe {
            TICK_COUNTER += 1;
        }

        {
            let world = physics.read().await;
            let _positions = world.get_positions();
            let ball_count = world.ball_count();

            // 1. Status
            let entity_count = ball_count as u16;
            let status_bytes: [u8; 4] = [
                0xAA,
                (entity_count >> 8) as u8,
                (entity_count & 0xFF) as u8,
                1,
            ];

            // Broadcast to ALL connections (Peers + Clients)
            if unsafe { TICK_COUNTER } % 60 == 0 {
                println!(
                    "[Node {}] Broadcasting: {} connections, {} balls in physics",
                    id,
                    engine.connections.len(),
                    ball_count
                );
            }
            for conn in &engine.connections {
                let _ = conn.send_datagram(status_bytes.to_vec().into());
            }

            // 2. Positions - Handled by ZeusEngine (0xCC packet)
            // (Orbiting logic removed)

            if unsafe { TICK_COUNTER } % 60 == 0 {
                // println!("[Node {}] Simulating {} balls", id, ball_count);
            }
        }

        let elapsed = loop_start.elapsed();
        if elapsed < tick_duration {
            tokio::time::sleep(tick_duration - elapsed).await;
        }
    }
}
