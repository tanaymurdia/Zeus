use clap::Parser;
use rapier3d::control::{CharacterLength, KinematicCharacterController};
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

        #[arg(long)]
        peers: Option<String>,
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

    character_controller: KinematicCharacterController,
    player_targets: HashMap<u64, (f32, f32, f32)>,
}

impl PhysicsWorld {
    fn new() -> Self {
        let mut rigid_body_set = RigidBodySet::new();
        let mut collider_set = ColliderSet::new();

        let ground = RigidBodyBuilder::fixed()
            .translation(vector![12.0, -1.0, 0.0])
            .build();
        let ground_handle = rigid_body_set.insert(ground);
        let ground_collider = ColliderBuilder::cuboid(500.0, 0.1, 500.0)
            .restitution(0.2)
            .friction(0.0)
            .build();
        collider_set.insert_with_parent(ground_collider, ground_handle, &mut rigid_body_set);

        let wall_front = RigidBodyBuilder::fixed()
            .translation(vector![12.0, 5.0, 14.0])
            .build();
        let wall_back = RigidBodyBuilder::fixed()
            .translation(vector![12.0, 5.0, -14.0])
            .build();
        let h_front = rigid_body_set.insert(wall_front);
        let h_back = rigid_body_set.insert(wall_back);

        let wall_fb_col = ColliderBuilder::cuboid(500.0, 10.0, 2.0).build();
        collider_set.insert_with_parent(wall_fb_col.clone(), h_front, &mut rigid_body_set);
        collider_set.insert_with_parent(wall_fb_col, h_back, &mut rigid_body_set);

        let wall_left = RigidBodyBuilder::fixed()
            .translation(vector![-0.5, 5.0, 0.0])
            .build();
        let wall_right = RigidBodyBuilder::fixed()
            .translation(vector![24.5, 5.0, 0.0])
            .build();
        let h_left = rigid_body_set.insert(wall_left);
        let h_right = rigid_body_set.insert(wall_right);

        let wall_lr_col = ColliderBuilder::cuboid(0.5, 10.0, 15.0).build();
        collider_set.insert_with_parent(wall_lr_col.clone(), h_left, &mut rigid_body_set);
        collider_set.insert_with_parent(wall_lr_col, h_right, &mut rigid_body_set);

        let mut character_controller = KinematicCharacterController::default();
        character_controller.offset = CharacterLength::Absolute(0.02);
        character_controller.autostep = None;
        character_controller.snap_to_ground = Some(CharacterLength::Absolute(0.5));

        Self {
            rigid_body_set,
            collider_set,
            integration_parameters: {
                let mut p = IntegrationParameters::default();
                p.dt = 1.0 / 128.0;
                p.num_solver_iterations = std::num::NonZeroUsize::new(8).unwrap();
                p.contact_natural_frequency = 60.0;
                p.contact_damping_ratio = 2.0;
                p.normalized_max_corrective_velocity = 20.0;
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
            character_controller,
            player_targets: HashMap::new(),
        }
    }

    fn spawn_ball(&mut self) -> Option<u64> {
        let id = self.next_ball_id;
        self.next_ball_id += 1;

        let hash = id.wrapping_mul(2654435761);
        let x = 1.0 + (hash % 100) as f32 * 0.2;
        let y = 3.0;
        let z = ((hash / 100) % 100) as f32 * 0.2 - 10.0;

        let rigid_body = RigidBodyBuilder::dynamic()
            .translation(vector![x, y, z])
            .linvel(vector![5.0, 0.0, 0.0])
            .linear_damping(0.5)
            .angular_damping(0.5)
            .ccd_enabled(true)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);

        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.7)
            .friction(0.2)
            .density(5.0)
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
            .linear_damping(0.5)
            .angular_damping(0.5)
            .ccd_enabled(true)
            .build();
        let handle = self.rigid_body_set.insert(rigid_body);
        let collider = ColliderBuilder::ball(0.8)
            .restitution(0.7)
            .friction(0.2)
            .density(5.0)
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
            .restitution(0.7)
            .friction(0.2)
            .density(5.0)
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
            if let Some(rb) = self.rigid_body_set.get(ball.rigid_body_handle) {
                if rb.is_kinematic() {
                    self.player_targets.insert(id, pos);
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
        let dt = self.integration_parameters.dt;
        let character_shape = rapier3d::geometry::Ball::new(0.8);
        let character_mass = 20.0;

        self.query_pipeline.update(&self.collider_set);

        let targets: Vec<(u64, (f32, f32, f32))> = self.player_targets.drain().collect();

        struct PlayerMove {
            handle: RigidBodyHandle,
            current_pos: Isometry<f32>,
            desired: rapier3d::na::Vector3<f32>,
        }

        let moves: Vec<PlayerMove> = targets
            .iter()
            .filter_map(|(id, target)| {
                let ball = self.balls.get(id)?;
                let rb = self.rigid_body_set.get(ball.rigid_body_handle)?;
                let current_pos = *rb.position();
                let desired = vector![
                    target.0 - current_pos.translation.x,
                    target.1 - current_pos.translation.y,
                    target.2 - current_pos.translation.z
                ];
                Some(PlayerMove {
                    handle: ball.rigid_body_handle,
                    current_pos,
                    desired,
                })
            })
            .collect();

        for m in moves {
            let filter = QueryFilter::default().exclude_rigid_body(m.handle);
            let mut collisions = vec![];
            let corrected = self.character_controller.move_shape(
                dt,
                &self.rigid_body_set,
                &self.collider_set,
                &self.query_pipeline,
                &character_shape,
                &m.current_pos,
                m.desired,
                filter,
                |c| collisions.push(c),
            );

            let new_pos = Isometry::translation(
                m.current_pos.translation.x + corrected.translation.x,
                m.current_pos.translation.y + corrected.translation.y,
                m.current_pos.translation.z + corrected.translation.z,
            );
            if let Some(rb) = self.rigid_body_set.get_mut(m.handle) {
                rb.set_next_kinematic_position(new_pos);
            }

            let filter2 = QueryFilter::default().exclude_rigid_body(m.handle);
            self.character_controller.solve_character_collision_impulses(
                dt,
                &mut self.rigid_body_set,
                &self.collider_set,
                &self.query_pipeline,
                &character_shape,
                character_mass,
                &collisions,
                filter2,
            );
        }

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

        let max_speed = 30.0_f32;
        for ball in self.balls.values() {
            if let Some(rb) = self.rigid_body_set.get_mut(ball.rigid_body_handle) {
                if !rb.is_dynamic() {
                    continue;
                }
                let vel = *rb.linvel();
                if vel.norm() > max_speed {
                    rb.set_linvel(vel.normalize() * max_speed, true);
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

    fn status_payload(&self) -> (u16, u8, u8) {
        (0, 24, 8)
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
            peers,
        }) => {
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
            run_physics_node(bind, id, boundary, seed_addrs, cli.max_balls).await
        }
        None => run_physics_node(cli.bind, 0, 20.0, Vec::new(), cli.max_balls).await,
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
    let mut active_ports: Vec<u16> = Vec::new();

    let spawn_node = |id: u8, port: u16, boundary: f32, peer_ports: &[u16]| {
        println!(
            "[Orchestrator] Spawning Node {} (Bound={}) on port {} -> Peers {:?}",
            id, boundary, port, peer_ports
        );
        let mut cmd = tokio::process::Command::new(std::env::current_exe().unwrap());
        cmd.arg("run-node")
            .arg("--bind")
            .arg(format!("127.0.0.1:{}", port))
            .arg("--id")
            .arg(id.to_string())
            .arg("--boundary")
            .arg(boundary.to_string());

        if !peer_ports.is_empty() {
            let peers_str: String = peer_ports
                .iter()
                .map(|p| format!("127.0.0.1:{}", p))
                .collect::<Vec<_>>()
                .join(",");
            cmd.arg("--peers").arg(peers_str);
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

    let mut cmd0 = spawn_node(0, start_port, 24.0, &[]);
    let mut child0 = cmd0.spawn()?;
    monitor_node(0, child0.stdout.take(), child0.stderr.take(), tx.clone());
    nodes.push(child0);
    active_ports.push(start_port);

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

                    let mut cmd = spawn_node(next_id, port, boundary, &active_ports);
                    if let Ok(mut child) = cmd.spawn() {
                        monitor_node(next_id, child.stdout.take(), child.stderr.take(), tx.clone());
                        nodes.push(child);
                        active_ports.push(port);
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
    seed_addrs: Vec<SocketAddr>,
    _max_balls: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!(
        "[Node {}] bind={} boundary={} peers={:?}",
        id, bind, boundary, seed_addrs
    );

    let config = ZeusConfig {
        bind_addr: bind,
        seed_addrs,
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
    let mut status_counter: u32 = 0;
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
            if game_loop.should_split(total_balls) && total_balls > last_split_request {
                last_split_request = total_balls;
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

        status_counter += 1;
        if status_counter % 16 == 0 {
            game_loop.broadcast_status();
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
            let col = ColliderBuilder::ball(0.8).restitution(0.7).friction(0.2).density(5.0).build();
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
