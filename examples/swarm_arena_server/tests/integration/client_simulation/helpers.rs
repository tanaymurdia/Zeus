use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_node::autoscaler::AutoScaler;
use zeus_node::cell::Cell;
use zeus_node::engine::ZeusConfig;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::{GameLoop, GameWorld};

pub const DT: f32 = 1.0 / 128.0;
pub const TICKS_PER_100MS: usize = 13;
pub const WORLD: Cell = Cell {
    x_min: 0.0, x_max: 24.0,
    y_min: -1.0, y_max: 25.0,
    z_min: -12.0, z_max: 12.0,
};

pub const MAX_DRONE_SPEED: f32 = 4.0;
pub const DRONE_RADIUS: f32 = 0.3;
pub const DRONE_DAMPING: f32 = 3.0;
pub const WANDER_FORCE: f32 = 0.8;
pub const WALL_REPEL_DIST: f32 = 4.0;
pub const WALL_REPEL_FORCE: f32 = 4.0;
pub const ATTRACT_STRENGTH: f32 = 30.0;
pub const REPEL_STRENGTH: f32 = 50.0;
pub const WELL_RADIUS: f32 = 30.0;

#[derive(Clone, Debug)]
pub struct DroneState {
    pub pos: (f32, f32, f32),
    pub vel: (f32, f32, f32),
    pub wander_seed: u32,
    pub radius: f32,
}

#[derive(Clone, Copy, Debug)]
pub struct GravityWell {
    pub pos: (f32, f32, f32),
    pub mode: u8,
}

pub struct BoundedPhysicsWorld {
    pub drones: HashMap<u64, DroneState>,
    pub local_ids: HashSet<u64>,
    pub next_id: u64,
    pub bounds: Cell,
    pub gravity_wells: HashMap<u64, GravityWell>,
    pub tick_counter: u64,
    pub realistic: bool,
}

impl BoundedPhysicsWorld {
    pub fn new(bounds: Cell) -> Self {
        Self {
            drones: HashMap::new(),
            local_ids: HashSet::new(),
            next_id: 1,
            bounds,
            gravity_wells: HashMap::new(),
            tick_counter: 0,
            realistic: false,
        }
    }

    pub fn new_realistic(bounds: Cell) -> Self {
        Self {
            drones: HashMap::new(),
            local_ids: HashSet::new(),
            next_id: 1,
            bounds,
            gravity_wells: HashMap::new(),
            tick_counter: 0,
            realistic: true,
        }
    }

    pub fn spawn_drone_at(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.drones.insert(id, DroneState {
            pos, vel,
            wander_seed: id as u32 ^ 0xDEAD_BEEF,
            radius: DRONE_RADIUS,
        });
        self.local_ids.insert(id);
        if id >= self.next_id {
            self.next_id = id + 1;
        }
    }

    pub fn set_gravity_well(&mut self, well_id: u64, pos: (f32, f32, f32), mode: u8) {
        if mode == 0 {
            self.gravity_wells.remove(&well_id);
        } else {
            self.gravity_wells.insert(well_id, GravityWell { pos, mode });
        }
    }

    fn wander_force(seed: u32, tick: u64) -> (f32, f32, f32) {
        let phase = (tick as f32) * 0.05 + (seed as f32) * 0.1;
        let fx = phase.sin() * WANDER_FORCE;
        let fy = (phase * 1.3 + 2.0).cos() * WANDER_FORCE;
        let fz = (phase * 0.7 + 4.0).sin() * WANDER_FORCE * 0.5;
        (fx, fy, fz)
    }

    fn wall_repel_force(pos: (f32, f32, f32), bounds: &Cell) -> (f32, f32, f32) {
        let mut fx = 0.0f32;
        let mut fy = 0.0f32;
        let mut fz = 0.0f32;
        let d_xmin = pos.0 - bounds.x_min;
        let d_xmax = bounds.x_max - pos.0;
        let d_ymin = pos.1 - bounds.y_min;
        let d_ymax = bounds.y_max - pos.1;
        let d_zmin = pos.2 - bounds.z_min;
        let d_zmax = bounds.z_max - pos.2;
        if d_xmin < WALL_REPEL_DIST { fx += WALL_REPEL_FORCE * (1.0 - d_xmin / WALL_REPEL_DIST); }
        if d_xmax < WALL_REPEL_DIST { fx -= WALL_REPEL_FORCE * (1.0 - d_xmax / WALL_REPEL_DIST); }
        if d_ymin < WALL_REPEL_DIST { fy += WALL_REPEL_FORCE * (1.0 - d_ymin / WALL_REPEL_DIST); }
        if d_ymax < WALL_REPEL_DIST { fy -= WALL_REPEL_FORCE * (1.0 - d_ymax / WALL_REPEL_DIST); }
        if d_zmin < WALL_REPEL_DIST { fz += WALL_REPEL_FORCE * (1.0 - d_zmin / WALL_REPEL_DIST); }
        if d_zmax < WALL_REPEL_DIST { fz -= WALL_REPEL_FORCE * (1.0 - d_zmax / WALL_REPEL_DIST); }
        (fx, fy, fz)
    }

    fn gravity_force(
        drone_pos: (f32, f32, f32),
        wells: &HashMap<u64, GravityWell>,
    ) -> (f32, f32, f32) {
        let mut fx = 0.0f32;
        let mut fy = 0.0f32;
        let mut fz = 0.0f32;
        for well in wells.values() {
            let dx = well.pos.0 - drone_pos.0;
            let dy = well.pos.1 - drone_pos.1;
            let dz = well.pos.2 - drone_pos.2;
            let dist = (dx * dx + dy * dy + dz * dz).sqrt().max(0.5);
            if dist > WELL_RADIUS { continue; }
            let strength = match well.mode {
                1 => ATTRACT_STRENGTH / dist,
                2 => -REPEL_STRENGTH / dist,
                _ => continue,
            };
            let inv = strength / dist;
            fx += dx * inv;
            fy += dy * inv;
            fz += dz * inv;
        }
        (fx, fy, fz)
    }

    fn clamp_speed(vel: (f32, f32, f32)) -> (f32, f32, f32) {
        let speed = (vel.0 * vel.0 + vel.1 * vel.1 + vel.2 * vel.2).sqrt();
        if speed > MAX_DRONE_SPEED {
            let s = MAX_DRONE_SPEED / speed;
            (vel.0 * s, vel.1 * s, vel.2 * s)
        } else {
            vel
        }
    }
}

impl GameWorld for BoundedPhysicsWorld {
    fn step(&mut self, dt: f32) {
        self.tick_counter += 1;
        let ids: Vec<u64> = self.local_ids.iter().copied().collect();
        if self.realistic {
            let margin = DRONE_RADIUS + 0.2;
            for id in ids {
                if let Some(drone) = self.drones.get(&id).cloned() {
                    let wander = Self::wander_force(drone.wander_seed, self.tick_counter);
                    let wall = Self::wall_repel_force(drone.pos, &self.bounds);
                    let grav = Self::gravity_force(drone.pos, &self.gravity_wells);

                    let damping = (-DRONE_DAMPING * dt).exp();
                    let mut vx = drone.vel.0 * damping + (wander.0 + wall.0 + grav.0) * dt;
                    let mut vy = drone.vel.1 * damping + (wander.1 + wall.1 + grav.1) * dt;
                    let mut vz = drone.vel.2 * damping + (wander.2 + wall.2 + grav.2) * dt;

                    let (cvx, cvy, cvz) = Self::clamp_speed((vx, vy, vz));
                    vx = cvx; vy = cvy; vz = cvz;

                    let mut nx = drone.pos.0 + vx * dt;
                    let mut ny = drone.pos.1 + vy * dt;
                    let mut nz = drone.pos.2 + vz * dt;

                    if nx <= self.bounds.x_min + margin { nx = self.bounds.x_min + margin; vx = vx.abs(); }
                    if nx >= self.bounds.x_max - margin { nx = self.bounds.x_max - margin; vx = -vx.abs(); }
                    if ny <= self.bounds.y_min + margin { ny = self.bounds.y_min + margin; vy = vy.abs(); }
                    if ny >= self.bounds.y_max - margin { ny = self.bounds.y_max - margin; vy = -vy.abs(); }
                    if nz <= self.bounds.z_min + margin { nz = self.bounds.z_min + margin; vz = vz.abs(); }
                    if nz >= self.bounds.z_max - margin { nz = self.bounds.z_max - margin; vz = -vz.abs(); }

                    if let Some(d) = self.drones.get_mut(&id) {
                        d.pos = (nx, ny, nz);
                        d.vel = (vx, vy, vz);
                    }
                }
            }
        } else {
            let margin = 0.5;
            for id in ids {
                if let Some(drone) = self.drones.get(&id).cloned() {
                    let mut nx = drone.pos.0 + drone.vel.0 * dt;
                    let mut ny = drone.pos.1 + drone.vel.1 * dt;
                    let mut nz = drone.pos.2 + drone.vel.2 * dt;
                    let mut vx = drone.vel.0;
                    let mut vy = drone.vel.1;
                    let mut vz = drone.vel.2;

                    if nx <= self.bounds.x_min + margin { nx = self.bounds.x_min + margin; vx = vx.abs(); }
                    if nx >= self.bounds.x_max - margin { nx = self.bounds.x_max - margin; vx = -vx.abs(); }
                    if ny <= self.bounds.y_min + margin { ny = self.bounds.y_min + margin; vy = vy.abs(); }
                    if ny >= self.bounds.y_max - margin { ny = self.bounds.y_max - margin; vy = -vy.abs(); }
                    if nz <= self.bounds.z_min + margin { nz = self.bounds.z_min + margin; vz = vz.abs(); }
                    if nz >= self.bounds.z_max - margin { nz = self.bounds.z_max - margin; vz = -vz.abs(); }

                    if let Some(d) = self.drones.get_mut(&id) {
                        d.pos = (nx, ny, nz);
                        d.vel = (vx, vy, vz);
                    }
                }
            }
        }
    }

    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.drones.insert(id, DroneState {
            pos, vel,
            wander_seed: id as u32 ^ 0xDEAD_BEEF,
            radius: DRONE_RADIUS,
        });
        self.local_ids.insert(id);
    }

    fn on_entity_departed(&mut self, id: u64) {
        self.drones.remove(&id);
        self.local_ids.remove(&id);
    }

    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if let Some(d) = self.drones.get_mut(&id) {
            d.pos = pos;
            d.vel = vel;
        } else {
            self.drones.insert(id, DroneState {
                pos, vel,
                wander_seed: id as u32 ^ 0xDEAD_BEEF,
                radius: DRONE_RADIUS,
            });
        }
    }

    fn locally_simulated_ids(&self) -> &HashSet<u64> {
        &self.local_ids
    }

    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.drones.get(&id).map(|d| (d.pos, d.vel))
    }

    fn status_payload(&self) -> (u16, u8, u8) {
        (self.drones.len() as u16, 24, 3)
    }
}

pub fn make_config(cell: Cell, peers: Vec<std::net::SocketAddr>) -> ZeusConfig {
    ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: peers,
        boundary: cell.x_max,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: cell.x_min,
        cell: Some(cell),
    }
}

pub fn spawn_batch(
    node: &mut GameLoop<BoundedPhysicsWorld>,
    count: usize,
    center: (f32, f32, f32),
    base_id: u64,
    speed: f32,
) -> Vec<u64> {
    let mut ids = Vec::new();
    for i in 0..count {
        let offset = (i as f32) * 0.6;
        let x = (center.0 + (offset % 8.0) - 4.0).clamp(WORLD.x_min + 1.0, WORLD.x_max - 1.0);
        let y = (center.1 + ((i / 10) as f32) * 0.6 - 2.0).clamp(WORLD.y_min + 1.0, WORLD.y_max - 1.0);
        let z = (center.2 + (offset * 0.7).sin() * 3.0).clamp(WORLD.z_min + 1.0, WORLD.z_max - 1.0);
        let vx = ((i * 17 + 3) % 7) as f32 - 3.0;
        let vy = ((i * 13 + 5) % 5) as f32 - 2.0;
        let vz = ((i * 19 + 7) % 9) as f32 - 4.0;
        let mag = (vx * vx + vy * vy + vz * vz).sqrt().max(0.1);
        let scale = speed / mag;
        let id = base_id + i as u64;
        node.world.spawn_drone_at(id, (x, y, z), (vx * scale, vy * scale, vz * scale));
        node.engine.node.manager.add_entity(Entity {
            id,
            pos: (x, y, z),
            vel: (vx * scale, vy * scale, vz * scale),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        ids.push(id);
    }
    ids
}

pub fn spawn_stationary(
    node: &mut GameLoop<BoundedPhysicsWorld>,
    count: usize,
    center: (f32, f32, f32),
    base_id: u64,
) -> Vec<u64> {
    spawn_batch(node, count, center, base_id, 0.0)
}

pub fn total_local(nodes: &[GameLoop<BoundedPhysicsWorld>]) -> usize {
    nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::Local)
        .count()
}

pub fn assert_single_ownership(nodes: &[GameLoop<BoundedPhysicsWorld>], label: &str) {
    let mut seen: HashMap<u64, usize> = HashMap::new();
    for (ni, node) in nodes.iter().enumerate() {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                if let Some(prev) = seen.insert(*id, ni) {
                    panic!("[{}] Entity {} is Local on both node {} and node {}", label, id, prev, ni);
                }
            }
        }
    }
}

pub fn local_positions_for(node: &GameLoop<BoundedPhysicsWorld>) -> Vec<(f32, f32, f32)> {
    node.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local)
        .map(|e| e.pos)
        .collect()
}

pub fn local_count_for(node: &GameLoop<BoundedPhysicsWorld>) -> usize {
    node.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local)
        .count()
}

pub fn entity_state_summary(nodes: &[GameLoop<BoundedPhysicsWorld>]) -> String {
    let mut parts = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let mut local = 0; let mut ho = 0; let mut hi = 0; let mut rem = 0;
        for e in node.engine.node.manager.entities.values() {
            match e.state {
                AuthorityState::Local => local += 1,
                AuthorityState::HandoffOut => ho += 1,
                AuthorityState::HandoffIn => hi += 1,
                AuthorityState::Remote => rem += 1,
            }
        }
        parts.push(format!("N{}[L={} HO={} HI={} R={}]", i, local, ho, hi, rem));
    }
    parts.join(" ")
}

pub fn collect_all_local(
    nodes: &[GameLoop<BoundedPhysicsWorld>],
) -> HashMap<u64, ((f32, f32, f32), (f32, f32, f32))> {
    let mut all = HashMap::new();
    for node in nodes {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                all.insert(*id, (e.pos, e.vel));
            }
        }
    }
    all
}

pub fn vel_magnitude(v: (f32, f32, f32)) -> f32 {
    (v.0 * v.0 + v.1 * v.1 + v.2 * v.2).sqrt()
}

pub fn pos_distance(a: (f32, f32, f32), b: (f32, f32, f32)) -> f32 {
    let dx = a.0 - b.0;
    let dy = a.1 - b.1;
    let dz = a.2 - b.2;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

pub async fn tick_all(nodes: &mut [GameLoop<BoundedPhysicsWorld>], ticks: usize) {
    for _ in 0..ticks {
        for node in nodes.iter_mut() {
            node.tick(DT).await.unwrap();
        }
        sleep(Duration::from_millis(2)).await;
    }
}

pub async fn do_split(
    nodes: &mut Vec<GameLoop<BoundedPhysicsWorld>>,
    source_idx: usize,
) {
    let cell = nodes[source_idx].engine.node.manager.cell().clone();
    let positions = local_positions_for(&nodes[source_idx]);
    if positions.len() < 4 { return; }
    let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);

    let addrs: Vec<_> = nodes.iter()
        .map(|n| n.engine.endpoint.local_addr().unwrap())
        .collect();
    let new_node = GameLoop::new(
        make_config(new, addrs),
        BoundedPhysicsWorld::new(WORLD.clone()),
    ).await.unwrap();
    nodes.push(new_node);

    for _ in 0..80 {
        for n in nodes.iter_mut() { n.tick(DT).await.unwrap(); }
        sleep(Duration::from_millis(3)).await;
    }

    nodes[source_idx].set_cell(keep);
    nodes[source_idx].evict_out_of_cell_from_physics();

    tick_all(nodes, 400).await;
}

pub fn remove_entities(
    node: &mut GameLoop<BoundedPhysicsWorld>,
    count: usize,
) -> Vec<u64> {
    let ids: Vec<u64> = node.engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id)
        .take(count)
        .collect();
    for id in &ids {
        node.world.drones.remove(id);
        node.world.local_ids.remove(id);
        node.engine.node.manager.remove_entity(*id);
    }
    ids
}

pub fn new_world() -> BoundedPhysicsWorld {
    BoundedPhysicsWorld::new(WORLD.clone())
}

pub fn new_world_realistic() -> BoundedPhysicsWorld {
    BoundedPhysicsWorld::new_realistic(WORLD.clone())
}
