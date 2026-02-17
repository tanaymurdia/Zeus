use super::helpers::*;
use rand::prelude::*;
use rand::rngs::SmallRng;
use std::collections::{HashMap, HashSet};
use zeus_node::autoscaler::AutoScaler;
use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::game_loop::{GameLoop, GameWorld};

#[derive(Debug, Clone)]
enum SimAction {
    Spawn { count: usize, near_boundary: bool },
    Despawn { count: usize },
    Impulse { dx: f32, dy: f32, dz: f32 },
    GravityWell { well_id: u64, mode: u8 },
    Split,
    DrainMerge,
    Idle { ticks: usize },
}

#[derive(Clone, Debug)]
struct EntityRecord {
    pos: (f32, f32, f32),
    vel: (f32, f32, f32),
    state: AuthorityState,
    node_idx: usize,
    wander_seed: Option<u32>,
    radius: f32,
    in_grace: bool,
}

#[derive(Clone, Debug)]
struct NodeRecord {
    cell: zeus_node::cell::Cell,
    local_count: usize,
    handoff_out_count: usize,
    handoff_in_count: usize,
    remote_count: usize,
    physics_ids_count: usize,
    status_entity_count: u16,
    status_map_width: u8,
    status_ball_radius: u8,
}

#[derive(Clone)]
struct FullSnapshot {
    entities: HashMap<u64, EntityRecord>,
    nodes: Vec<NodeRecord>,
    #[allow(dead_code)]
    tick: u64,
}

struct ChaosSimulator {
    rng: SmallRng,
    seed: u64,
    nodes: Vec<GameLoop<BoundedPhysicsWorld>>,
    next_entity_id: u64,
    expected_total: usize,
    violations: Vec<String>,
    tick_count: u64,
    action_log: Vec<(u64, SimAction)>,
    max_nodes: usize,
    active_wells: HashMap<u64, (f32, f32, f32, u8)>,
}

impl ChaosSimulator {
    async fn new(seed: u64) -> Self {
        let rng = SmallRng::seed_from_u64(seed);
        let full_cell = WORLD.clone();
        let node0 = GameLoop::new(make_config(full_cell, vec![]), new_world_realistic())
            .await
            .unwrap();
        Self {
            rng,
            seed,
            nodes: vec![node0],
            next_entity_id: 1,
            expected_total: 0,
            violations: Vec::new(),
            tick_count: 0,
            action_log: Vec::new(),
            max_nodes: 4,
            active_wells: HashMap::new(),
        }
    }

    fn full_snapshot(&self) -> FullSnapshot {
        let mut entities = HashMap::new();
        let mut nodes = Vec::new();
        for (ni, node) in self.nodes.iter().enumerate() {
            let mut local = 0; let mut ho = 0; let mut hi = 0; let mut rem = 0;
            for (id, e) in &node.engine.node.manager.entities {
                match e.state {
                    AuthorityState::Local => local += 1,
                    AuthorityState::HandoffOut => ho += 1,
                    AuthorityState::HandoffIn => hi += 1,
                    AuthorityState::Remote => rem += 1,
                }
                let wander_seed = node.world.drones.get(id).map(|d| d.wander_seed);
                let radius = node.world.drones.get(id).map_or(DRONE_RADIUS, |d| d.radius);
                let in_grace = node.engine.node.manager.has_grace(*id);
                entities.insert(*id, EntityRecord {
                    pos: e.pos,
                    vel: e.vel,
                    state: e.state.clone(),
                    node_idx: ni,
                    wander_seed,
                    radius,
                    in_grace,
                });
            }
            let (sc, sw, sr) = node.world.status_payload();
            nodes.push(NodeRecord {
                cell: node.engine.node.manager.cell().clone(),
                local_count: local,
                handoff_out_count: ho,
                handoff_in_count: hi,
                remote_count: rem,
                physics_ids_count: node.world.locally_simulated_ids().len(),
                status_entity_count: sc,
                status_map_width: sw,
                status_ball_radius: sr,
            });
        }
        FullSnapshot { entities, nodes, tick: self.tick_count }
    }

    fn gen_action(&mut self) -> SimAction {
        let node_count = self.nodes.len();
        let entity_count = self.expected_total;

        let weights: Vec<(SimAction, f32)> = vec![
            (SimAction::Spawn {
                count: self.rng.random_range(1..=15),
                near_boundary: self.rng.random_bool(0.3),
            }, 0.20),
            (SimAction::Despawn {
                count: if entity_count > 0 { self.rng.random_range(1..=entity_count.min(10)) } else { 0 },
            }, if entity_count > 5 { 0.12 } else { 0.02 }),
            (SimAction::Impulse {
                dx: self.rng.random_range(-12.0..=12.0),
                dy: self.rng.random_range(-12.0..=12.0),
                dz: self.rng.random_range(-8.0..=8.0),
            }, 0.20),
            (SimAction::GravityWell {
                well_id: self.rng.random_range(1..=4),
                mode: self.rng.random_range(0..=2),
            }, 0.08),
            (SimAction::Split,
                if entity_count >= 8 && node_count < self.max_nodes { 0.15 } else { 0.0 }),
            (SimAction::DrainMerge,
                if node_count > 1 { 0.10 } else { 0.0 }),
            (SimAction::Idle { ticks: self.rng.random_range(5..=30) }, 0.15),
        ];

        let total_weight: f32 = weights.iter().map(|(_, w)| w).sum();
        let mut r = self.rng.random_range(0.0..total_weight);
        for (action, weight) in &weights {
            r -= weight;
            if r <= 0.0 {
                return action.clone();
            }
        }
        SimAction::Idle { ticks: 10 }
    }

    async fn tick_all_once(&mut self) {
        for node in self.nodes.iter_mut() {
            node.tick(DT).await.unwrap();
        }
        self.tick_count += 1;
    }

    async fn tick_n(&mut self, n: usize) {
        for _ in 0..n {
            self.tick_all_once().await;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    fn check_single_ownership(&mut self) {
        let mut seen: HashMap<u64, usize> = HashMap::new();
        for (ni, node) in self.nodes.iter().enumerate() {
            for (id, e) in &node.engine.node.manager.entities {
                if e.state == AuthorityState::Local {
                    if let Some(prev) = seen.insert(*id, ni) {
                        self.violations.push(format!(
                            "t{}: DUAL_OWNERSHIP entity {} Local on N{} AND N{}",
                            self.tick_count, id, prev, ni
                        ));
                    }
                }
            }
        }
    }

    fn check_no_teleport(&mut self, before: &FullSnapshot, max_delta: f32) {
        for (id, rec) in &before.entities {
            if !matches!(rec.state, AuthorityState::Local) { continue; }
            for node in &self.nodes {
                if let Some(e) = node.engine.node.manager.get_entity(*id) {
                    if e.state == AuthorityState::Local {
                        let dist = pos_distance(rec.pos, e.pos);
                        if dist > max_delta {
                            self.violations.push(format!(
                                "t{}: TELEPORT entity {} moved {:.3} (max {:.1}) {:?}→{:?} vel_was={:?}",
                                self.tick_count, id, dist, max_delta, rec.pos, e.pos, rec.vel
                            ));
                        }
                        break;
                    }
                }
            }
        }
    }

    fn check_entity_conservation(&mut self) {
        let mut unique_ids: HashSet<u64> = HashSet::new();
        for node in &self.nodes {
            for (id, e) in &node.engine.node.manager.entities {
                if !matches!(e.state, AuthorityState::Remote) {
                    unique_ids.insert(*id);
                }
            }
        }
        let accounted = unique_ids.len();
        if accounted > self.expected_total + 2 || (self.expected_total > 2 && accounted + 5 < self.expected_total) {
            let local = total_local(&self.nodes);
            self.violations.push(format!(
                "t{}: CONSERVATION expected ~{}, unique_nonremote={}, local={} | {}",
                self.tick_count, self.expected_total, accounted, local,
                entity_state_summary(&self.nodes)
            ));
        }
    }

    fn check_speed_cap(&mut self) {
        let cap = MAX_DRONE_SPEED + 1.0;
        for (ni, node) in self.nodes.iter().enumerate() {
            for (id, e) in &node.engine.node.manager.entities {
                if e.state == AuthorityState::Local {
                    let speed = vel_magnitude(e.vel);
                    if speed > cap {
                        self.violations.push(format!(
                            "t{}: SPEED_CAP entity {} on N{} speed={:.2} (cap={:.1}) vel={:?}",
                            self.tick_count, id, ni, speed, cap, e.vel
                        ));
                    }
                }
            }
        }
    }

    fn check_position_in_world_bounds(&mut self) {
        let margin = 2.0;
        for (ni, node) in self.nodes.iter().enumerate() {
            for (id, e) in &node.engine.node.manager.entities {
                if e.state == AuthorityState::Local {
                    if !WORLD.contains_with_margin(e.pos, margin) {
                        self.violations.push(format!(
                            "t{}: OUT_OF_WORLD entity {} on N{} pos={:?} world=[{:.0},{:.0}]x[{:.0},{:.0}]x[{:.0},{:.0}]",
                            self.tick_count, id, ni, e.pos,
                            WORLD.x_min, WORLD.x_max, WORLD.y_min, WORLD.y_max, WORLD.z_min, WORLD.z_max
                        ));
                    }
                }
            }
        }
    }

    fn check_physics_manager_sync(&mut self) {
        for (ni, node) in self.nodes.iter().enumerate() {
            let mgr_local: HashSet<u64> = node.engine.node.manager.entities.iter()
                .filter(|(_, e)| e.state == AuthorityState::Local)
                .map(|(id, _)| *id).collect();
            let physics_ids = node.world.locally_simulated_ids();
            for id in &mgr_local {
                if !physics_ids.contains(id) {
                    self.violations.push(format!(
                        "t{}: DESYNC entity {} Local in manager but missing from physics on N{}",
                        self.tick_count, id, ni
                    ));
                }
            }
            for id in physics_ids {
                if !mgr_local.contains(id) && !node.engine.node.manager.entities.contains_key(id) {
                    self.violations.push(format!(
                        "t{}: DESYNC entity {} in physics but not in manager on N{}",
                        self.tick_count, id, ni
                    ));
                }
            }
        }
    }

    fn check_pos_vel_sync(&mut self) {
        for (ni, node) in self.nodes.iter().enumerate() {
            for (id, e) in &node.engine.node.manager.entities {
                if e.state != AuthorityState::Local { continue; }
                if let Some(drone) = node.world.drones.get(id) {
                    let pd = pos_distance(e.pos, drone.pos);
                    if pd > 0.5 {
                        self.violations.push(format!(
                            "t{}: POS_DRIFT entity {} on N{} manager={:?} physics={:?} delta={:.3}",
                            self.tick_count, id, ni, e.pos, drone.pos, pd
                        ));
                    }
                }
            }
        }
    }

    fn check_cell_coverage(&mut self) {
        if self.nodes.len() < 2 { return; }
        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                let ci = self.nodes[i].engine.node.manager.cell();
                let cj = self.nodes[j].engine.node.manager.cell();
                let x_overlap = ci.x_min < cj.x_max && ci.x_max > cj.x_min;
                let y_overlap = ci.y_min < cj.y_max && ci.y_max > cj.y_min;
                let z_overlap = ci.z_min < cj.z_max && ci.z_max > cj.z_min;
                if x_overlap && y_overlap && z_overlap {
                    let ox = (ci.x_max.min(cj.x_max) - ci.x_min.max(cj.x_min)).max(0.0);
                    let oy = (ci.y_max.min(cj.y_max) - ci.y_min.max(cj.y_min)).max(0.0);
                    let oz = (ci.z_max.min(cj.z_max) - ci.z_min.max(cj.z_min)).max(0.0);
                    let overlap_vol = ox * oy * oz;
                    if overlap_vol > 1.0 {
                        self.violations.push(format!(
                            "t{}: CELL_OVERLAP N{} and N{} overlap vol={:.1} cells={:?} {:?}",
                            self.tick_count, i, j, overlap_vol,
                            ci, cj
                        ));
                    }
                }
            }
        }
    }

    fn check_status_payload(&mut self) {
        for (ni, node) in self.nodes.iter().enumerate() {
            let (sc, sw, sr) = node.world.status_payload();
            let drone_count = node.world.drones.len() as u16;
            if sc != drone_count {
                self.violations.push(format!(
                    "t{}: STATUS N{} status_count={} but drones.len={}",
                    self.tick_count, ni, sc, drone_count
                ));
            }
            if sw != 24 || sr != 3 {
                self.violations.push(format!(
                    "t{}: STATUS N{} map_width={} ball_radius={} expected 24,3",
                    self.tick_count, ni, sw, sr
                ));
            }
        }
    }

    fn check_entity_state_validity(&mut self) {
        for (ni, node) in self.nodes.iter().enumerate() {
            for (id, e) in &node.engine.node.manager.entities {
                if e.pos.0.is_nan() || e.pos.1.is_nan() || e.pos.2.is_nan() {
                    self.violations.push(format!(
                        "t{}: NAN_POS entity {} on N{} pos={:?}",
                        self.tick_count, id, ni, e.pos
                    ));
                }
                if e.vel.0.is_nan() || e.vel.1.is_nan() || e.vel.2.is_nan() {
                    self.violations.push(format!(
                        "t{}: NAN_VEL entity {} on N{} vel={:?}",
                        self.tick_count, id, ni, e.vel
                    ));
                }
                if e.pos.0.is_infinite() || e.pos.1.is_infinite() || e.pos.2.is_infinite() {
                    self.violations.push(format!(
                        "t{}: INF_POS entity {} on N{} pos={:?}",
                        self.tick_count, id, ni, e.pos
                    ));
                }
                if e.vel.0.is_infinite() || e.vel.1.is_infinite() || e.vel.2.is_infinite() {
                    self.violations.push(format!(
                        "t{}: INF_VEL entity {} on N{} vel={:?}",
                        self.tick_count, id, ni, e.vel
                    ));
                }
            }
        }
    }

    fn check_drone_radius(&mut self) {
        for (ni, node) in self.nodes.iter().enumerate() {
            for (id, drone) in &node.world.drones {
                if (drone.radius - DRONE_RADIUS).abs() > f32::EPSILON {
                    self.violations.push(format!(
                        "t{}: RADIUS entity {} on N{} radius={:.3} expected={:.3}",
                        self.tick_count, id, ni, drone.radius, DRONE_RADIUS
                    ));
                }
            }
        }
    }

    fn run_all_invariants(&mut self, before: &FullSnapshot) {
        self.check_single_ownership();
        self.check_no_teleport(before, 2.0);
        self.check_entity_conservation();
        self.check_speed_cap();
        self.check_position_in_world_bounds();
        self.check_physics_manager_sync();
        self.check_pos_vel_sync();
        self.check_cell_coverage();
        self.check_status_payload();
        self.check_entity_state_validity();
        self.check_drone_radius();
    }

    fn run_structural_invariants(&mut self) {
        self.check_single_ownership();
        self.check_entity_conservation();
        self.check_speed_cap();
        self.check_position_in_world_bounds();
        self.check_physics_manager_sync();
        self.check_cell_coverage();
        self.check_status_payload();
        self.check_entity_state_validity();
        self.check_drone_radius();
    }

    async fn apply_spawn(&mut self, count: usize, near_boundary: bool) {
        let node_idx = self.rng.random_range(0..self.nodes.len());
        let cell = self.nodes[node_idx].engine.node.manager.cell().clone();

        for _ in 0..count {
            let id = self.next_entity_id;
            self.next_entity_id += 1;

            let (px, py, pz) = if near_boundary {
                let face = self.rng.random_range(0..6u8);
                match face {
                    0 => (cell.x_max - self.rng.random_range(0.3..=1.5),
                          self.rng.random_range(cell.y_min + 1.0..=cell.y_max - 1.0),
                          self.rng.random_range(cell.z_min + 1.0..=cell.z_max - 1.0)),
                    1 => (cell.x_min + self.rng.random_range(0.3..=1.5),
                          self.rng.random_range(cell.y_min + 1.0..=cell.y_max - 1.0),
                          self.rng.random_range(cell.z_min + 1.0..=cell.z_max - 1.0)),
                    2 => (self.rng.random_range(cell.x_min + 1.0..=cell.x_max - 1.0),
                          cell.y_max - self.rng.random_range(0.3..=1.5),
                          self.rng.random_range(cell.z_min + 1.0..=cell.z_max - 1.0)),
                    3 => (self.rng.random_range(cell.x_min + 1.0..=cell.x_max - 1.0),
                          cell.y_min + self.rng.random_range(0.3..=1.5),
                          self.rng.random_range(cell.z_min + 1.0..=cell.z_max - 1.0)),
                    4 => (self.rng.random_range(cell.x_min + 1.0..=cell.x_max - 1.0),
                          self.rng.random_range(cell.y_min + 1.0..=cell.y_max - 1.0),
                          cell.z_max - self.rng.random_range(0.3..=1.5)),
                    _ => (self.rng.random_range(cell.x_min + 1.0..=cell.x_max - 1.0),
                          self.rng.random_range(cell.y_min + 1.0..=cell.y_max - 1.0),
                          cell.z_min + self.rng.random_range(0.3..=1.5)),
                }
            } else {
                let cx = self.rng.random_range(cell.x_min + 1.0..=cell.x_max - 1.0);
                let cy = self.rng.random_range(cell.y_min + 1.0..=cell.y_max - 1.0);
                let cz = self.rng.random_range(cell.z_min + 1.0..=cell.z_max - 1.0);
                (cx, cy, cz)
            };

            let vx = self.rng.random_range(-MAX_DRONE_SPEED..=MAX_DRONE_SPEED);
            let vy = self.rng.random_range(-MAX_DRONE_SPEED..=MAX_DRONE_SPEED);
            let vz = self.rng.random_range(-MAX_DRONE_SPEED..=MAX_DRONE_SPEED);

            self.nodes[node_idx].world.spawn_drone_at(id, (px, py, pz), (vx, vy, vz));
            self.nodes[node_idx].engine.node.manager.add_entity(Entity {
                id,
                pos: (px, py, pz),
                vel: (vx, vy, vz),
                state: AuthorityState::Local,
                verifying_key: None,
            });
        }
        self.expected_total += count;
    }

    async fn apply_despawn(&mut self, count: usize) {
        if self.nodes.is_empty() || self.expected_total == 0 { return; }
        let node_idx = self.rng.random_range(0..self.nodes.len());
        let removed = remove_entities(&mut self.nodes[node_idx], count);
        self.expected_total = self.expected_total.saturating_sub(removed.len());
    }

    async fn apply_impulse(&mut self, dx: f32, dy: f32, dz: f32) {
        for node in &mut self.nodes {
            for (id, e) in node.engine.node.manager.entities.iter_mut() {
                if e.state == AuthorityState::Local {
                    e.vel.0 += dx * DT;
                    e.vel.1 += dy * DT;
                    e.vel.2 += dz * DT;
                    if let Some(drone) = node.world.drones.get_mut(id) {
                        drone.vel.0 += dx * DT;
                        drone.vel.1 += dy * DT;
                        drone.vel.2 += dz * DT;
                    }
                }
            }
        }
    }

    async fn apply_gravity_well(&mut self, well_id: u64, mode: u8) {
        let pos = (
            self.rng.random_range(WORLD.x_min + 2.0..=WORLD.x_max - 2.0),
            self.rng.random_range(WORLD.y_min + 2.0..=WORLD.y_max - 2.0),
            self.rng.random_range(WORLD.z_min + 2.0..=WORLD.z_max - 2.0),
        );
        for node in &mut self.nodes {
            node.world.set_gravity_well(well_id, pos, mode);
        }
        if mode == 0 {
            self.active_wells.remove(&well_id);
        } else {
            self.active_wells.insert(well_id, (pos.0, pos.1, pos.2, mode));
        }
    }

    async fn apply_split(&mut self) {
        if self.nodes.len() >= self.max_nodes { return; }
        let mut best_idx = None;
        let mut best_count = 0;
        for (i, node) in self.nodes.iter().enumerate() {
            let c = local_count_for(node);
            if c > best_count { best_count = c; best_idx = Some(i); }
        }
        let source_idx = match best_idx {
            Some(i) if best_count >= 4 => i,
            _ => return,
        };

        let cell = self.nodes[source_idx].engine.node.manager.cell().clone();
        let positions = local_positions_for(&self.nodes[source_idx]);
        let (keep, new_cell, _, _) = AutoScaler::compute_binary_split(&cell, &positions);

        let addrs: Vec<_> = self.nodes.iter()
            .map(|n| n.engine.endpoint.local_addr().unwrap()).collect();
        let mut new_node = GameLoop::new(
            make_config(new_cell, addrs),
            BoundedPhysicsWorld::new_realistic(WORLD.clone()),
        ).await.unwrap();
        for (&wid, &(x, y, z, mode)) in &self.active_wells {
            new_node.world.set_gravity_well(wid, (x, y, z), mode);
        }
        self.nodes.push(new_node);

        self.tick_n(60).await;

        self.nodes[source_idx].set_cell(keep);
        self.nodes[source_idx].evict_out_of_cell_from_physics();

        self.tick_n(300).await;
    }

    async fn apply_drain_merge(&mut self) {
        if self.nodes.len() <= 1 { return; }
        let drain_idx = self.rng.random_range(0..self.nodes.len());
        let absorber_idx = if drain_idx == 0 { 1 } else { 0 };

        self.tick_n(100).await;

        let drain_cell = self.nodes[drain_idx].engine.node.manager.cell().clone();
        let absorber_cell = self.nodes[absorber_idx].engine.node.manager.cell().clone();
        let mut expanded = absorber_cell.expand_toward(&drain_cell)
            .unwrap_or_else(|| absorber_cell.union(&drain_cell));
        for (i, node) in self.nodes.iter().enumerate() {
            if i == drain_idx || i == absorber_idx { continue; }
            expanded = expanded.clip_against(node.engine.node.manager.cell());
        }
        self.nodes[absorber_idx].set_cell(expanded);
        self.tick_n(10).await;

        let transfer_ids: Vec<(u64, (f32, f32, f32), (f32, f32, f32))> = self.nodes[drain_idx]
            .engine.node.manager.entities.iter()
            .filter(|(_, e)| !matches!(e.state, AuthorityState::Remote))
            .map(|(id, e)| (*id, e.pos, e.vel)).collect();

        for (id, pos, vel) in &transfer_ids {
            let existing = self.nodes[absorber_idx].engine.node.manager.get_entity(*id);
            let already_local = existing.map_or(false, |e| e.state == AuthorityState::Local);
            if already_local { continue; }
            self.nodes[absorber_idx].world.on_entity_arrived(*id, *pos, *vel);
            self.nodes[absorber_idx].engine.node.manager.add_entity(Entity {
                id: *id, pos: *pos, vel: *vel,
                state: AuthorityState::Local, verifying_key: None,
            });
            self.nodes[absorber_idx].engine.node.manager.mark_arrived(*id);
        }

        let all_drain_ids: Vec<u64> = self.nodes[drain_idx]
            .engine.node.manager.entities.keys().copied().collect();
        for id in &all_drain_ids {
            self.nodes[drain_idx].world.on_entity_departed(*id);
            self.nodes[drain_idx].engine.node.manager.remove_entity(*id);
        }

        self.nodes.remove(drain_idx);
        self.tick_n(50).await;
    }

    async fn run_session(&mut self, num_actions: usize) {
        self.tick_n(5).await;

        for _ in 0..num_actions {
            let action = self.gen_action();
            self.action_log.push((self.tick_count, action.clone()));

            match action {
                SimAction::Spawn { count, near_boundary } => {
                    self.apply_spawn(count, near_boundary).await;
                    self.tick_n(3).await;
                }
                SimAction::Despawn { count } => {
                    self.apply_despawn(count).await;
                    self.tick_n(3).await;
                }
                SimAction::Impulse { dx, dy, dz } => {
                    self.apply_impulse(dx, dy, dz).await;
                    self.tick_n(2).await;
                }
                SimAction::GravityWell { well_id, mode } => {
                    self.apply_gravity_well(well_id, mode).await;
                    self.tick_n(5).await;
                }
                SimAction::Split => {
                    self.apply_split().await;
                }
                SimAction::DrainMerge => {
                    self.apply_drain_merge().await;
                }
                SimAction::Idle { ticks } => {
                    self.tick_n(ticks).await;
                }
            }

            self.run_structural_invariants();

            for _ in 0..5 {
                let s = self.full_snapshot();
                self.tick_all_once().await;
                self.run_all_invariants(&s);
            }
        }
    }

    fn format_failure_report(&self) -> String {
        let mut report = format!("=== SEED {} | {} violations ===\n", self.seed, self.violations.len());
        report.push_str("Actions:\n");
        for (tick, action) in &self.action_log {
            report.push_str(&format!("  t{}: {:?}\n", tick, action));
        }
        report.push_str(&format!("Active wells: {:?}\n", self.active_wells));
        report.push_str(&format!("Nodes: {} | Expected entities: {}\n", self.nodes.len(), self.expected_total));
        let snap = self.full_snapshot();
        for (ni, nr) in snap.nodes.iter().enumerate() {
            report.push_str(&format!(
                "  N{}: cell=[{:.1},{:.1}]x[{:.1},{:.1}]x[{:.1},{:.1}] L={} HO={} HI={} R={} phys={} status=({},{},{})\n",
                ni, nr.cell.x_min, nr.cell.x_max, nr.cell.y_min, nr.cell.y_max,
                nr.cell.z_min, nr.cell.z_max,
                nr.local_count, nr.handoff_out_count, nr.handoff_in_count, nr.remote_count,
                nr.physics_ids_count,
                nr.status_entity_count, nr.status_map_width, nr.status_ball_radius
            ));
        }
        let mut shown = 0;
        for (id, er) in &snap.entities {
            if shown >= 20 { report.push_str("  ... (truncated)\n"); break; }
            report.push_str(&format!(
                "  E{}: N{} {:?} pos=({:.2},{:.2},{:.2}) vel=({:.2},{:.2},{:.2}) spd={:.2} wseed={:?} r={:.2} grace={}\n",
                id, er.node_idx, er.state,
                er.pos.0, er.pos.1, er.pos.2,
                er.vel.0, er.vel.1, er.vel.2,
                vel_magnitude(er.vel),
                er.wander_seed, er.radius, er.in_grace
            ));
            shown += 1;
        }
        report.push_str("Violations:\n");
        for v in &self.violations {
            report.push_str(&format!("  - {}\n", v));
        }
        report
    }
}

#[tokio::test]
async fn test_chaos_100_seeds_short_session() {
    let mut failures: Vec<String> = Vec::new();
    for seed in 0..100u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.run_session(20).await;
        if !sim.violations.is_empty() {
            failures.push(sim.format_failure_report());
        }
    }
    if !failures.is_empty() {
        panic!("{} of 100 seeds failed:\n{}", failures.len(), failures.join("\n"));
    }
}

#[tokio::test]
async fn test_chaos_10_seeds_long_session() {
    let mut failures: Vec<String> = Vec::new();
    for seed in 1000..1010u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.run_session(50).await;
        if !sim.violations.is_empty() {
            failures.push(sim.format_failure_report());
        }
    }
    if !failures.is_empty() {
        panic!("{} of 10 seeds failed:\n{}", failures.len(), failures.join("\n"));
    }
}

#[tokio::test]
async fn test_chaos_rapid_split_merge_cycles() {
    for seed in 2000..2020u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.apply_spawn(20, false).await;
        sim.tick_n(10).await;

        for cycle in 0..5 {
            sim.apply_split().await;
            sim.run_structural_invariants();

            sim.apply_drain_merge().await;
            sim.run_structural_invariants();

            assert!(
                sim.violations.is_empty(),
                "seed {} cycle {}:\n{}", seed, cycle, sim.format_failure_report()
            );
        }
    }
}

#[tokio::test]
async fn test_chaos_spawn_despawn_storm() {
    for seed in 3000..3050u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        for _ in 0..30 {
            let count = sim.rng.random_range(1..=10);
            let near_b = sim.rng.random_bool(0.3);
            sim.apply_spawn(count, near_b).await;
            sim.tick_n(2).await;
            if sim.expected_total > 5 {
                let rm = sim.rng.random_range(1..=sim.expected_total.min(8));
                sim.apply_despawn(rm).await;
                sim.tick_n(2).await;
            }
            sim.run_structural_invariants();
        }
        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test]
async fn test_chaos_movement_boundary_stress() {
    for seed in 4000..4030u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.apply_spawn(15, true).await;
        sim.tick_n(5).await;
        if sim.expected_total >= 8 {
            sim.apply_split().await;
        }
        for _ in 0..40 {
            let s = sim.full_snapshot();
            let dx = sim.rng.random_range(-15.0..=15.0);
            let dy = sim.rng.random_range(-15.0..=15.0);
            let dz = sim.rng.random_range(-10.0..=10.0);
            sim.apply_impulse(dx, dy, dz).await;
            sim.tick_n(5).await;
            sim.run_all_invariants(&s);
        }
        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test]
async fn test_chaos_entity_conservation_through_all_operations() {
    for seed in 5000..5020u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.apply_spawn(25, false).await;
        sim.tick_n(10).await;
        sim.apply_split().await;
        sim.run_structural_invariants();
        sim.apply_spawn(10, true).await;
        sim.tick_n(30).await;
        sim.run_structural_invariants();
        sim.apply_despawn(5).await;
        sim.tick_n(20).await;
        sim.run_structural_invariants();
        if sim.nodes.len() > 1 {
            sim.apply_drain_merge().await;
            sim.run_structural_invariants();
        }
        sim.tick_n(50).await;
        sim.run_structural_invariants();
        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test]
async fn test_chaos_position_continuity_per_tick() {
    for seed in 6000..6020u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.apply_spawn(12, false).await;
        sim.tick_n(5).await;
        sim.apply_split().await;
        for _ in 0..200 {
            let s = sim.full_snapshot();
            sim.tick_all_once().await;
            sim.check_no_teleport(&s, 1.5);
            sim.check_entity_state_validity();
            sim.check_speed_cap();
        }
        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test]
async fn test_chaos_gravity_wells_with_handoff() {
    for seed in 7000..7020u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.apply_spawn(15, false).await;
        sim.tick_n(5).await;
        sim.apply_split().await;

        sim.apply_gravity_well(1, 1).await;
        sim.tick_n(30).await;
        sim.run_structural_invariants();

        sim.apply_gravity_well(2, 2).await;
        sim.tick_n(30).await;
        sim.run_structural_invariants();

        sim.apply_gravity_well(1, 0).await;
        sim.apply_gravity_well(2, 0).await;
        sim.tick_n(20).await;
        sim.run_structural_invariants();

        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test]
async fn test_chaos_boundary_spawn_split_stress() {
    for seed in 8000..8030u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.apply_spawn(20, true).await;
        sim.tick_n(5).await;
        sim.apply_split().await;
        sim.run_structural_invariants();
        sim.apply_spawn(10, true).await;
        sim.tick_n(10).await;
        if sim.nodes.len() < sim.max_nodes && sim.expected_total >= 8 {
            sim.apply_split().await;
        }
        sim.run_structural_invariants();

        for _ in 0..100 {
            let s = sim.full_snapshot();
            sim.tick_all_once().await;
            sim.check_no_teleport(&s, 1.5);
            sim.check_speed_cap();
            sim.check_position_in_world_bounds();
        }
        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test]
async fn test_chaos_full_lifecycle_with_wells_and_boundary() {
    for seed in 9000..9010u64 {
        let mut sim = ChaosSimulator::new(seed).await;
        sim.run_session(40).await;
        assert!(sim.violations.is_empty(), "seed {}:\n{}", seed, sim.format_failure_report());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore]
async fn test_chaos_1000_seeds_sdk_stress() {
    let total_seeds = 1000u64;
    let actions_per_seed = 30;
    let parallelism = 10usize;

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(parallelism));
    let completed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let failed_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let start = std::time::Instant::now();

    let mut handles = Vec::new();

    for seed in 0..total_seeds {
        let sem = semaphore.clone();
        let done = completed.clone();
        let fails = failed_count.clone();

        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let seed_start = std::time::Instant::now();
            let mut sim = ChaosSimulator::new(seed).await;
            sim.run_session(actions_per_seed).await;
            let seed_elapsed = seed_start.elapsed().as_secs_f64();

            let report = if sim.violations.is_empty() {
                None
            } else {
                fails.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(sim.format_failure_report())
            };

            let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n % 50 == 0 || n == total_seeds {
                let f = fails.load(std::sync::atomic::Ordering::Relaxed);
                eprintln!(
                    "[{:>4}/{}] passed={} failed={} | {:.1}s elapsed | seed {} took {:.1}s",
                    n, total_seeds, n - f, f,
                    seed_start.elapsed().as_secs_f64() + (n as f64 * 0.001),
                    seed, seed_elapsed
                );
            }

            (seed, report)
        });

        handles.push(handle);
    }

    let mut failures: Vec<(u64, String)> = Vec::new();
    for handle in handles {
        let (seed, report) = handle.await.unwrap();
        if let Some(r) = report {
            failures.push((seed, r));
        }
    }
    failures.sort_by_key(|(s, _)| *s);

    let elapsed = start.elapsed().as_secs_f64();
    let passed = total_seeds - failures.len() as u64;
    eprintln!("\n=== CHAOS 1000 COMPLETE ({} parallel) ===", parallelism);
    eprintln!("  Total:   {}", total_seeds);
    eprintln!("  Passed:  {}", passed);
    eprintln!("  Failed:  {}", failures.len());
    eprintln!("  Time:    {:.1}s ({:.1} seeds/s)", elapsed, total_seeds as f64 / elapsed);
    eprintln!("  Speedup: ~{:.1}x vs sequential", (total_seeds as f64 * 2.5) / elapsed);

    if !failures.is_empty() {
        let mut msg = format!("{} of {} seeds failed:\n\n", failures.len(), total_seeds);
        for (seed, report) in &failures {
            msg.push_str(&format!("--- SEED {} ---\n{}\n", seed, report));
        }
        panic!("{}", msg);
    }
}
