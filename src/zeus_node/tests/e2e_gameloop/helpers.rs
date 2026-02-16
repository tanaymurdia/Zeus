use std::collections::{HashMap, HashSet};
use zeus_node::cell::Cell;
use zeus_node::engine::{decode_compact_client, ZeusConfig};
use zeus_node::game_loop::{GameLoop, GameWorld};

pub fn parse_0xcc_datagram(data: &[u8]) -> Vec<(u64, (f32, f32, f32), (f32, f32, f32))> {
    decode_compact_client(data)
}

pub struct TestWorld {
    pub local_ids: HashSet<u64>,
    pub states: HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>,
    pub arrived: Vec<u64>,
    pub departed: Vec<u64>,
    pub step_count: u32,
}

impl TestWorld {
    pub fn new() -> Self {
        Self {
            local_ids: HashSet::new(),
            states: HashMap::new(),
            arrived: Vec::new(),
            departed: Vec::new(),
            step_count: 0,
        }
    }

    pub fn spawn_local(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.local_ids.insert(id);
        self.states.insert(id, (pos, vel));
    }
}

impl GameWorld for TestWorld {
    fn step(&mut self, _dt: f32) {
        self.step_count += 1;
        let ids: Vec<u64> = self.local_ids.iter().copied().collect();
        for id in ids {
            if let Some((pos, vel)) = self.states.get(&id).copied() {
                self.states.insert(
                    id,
                    (
                        (pos.0 + vel.0 * _dt, pos.1 + vel.1 * _dt, pos.2 + vel.2 * _dt),
                        vel,
                    ),
                );
            }
        }
    }

    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.arrived.push(id);
        if id < 1_000_000 {
            self.local_ids.insert(id);
        }
        self.states.insert(id, (pos, vel));
    }

    fn on_entity_departed(&mut self, id: u64) {
        self.departed.push(id);
        self.local_ids.remove(&id);
        self.states.remove(&id);
    }

    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.states.insert(id, (pos, vel));
    }

    fn locally_simulated_ids(&self) -> &HashSet<u64> {
        &self.local_ids
    }

    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.states.get(&id).copied()
    }
}

pub async fn make_node(cell: Cell, peers: Vec<std::net::SocketAddr>) -> GameLoop<TestWorld> {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: peers,
        boundary: cell.x_max,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: cell.x_min,
        cell: Some(cell),
    };
    GameLoop::new(config, TestWorld::new()).await.unwrap()
}

pub fn assert_single_ownership(nodes: &[GameLoop<TestWorld>], label: &str) {
    use zeus_node::entity_manager::AuthorityState;
    let mut ownership: HashMap<u64, Vec<usize>> = HashMap::new();
    for (idx, node) in nodes.iter().enumerate() {
        for (id, e) in &node.engine.node.manager.entities {
            if e.state == AuthorityState::Local {
                ownership.entry(*id).or_default().push(idx);
            }
        }
    }
    for (id, owners) in &ownership {
        assert!(
            owners.len() <= 1,
            "{}: Entity {} is Local on {} nodes: {:?}",
            label, id, owners.len(), owners
        );
    }
}

pub fn total_local_count(nodes: &[GameLoop<TestWorld>]) -> usize {
    use zeus_node::entity_manager::AuthorityState;
    nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::Local)
        .count()
}

#[allow(dead_code)]
pub fn total_handoff_out_count(nodes: &[GameLoop<TestWorld>]) -> usize {
    use zeus_node::entity_manager::AuthorityState;
    nodes.iter()
        .flat_map(|n| n.engine.node.manager.entities.values())
        .filter(|e| e.state == AuthorityState::HandoffOut)
        .count()
}
