use std::collections::{HashMap, HashSet};

use zeus_node::game_loop::GameWorld;

pub struct TestDroneWorld {
    pub drones: HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>,
    pub local_ids: HashSet<u64>,
    pub next_id: u64,
}

impl TestDroneWorld {
    pub fn new() -> Self {
        Self {
            drones: HashMap::new(),
            local_ids: HashSet::new(),
            next_id: 1,
        }
    }

    pub fn spawn_drone(&mut self, pos: (f32, f32, f32), vel: (f32, f32, f32)) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.drones.insert(id, (pos, vel));
        self.local_ids.insert(id);
        id
    }
}

impl GameWorld for TestDroneWorld {
    fn step(&mut self, _dt: f32) {}

    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.drones.insert(id, (pos, vel));
        self.local_ids.insert(id);
    }

    fn on_entity_departed(&mut self, id: u64) {
        self.drones.remove(&id);
        self.local_ids.remove(&id);
    }

    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        self.drones.insert(id, (pos, vel));
    }

    fn locally_simulated_ids(&self) -> &HashSet<u64> {
        &self.local_ids
    }

    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
        self.drones.get(&id).copied()
    }

    fn status_payload(&self) -> (u16, u8, u8) {
        (self.drones.len() as u16, 24, 3)
    }
}
