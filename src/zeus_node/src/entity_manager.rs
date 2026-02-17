use std::collections::HashMap;
use crate::cell::{Cell, Face};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityState {
    Local,
    HandoffOut,
    Remote,
    HandoffIn,
}

use ed25519_dalek::VerifyingKey;

#[derive(Debug, Clone)]
pub struct Entity {
    pub id: u64,
    pub pos: (f32, f32, f32),
    pub vel: (f32, f32, f32),
    pub state: AuthorityState,
    pub verifying_key: Option<VerifyingKey>,
}

pub struct EntityManager {
    pub entities: HashMap<u64, Entity>,
    cell: Cell,
    margin: f32,
    arrival_grace: HashMap<u64, u8>,
}

impl EntityManager {
    pub fn new(boundary: f32, margin: f32, lower_boundary: f32) -> Self {
        Self {
            entities: HashMap::new(),
            cell: Cell::from_1d(lower_boundary, boundary),
            margin,
            arrival_grace: HashMap::new(),
        }
    }

    pub fn new_3d(cell: Cell, margin: f32) -> Self {
        Self {
            entities: HashMap::new(),
            cell,
            margin,
            arrival_grace: HashMap::new(),
        }
    }

    pub fn add_entity(&mut self, entity: Entity) {
        self.entities.insert(entity.id, entity);
    }

    pub fn get_entity(&self, id: u64) -> Option<&Entity> {
        self.entities.get(&id)
    }

    pub fn get_entity_mut(&mut self, id: u64) -> Option<&mut Entity> {
        self.entities.get_mut(&id)
    }

    pub fn iter_mut(&mut self) -> std::collections::hash_map::IterMut<'_, u64, Entity> {
        self.entities.iter_mut()
    }

    pub fn remove_entity(&mut self, id: u64) -> Option<Entity> {
        self.arrival_grace.remove(&id);
        self.entities.remove(&id)
    }

    pub fn mark_arrived(&mut self, id: u64) {
        self.arrival_grace.insert(id, 32);
    }

    pub fn tick_grace(&mut self) {
        self.arrival_grace.retain(|_, g| {
            *g = g.saturating_sub(1);
            *g > 0
        });
    }

    pub fn has_grace(&self, id: u64) -> bool {
        self.arrival_grace.get(&id).copied().unwrap_or(0) > 0
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    pub fn update(&mut self, _dt: f32) -> Vec<(u64, Face)> {
        let mut handoff_candidates = Vec::new();

        for entity in self.entities.values() {
            if entity.state == AuthorityState::Local && !self.has_grace(entity.id) {
                if let Some(face) = self.cell.exit_face(entity.pos, self.margin) {
                    if velocity_exits_face(entity.vel, face) {
                        handoff_candidates.push((entity.id, face));
                    }
                }
            }
        }

        handoff_candidates
    }

    pub fn set_state(&mut self, id: u64, new_state: AuthorityState) {
        if let Some(e) = self.entities.get_mut(&id) {
            e.state = new_state;
        }
    }

    pub fn set_boundary(&mut self, boundary: f32) {
        self.cell.x_max = boundary;
    }

    pub fn set_lower_boundary(&mut self, lower_boundary: f32) {
        self.cell.x_min = lower_boundary;
    }

    pub fn boundary(&self) -> f32 {
        self.cell.x_max
    }

    pub fn lower_boundary(&self) -> f32 {
        self.cell.x_min
    }

    pub fn cell(&self) -> &Cell {
        &self.cell
    }

    pub fn set_cell(&mut self, cell: Cell) {
        self.cell = cell;
    }

    pub fn margin(&self) -> f32 {
        self.margin
    }

    pub fn force_exit_check(&self) -> Vec<(u64, Face)> {
        let hysteresis = 0.3_f32;
        let mut exits = Vec::new();
        for entity in self.entities.values() {
            if entity.state == AuthorityState::Local && !self.has_grace(entity.id) {
                if let Some(face) = self.cell.exit_face(entity.pos, hysteresis) {
                    if velocity_exits_face(entity.vel, face) {
                        exits.push((entity.id, face));
                    }
                } else if !self.cell.contains_with_margin(entity.pos, hysteresis) {
                    let dx_pos = entity.pos.0 - self.cell.x_max;
                    let dx_neg = self.cell.x_min - entity.pos.0;
                    let dy_pos = entity.pos.1 - self.cell.y_max;
                    let dy_neg = self.cell.y_min - entity.pos.1;
                    let dz_pos = entity.pos.2 - self.cell.z_max;
                    let dz_neg = self.cell.z_min - entity.pos.2;
                    let max = dx_pos.max(dx_neg).max(dy_pos).max(dy_neg).max(dz_pos).max(dz_neg);
                    let face = if (max - dx_pos).abs() < 1e-6 { Face::XPos }
                        else if (max - dx_neg).abs() < 1e-6 { Face::XNeg }
                        else if (max - dy_pos).abs() < 1e-6 { Face::YPos }
                        else if (max - dy_neg).abs() < 1e-6 { Face::YNeg }
                        else if (max - dz_pos).abs() < 1e-6 { Face::ZPos }
                        else { Face::ZNeg };
                    if velocity_exits_face(entity.vel, face) {
                        exits.push((entity.id, face));
                    }
                }
            }
        }
        exits
    }
}

pub fn velocity_exits_face(vel: (f32, f32, f32), face: Face) -> bool {
    match face {
        Face::XPos => vel.0 >= 0.0,
        Face::XNeg => vel.0 <= 0.0,
        Face::YPos => vel.1 >= 0.0,
        Face::YNeg => vel.1 <= 0.0,
        Face::ZPos => vel.2 >= 0.0,
        Face::ZNeg => vel.2 <= 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hysteresis_boundary() {
        let mut mgr = EntityManager::new(0.0, 5.0, 0.0);

        mgr.add_entity(Entity {
            id: 1,
            pos: (4.0, 0.0, 0.0),
            vel: (1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        let candidates = mgr.update(1.0);
        assert!(candidates.is_empty(), "Entity inside boundary+margin should not be a candidate");

        mgr.get_entity_mut(1).unwrap().pos.0 = 5.5;
        let candidates = mgr.update(0.1);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, 1);
        assert_eq!(candidates[0].1, Face::XPos);
    }

    #[test]
    fn test_bidirectional_handoff_right() {
        let mut mgr = EntityManager::new(10.0, 5.0, 0.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (16.0, 0.0, 0.0),
            vel: (2.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, 1);
        assert_eq!(candidates[0].1, Face::XPos);
    }

    #[test]
    fn test_bidirectional_handoff_left() {
        let mut mgr = EntityManager::new(20.0, 5.0, 8.0);
        mgr.add_entity(Entity {
            id: 2,
            pos: (2.0, 0.0, 0.0),
            vel: (-1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1, "Entity below lower_boundary-margin should be a handoff candidate");
        assert_eq!(candidates[0].0, 2);
        assert_eq!(candidates[0].1, Face::XNeg);
    }

    #[test]
    fn test_no_handoff_in_zone() {
        let mut mgr = EntityManager::new(16.0, 2.0, 8.0);
        mgr.add_entity(Entity {
            id: 3,
            pos: (12.0, 0.0, 0.0),
            vel: (0.5, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert!(candidates.is_empty(), "Entity within zone should not be a handoff candidate");
    }

    #[test]
    fn test_getters() {
        let mgr = EntityManager::new(10.0, 5.0, 3.0);
        assert_eq!(mgr.boundary(), 10.0);
        assert_eq!(mgr.lower_boundary(), 3.0);
    }

    #[test]
    fn test_set_lower_boundary() {
        let mut mgr = EntityManager::new(10.0, 5.0, 0.0);
        assert_eq!(mgr.lower_boundary(), 0.0);
        mgr.set_lower_boundary(5.0);
        assert_eq!(mgr.lower_boundary(), 5.0);
    }

    #[test]
    fn test_handoff_y_positive() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, 11.5, 5.0),
            vel: (0.0, 2.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1, Face::YPos);
    }

    #[test]
    fn test_handoff_y_negative() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, -1.5, 5.0),
            vel: (0.0, -2.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1, Face::YNeg);
    }

    #[test]
    fn test_handoff_z_positive() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, 5.0, 11.5),
            vel: (0.0, 0.0, 2.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1, Face::ZPos);
    }

    #[test]
    fn test_handoff_z_negative() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, 5.0, -1.5),
            vel: (0.0, 0.0, -2.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1, Face::ZNeg);
    }

    #[test]
    fn test_no_handoff_3d_in_cell() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, 5.0, 5.0),
            vel: (0.5, 0.5, 0.5),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_handoff_diagonal_exit_picks_first_face() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (12.0, 12.0, 5.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(0.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1, Face::XPos);
    }

    #[test]
    fn test_3d_cell_getters() {
        let cell = Cell::new(1.0, 10.0, 2.0, 20.0, 3.0, 30.0);
        let mgr = EntityManager::new_3d(cell.clone(), 2.0);
        assert_eq!(mgr.cell(), &cell);
        assert_eq!(mgr.margin(), 2.0);
    }

    #[test]
    fn test_arrival_grace_prevents_exit() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (12.0, 5.0, 5.0),
            vel: (2.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        mgr.mark_arrived(1);
        let candidates = mgr.update(1.0);
        assert!(candidates.is_empty(), "Entity with arrival grace should not be an exit candidate");
        let forced = mgr.force_exit_check();
        assert!(forced.is_empty(), "Entity with arrival grace should not be a force_exit candidate");
    }

    #[test]
    fn test_arrival_grace_expires() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (12.0, 5.0, 5.0),
            vel: (2.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        mgr.mark_arrived(1);
        for _ in 0..32 {
            mgr.tick_grace();
        }
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1, "Grace expired, entity should be a candidate");
    }

    #[test]
    fn test_velocity_check_prevents_exit_when_returning() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (12.0, 5.0, 5.0),
            vel: (-3.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert!(candidates.is_empty(), "Entity moving back into cell should not trigger exit");
    }

    #[test]
    fn test_velocity_check_allows_exit_when_leaving() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (12.0, 5.0, 5.0),
            vel: (3.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1, "Entity moving away from cell should trigger exit");
        assert_eq!(candidates[0].1, Face::XPos);
    }

    #[test]
    fn test_velocity_check_force_exit_z_negative() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, 5.0, -0.5),
            vel: (0.0, 0.0, 1.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let forced = mgr.force_exit_check();
        assert!(forced.is_empty(), "Entity at z=-0.5 but moving +z (back into cell) should not force exit");

        mgr.get_entity_mut(1).unwrap().vel = (0.0, 0.0, -2.0);
        let forced = mgr.force_exit_check();
        assert_eq!(forced.len(), 1, "Entity at z=-0.5 moving -z (away from cell) should force exit");
        assert_eq!(forced[0].1, Face::ZNeg);
    }

    #[test]
    fn test_velocity_exits_face_function() {
        assert!(velocity_exits_face((1.0, 0.0, 0.0), Face::XPos));
        assert!(!velocity_exits_face((-1.0, 0.0, 0.0), Face::XPos));
        assert!(velocity_exits_face((-1.0, 0.0, 0.0), Face::XNeg));
        assert!(!velocity_exits_face((1.0, 0.0, 0.0), Face::XNeg));
        assert!(velocity_exits_face((0.0, 1.0, 0.0), Face::YPos));
        assert!(!velocity_exits_face((0.0, -1.0, 0.0), Face::YPos));
        assert!(velocity_exits_face((0.0, 0.0, -2.0), Face::ZNeg));
        assert!(!velocity_exits_face((0.0, 0.0, 2.0), Face::ZNeg));
        assert!(velocity_exits_face((0.0, 0.0, 0.0), Face::XPos));
        assert!(velocity_exits_face((0.0, 0.0, 0.0), Face::XNeg));
    }

    #[test]
    fn test_remove_entity_clears_grace() {
        let mut mgr = EntityManager::new_3d(Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0), 1.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (5.0, 5.0, 5.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        mgr.mark_arrived(1);
        assert!(mgr.has_grace(1));
        mgr.remove_entity(1);
        assert!(!mgr.has_grace(1));
    }

    #[test]
    fn test_ping_pong_scenario_prevented() {
        let _cell_a = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let cell_b = Cell::new(10.0, 20.0, 0.0, 10.0, 0.0, 10.0);
        let mut mgr_b = EntityManager::new_3d(cell_b, 1.0);

        mgr_b.add_entity(Entity {
            id: 42,
            pos: (10.2, 5.0, 5.0),
            vel: (-2.0, 1.0, 0.5),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        mgr_b.mark_arrived(42);

        let exits_b = mgr_b.update(1.0);
        assert!(exits_b.is_empty(), "Entity just arrived should not exit (grace period)");
        let force_b = mgr_b.force_exit_check();
        assert!(force_b.is_empty(), "Entity just arrived should not force exit (grace period)");

        for _ in 0..32 {
            mgr_b.tick_grace();
        }

        let exits_b = mgr_b.update(1.0);
        assert!(exits_b.is_empty(), "Entity at x=10.2 with vel.x=-2.0 (moving into cell) should not exit");
        let force_b = mgr_b.force_exit_check();
        assert!(force_b.is_empty(), "Entity at x=10.2 with vel.x=-2.0 should not force exit");

        mgr_b.get_entity_mut(42).unwrap().vel = (2.0, 1.0, 0.5);
        let force_b = mgr_b.force_exit_check();
        assert!(force_b.is_empty(), "Entity at x=10.2 is within cell [10,20] so no exit");
    }
}
