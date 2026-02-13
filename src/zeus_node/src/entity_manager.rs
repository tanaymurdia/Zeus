use std::collections::HashMap;

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
    boundary: f32,
    lower_boundary: f32,
    margin: f32,
}

impl EntityManager {
    pub fn new(boundary: f32, margin: f32, lower_boundary: f32) -> Self {
        Self {
            entities: HashMap::new(),
            boundary,
            lower_boundary,
            margin,
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

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    pub fn update(&mut self, dt: f32) -> Vec<u64> {
        let mut handoff_candidates = Vec::new();

        for entity in self.entities.values_mut() {
            if entity.state == AuthorityState::Local {
                entity.pos.0 += entity.vel.0 * dt;
                entity.pos.1 += entity.vel.1 * dt;
                entity.pos.2 += entity.vel.2 * dt;

                if entity.pos.0 > self.boundary + self.margin {
                    handoff_candidates.push(entity.id);
                } else if entity.pos.0 < self.lower_boundary - self.margin {
                    handoff_candidates.push(entity.id);
                }
            } else if entity.state == AuthorityState::HandoffOut {
                entity.pos.0 += entity.vel.0 * dt;
                entity.pos.1 += entity.vel.1 * dt;
                entity.pos.2 += entity.vel.2 * dt;
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
        self.boundary = boundary;
    }

    pub fn set_lower_boundary(&mut self, lower_boundary: f32) {
        self.lower_boundary = lower_boundary;
    }

    pub fn boundary(&self) -> f32 {
        self.boundary
    }

    pub fn lower_boundary(&self) -> f32 {
        self.lower_boundary
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
        assert!(candidates.is_empty());
        assert_eq!(mgr.get_entity(1).unwrap().pos.0, 5.0);

        let candidates = mgr.update(0.1);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], 1);
    }

    #[test]
    fn test_bidirectional_handoff_right() {
        let mut mgr = EntityManager::new(10.0, 5.0, 0.0);
        mgr.add_entity(Entity {
            id: 1,
            pos: (14.0, 0.0, 0.0),
            vel: (2.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], 1);
    }

    #[test]
    fn test_bidirectional_handoff_left() {
        let mut mgr = EntityManager::new(20.0, 5.0, 8.0);
        mgr.add_entity(Entity {
            id: 2,
            pos: (3.0, 0.0, 0.0),
            vel: (-1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let candidates = mgr.update(1.0);
        assert_eq!(candidates.len(), 1, "Entity below lower_boundary-margin should be a handoff candidate");
        assert_eq!(candidates[0], 2);
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
}
