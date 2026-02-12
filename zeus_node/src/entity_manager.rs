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
    pub verifying_key: Option<VerifyingKey>, // None if unknown (should enforce known)
}

pub struct EntityManager {
    pub entities: HashMap<u64, Entity>,
    boundary: f32,
    margin: f32,
}

impl EntityManager {
    pub fn new(boundary: f32, margin: f32) -> Self {
        Self {
            entities: HashMap::new(),
            boundary,
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

    // Allow iteration
    pub fn iter_mut(&mut self) -> std::collections::hash_map::IterMut<'_, u64, Entity> {
        self.entities.iter_mut()
    }

    /// Returns the number of entities being managed
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Run simulation step for all Local entities
    pub fn update(&mut self, dt: f32) -> Vec<u64> {
        let mut handoff_candidates = Vec::new();

        for entity in self.entities.values_mut() {
            if entity.state == AuthorityState::Local {
                entity.pos.0 += entity.vel.0 * dt;
                entity.pos.1 += entity.vel.1 * dt;
                entity.pos.2 += entity.vel.2 * dt;

                // Hysteresis Check (Assuming we are Left Node, handing off to Right Node)
                // If pos.x > boundary + margin, we offer it away.
                // NOTE: This logic assumes we are the "Lower" node.
                // A "Higher" node would check pos.x < boundary - margin.
                if entity.pos.0 > self.boundary + self.margin {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hysteresis_boundary() {
        let mut mgr = EntityManager::new(0.0, 5.0);

        // Entity at 4.0 (Inside Margin) - Should be owned
        mgr.add_entity(Entity {
            id: 1,
            pos: (4.0, 0.0, 0.0),
            vel: (1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        // Update 1s -> Pos 5.0. Still <= boundary + margin (5.0). No handoff.
        let candidates = mgr.update(1.0);
        assert!(candidates.is_empty());
        assert_eq!(mgr.get_entity(1).unwrap().pos.0, 5.0);

        // Update 0.1s -> Pos 5.1. Now > 5.0. Handoff trigger.
        let candidates = mgr.update(0.1);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], 1);
    }
}
