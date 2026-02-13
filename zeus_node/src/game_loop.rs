use std::collections::HashSet;

use crate::engine::{ZeusConfig, ZeusEngine, ZeusEvent};
use crate::entity_manager::AuthorityState;
use zeus_common::HandoffType;

pub trait GameWorld: Send {
    fn step(&mut self, dt: f32);
    fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32));
    fn on_entity_departed(&mut self, id: u64);
    fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32));
    fn locally_simulated_ids(&self) -> &HashSet<u64>;
    fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))>;
}

pub struct GameLoop<W: GameWorld> {
    pub engine: ZeusEngine,
    pub world: W,
}

impl<W: GameWorld> GameLoop<W> {
    pub async fn new(config: ZeusConfig, world: W) -> Result<Self, Box<dyn std::error::Error>> {
        let engine = ZeusEngine::new(config).await?;
        Ok(Self { engine, world })
    }

    pub async fn tick(&mut self, dt: f32) -> Result<Vec<ZeusEvent>, Box<dyn std::error::Error>> {
        let local_sim = self.world.locally_simulated_ids().clone();
        let events = self.engine.tick(dt).await?;

        for (id, entity) in self.engine.node.manager.entities.iter_mut() {
            if entity.state == AuthorityState::HandoffOut && !local_sim.contains(id) {
                entity.state = AuthorityState::Local;
            }
        }
        self.engine.node.outgoing_messages.retain(|(id, msg_type)| {
            if *msg_type == HandoffType::Offer {
                local_sim.contains(id)
            } else {
                true
            }
        });

        for event in &events {
            match event {
                ZeusEvent::EntityArrived { id, pos, vel } => {
                    self.world.on_entity_arrived(*id, *pos, *vel);
                }
                ZeusEvent::EntityDeparted { id } => {
                    self.world.on_entity_departed(*id);
                }
                ZeusEvent::RemoteUpdate { id, pos, vel } => {
                    if !self.world.locally_simulated_ids().contains(id) {
                        self.world.on_entity_update(*id, *pos, *vel);
                    }
                }
            }
        }

        let local_ids = self.world.locally_simulated_ids().clone();
        for (id, entity) in &self.engine.node.manager.entities {
            if !local_ids.contains(id)
                && (entity.state == AuthorityState::Local
                    || entity.state == AuthorityState::HandoffOut)
            {
                self.world.on_entity_update(*id, entity.pos, entity.vel);
            }
        }

        for (id, remote) in &self.engine.remote_entity_states {
            if !local_ids.contains(id) && !self.engine.node.manager.entities.contains_key(id) {
                self.world.on_entity_update(*id, remote.pos, remote.vel);
            }
        }

        self.world.step(dt);

        for id in &local_ids {
            if let Some((pos, vel)) = self.world.get_entity_state(*id) {
                self.engine.update_entity(*id, pos, vel);
            }
        }

        self.engine.broadcast_state_to_clients().await;

        let local_sim_for_peers = self.world.locally_simulated_ids().clone();
        self.engine.broadcast_state_to_peers(&local_sim_for_peers);

        self.engine.cleanup_remote_states(std::time::Duration::from_millis(300));

        for event in &events {
            if let ZeusEvent::EntityDeparted { id } = event {
                self.engine.remove_remote_entity(*id);
            }
        }

        Ok(events)
    }

    pub fn player_entity_ids(&self) -> Vec<u64> {
        self.engine
            .node
            .manager
            .entities
            .keys()
            .filter(|id| **id >= 1_000_000)
            .copied()
            .collect()
    }

    pub fn set_boundary(&mut self, boundary: f32) {
        self.engine.set_boundary(boundary);
    }

    pub fn set_lower_boundary(&mut self, lower_boundary: f32) {
        self.engine.set_lower_boundary(lower_boundary);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq)]
    enum MockCall {
        Step(String),
        EntityArrived(u64, (f32, f32, f32), (f32, f32, f32)),
        EntityDeparted(u64),
        EntityUpdate(u64, (f32, f32, f32), (f32, f32, f32)),
    }

    struct MockGameWorld {
        calls: Arc<Mutex<Vec<MockCall>>>,
        local_ids: HashSet<u64>,
        states: std::collections::HashMap<u64, ((f32, f32, f32), (f32, f32, f32))>,
    }

    impl MockGameWorld {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                local_ids: HashSet::new(),
                states: std::collections::HashMap::new(),
            }
        }

        fn with_local_ids(mut self, ids: HashSet<u64>) -> Self {
            self.local_ids = ids;
            self
        }

        fn with_state(mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) -> Self {
            self.states.insert(id, (pos, vel));
            self
        }

        fn get_calls(&self) -> Vec<MockCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl GameWorld for MockGameWorld {
        fn step(&mut self, dt: f32) {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Step(format!("{:.3}", dt)));
        }

        fn on_entity_arrived(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::EntityArrived(id, pos, vel));
        }

        fn on_entity_departed(&mut self, id: u64) {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::EntityDeparted(id));
        }

        fn on_entity_update(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::EntityUpdate(id, pos, vel));
        }

        fn locally_simulated_ids(&self) -> &HashSet<u64> {
            &self.local_ids
        }

        fn get_entity_state(&self, id: u64) -> Option<((f32, f32, f32), (f32, f32, f32))> {
            self.states.get(&id).copied()
        }
    }

    #[tokio::test]
    async fn test_tick_calls_step() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mock = MockGameWorld::new();
        let mut game_loop = GameLoop::new(config, mock).await.unwrap();
        game_loop.tick(0.016).await.unwrap();
        let calls = game_loop.world.get_calls();
        let step_calls: Vec<_> = calls
            .iter()
            .filter(|c| matches!(c, MockCall::Step(_)))
            .collect();
        assert_eq!(step_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_entity_arrived_routed() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mock = MockGameWorld::new();
        let mut game_loop = GameLoop::new(config, mock).await.unwrap();

        game_loop.engine.node.manager.add_entity(crate::entity_manager::Entity {
            id: 42,
            pos: (1.0, 2.0, 3.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        game_loop.tick(0.016).await.unwrap();

        let calls = game_loop.world.get_calls();
        let update_calls: Vec<_> = calls
            .iter()
            .filter(|c| matches!(c, MockCall::EntityUpdate(42, _, _)))
            .collect();
        assert!(
            !update_calls.is_empty(),
            "External entity 42 should get on_entity_update"
        );
    }

    #[tokio::test]
    async fn test_entity_departed_routed() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mock = MockGameWorld::new();
        let mut game_loop = GameLoop::new(config, mock).await.unwrap();
        game_loop.tick(0.016).await.unwrap();
        let calls = game_loop.world.get_calls();
        let departed_calls: Vec<_> = calls
            .iter()
            .filter(|c| matches!(c, MockCall::EntityDeparted(_)))
            .collect();
        assert!(
            departed_calls.is_empty(),
            "No departures should happen with no entities"
        );
    }

    #[tokio::test]
    async fn test_external_entity_sync() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let local_ids: HashSet<u64> = [10].into_iter().collect();
        let mock = MockGameWorld::new()
            .with_local_ids(local_ids)
            .with_state(10, (99.0, 88.0, 77.0), (1.0, 2.0, 3.0));
        let mut game_loop = GameLoop::new(config, mock).await.unwrap();

        game_loop.engine.node.manager.add_entity(crate::entity_manager::Entity {
            id: 10,
            pos: (0.0, 0.0, 0.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        game_loop.engine.node.manager.add_entity(crate::entity_manager::Entity {
            id: 20,
            pos: (5.0, 6.0, 7.0),
            vel: (0.1, 0.2, 0.3),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        game_loop.tick(0.016).await.unwrap();

        let calls = game_loop.world.get_calls();
        let update_20: Vec<_> = calls
            .iter()
            .filter(|c| matches!(c, MockCall::EntityUpdate(20, _, _)))
            .collect();
        assert!(
            !update_20.is_empty(),
            "External entity 20 should get on_entity_update"
        );

        let update_10: Vec<_> = calls
            .iter()
            .filter(|c| matches!(c, MockCall::EntityUpdate(10, _, _)))
            .collect();
        assert!(
            update_10.is_empty(),
            "Local entity 10 should NOT get on_entity_update"
        );
    }

    #[tokio::test]
    async fn test_local_entity_readback() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let local_ids: HashSet<u64> = [10].into_iter().collect();
        let mock = MockGameWorld::new()
            .with_local_ids(local_ids)
            .with_state(10, (99.0, 88.0, 77.0), (1.0, 2.0, 3.0));
        let mut game_loop = GameLoop::new(config, mock).await.unwrap();

        game_loop.engine.node.manager.add_entity(crate::entity_manager::Entity {
            id: 10,
            pos: (0.0, 0.0, 0.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        game_loop.tick(0.016).await.unwrap();

        let entity = game_loop.engine.node.manager.get_entity(10).unwrap();
        assert!(
            (entity.pos.0 - 99.0).abs() < 0.01,
            "Entity 10 pos.x should be 99.0 from GameWorld readback, got {}",
            entity.pos.0
        );
        assert!(
            (entity.vel.2 - 3.0).abs() < 0.01,
            "Entity 10 vel.z should be 3.0 from GameWorld readback, got {}",
            entity.vel.2
        );
    }

    #[tokio::test]
    async fn test_player_entity_ids() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let local_ids: HashSet<u64> = [1, 2, 3].into_iter().collect();
        let mock = MockGameWorld::new().with_local_ids(local_ids);
        let mut game_loop = GameLoop::new(config, mock).await.unwrap();

        for id in [1, 2, 3, 100, 200] {
            game_loop.engine.node.manager.add_entity(crate::entity_manager::Entity {
                id,
                pos: (0.0, 0.0, 0.0),
                vel: (0.0, 0.0, 0.0),
                state: AuthorityState::Local,
                verifying_key: None,
            });
        }

        let mut player_ids = game_loop.player_entity_ids();
        player_ids.sort();
        assert_eq!(player_ids, vec![100, 200]);
    }
}
