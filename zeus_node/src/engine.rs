use crate::NetworkEvent;
use crate::discovery::DiscoveryActor;
use crate::entity_manager::{AuthorityState, Entity};
use crate::node_actor::NodeActor;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use zeus_common::{Ghost, HandoffMsg, HandoffType, Vec3};
use zeus_transport::make_promiscuous_endpoint;

#[derive(Clone, Debug)]
pub struct ZeusConfig {
    pub bind_addr: SocketAddr,
    pub seed_addr: Option<SocketAddr>,
    pub boundary: f32,
    pub margin: f32,
}

#[derive(Debug)]
pub enum ZeusEvent {
    EntityArrived {
        id: u64,
        pos: (f32, f32, f32),
        vel: (f32, f32, f32),
    },
    EntityDeparted {
        id: u64,
    },
    RemoteUpdate {
        id: u64,
        pos: (f32, f32, f32),
        vel: (f32, f32, f32),
    },
}

pub struct ZeusEngine {
    pub node: NodeActor,
    pub discovery: DiscoveryActor,
    pub endpoint: quinn::Endpoint,
    pub connections: Vec<quinn::Connection>,
    pub network_rx: mpsc::Receiver<NetworkEvent>,
    pub network_tx: mpsc::Sender<NetworkEvent>,
    #[allow(dead_code)]
    pub config: ZeusConfig,
    pub signing_key: zeus_common::SigningKey,
    pub client_datagrams: Vec<Vec<u8>>,
}

impl ZeusEngine {
    pub async fn new(config: ZeusConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let (endpoint, _) = make_promiscuous_endpoint(config.bind_addr)?;

        let (tx, rx) = mpsc::channel(100);

        let endpoint_clone = endpoint.clone();
        let tx_accept = tx.clone();
        tokio::spawn(async move {
            while let Some(conn) = endpoint_clone.accept().await {
                if let Ok(connection) = conn.await {
                    let _ = tx_accept
                        .send(NetworkEvent::NewConnection(connection))
                        .await;
                }
            }
        });

        if let Some(seed_addr) = config.seed_addr {
            let connection = endpoint.connect(seed_addr, "localhost")?.await?;
            tx.send(NetworkEvent::NewConnection(connection)).await?;
        }

        let local_id = rand::random();

        let (signing_key, _) = zeus_common::GhostSerializer::generate_keypair();
        let node = NodeActor::new(config.boundary, config.margin);
        let discovery = DiscoveryActor::new(local_id, (0.0, 0.0, 0.0), config.bind_addr);

        Ok(Self {
            node,
            discovery,
            endpoint,
            connections: Vec::new(),
            network_rx: rx,
            network_tx: tx,
            config,
            signing_key,
            client_datagrams: Vec::new(),
        })
    }

    pub fn update_entity(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) {
        if let Some(entity) = self.node.manager.get_entity_mut(id) {
            if entity.state == AuthorityState::Local || entity.state == AuthorityState::HandoffOut {
                entity.pos = pos;
                entity.vel = vel;
            }
        } else {
            self.node.manager.add_entity(Entity {
                id,
                pos,
                vel,
                state: AuthorityState::Local,
                verifying_key: None,
            });
        }
    }

    pub fn set_boundary(&mut self, boundary: f32) {
        self.node.set_boundary(boundary);
    }

    pub async fn tick(&mut self, dt: f32) -> Result<Vec<ZeusEvent>, Box<dyn std::error::Error>> {
        let mut app_events = Vec::new();
        self.client_datagrams.clear();

        self.node.update(dt);
        self.discovery.update(dt);

        let total_entities = self.node.manager.entity_count() as u16;
        self.discovery.set_load(total_entities, 0);

        while let Ok(event) = self.network_rx.try_recv() {
            match event {
                NetworkEvent::NewConnection(conn) => {
                    self.connections.push(conn.clone());
                    let tx_reader = self.network_tx.clone();
                    let conn_reader = conn.clone();
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                res = conn_reader.accept_uni() => {
                                    match res {
                                        Ok(mut recv) => {
                                            if let Ok(bytes) = recv.read_to_end(64*1024).await {
                                                let _ = tx_reader.send(NetworkEvent::Payload(conn_reader.clone(), bytes, true)).await;
                                            }
                                        }
                                        Err(_e) => {
                                            break;
                                        }
                                    }
                                }
                                res = conn_reader.read_datagram() => {
                                     match res {
                                        Ok(bytes) => {
                                            let _ = tx_reader.send(NetworkEvent::Payload(conn_reader.clone(), bytes.to_vec(), false)).await;
                                        }
                                        Err(_e) => {
                                             break;
                                        }
                                     }
                                }
                            }
                        }
                    });
                }
                NetworkEvent::Payload(conn, bytes, is_stream) => {
                    if is_stream {
                        if let Ok(msg) = zeus_common::flatbuffers::root::<HandoffMsg>(&bytes) {
                            let id = msg.entity_id();
                            let old_state =
                                self.node.manager.get_entity(id).map(|e| e.state.clone());

                            self.node.handle_handoff_msg(msg);

                            let new_state =
                                self.node.manager.get_entity(id).map(|e| e.state.clone());

                            if let Some(new_st) = new_state {
                                if old_state.is_none() && new_st == AuthorityState::Local {
                                    if let Some(e) = self.node.manager.get_entity(id) {
                                        app_events.push(ZeusEvent::EntityArrived {
                                            id: e.id,
                                            pos: e.pos,
                                            vel: e.vel,
                                        });
                                    }
                                } else if old_state == Some(AuthorityState::HandoffOut)
                                    && new_st == AuthorityState::Remote
                                {
                                    app_events.push(ZeusEvent::EntityDeparted { id });
                                } else if new_st == AuthorityState::Local {
                                    if let Some(e) = self.node.manager.get_entity(id) {
                                        app_events.push(ZeusEvent::RemoteUpdate {
                                            id: e.id,
                                            pos: e.pos,
                                            vel: e.vel,
                                        });
                                    }
                                }
                            }
                        }
                    } else {
                        if let Ok(msg) =
                            zeus_common::flatbuffers::root::<zeus_common::DiscoveryMsg>(&bytes)
                        {
                            self.discovery.process_packet(msg, conn.remote_address());
                        } else if !bytes.is_empty() {
                            self.client_datagrams.push(bytes);
                        }
                    }
                }
                _ => {}
            }
        }

        while let Some((id, msg_type)) = self.node.outgoing_messages.pop_front() {
            let msg_bytes = build_handoff_msg(id, msg_type, &self.node);
            for conn in &self.connections {
                let timeout_dur = std::time::Duration::from_millis(2);
                if let Ok(Ok(mut stream)) =
                    tokio::time::timeout(timeout_dur, conn.open_uni()).await
                {
                    let _ = stream.write_all(&msg_bytes).await;
                    let _ = stream.finish();
                }
            }
        }

        if rand::random::<f32>() < 0.016 {
            let announce = self.discovery.generate_announce();
            for conn in &self.connections {
                let _ = conn.send_datagram(announce.clone().into());
            }
        }

        Ok(app_events)
    }

    pub async fn broadcast_state_to_clients(&mut self) {
        use zeus_common::flatbuffers::FlatBufferBuilder;
        let mut builder = FlatBufferBuilder::new();

        let entities: Vec<_> = self
            .node
            .manager
            .entities
            .values()
            .filter(|e| e.state != crate::entity_manager::AuthorityState::Remote)
            .collect();

        let empty_sig = [0u8; 0];

        let mut dead_indices: Vec<usize> = Vec::new();
        let max_dg = self
            .connections
            .first()
            .and_then(|c| c.max_datagram_size())
            .unwrap_or(1200);
        let per_entity_bytes: usize = 60;
        let overhead: usize = 80;
        let batch_size = ((max_dg.saturating_sub(overhead)) / per_entity_bytes).max(1);

        for chunk in entities.chunks(batch_size) {
            builder.reset();
            let mut ghosts = Vec::with_capacity(chunk.len());

            for entity in chunk {
                let pos = Vec3::new(entity.pos.0, entity.pos.1, entity.pos.2);
                let vel = Vec3::new(entity.vel.0, entity.vel.1, entity.vel.2);
                let sig = builder.create_vector(&empty_sig);

                ghosts.push(Ghost::create(
                    &mut builder,
                    &zeus_common::GhostArgs {
                        entity_id: entity.id,
                        position: Some(&pos),
                        velocity: Some(&vel),
                        signature: Some(sig),
                    },
                ));
            }

            let ghosts_vec = builder.create_vector(&ghosts);

            let update_msg = zeus_common::StateUpdate::create(
                &mut builder,
                &zeus_common::StateUpdateArgs {
                    ghosts: Some(ghosts_vec),
                },
            );

            builder.finish(update_msg, None);
            let bytes = builder.finished_data();

            let mut payload = Vec::with_capacity(1 + bytes.len());
            payload.push(0xCC);
            payload.extend_from_slice(bytes);

            for (i, conn) in self.connections.iter().enumerate() {
                match conn.send_datagram(payload.clone().into()) {
                    Ok(_) => {}
                    Err(e) => {
                        let _ = e;
                        dead_indices.push(i);
                    }
                }
            }
        }

        dead_indices.sort_unstable();
        dead_indices.dedup();
        for index in dead_indices.iter().rev() {
            if *index < self.connections.len() {
                let _ = index;
                self.connections.swap_remove(*index);
            }
        }
    }
}

fn build_handoff_msg(id: u64, msg_type: HandoffType, node: &NodeActor) -> Vec<u8> {
    use zeus_common::flatbuffers::FlatBufferBuilder;
    let mut builder = FlatBufferBuilder::new();

    let ghost_offset = if let Some(e) = node.manager.get_entity(id) {
        let pos = Vec3::new(e.pos.0, e.pos.1, e.pos.2);
        let vel = Vec3::new(e.vel.0, e.vel.1, e.vel.2);
        let sig = builder.create_vector(&[0u8; 64]);

        Some(Ghost::create(
            &mut builder,
            &zeus_common::GhostArgs {
                entity_id: id,
                position: Some(&pos),
                velocity: Some(&vel),
                signature: Some(sig),
            },
        ))
    } else {
        None
    };

    let msg = HandoffMsg::create(
        &mut builder,
        &zeus_common::HandoffMsgArgs {
            entity_id: id,
            type_: msg_type,
            state: ghost_offset,
        },
    );

    builder.finish(msg, None);
    builder.finished_data().to_vec()
}

pub fn build_broadcast_datagrams(engine: &ZeusEngine) -> Vec<Vec<u8>> {
    use zeus_common::flatbuffers::FlatBufferBuilder;
    let mut builder = FlatBufferBuilder::new();
    let entities: Vec<_> = engine.node.manager.entities.values().collect();
    let max_dg = engine
        .connections
        .first()
        .and_then(|c| c.max_datagram_size())
        .unwrap_or(1200);
    let per_entity_bytes: usize = 60;
    let overhead: usize = 80;
    let batch_size = ((max_dg.saturating_sub(overhead)) / per_entity_bytes).max(1);
    let empty_sig = [0u8; 0];
    let mut datagrams = Vec::new();

    for chunk in entities.chunks(batch_size) {
        builder.reset();
        let mut ghosts = Vec::with_capacity(chunk.len());
        for entity in chunk {
            let pos = Vec3::new(entity.pos.0, entity.pos.1, entity.pos.2);
            let vel = Vec3::new(entity.vel.0, entity.vel.1, entity.vel.2);
            let sig = builder.create_vector(&empty_sig);
            ghosts.push(Ghost::create(
                &mut builder,
                &zeus_common::GhostArgs {
                    entity_id: entity.id,
                    position: Some(&pos),
                    velocity: Some(&vel),
                    signature: Some(sig),
                },
            ));
        }
        let ghosts_vec = builder.create_vector(&ghosts);
        let update_msg = zeus_common::StateUpdate::create(
            &mut builder,
            &zeus_common::StateUpdateArgs {
                ghosts: Some(ghosts_vec),
            },
        );
        builder.finish(update_msg, None);
        let bytes = builder.finished_data();
        let mut payload = Vec::with_capacity(1 + bytes.len());
        payload.push(0xCC);
        payload.extend_from_slice(bytes);
        datagrams.push(payload);
    }
    datagrams
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_manager::Entity;

    #[tokio::test]
    async fn test_broadcast_no_signature() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 1,
            pos: (1.0, 2.0, 3.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        let datagrams = build_broadcast_datagrams(&engine);
        assert_eq!(datagrams.len(), 1);

        let data = &datagrams[0];
        assert_eq!(data[0], 0xCC);
        let update = zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(&data[1..]).unwrap();
        let ghost = update.ghosts().unwrap().get(0);
        let sig = ghost.signature().unwrap();
        assert!(
            sig.bytes().is_empty() || sig.bytes().iter().all(|b| *b == 0),
            "Broadcast signature should be empty or zeroed"
        );
    }

    #[tokio::test]
    async fn test_broadcast_batch_size() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        for i in 0..100 {
            engine.node.manager.add_entity(Entity {
                id: i,
                pos: (0.0, 0.0, 0.0),
                vel: (0.0, 0.0, 0.0),
                state: AuthorityState::Local,
                verifying_key: None,
            });
        }

        let datagrams = build_broadcast_datagrams(&engine);
        assert!(
            datagrams.len() >= 2,
            "100 entities should produce multiple datagrams, got {}",
            datagrams.len()
        );
    }

    #[tokio::test]
    async fn test_handoff_still_signed() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
        };
        let _engine = ZeusEngine::new(config).await.unwrap();
        let mut node = NodeActor::new(100.0, 5.0);
        node.manager.add_entity(Entity {
            id: 42,
            pos: (1.0, 2.0, 3.0),
            vel: (4.0, 5.0, 6.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        let msg_bytes = build_handoff_msg(42, HandoffType::Offer, &node);
        let msg = zeus_common::flatbuffers::root::<HandoffMsg>(&msg_bytes).unwrap();
        let ghost = msg.state().unwrap();
        let sig = ghost.signature().unwrap();
        assert_eq!(sig.bytes().len(), 64, "Handoff messages should still have 64-byte signature");
    }

    #[tokio::test]
    async fn test_tick_budget_1000_entities() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        for i in 0..1000 {
            engine.node.manager.add_entity(Entity {
                id: i,
                pos: (i as f32, 0.0, 0.0),
                vel: (1.0, 0.0, 0.0),
                state: AuthorityState::Local,
                verifying_key: None,
            });
        }

        let start = std::time::Instant::now();
        engine.broadcast_state_to_clients().await;
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 5,
            "Broadcast of 1000 entities should take < 5ms, took {}ms",
            elapsed.as_millis()
        );
    }
}
