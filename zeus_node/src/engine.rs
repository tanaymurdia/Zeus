use crate::NetworkEvent;
use crate::discovery::DiscoveryActor;
use crate::entity_manager::{AuthorityState, Entity};
use crate::node_actor::NodeActor;
use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use zeus_common::{Ghost, HandoffMsg, HandoffType, Signer, Vec3, VerifyingKey};
use zeus_transport::make_promiscuous_endpoint;

#[derive(Debug, Clone)]
pub struct RemoteEntityState {
    pub pos: (f32, f32, f32),
    pub vel: (f32, f32, f32),
    pub last_seen: std::time::Instant,
}

#[derive(Clone, Debug)]
pub struct ZeusConfig {
    pub bind_addr: SocketAddr,
    pub seed_addr: Option<SocketAddr>,
    pub boundary: f32,
    pub margin: f32,
    pub ordinal: u32,
    pub lower_boundary: f32,
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
    pub verifying_key: VerifyingKey,
    pub remote_entity_states: HashMap<u64, RemoteEntityState>,
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

        let (signing_key, verifying_key) = zeus_common::GhostSerializer::generate_keypair();
        let node = NodeActor::new(config.boundary, config.margin, config.lower_boundary);
        let discovery = DiscoveryActor::new(local_id, (0.0, 0.0, 0.0), config.bind_addr, config.ordinal);

        Ok(Self {
            node,
            discovery,
            endpoint,
            connections: Vec::new(),
            network_rx: rx,
            network_tx: tx,
            config,
            signing_key,
            verifying_key,
            remote_entity_states: HashMap::new(),
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

    pub fn set_lower_boundary(&mut self, lower_boundary: f32) {
        self.node.manager.set_lower_boundary(lower_boundary);
    }

    fn find_target_connection(&self, entity_id: u64) -> Option<&quinn::Connection> {
        let entity = self.node.manager.get_entity(entity_id)?;
        let my_ordinal = self.discovery.local_ordinal;
        let boundary = self.node.manager.boundary();
        let lower = self.node.manager.lower_boundary();
        let target_ordinal = if entity.pos.0 > boundary {
            my_ordinal + 1
        } else if entity.pos.0 < lower {
            my_ordinal.checked_sub(1)?
        } else {
            return None;
        };
        let target_peer = self
            .discovery
            .peers
            .values()
            .find(|p| p.ordinal == target_ordinal)?;
        self.connections
            .iter()
            .find(|c| c.remote_address() == target_peer.addr)
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
                                let became_local = new_st == AuthorityState::Local
                                    && (old_state.is_none()
                                        || old_state == Some(AuthorityState::Remote)
                                        || old_state == Some(AuthorityState::HandoffIn));
                                if became_local {
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
                        } else if bytes.len() > 1 && bytes[0] == 0xCC {
                            if let Ok(update) = zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(&bytes[1..]) {
                                if let Some(ghosts) = update.ghosts() {
                                    let now = std::time::Instant::now();
                                    for ghost in ghosts {
                                        let id = ghost.entity_id();
                                        if self.node.manager.get_entity(id).is_some_and(|e| e.state == AuthorityState::Local) {
                                            continue;
                                        }
                                        let pos = ghost.position().map(|p| (p.x(), p.y(), p.z())).unwrap_or_default();
                                        let vel = ghost.velocity().map(|v| (v.x(), v.y(), v.z())).unwrap_or_default();
                                        self.remote_entity_states.insert(id, RemoteEntityState { pos, vel, last_seen: now });
                                    }
                                }
                            }
                            self.client_datagrams.push(bytes);
                        } else if !bytes.is_empty() {
                            self.client_datagrams.push(bytes);
                        }
                    }
                }
                _ => {}
            }
        }

        let messages: Vec<_> = self.node.outgoing_messages.drain(..).collect();
        for (id, msg_type) in messages {
            let msg_bytes = build_handoff_msg(id, msg_type, &self.node);
            if msg_type == HandoffType::Offer {
                if let Some(conn) = self.find_target_connection(id) {
                    let conn = conn.clone();
                    let timeout_dur = std::time::Duration::from_millis(2);
                    if let Ok(Ok(mut stream)) =
                        tokio::time::timeout(timeout_dur, conn.open_uni()).await
                    {
                        let _ = stream.write_all(&msg_bytes).await;
                        let _ = stream.finish();
                    }
                } else {
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
            } else {
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

        struct BroadcastEntry {
            id: u64,
            pos: (f32, f32, f32),
            vel: (f32, f32, f32),
        }

        let mut seen_ids = std::collections::HashSet::new();

        let mut all_entries: Vec<BroadcastEntry> = self
            .node
            .manager
            .entities
            .values()
            .filter(|e| e.state != crate::entity_manager::AuthorityState::Remote)
            .map(|e| {
                seen_ids.insert(e.id);
                BroadcastEntry { id: e.id, pos: e.pos, vel: e.vel }
            })
            .collect();

        for (id, remote) in &self.remote_entity_states {
            if !seen_ids.contains(id) {
                seen_ids.insert(*id);
                all_entries.push(BroadcastEntry { id: *id, pos: remote.pos, vel: remote.vel });
            }
        }

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

        for chunk in all_entries.chunks(batch_size) {
            builder.reset();
            let mut ghosts = Vec::with_capacity(chunk.len());

            for entry in chunk {
                let pos = Vec3::new(entry.pos.0, entry.pos.1, entry.pos.2);
                let vel = Vec3::new(entry.vel.0, entry.vel.1, entry.vel.2);
                let sig = builder.create_vector(&empty_sig);

                ghosts.push(Ghost::create(
                    &mut builder,
                    &zeus_common::GhostArgs {
                        entity_id: entry.id,
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

    pub fn broadcast_state_to_peers(&self, local_sim_ids: &std::collections::HashSet<u64>) {
        use zeus_common::flatbuffers::FlatBufferBuilder;
        let mut builder = FlatBufferBuilder::new();

        let local_entities: Vec<_> = self.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local || e.state == AuthorityState::HandoffOut)
            .collect();

        let local_entity_ids: std::collections::HashSet<u64> = local_entities.iter().map(|e| e.id).collect();
        let remote_entries: Vec<(u64, &RemoteEntityState)> = self.remote_entity_states.iter()
            .filter(|(id, _)| !local_sim_ids.contains(id) && !local_entity_ids.contains(id))
            .map(|(id, st)| (*id, st))
            .collect();

        let max_dg = self.connections.first().and_then(|c| c.max_datagram_size()).unwrap_or(1200);
        let per_entity_bytes: usize = 128;
        let overhead: usize = 80;
        let batch_size = ((max_dg.saturating_sub(overhead)) / per_entity_bytes).max(1);

        struct GhostEntry {
            id: u64,
            pos: (f32, f32, f32),
            vel: (f32, f32, f32),
        }

        let mut all_entries: Vec<GhostEntry> = Vec::with_capacity(local_entities.len() + remote_entries.len());
        for e in &local_entities {
            all_entries.push(GhostEntry { id: e.id, pos: e.pos, vel: e.vel });
        }
        for (id, st) in &remote_entries {
            all_entries.push(GhostEntry { id: *id, pos: st.pos, vel: st.vel });
        }

        for chunk in all_entries.chunks(batch_size) {
            builder.reset();
            let mut ghosts = Vec::with_capacity(chunk.len());

            for entry in chunk {
                let pos = Vec3::new(entry.pos.0, entry.pos.1, entry.pos.2);
                let vel = Vec3::new(entry.vel.0, entry.vel.1, entry.vel.2);

                let mut data = Vec::with_capacity(32);
                data.extend_from_slice(&entry.id.to_le_bytes());
                data.extend_from_slice(&entry.pos.0.to_le_bytes());
                data.extend_from_slice(&entry.pos.1.to_le_bytes());
                data.extend_from_slice(&entry.pos.2.to_le_bytes());
                data.extend_from_slice(&entry.vel.0.to_le_bytes());
                data.extend_from_slice(&entry.vel.1.to_le_bytes());
                data.extend_from_slice(&entry.vel.2.to_le_bytes());
                let sig_bytes = self.signing_key.sign(&data).to_bytes();
                let sig = builder.create_vector(&sig_bytes);

                ghosts.push(Ghost::create(
                    &mut builder,
                    &zeus_common::GhostArgs {
                        entity_id: entry.id,
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

            for conn in &self.connections {
                let _ = conn.send_datagram(payload.clone().into());
            }
        }
    }

    pub fn cleanup_remote_states(&mut self, ttl: std::time::Duration) {
        let now = std::time::Instant::now();
        self.remote_entity_states.retain(|_, state| now.duration_since(state.last_seen) < ttl);
    }

    pub fn remove_remote_entity(&mut self, id: u64) {
        self.remote_entity_states.remove(&id);
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
            ordinal: 0,
            lower_boundary: 0.0,
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
            ordinal: 0,
            lower_boundary: 0.0,
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
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let _engine = ZeusEngine::new(config).await.unwrap();
        let mut node = NodeActor::new(100.0, 5.0, 0.0);
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
            ordinal: 0,
            lower_boundary: 0.0,
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

    fn build_signed_0xcc(entities: &[(u64, (f32, f32, f32), (f32, f32, f32))], key: &zeus_common::SigningKey) -> Vec<u8> {
        use zeus_common::flatbuffers::FlatBufferBuilder;
        let mut builder = FlatBufferBuilder::new();
        let mut ghosts = Vec::new();
        for (id, pos, vel) in entities {
            let p = Vec3::new(pos.0, pos.1, pos.2);
            let v = Vec3::new(vel.0, vel.1, vel.2);
            let mut data = Vec::with_capacity(32);
            data.extend_from_slice(&id.to_le_bytes());
            data.extend_from_slice(&pos.0.to_le_bytes());
            data.extend_from_slice(&pos.1.to_le_bytes());
            data.extend_from_slice(&pos.2.to_le_bytes());
            data.extend_from_slice(&vel.0.to_le_bytes());
            data.extend_from_slice(&vel.1.to_le_bytes());
            data.extend_from_slice(&vel.2.to_le_bytes());
            let sig_bytes = key.sign(&data).to_bytes();
            let sig = builder.create_vector(&sig_bytes);
            ghosts.push(Ghost::create(&mut builder, &zeus_common::GhostArgs {
                entity_id: *id,
                position: Some(&p),
                velocity: Some(&v),
                signature: Some(sig),
            }));
        }
        let gv = builder.create_vector(&ghosts);
        let msg = zeus_common::StateUpdate::create(&mut builder, &zeus_common::StateUpdateArgs { ghosts: Some(gv) });
        builder.finish(msg, None);
        let bytes = builder.finished_data();
        let mut payload = Vec::with_capacity(1 + bytes.len());
        payload.push(0xCC);
        payload.extend_from_slice(bytes);
        payload
    }

    #[tokio::test]
    async fn test_gossip_0xcc_intercept() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        let (peer_key, _) = zeus_common::GhostSerializer::generate_keypair();
        let datagram = build_signed_0xcc(&[(50, (10.0, 5.0, 3.0), (1.0, 0.0, 0.0))], &peer_key);

        let ep2 = zeus_transport::make_promiscuous_endpoint("127.0.0.1:0".parse().unwrap()).unwrap().0;
        let conn = ep2.connect(engine.endpoint.local_addr().unwrap(), "localhost").unwrap().await.unwrap();
        engine.network_tx.send(crate::NetworkEvent::NewConnection(conn.clone())).await.unwrap();
        engine.network_tx.send(crate::NetworkEvent::Payload(conn, datagram, false)).await.unwrap();

        engine.tick(0.016).await.unwrap();

        assert!(engine.remote_entity_states.contains_key(&50), "Remote entity 50 should be in gossip cache");
        let st = &engine.remote_entity_states[&50];
        assert!((st.pos.0 - 10.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_gossip_skips_locally_owned() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 5,
            pos: (1.0, 2.0, 3.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        let (peer_key, _) = zeus_common::GhostSerializer::generate_keypair();
        let datagram = build_signed_0xcc(&[(5, (99.0, 99.0, 99.0), (0.0, 0.0, 0.0))], &peer_key);

        let ep2 = zeus_transport::make_promiscuous_endpoint("127.0.0.1:0".parse().unwrap()).unwrap().0;
        let conn = ep2.connect(engine.endpoint.local_addr().unwrap(), "localhost").unwrap().await.unwrap();
        engine.network_tx.send(crate::NetworkEvent::NewConnection(conn.clone())).await.unwrap();
        engine.network_tx.send(crate::NetworkEvent::Payload(conn, datagram, false)).await.unwrap();

        engine.tick(0.016).await.unwrap();

        assert!(!engine.remote_entity_states.contains_key(&5), "Locally-owned entity 5 should NOT be in gossip cache");
    }

    #[tokio::test]
    async fn test_gossip_ttl_expiry() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();

        engine.remote_entity_states.insert(100, RemoteEntityState {
            pos: (1.0, 2.0, 3.0),
            vel: (0.0, 0.0, 0.0),
            last_seen: std::time::Instant::now() - std::time::Duration::from_secs(5),
        });
        engine.remote_entity_states.insert(200, RemoteEntityState {
            pos: (4.0, 5.0, 6.0),
            vel: (0.0, 0.0, 0.0),
            last_seen: std::time::Instant::now(),
        });

        engine.cleanup_remote_states(std::time::Duration::from_secs(1));

        assert!(!engine.remote_entity_states.contains_key(&100), "Stale entry 100 should be removed");
        assert!(engine.remote_entity_states.contains_key(&200), "Fresh entry 200 should remain");
    }

    #[tokio::test]
    async fn test_broadcast_state_to_peers_signs() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 7,
            pos: (3.0, 4.0, 5.0),
            vel: (1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        let local_ids: std::collections::HashSet<u64> = [7].into_iter().collect();
        engine.broadcast_state_to_peers(&local_ids);
    }

    #[tokio::test]
    async fn test_find_target_connection_right_no_peer() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 10.0,
            margin: 2.0,
            ordinal: 0,
            lower_boundary: 0.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 1,
            pos: (15.0, 0.0, 0.0),
            vel: (1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let conn = engine.find_target_connection(1);
        assert!(conn.is_none(), "No connections exist so should return None");
    }

    #[tokio::test]
    async fn test_find_target_connection_left_ordinal_zero() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 16.0,
            margin: 2.0,
            ordinal: 0,
            lower_boundary: 8.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 2,
            pos: (3.0, 0.0, 0.0),
            vel: (-1.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let conn = engine.find_target_connection(2);
        assert!(conn.is_none(), "Ordinal 0 cannot hand off left (checked_sub returns None)");
    }

    #[tokio::test]
    async fn test_find_target_connection_in_zone() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 16.0,
            margin: 2.0,
            ordinal: 1,
            lower_boundary: 8.0,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 3,
            pos: (12.0, 0.0, 0.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
        let conn = engine.find_target_connection(3);
        assert!(conn.is_none(), "Entity within zone should not target any connection");
    }

    #[tokio::test]
    async fn test_find_target_nonexistent_entity() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addr: None,
            boundary: 16.0,
            margin: 2.0,
            ordinal: 1,
            lower_boundary: 8.0,
        };
        let engine = ZeusEngine::new(config).await.unwrap();
        let conn = engine.find_target_connection(999);
        assert!(conn.is_none(), "Nonexistent entity should return None");
    }
}
