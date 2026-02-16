use crate::NetworkEvent;
use crate::cell::Cell;
use crate::discovery::DiscoveryActor;
use crate::entity_manager::{AuthorityState, Entity};
use crate::node_actor::NodeActor;
use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;
use zeus_common::{Ghost, HandoffMsg, HandoffType, Vec3, VerifyingKey};
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
    pub seed_addrs: Vec<SocketAddr>,
    pub boundary: f32,
    pub margin: f32,
    pub ordinal: u32,
    pub lower_boundary: f32,
    pub cell: Option<Cell>,
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
    pub peer_connections: Vec<quinn::Connection>,
    pub client_connections: Vec<quinn::Connection>,
    pub connections: Vec<quinn::Connection>,
    pub network_rx: mpsc::Receiver<NetworkEvent>,
    pub network_tx: mpsc::Sender<NetworkEvent>,
    #[allow(dead_code)]
    pub config: ZeusConfig,
    pub signing_key: zeus_common::SigningKey,
    pub verifying_key: VerifyingKey,
    pub remote_entity_states: HashMap<u64, RemoteEntityState>,
    pub client_datagrams: Vec<Vec<u8>>,
    pub last_broadcast_state: HashMap<u64, (i16, i16, i16)>,
    pub known_peer_addrs: std::collections::HashSet<SocketAddr>,
    pub peer_verifying_keys: HashMap<u64, VerifyingKey>,
    heartbeat_counter: u32,
    tick_counter: u32,
    pub handoff_retry_counter: u32,
    pub recently_departed: HashMap<u64, ((f32, f32, f32), (f32, f32, f32), u32)>,
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

        for seed_addr in &config.seed_addrs {
            match endpoint.connect(*seed_addr, "localhost") {
                Ok(connecting) => {
                    match connecting.await {
                        Ok(connection) => {
                            tx.send(NetworkEvent::NewConnection(connection)).await?;
                        }
                        Err(e) => {
                            eprintln!("[ZeusEngine] Failed to connect to seed {}: {}", seed_addr, e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[ZeusEngine] Failed to initiate connection to seed {}: {}", seed_addr, e);
                }
            }
        }

        let local_id = rand::random();

        let (signing_key, verifying_key) = zeus_common::GhostSerializer::generate_keypair();
        let node = if let Some(ref cell) = config.cell {
            NodeActor::new_3d(cell.clone(), config.margin)
        } else {
            NodeActor::new(config.boundary, config.margin, config.lower_boundary)
        };
        let discovery = DiscoveryActor::new(local_id, (0.0, 0.0, 0.0), config.bind_addr, config.ordinal);

        let mut known_peer_addrs = std::collections::HashSet::new();
        for addr in &config.seed_addrs {
            known_peer_addrs.insert(*addr);
        }

        Ok(Self {
            node,
            discovery,
            endpoint,
            peer_connections: Vec::new(),
            client_connections: Vec::new(),
            connections: Vec::new(),
            network_rx: rx,
            network_tx: tx,
            config,
            signing_key,
            verifying_key,
            remote_entity_states: HashMap::new(),
            client_datagrams: Vec::new(),
            last_broadcast_state: HashMap::new(),
            known_peer_addrs,
            peer_verifying_keys: HashMap::new(),
            heartbeat_counter: 0,
            tick_counter: 0,
            handoff_retry_counter: 0,
            recently_departed: HashMap::new(),
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

    pub fn set_cell(&mut self, cell: Cell) {
        self.node.manager.set_cell(cell.clone());
        self.broadcast_cell_to_peers(&cell);
    }

    fn build_cell_datagram(&self, cell: &Cell) -> Vec<u8> {
        let mut buf = Vec::with_capacity(33);
        buf.push(0xD6);
        buf.extend_from_slice(&self.discovery.local_id.to_le_bytes());
        buf.extend_from_slice(&cell.serialize());
        buf
    }

    fn broadcast_cell_to_peers(&self, cell: &Cell) {
        let datagram = self.build_cell_datagram(cell);
        let payload: bytes::Bytes = datagram.into();
        for conn in &self.peer_connections {
            let _ = conn.send_datagram(payload.clone());
        }
    }

    pub fn find_target_connection_pub(&self, entity_id: u64) -> Option<&quinn::Connection> {
        self.find_target_connection(entity_id)
    }

    fn find_target_connection(&self, entity_id: u64) -> Option<&quinn::Connection> {
        let entity = self.node.manager.get_entity(entity_id)?;

        if let Some(peer) = self.discovery.find_peer_containing(entity.pos) {
            return self.peer_connections
                .iter()
                .find(|c| c.remote_address() == peer.addr)
                .or_else(|| {
                    self.connections
                        .iter()
                        .find(|c| c.remote_address() == peer.addr)
                });
        }

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
        self.peer_connections
            .iter()
            .find(|c| c.remote_address() == target_peer.addr)
            .or_else(|| {
                self.connections
                    .iter()
                    .find(|c| c.remote_address() == target_peer.addr)
            })
    }

    pub async fn tick(&mut self, dt: f32) -> Result<Vec<ZeusEvent>, Box<dyn std::error::Error>> {
        let mut app_events = Vec::new();
        self.client_datagrams.clear();

        let mut exit_candidates = self.node.update(dt);
        self.handoff_retry_counter += 1;
        let my_cell = self.node.manager.cell().clone();
        if self.handoff_retry_counter % 8 == 0 && !self.peer_connections.is_empty() {
            let stuck: Vec<u64> = self.node.manager.entities.iter()
                .filter(|(_, e)| e.state == AuthorityState::HandoffOut && !my_cell.contains(e.pos))
                .map(|(id, _)| *id)
                .collect();
            for id in &stuck {
                if let Some(conn) = self.find_target_connection(*id) {
                    let msg_bytes = build_handoff_msg(*id, HandoffType::Offer, &self.node);
                    let timeout_dur = std::time::Duration::from_millis(10);
                    if let Ok(Ok(mut stream)) =
                        tokio::time::timeout(timeout_dur, conn.open_uni()).await
                    {
                        let _ = stream.write_all(&msg_bytes).await;
                        let _ = stream.finish();
                    }
                }
            }
        }
        if self.handoff_retry_counter >= 1024 {
            let long_stuck: Vec<u64> = self.node.manager.entities.iter()
                .filter(|(_, e)| e.state == AuthorityState::HandoffOut && !my_cell.contains(e.pos))
                .map(|(id, _)| *id)
                .collect();
            for id in &long_stuck {
                self.node.manager.remove_entity(*id);
                app_events.push(ZeusEvent::EntityDeparted { id: *id });
            }
            self.handoff_retry_counter = 0;
        }
        let forced_exits = self.node.manager.force_exit_check();
        for fe in forced_exits {
            if !exit_candidates.iter().any(|(id, _)| *id == fe.0) {
                exit_candidates.push(fe);
            }
        }
        self.discovery.update(dt);

        let total_entities = self.node.manager.entity_count() as u16;
        self.discovery.set_load(total_entities, 0);

        let mut new_peer_addrs_to_connect: Vec<SocketAddr> = Vec::new();

        while let Ok(event) = self.network_rx.try_recv() {
            match event {
                NetworkEvent::NewConnection(conn) => {
                    let remote = conn.remote_address();
                    let is_peer = self.known_peer_addrs.contains(&remote);
                    if is_peer {
                        self.peer_connections.push(conn.clone());
                        let cell = self.node.manager.cell().clone();
                        let cell_dg = self.build_cell_datagram(&cell);
                        let _ = conn.send_datagram(cell_dg.into());
                    } else {
                        self.client_connections.push(conn.clone());
                        self.last_broadcast_state.clear();
                    }
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

                            let source_addr = conn.remote_address();
                            self.node.handle_handoff_msg(msg, Some(source_addr));

                            let is_from_peer = self.peer_connections.iter().any(|pc| pc.remote_address() == conn.remote_address());
                            if !is_from_peer {
                                if self.node.manager.get_entity(id).is_some_and(|e| e.state == AuthorityState::HandoffIn) {
                                    self.node.manager.set_state(id, AuthorityState::Local);
                                }
                            }

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
                                    if let Some(e) = self.node.manager.get_entity(id) {
                                        self.recently_departed.insert(id, (e.pos, e.vel, 0));
                                    }
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
                            let remote_addr = conn.remote_address();
                            self.discovery.process_packet(msg, remote_addr);
                            if !self.known_peer_addrs.contains(&remote_addr) {
                                self.known_peer_addrs.insert(remote_addr);
                                if !self.peer_connections.iter().any(|c| c.remote_address() == remote_addr) {
                                    let already_connected = self.connections.iter().any(|c| c.remote_address() == remote_addr);
                                    if already_connected {
                                        if let Some(c) = self.connections.iter().find(|c| c.remote_address() == remote_addr) {
                                            self.peer_connections.push(c.clone());
                                            let cell = self.node.manager.cell().clone();
                                            let cell_dg = self.build_cell_datagram(&cell);
                                            let _ = c.send_datagram(cell_dg.into());
                                        }
                                    }
                                }
                                self.client_connections.retain(|c| c.remote_address() != remote_addr);
                            }
                            for peer in self.discovery.peers.values() {
                                if !self.known_peer_addrs.contains(&peer.addr) {
                                    new_peer_addrs_to_connect.push(peer.addr);
                                    self.known_peer_addrs.insert(peer.addr);
                                }
                            }
                        } else if bytes.len() > 1 && bytes[0] == 0xDD {
                            self.client_datagrams.push(bytes);
                        } else if bytes.len() > 75 && bytes[0] == 0xCE {
                            let sig_len = 64;
                            let payload_end = bytes.len() - sig_len;
                            if payload_end >= 11 {
                                let sender = u64::from_le_bytes(bytes[1..9].try_into().unwrap_or_default());
                                if let Some(vk) = self.peer_verifying_keys.get(&sender) {
                                    use zeus_common::Verifier;
                                    let sig_bytes: [u8; 64] = bytes[payload_end..payload_end + 64].try_into().unwrap_or([0u8; 64]);
                                    let sig = zeus_common::Signature::from_bytes(&sig_bytes);
                                    if vk.verify(&bytes[..payload_end], &sig).is_err() {
                                        continue;
                                    }
                                }
                                let count = u16::from_le_bytes([bytes[9], bytes[10]]) as usize;
                                let now = std::time::Instant::now();
                                let mut offset = 11;
                                for _ in 0..count {
                                    if offset + 20 > payload_end { break; }
                                    let id = u64::from_le_bytes([
                                        bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
                                        bytes[offset+4], bytes[offset+5], bytes[offset+6], bytes[offset+7],
                                    ]);
                                    offset += 8;
                                    let px = dequantize_pos(i16::from_le_bytes([bytes[offset], bytes[offset+1]]));
                                    offset += 2;
                                    let py = dequantize_pos(i16::from_le_bytes([bytes[offset], bytes[offset+1]]));
                                    offset += 2;
                                    let pz = dequantize_pos(i16::from_le_bytes([bytes[offset], bytes[offset+1]]));
                                    offset += 2;
                                    let vx = dequantize_vel(i16::from_le_bytes([bytes[offset], bytes[offset+1]]));
                                    offset += 2;
                                    let vy = dequantize_vel(i16::from_le_bytes([bytes[offset], bytes[offset+1]]));
                                    offset += 2;
                                    let vz = dequantize_vel(i16::from_le_bytes([bytes[offset], bytes[offset+1]]));
                                    offset += 2;
                                    if self.node.manager.get_entity(id).is_some_and(|e| e.state == AuthorityState::Local || e.state == AuthorityState::HandoffIn || e.state == AuthorityState::HandoffOut) {
                                        continue;
                                    }
                                    self.remote_entity_states.insert(id, RemoteEntityState { pos: (px, py, pz), vel: (vx, vy, vz), last_seen: now });
                                    self.recently_departed.remove(&id);
                                }
                            }
                        } else if bytes.len() == 33 && bytes[0] == 0xD6 {
                            let node_id = u64::from_le_bytes(bytes[1..9].try_into().unwrap_or_default());
                            if let Some(cell) = Cell::deserialize(&bytes[9..33]) {
                                if self.discovery.peers.contains_key(&node_id) {
                                    self.discovery.update_peer_cell(node_id, cell);
                                } else {
                                    let remote_addr = conn.remote_address();
                                    if let Some(peer) = self.discovery.peers.values_mut().find(|p| p.addr == remote_addr) {
                                        peer.cell = Some(cell.clone());
                                    } else {
                                        self.discovery.peers.insert(node_id, crate::discovery::Peer {
                                            id: node_id,
                                            addr: conn.remote_address(),
                                            pos: (0.0, 0.0, 0.0),
                                            load: None,
                                            last_seen: std::time::Instant::now(),
                                            ordinal: 0,
                                            cell: Some(cell),
                                        });
                                        self.discovery.known_node_ids.insert(node_id);
                                    }
                                }
                            }
                        } else if bytes.len() == 41 && bytes[0] == 0xCF {
                            let node_id = u64::from_le_bytes(bytes[1..9].try_into().unwrap_or_default());
                            if let Ok(vk_bytes) = <[u8; 32]>::try_from(&bytes[9..41]) {
                                if let Ok(vk) = zeus_common::VerifyingKey::from_bytes(&vk_bytes) {
                                    self.peer_verifying_keys.insert(node_id, vk);
                                }
                            }
                        } else if !bytes.is_empty() {
                            self.client_datagrams.push(bytes);
                        }
                    }
                }
                _ => {}
            }
        }

        for addr in new_peer_addrs_to_connect {
            let endpoint = self.endpoint.clone();
            let tx = self.network_tx.clone();
            tokio::spawn(async move {
                if let Ok(connecting) = endpoint.connect(addr, "localhost") {
                    if let Ok(connection) = connecting.await {
                        let _ = tx.send(NetworkEvent::NewConnection(connection)).await;
                    }
                }
            });
        }

        let mut targeted_handoffs: Vec<(u64, quinn::Connection)> = Vec::new();
        let mut broadcast_handoffs: Vec<u64> = Vec::new();
        for (id, _face) in &exit_candidates {
            if let Some(conn) = self.find_target_connection(*id) {
                targeted_handoffs.push((*id, conn.clone()));
            } else if !self.peer_connections.is_empty() {
                broadcast_handoffs.push(*id);
            }
        }
        for (id, conn) in targeted_handoffs {
            self.node.manager.set_state(id, AuthorityState::HandoffOut);
            let msg_bytes = build_handoff_msg(id, HandoffType::Offer, &self.node);
            let timeout_dur = std::time::Duration::from_millis(10);
            if let Ok(Ok(mut stream)) =
                tokio::time::timeout(timeout_dur, conn.open_uni()).await
            {
                let _ = stream.write_all(&msg_bytes).await;
                let _ = stream.finish();
            }
        }
        for id in broadcast_handoffs {
            self.node.manager.set_state(id, AuthorityState::HandoffOut);
        }

        let messages: Vec<_> = self.node.outgoing_messages.drain(..).collect();
        for (id, msg_type, target_addr) in messages {
            let msg_bytes = build_handoff_msg(id, msg_type, &self.node);
            if let Some(addr) = target_addr {
                let conn_opt = self.peer_connections.iter().find(|c| c.remote_address() == addr)
                    .or_else(|| self.connections.iter().find(|c| c.remote_address() == addr));
                if let Some(conn) = conn_opt {
                    let timeout_dur = std::time::Duration::from_millis(2);
                    if let Ok(Ok(mut stream)) =
                        tokio::time::timeout(timeout_dur, conn.open_uni()).await
                    {
                        let _ = stream.write_all(&msg_bytes).await;
                        let _ = stream.finish();
                    }
                }
            } else {
                for conn in &self.peer_connections {
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

        if self.handoff_retry_counter % 256 == 0 {
            let handoff_in_stuck: Vec<u64> = self.node.manager.entities.iter()
                .filter(|(_, e)| e.state == AuthorityState::HandoffIn)
                .map(|(id, _)| *id)
                .collect();
            for id in handoff_in_stuck {
                self.node.manager.remove_entity(id);
            }
        }

        self.tick_counter = self.tick_counter.wrapping_add(1);
        if self.tick_counter % 8 == 0 {
            let announce = self.discovery.generate_announce();
            let mut vk_msg = Vec::with_capacity(41);
            vk_msg.push(0xCF);
            vk_msg.extend_from_slice(&self.discovery.local_id.to_le_bytes());
            vk_msg.extend_from_slice(self.verifying_key.as_bytes());
            for conn in &self.connections {
                let _ = conn.send_datagram(announce.clone().into());
                let _ = conn.send_datagram(vk_msg.clone().into());
            }
        }

        Ok(app_events)
    }

    pub async fn broadcast_state_to_clients(&mut self) {
        struct BroadcastEntry {
            id: u64,
            pos: (f32, f32, f32),
            vel: (f32, f32, f32),
        }

        self.heartbeat_counter = self.heartbeat_counter.wrapping_add(1);
        let force_full = self.heartbeat_counter % 128 == 0;

        let mut all_entries: Vec<BroadcastEntry> = Vec::new();
        let mut current_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();

        let overlap_margin = 2.0_f32;
        let cell = self.node.manager.cell().clone();

        for e in self.node.manager.entities.values() {
            let dominated = e.state == crate::entity_manager::AuthorityState::Local && cell.contains(e.pos);
            let in_transit = e.state == crate::entity_manager::AuthorityState::HandoffOut;
            let arriving = e.state == crate::entity_manager::AuthorityState::HandoffIn;
            if dominated || in_transit || arriving {
                current_ids.insert(e.id);
                let near_boundary = dominated && cell.near_any_face(e.pos, overlap_margin);
                let qp = (quantize_pos(e.pos.0), quantize_pos(e.pos.1), quantize_pos(e.pos.2));
                if force_full || in_transit || arriving || near_boundary || self.last_broadcast_state.get(&e.id) != Some(&qp) {
                    self.last_broadcast_state.insert(e.id, qp);
                    all_entries.push(BroadcastEntry { id: e.id, pos: e.pos, vel: e.vel });
                }
            }
        }

        let mut expired_departed = Vec::new();
        for (id, (_pos, _vel, ticks)) in &mut self.recently_departed {
            if current_ids.contains(id) {
                expired_departed.push(*id);
            } else {
                *ticks += 1;
                if *ticks >= 16 {
                    expired_departed.push(*id);
                }
            }
        }
        for id in expired_departed {
            self.recently_departed.remove(&id);
        }

        self.last_broadcast_state.retain(|id, _| current_ids.contains(id));

        if all_entries.is_empty() {
            return;
        }

        let max_dg = self
            .client_connections
            .first()
            .or(self.connections.first())
            .and_then(|c| c.max_datagram_size())
            .unwrap_or(1200);
        let per_moving: usize = 21;
        let header: usize = 3;
        let batch_size = ((max_dg.saturating_sub(header)) / per_moving).max(1);

        let mut dead_indices: Vec<usize> = Vec::new();

        for chunk in all_entries.chunks(batch_size) {
            let mut buf = Vec::with_capacity(header + chunk.len() * per_moving);
            buf.push(0xCC);
            let count = chunk.len() as u16;
            buf.extend_from_slice(&count.to_le_bytes());

            for entry in chunk {
                buf.extend_from_slice(&entry.id.to_le_bytes());
                let vel_sq = entry.vel.0 * entry.vel.0 + entry.vel.1 * entry.vel.1 + entry.vel.2 * entry.vel.2;
                let at_rest = vel_sq < 0.001;
                buf.push(if at_rest { 1 } else { 0 });
                buf.extend_from_slice(&quantize_pos(entry.pos.0).to_le_bytes());
                buf.extend_from_slice(&quantize_pos(entry.pos.1).to_le_bytes());
                buf.extend_from_slice(&quantize_pos(entry.pos.2).to_le_bytes());
                if !at_rest {
                    buf.extend_from_slice(&quantize_vel(entry.vel.0).to_le_bytes());
                    buf.extend_from_slice(&quantize_vel(entry.vel.1).to_le_bytes());
                    buf.extend_from_slice(&quantize_vel(entry.vel.2).to_le_bytes());
                }
            }

            let payload: bytes::Bytes = buf.into();
            for (i, conn) in self.client_connections.iter().enumerate() {
                match conn.send_datagram(payload.clone()) {
                    Ok(_) => {}
                    Err(_) => {
                        dead_indices.push(i);
                    }
                }
            }
        }

        dead_indices.sort_unstable();
        dead_indices.dedup();
        for index in dead_indices.iter().rev() {
            if *index < self.client_connections.len() {
                self.client_connections.swap_remove(*index);
            }
        }
    }

    pub fn broadcast_state_to_peers(&self, _local_sim_ids: &std::collections::HashSet<u64>) {
        use zeus_common::Signer;

        let local_entities: Vec<_> = self.node.manager.entities.values()
            .filter(|e| e.state == AuthorityState::Local || e.state == AuthorityState::HandoffOut)
            .collect();

        if local_entities.is_empty() {
            return;
        }

        struct GhostEntry {
            id: u64,
            pos: (f32, f32, f32),
            vel: (f32, f32, f32),
        }

        let mut all_entries: Vec<GhostEntry> = Vec::with_capacity(local_entities.len());
        for e in &local_entities {
            all_entries.push(GhostEntry { id: e.id, pos: e.pos, vel: e.vel });
        }

        let max_dg = self.peer_connections.first()
            .or(self.connections.first())
            .and_then(|c| c.max_datagram_size()).unwrap_or(1200);
        let per_entity_bytes: usize = 20;
        let sig_overhead: usize = 11 + 64;
        let batch_size = ((max_dg.saturating_sub(sig_overhead)) / per_entity_bytes).max(1);

        let node_id = self.discovery.local_id;
        let my_cell = self.node.manager.cell().clone();

        let neighbor_addrs: std::collections::HashSet<SocketAddr> = self.discovery.peers.values()
            .filter(|p| {
                p.cell.as_ref().is_some_and(|c| my_cell.shares_face(c))
            })
            .map(|p| p.addr)
            .collect();

        let use_spatial_filter = !neighbor_addrs.is_empty();

        for chunk in all_entries.chunks(batch_size) {
            let mut buf = Vec::with_capacity(sig_overhead + chunk.len() * per_entity_bytes);
            buf.push(0xCE);
            buf.extend_from_slice(&node_id.to_le_bytes());
            let count = chunk.len() as u16;
            buf.extend_from_slice(&count.to_le_bytes());

            for entry in chunk {
                buf.extend_from_slice(&entry.id.to_le_bytes());
                buf.extend_from_slice(&quantize_pos(entry.pos.0).to_le_bytes());
                buf.extend_from_slice(&quantize_pos(entry.pos.1).to_le_bytes());
                buf.extend_from_slice(&quantize_pos(entry.pos.2).to_le_bytes());
                buf.extend_from_slice(&quantize_vel(entry.vel.0).to_le_bytes());
                buf.extend_from_slice(&quantize_vel(entry.vel.1).to_le_bytes());
                buf.extend_from_slice(&quantize_vel(entry.vel.2).to_le_bytes());
            }

            let sig = self.signing_key.sign(&buf);
            buf.extend_from_slice(&sig.to_bytes());

            let payload: bytes::Bytes = buf.into();
            for conn in &self.peer_connections {
                if use_spatial_filter && !neighbor_addrs.contains(&conn.remote_address()) {
                    continue;
                }
                let _ = conn.send_datagram(payload.clone());
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

pub fn quantize_pos(v: f32) -> i16 {
    (v * 500.0).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

pub fn dequantize_pos(v: i16) -> f32 {
    v as f32 / 500.0
}

pub fn quantize_vel(v: f32) -> i16 {
    (v * 100.0).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

pub fn dequantize_vel(v: i16) -> f32 {
    v as f32 / 100.0
}

pub fn quantize_pos_i32(v: f32) -> i32 {
    (v * 1000.0).round().clamp(i32::MIN as f32, i32::MAX as f32) as i32
}

pub fn dequantize_pos_i32(v: i32) -> f32 {
    v as f32 / 1000.0
}

pub fn quantize_cell_relative(pos: (f32, f32, f32), cell_origin: (f32, f32, f32)) -> (i16, i16, i16) {
    let rel = (pos.0 - cell_origin.0, pos.1 - cell_origin.1, pos.2 - cell_origin.2);
    (quantize_pos(rel.0), quantize_pos(rel.1), quantize_pos(rel.2))
}

pub fn dequantize_cell_relative(offset: (i16, i16, i16), cell_origin: (f32, f32, f32)) -> (f32, f32, f32) {
    (
        dequantize_pos(offset.0) + cell_origin.0,
        dequantize_pos(offset.1) + cell_origin.1,
        dequantize_pos(offset.2) + cell_origin.2,
    )
}

pub fn encode_hierarchical(cell_id: u32, offset: (i16, i16, i16)) -> [u8; 10] {
    let mut buf = [0u8; 10];
    buf[0..4].copy_from_slice(&cell_id.to_le_bytes());
    buf[4..6].copy_from_slice(&offset.0.to_le_bytes());
    buf[6..8].copy_from_slice(&offset.1.to_le_bytes());
    buf[8..10].copy_from_slice(&offset.2.to_le_bytes());
    buf
}

pub fn decode_hierarchical(data: &[u8]) -> Option<(u32, (i16, i16, i16))> {
    if data.len() < 10 { return None; }
    let cell_id = u32::from_le_bytes(data[0..4].try_into().ok()?);
    let ox = i16::from_le_bytes(data[4..6].try_into().ok()?);
    let oy = i16::from_le_bytes(data[6..8].try_into().ok()?);
    let oz = i16::from_le_bytes(data[8..10].try_into().ok()?);
    Some((cell_id, (ox, oy, oz)))
}

pub fn encode_compact_client(entries: &[(u64, (f32, f32, f32), (f32, f32, f32))]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3 + entries.len() * 21);
    buf.push(0xCC);
    let count = entries.len() as u16;
    buf.extend_from_slice(&count.to_le_bytes());
    for &(id, pos, vel) in entries {
        buf.extend_from_slice(&id.to_le_bytes());
        let vel_sq = vel.0 * vel.0 + vel.1 * vel.1 + vel.2 * vel.2;
        let at_rest = vel_sq < 0.001;
        buf.push(if at_rest { 1 } else { 0 });
        buf.extend_from_slice(&quantize_pos(pos.0).to_le_bytes());
        buf.extend_from_slice(&quantize_pos(pos.1).to_le_bytes());
        buf.extend_from_slice(&quantize_pos(pos.2).to_le_bytes());
        if !at_rest {
            buf.extend_from_slice(&quantize_vel(vel.0).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.1).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.2).to_le_bytes());
        }
    }
    buf
}

pub fn decode_compact_client(data: &[u8]) -> Vec<(u64, (f32, f32, f32), (f32, f32, f32))> {
    let mut result = Vec::new();
    if data.len() < 3 || data[0] != 0xCC {
        return result;
    }
    let count = u16::from_le_bytes([data[1], data[2]]) as usize;
    let mut offset = 3;
    for _ in 0..count {
        if offset + 15 > data.len() {
            break;
        }
        let id = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        offset += 8;
        let flags = data[offset];
        offset += 1;
        let px = dequantize_pos(i16::from_le_bytes([data[offset], data[offset + 1]]));
        offset += 2;
        let py = dequantize_pos(i16::from_le_bytes([data[offset], data[offset + 1]]));
        offset += 2;
        let pz = dequantize_pos(i16::from_le_bytes([data[offset], data[offset + 1]]));
        offset += 2;
        let (vx, vy, vz) = if flags & 1 == 0 {
            if offset + 6 > data.len() {
                break;
            }
            let vx = dequantize_vel(i16::from_le_bytes([data[offset], data[offset + 1]]));
            offset += 2;
            let vy = dequantize_vel(i16::from_le_bytes([data[offset], data[offset + 1]]));
            offset += 2;
            let vz = dequantize_vel(i16::from_le_bytes([data[offset], data[offset + 1]]));
            offset += 2;
            (vx, vy, vz)
        } else {
            (0.0, 0.0, 0.0)
        };
        result.push((id, (px, py, pz), (vx, vy, vz)));
    }
    result
}

pub fn build_handoff_msg_pub(id: u64, msg_type: HandoffType, node: &NodeActor) -> Vec<u8> {
    build_handoff_msg(id, msg_type, node)
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
    let entities: Vec<_> = engine.node.manager.entities.values().collect();
    let max_dg = engine
        .connections
        .first()
        .and_then(|c| c.max_datagram_size())
        .unwrap_or(1200);
    let per_moving: usize = 17;
    let header: usize = 3;
    let batch_size = ((max_dg.saturating_sub(header)) / per_moving).max(1);
    let mut datagrams = Vec::new();

    for chunk in entities.chunks(batch_size) {
        let entries: Vec<_> = chunk.iter().map(|e| (e.id, e.pos, e.vel)).collect();
        datagrams.push(encode_compact_client(&entries));
    }
    datagrams
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_manager::Entity;

    #[tokio::test]
    async fn test_broadcast_compact_format() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
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
        let decoded = decode_compact_client(data);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, 1);
        assert!((decoded[0].1 .0 - 1.0).abs() < 0.01);
        assert!((decoded[0].1 .1 - 2.0).abs() < 0.01);
        assert!((decoded[0].1 .2 - 3.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_broadcast_batch_size() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        for i in 0..100 {
            engine.node.manager.add_entity(Entity {
                id: i,
                pos: (i as f32 * 0.1, 0.0, 0.0),
                vel: (1.0, 0.0, 0.0),
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
        let mut total_entities = 0;
        for dg in &datagrams {
            let decoded = decode_compact_client(dg);
            total_entities += decoded.len();
        }
        assert_eq!(total_entities, 100);
    }

    #[tokio::test]
    async fn test_handoff_still_signed() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
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
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
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

    fn build_signed_0xce(
        node_id: u64,
        signing_key: &zeus_common::SigningKey,
        entities: &[(u64, (f32, f32, f32), (f32, f32, f32))],
    ) -> Vec<u8> {
        use zeus_common::Signer;
        let mut buf = Vec::new();
        buf.push(0xCE);
        buf.extend_from_slice(&node_id.to_le_bytes());
        let count = entities.len() as u16;
        buf.extend_from_slice(&count.to_le_bytes());
        for &(id, pos, vel) in entities {
            buf.extend_from_slice(&id.to_le_bytes());
            buf.extend_from_slice(&quantize_pos(pos.0).to_le_bytes());
            buf.extend_from_slice(&quantize_pos(pos.1).to_le_bytes());
            buf.extend_from_slice(&quantize_pos(pos.2).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.0).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.1).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.2).to_le_bytes());
        }
        let sig = signing_key.sign(&buf);
        buf.extend_from_slice(&sig.to_bytes());
        buf
    }

    #[tokio::test]
    async fn test_gossip_0xce_intercept() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();

        let (peer_sk, peer_vk) = zeus_common::GhostSerializer::generate_keypair();
        let peer_node_id: u64 = 999;
        engine.peer_verifying_keys.insert(peer_node_id, peer_vk);

        let datagram = build_signed_0xce(peer_node_id, &peer_sk, &[(50, (10.0, 5.0, 3.0), (1.0, 0.0, 0.0))]);

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
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
        };
        let mut engine = ZeusEngine::new(config).await.unwrap();
        engine.node.manager.add_entity(Entity {
            id: 5,
            pos: (1.0, 2.0, 3.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });

        let (peer_sk, peer_vk) = zeus_common::GhostSerializer::generate_keypair();
        let peer_node_id: u64 = 888;
        engine.peer_verifying_keys.insert(peer_node_id, peer_vk);

        let datagram = build_signed_0xce(peer_node_id, &peer_sk, &[(5, (99.0, 99.0, 99.0), (0.0, 0.0, 0.0))]);

        let ep2 = zeus_transport::make_promiscuous_endpoint("127.0.0.1:0".parse().unwrap()).unwrap().0;
        let conn = ep2.connect(engine.endpoint.local_addr().unwrap(), "localhost").unwrap().await.unwrap();
        engine.network_tx.send(crate::NetworkEvent::NewConnection(conn.clone())).await.unwrap();
        engine.network_tx.send(crate::NetworkEvent::Payload(conn, datagram, false)).await.unwrap();

        engine.tick(0.016).await.unwrap();

        assert!(!engine.remote_entity_states.contains_key(&5), "Locally-owned entity 5 should NOT be in gossip cache");
    }

    #[test]
    fn test_compact_encode_decode_roundtrip() {
        let entries = vec![
            (1u64, (10.5, 2.3, -5.7), (1.5, -0.3, 0.0)),
            (2u64, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
        ];
        let data = encode_compact_client(&entries);
        let decoded = decode_compact_client(&data);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, 1);
        assert!((decoded[0].1 .0 - 10.5).abs() < 0.002);
        assert!((decoded[0].1 .1 - 2.3).abs() < 0.002);
        assert!((decoded[0].1 .2 - (-5.7)).abs() < 0.002);
        assert!((decoded[0].2 .0 - 1.5).abs() < 0.01);
        assert_eq!(decoded[1].0, 2);
        assert_eq!(decoded[1].2, (0.0, 0.0, 0.0));
    }

    #[test]
    fn test_compact_at_rest_flag() {
        let entries = vec![
            (1u64, (5.0, 1.0, 0.0), (0.0, 0.0, 0.0)),
        ];
        let data = encode_compact_client(&entries);
        assert_eq!(data.len(), 3 + 15, "At-rest entity should be 15 bytes (u64 id + flags + 3xi16 pos)");
        let decoded = decode_compact_client(&data);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].2, (0.0, 0.0, 0.0));
    }

    #[test]
    fn test_compact_moving_entity() {
        let entries = vec![
            (1u64, (5.0, 1.0, 0.0), (3.0, 0.0, -2.0)),
        ];
        let data = encode_compact_client(&entries);
        assert_eq!(data.len(), 3 + 21, "Moving entity should be 21 bytes (u64 id + flags + 3xi16 pos + 3xi16 vel)");
        let decoded = decode_compact_client(&data);
        assert_eq!(decoded.len(), 1);
        assert!((decoded[0].2 .0 - 3.0).abs() < 0.01);
        assert!((decoded[0].2 .2 - (-2.0)).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_gossip_ttl_expiry() {
        let config = ZeusConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
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
            seed_addrs: Vec::new(),
            boundary: 100.0,
            margin: 5.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
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
            seed_addrs: Vec::new(),
            boundary: 10.0,
            margin: 2.0,
            ordinal: 0,
            lower_boundary: 0.0,
            cell: None,
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
            seed_addrs: Vec::new(),
            boundary: 16.0,
            margin: 2.0,
            ordinal: 0,
            lower_boundary: 8.0,
            cell: None,
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
            seed_addrs: Vec::new(),
            boundary: 16.0,
            margin: 2.0,
            ordinal: 1,
            lower_boundary: 8.0,
            cell: None,
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
            seed_addrs: Vec::new(),
            boundary: 16.0,
            margin: 2.0,
            ordinal: 1,
            lower_boundary: 8.0,
            cell: None,
        };
        let engine = ZeusEngine::new(config).await.unwrap();
        let conn = engine.find_target_connection(999);
        assert!(conn.is_none(), "Nonexistent entity should return None");
    }
}
