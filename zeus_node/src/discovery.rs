use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use zeus_common::flatbuffers::FlatBufferBuilder;
use zeus_common::{DiscoveryAnnounce, DiscoveryAnnounceArgs, DiscoveryMsg, NodeLoad, Vec3};

const LOAD_CRITICAL_THRESHOLD: u16 = 2000;

pub struct Peer {
    pub id: u64,
    pub addr: SocketAddr,
    pub pos: (f32, f32, f32),
    pub load: Option<NodeLoad>,
    pub last_seen: Instant,
    pub ordinal: u32,
}

pub struct DiscoveryActor {
    pub peers: HashMap<u64, Peer>,
    pub local_id: u64,
    pub local_pos: (f32, f32, f32),
    pub local_addr: SocketAddr,
    pub local_load: NodeLoad,
    pub known_node_ids: HashSet<u64>,
    pub local_ordinal: u32,
    last_warned_milestone: u16,
}

impl DiscoveryActor {
    pub fn new(id: u64, pos: (f32, f32, f32), addr: SocketAddr, ordinal: u32) -> Self {
        let mut known = HashSet::new();
        known.insert(id);
        Self {
            peers: HashMap::new(),
            local_id: id,
            local_pos: pos,
            local_addr: addr,
            local_load: NodeLoad::new(0, 0),
            known_node_ids: known,
            local_ordinal: ordinal,
            last_warned_milestone: 0,
        }
    }

    pub fn total_node_count(&self) -> usize {
        self.known_node_ids.len()
    }

    pub fn set_load(&mut self, entity_count: u16, cpu_percent: u8) {
        self.local_load = NodeLoad::new(entity_count, cpu_percent);
        if entity_count > LOAD_CRITICAL_THRESHOLD {
            let milestone = (entity_count / 1000) * 1000;
            if milestone > self.last_warned_milestone {
                println!(
                    "[Node] ⚠️  CRITICAL LOAD: {} entities. REQUESTING SPLIT.",
                    entity_count
                );
                self.last_warned_milestone = milestone;
            }
        }
    }

    pub fn update(&mut self, _dt: f32) {
        let now = Instant::now();
        self.peers
            .retain(|_, p| now.duration_since(p.last_seen) < Duration::from_secs(10));
    }

    pub fn process_packet(&mut self, msg: DiscoveryMsg, src_addr: SocketAddr) {
        match msg.payload_as_discovery_announce() {
            Some(announce) => {
                let id = announce.node_id();
                if id == self.local_id {
                    return;
                }

                let p = announce.position();
                let pos = if let Some(p) = p {
                    (p.x(), p.y(), p.z())
                } else {
                    (0.0, 0.0, 0.0)
                };
                let load = announce.load();
                if let Some(l) = load {
                    if l.entity_count() > LOAD_CRITICAL_THRESHOLD {
                        println!(
                            "[Mesh] ⚠️  ADVISING SPLIT for Node {} (Load: {} entities)",
                            id,
                            l.entity_count()
                        );
                    }
                }

                let ordinal = announce.ordinal();

                if let Some(ids) = announce.known_node_ids() {
                    for i in 0..ids.len() {
                        self.known_node_ids.insert(ids.get(i));
                    }
                }
                self.known_node_ids.insert(id);

                self.peers.insert(
                    id,
                    Peer {
                        id,
                        addr: src_addr,
                        pos,
                        load: load.copied(),
                        last_seen: Instant::now(),
                        ordinal,
                    },
                );
            }
            None => {}
        }
    }

    pub fn generate_announce(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();

        let addr_str = self.local_addr.to_string();
        let addr = builder.create_string(&addr_str);

        let ids_vec: Vec<u64> = self.known_node_ids.iter().copied().collect();
        let known_ids = builder.create_vector(&ids_vec);

        let pos = Vec3::new(self.local_pos.0, self.local_pos.1, self.local_pos.2);

        let announce = DiscoveryAnnounce::create(
            &mut builder,
            &DiscoveryAnnounceArgs {
                node_id: self.local_id,
                address: Some(addr),
                position: Some(&pos),
                load: Some(&self.local_load),
                known_node_ids: Some(known_ids),
                ordinal: self.local_ordinal,
            },
        );

        let msg = DiscoveryMsg::create(
            &mut builder,
            &zeus_common::DiscoveryMsgArgs {
                payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
                payload: Some(announce.as_union_value()),
            },
        );

        builder.finish(msg, None);
        builder.finished_data().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_management() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);

        let mut builder = FlatBufferBuilder::new();
        let addr_str = "127.0.0.1:9090";
        let addr_off = builder.create_string(addr_str);
        let pos = Vec3::new(10.0, 0.0, 0.0);

        let announce = DiscoveryAnnounce::create(
            &mut builder,
            &DiscoveryAnnounceArgs {
                node_id: 2,
                address: Some(addr_off),
                position: Some(&pos),
                load: None,
                known_node_ids: None,
                ordinal: 0,
            },
        );

        let msg = DiscoveryMsg::create(
            &mut builder,
            &zeus_common::DiscoveryMsgArgs {
                payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
                payload: Some(announce.as_union_value()),
            },
        );
        builder.finish(msg, None);
        let buf = builder.finished_data().to_vec();

        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor.process_packet(root, src);
        assert_eq!(actor.peers.len(), 1);
        let peer = actor.peers.get(&2).unwrap();
        assert_eq!(peer.addr, src);
        assert_eq!(peer.pos, (10.0, 0.0, 0.0));
    }

    #[test]
    fn test_peer_expiration() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);

        actor.peers.insert(
            99,
            Peer {
                id: 99,
                addr,
                pos: (0.0, 0.0, 0.0),
                load: None,
                last_seen: Instant::now() - Duration::from_secs(11),
                ordinal: 0,
            },
        );
        actor.peers.insert(
            100,
            Peer {
                id: 100,
                addr,
                pos: (0.0, 0.0, 0.0),
                load: None,
                last_seen: Instant::now(),
                ordinal: 0,
            },
        );

        assert_eq!(actor.peers.len(), 2);
        actor.update(1.0);

        assert_eq!(actor.peers.len(), 1);
        assert!(actor.peers.contains_key(&100));
        assert!(!actor.peers.contains_key(&99));
    }

    #[test]
    fn test_load_broadcasting() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);
        actor.set_load(1500, 50);
        assert_eq!(actor.local_load.entity_count(), 1500);
        assert_eq!(actor.local_load.cpu_percent(), 50);
        actor.set_load(2500, 80);
        assert_eq!(actor.local_load.entity_count(), 2500);
        let announce_bytes = actor.generate_announce();
        let msg = zeus_common::flatbuffers::root::<DiscoveryMsg>(&announce_bytes).unwrap();
        let announce = msg.payload_as_discovery_announce().unwrap();

        let load = announce.load().expect("Load should be present in announce");
        assert_eq!(load.entity_count(), 2500);
        assert_eq!(load.cpu_percent(), 80);
    }

    #[test]
    fn test_load_detection_from_peer() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);
        let mut builder = FlatBufferBuilder::new();
        let addr_off = builder.create_string("127.0.0.1:9090");
        let pos = Vec3::new(10.0, 0.0, 0.0);
        let load = NodeLoad::new(2500, 90);

        let announce = DiscoveryAnnounce::create(
            &mut builder,
            &DiscoveryAnnounceArgs {
                node_id: 99,
                address: Some(addr_off),
                position: Some(&pos),
                load: Some(&load),
                known_node_ids: None,
                ordinal: 0,
            },
        );

        let msg = DiscoveryMsg::create(
            &mut builder,
            &zeus_common::DiscoveryMsgArgs {
                payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
                payload: Some(announce.as_union_value()),
            },
        );
        builder.finish(msg, None);
        let buf = builder.finished_data().to_vec();
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor.process_packet(root, src);
        let peer = actor.peers.get(&99).expect("Peer should exist");
        let peer_load = peer.load.expect("Peer load should be present");
        assert_eq!(peer_load.entity_count(), 2500);
    }

    fn make_announce_bytes(node_id: u64, addr_str: &str, known_ids: &[u64], ordinal: u32) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let addr_off = builder.create_string(addr_str);
        let ids = builder.create_vector(known_ids);
        let pos = Vec3::new(0.0, 0.0, 0.0);
        let announce = DiscoveryAnnounce::create(
            &mut builder,
            &DiscoveryAnnounceArgs {
                node_id,
                address: Some(addr_off),
                position: Some(&pos),
                load: None,
                known_node_ids: Some(ids),
                ordinal,
            },
        );
        let msg = DiscoveryMsg::create(
            &mut builder,
            &zeus_common::DiscoveryMsgArgs {
                payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
                payload: Some(announce.as_union_value()),
            },
        );
        builder.finish(msg, None);
        builder.finished_data().to_vec()
    }

    #[test]
    fn test_known_node_ids_merge() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor_a = DiscoveryActor::new(100, (0.0, 0.0, 0.0), addr, 0);
        assert_eq!(actor_a.known_node_ids.len(), 1);
        assert!(actor_a.known_node_ids.contains(&100));

        let buf = make_announce_bytes(200, "127.0.0.1:9090", &[200], 1);
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor_a.process_packet(root, src);

        assert_eq!(actor_a.known_node_ids.len(), 2);
        assert!(actor_a.known_node_ids.contains(&100));
        assert!(actor_a.known_node_ids.contains(&200));
    }

    #[test]
    fn test_known_node_ids_transitive() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor_c = DiscoveryActor::new(300, (0.0, 0.0, 0.0), addr, 2);
        assert_eq!(actor_c.total_node_count(), 1);

        let buf = make_announce_bytes(200, "127.0.0.1:9090", &[100, 200], 1);
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor_c.process_packet(root, src);

        assert_eq!(actor_c.total_node_count(), 3);
        assert!(actor_c.known_node_ids.contains(&100));
        assert!(actor_c.known_node_ids.contains(&200));
        assert!(actor_c.known_node_ids.contains(&300));
    }

    #[test]
    fn test_ordinal_stored_in_peer() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);

        let buf = make_announce_bytes(2, "127.0.0.1:9090", &[2], 2);
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor.process_packet(root, src);

        let peer = actor.peers.get(&2).unwrap();
        assert_eq!(peer.ordinal, 2);
    }

    #[test]
    fn test_total_node_count() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);
        assert_eq!(actor.total_node_count(), 1);

        let buf = make_announce_bytes(2, "127.0.0.1:9090", &[2, 3, 4], 1);
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor.process_packet(root, src);

        assert_eq!(actor.total_node_count(), 4);
    }
}
