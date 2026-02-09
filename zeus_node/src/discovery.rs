use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Instant, Duration};
use zeus_common::{DiscoveryMsg, DiscoveryAnnounce, DiscoveryAnnounceArgs, Vec3, NodeLoad};
use zeus_common::flatbuffers::FlatBufferBuilder;

const LOAD_CRITICAL_THRESHOLD: u16 = 2000;

pub struct Peer {
    pub id: u64,
    pub addr: SocketAddr,
    pub pos: (f32, f32, f32),
    pub load: Option<NodeLoad>,
    pub last_seen: Instant,
}

pub struct DiscoveryActor {
    pub peers: HashMap<u64, Peer>,
    pub local_id: u64,
    pub local_pos: (f32, f32, f32),
    pub local_addr: SocketAddr,
    pub local_load: NodeLoad,
    last_warned_milestone: u16, // Tracks last milestone we warned at
}

impl DiscoveryActor {
    pub fn new(id: u64, pos: (f32, f32, f32), addr: SocketAddr) -> Self {
        Self {
            peers: HashMap::new(),
            local_id: id,
            local_pos: pos,
            local_addr: addr,
            local_load: NodeLoad::new(0, 0),
            last_warned_milestone: 0,
        }
    }

    pub fn set_load(&mut self, entity_count: u16, cpu_percent: u8) {
        self.local_load = NodeLoad::new(entity_count, cpu_percent);
        
        // Only warn at milestones (2000, 3000, 4000, etc) to avoid spam
        if entity_count > LOAD_CRITICAL_THRESHOLD {
            let milestone = (entity_count / 1000) * 1000;
            if milestone > self.last_warned_milestone {
                println!("[Node] ⚠️  CRITICAL LOAD: {} entities. REQUESTING SPLIT.", entity_count);
                self.last_warned_milestone = milestone;
            }
        }
    }

    pub fn update(&mut self, _dt: f32) {
        // Remove stale peers (e.g. > 10s timeout)
        let now = Instant::now();
        self.peers.retain(|_, p| now.duration_since(p.last_seen) < Duration::from_secs(10));
    }

    pub fn process_packet(&mut self, msg: DiscoveryMsg, src_addr: SocketAddr) {
         match msg.payload_as_discovery_announce() {
             Some(announce) => {
                 let id = announce.node_id();
                 // Self-loop check
                 if id == self.local_id { return; }

                 let p = announce.position();
                 let pos = if let Some(p) = p { (p.x(), p.y(), p.z()) } else { (0.0, 0.0, 0.0) };
                 
                 let load = announce.load();
                 
                 // Check if peer is overloaded
                 if let Some(l) = load {
                     if l.entity_count() > LOAD_CRITICAL_THRESHOLD {
                         println!("[Mesh] ⚠️  ADVISING SPLIT for Node {} (Load: {} entities)", id, l.entity_count());
                     }
                 }
                 
                 self.peers.insert(id, Peer {
                     id,
                     addr: src_addr,
                     pos,
                     load: load.copied(),
                     last_seen: Instant::now(),
                 });
             }
             None => {}
         }
    }

    pub fn generate_announce(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        
        let addr_str = self.local_addr.to_string();
        let addr = builder.create_string(&addr_str);
        
        let pos = Vec3::new(self.local_pos.0, self.local_pos.1, self.local_pos.2);
        
        let announce = DiscoveryAnnounce::create(&mut builder, &DiscoveryAnnounceArgs {
            node_id: self.local_id,
            address: Some(addr),
            position: Some(&pos),
            load: Some(&self.local_load),
        });
        
        let msg = DiscoveryMsg::create(&mut builder, &zeus_common::DiscoveryMsgArgs {
            payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
            payload: Some(announce.as_union_value()),
        });
        
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
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr);
        
        // 1. Generate Announce from "Remote" (simulated by manual construction or self-gen with ID swap)
        // Let's manually construct a discovery msg for Node 2.
        let mut builder = FlatBufferBuilder::new();
        let addr_str = "127.0.0.1:9090";
        let addr_off = builder.create_string(addr_str);
        let pos = Vec3::new(10.0, 0.0, 0.0);
        
        let announce = DiscoveryAnnounce::create(&mut builder, &DiscoveryAnnounceArgs {
            node_id: 2,
            address: Some(addr_off),
            position: Some(&pos),
            load: None,
        });
        
        let msg = DiscoveryMsg::create(&mut builder, &zeus_common::DiscoveryMsgArgs {
            payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
            payload: Some(announce.as_union_value()),
        });
        builder.finish(msg, None);
        let buf = builder.finished_data().to_vec(); // Copy to simulate network
        
        // 2. Process Packet
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor.process_packet(root, src);
        
        // Verify Peer Added
        assert_eq!(actor.peers.len(), 1);
        let peer = actor.peers.get(&2).unwrap();
        assert_eq!(peer.addr, src);
        assert_eq!(peer.pos, (10.0, 0.0, 0.0));
    }

    #[test]
    fn test_peer_expiration() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr);
        
        // Add fake peer manually with old timestamp
        actor.peers.insert(99, Peer {
            id: 99,
            addr, // doesn't matter
            pos: (0.0, 0.0, 0.0),
            load: None,
            last_seen: Instant::now() - Duration::from_secs(11), // > 10s limit
        });
        
        // Add fresh peer
        actor.peers.insert(100, Peer {
            id: 100,
            addr,
            pos: (0.0, 0.0, 0.0),
            load: None,
            last_seen: Instant::now(),
        });

        assert_eq!(actor.peers.len(), 2);
        
        // Update
        actor.update(1.0);
        
        // Verify expiration
        assert_eq!(actor.peers.len(), 1);
        assert!(actor.peers.contains_key(&100));
        assert!(!actor.peers.contains_key(&99));
    }

    #[test]
    fn test_load_broadcasting() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr);
        
        // 1. Set Load Below Threshold (should NOT trigger warning)
        actor.set_load(1500, 50);
        assert_eq!(actor.local_load.entity_count(), 1500);
        assert_eq!(actor.local_load.cpu_percent(), 50);
        
        // 2. Set Load Above Threshold (should trigger warning in stdout)
        actor.set_load(2500, 80);
        assert_eq!(actor.local_load.entity_count(), 2500);
        
        // 3. Generate Announce and Verify Load is Included
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
        let mut actor = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr);
        
        // Build announce from overloaded peer
        let mut builder = FlatBufferBuilder::new();
        let addr_off = builder.create_string("127.0.0.1:9090");
        let pos = Vec3::new(10.0, 0.0, 0.0);
        let load = NodeLoad::new(2500, 90); // OVER threshold
        
        let announce = DiscoveryAnnounce::create(&mut builder, &DiscoveryAnnounceArgs {
            node_id: 99,
            address: Some(addr_off),
            position: Some(&pos),
            load: Some(&load),
        });
        
        let msg = DiscoveryMsg::create(&mut builder, &zeus_common::DiscoveryMsgArgs {
            payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
            payload: Some(announce.as_union_value()),
        });
        builder.finish(msg, None);
        let buf = builder.finished_data().to_vec();
        
        // Process (this should log [Mesh] ADVISING SPLIT)
        let root = zeus_common::flatbuffers::root::<DiscoveryMsg>(&buf).unwrap();
        let src: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        actor.process_packet(root, src);
        
        // Verify peer stored with load
        let peer = actor.peers.get(&99).expect("Peer should exist");
        let peer_load = peer.load.expect("Peer load should be present");
        assert_eq!(peer_load.entity_count(), 2500);
    }
}
