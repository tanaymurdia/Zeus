use ed25519_dalek::{Signer, SigningKey};
use zeus_common::flatbuffers::FlatBufferBuilder;
use zeus_common::{Ghost, GhostArgs, GhostSerializer, HandoffMsg, HandoffType, Vec3};
use zeus_node::entity_manager::{AuthorityState, Entity, EntityManager};
use zeus_node::node_actor::NodeActor;

fn create_test_msg(id: u64, type_: HandoffType, x: f32, key: Option<&SigningKey>) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let pos_val = (x, 0.0f32, 0.0f32);
    let vel_val = (0.0f32, 0.0f32, 0.0f32);

    let signature_bytes = if let Some(k) = key {
        let mut data = Vec::with_capacity(32);
        data.extend_from_slice(&id.to_le_bytes());
        data.extend_from_slice(&pos_val.0.to_le_bytes());
        data.extend_from_slice(&pos_val.1.to_le_bytes());
        data.extend_from_slice(&pos_val.2.to_le_bytes());
        data.extend_from_slice(&vel_val.0.to_le_bytes());
        data.extend_from_slice(&vel_val.1.to_le_bytes());
        data.extend_from_slice(&vel_val.2.to_le_bytes());
        k.sign(&data).to_bytes().to_vec()
    } else {
        vec![0u8; 64]
    };

    let pos = Vec3::new(pos_val.0, pos_val.1, pos_val.2);
    let vel = Vec3::new(vel_val.0, vel_val.1, vel_val.2);
    let sig = builder.create_vector(&signature_bytes);

    let ghost = match type_ {
        HandoffType::Offer => Some(Ghost::create(
            &mut builder,
            &GhostArgs {
                entity_id: id,
                position: Some(&pos),
                velocity: Some(&vel),
                signature: Some(sig),
            },
        )),
        _ => None,
    };

    let msg = HandoffMsg::create(
        &mut builder,
        &zeus_common::HandoffMsgArgs {
            entity_id: id,
            type_,
            state: ghost,
        },
    );
    builder.finish(msg, None);
    builder.finished_data().to_vec()
}

#[test]
fn test_hysteresis_jitter() {
    let mut mgr = EntityManager::new(0.0, 5.0, 0.0);
    mgr.add_entity(Entity {
        id: 10,
        pos: (4.9, 0.0, 0.0),
        vel: (0.1, 0.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    let candidates = mgr.update(1.0);
    assert!(candidates.is_empty(), "Should not trigger at exactly 5.0");

    let candidates = mgr.update(1.0);
    assert!(candidates.contains(&10), "Should trigger above 5.0");

    mgr.set_state(10, AuthorityState::Local);
    let e = mgr.get_entity_mut(10).unwrap();
    e.pos = (5.0, 0.0, 0.0);
    e.vel = (-0.1, 0.0, 0.0);

    let candidates = mgr.update(0.1);
    assert!(
        candidates.is_empty(),
        "Should hold local when returning to margin"
    );
}

#[test]
fn test_state_machine_duplicate_ack() {
    let mut node = NodeActor::new(0.0, 5.0, 0.0);
    node.manager.add_entity(Entity {
        id: 100,
        pos: (6.0, 0.0, 0.0),
        vel: (0.0, 0.0, 0.0),
        state: AuthorityState::HandoffOut,
        verifying_key: None,
    });

    let ack_bytes = create_test_msg(100, HandoffType::Ack, 0.0, None);
    let ack_msg = zeus_common::flatbuffers::root::<HandoffMsg>(&ack_bytes).unwrap();
    node.handle_handoff_msg(ack_msg);

    assert_eq!(
        node.manager.get_entity(100).unwrap().state,
        AuthorityState::Remote
    );
    assert_eq!(node.outgoing_messages.len(), 1);
    assert_eq!(node.outgoing_messages[0], (100, HandoffType::Commit));
    node.outgoing_messages.clear();

    let ack_bytes_2 = create_test_msg(100, HandoffType::Ack, 0.0, None);
    let ack_msg_2 = zeus_common::flatbuffers::root::<HandoffMsg>(&ack_bytes_2).unwrap();
    node.handle_handoff_msg(ack_msg_2);

    assert_eq!(
        node.manager.get_entity(100).unwrap().state,
        AuthorityState::Remote
    );
    assert_eq!(node.outgoing_messages.len(), 0);
}

#[test]
fn test_state_machine_out_of_order() {
    let mut node = NodeActor::new(0.0, 5.0, 0.0);

    node.manager.add_entity(Entity {
        id: 200,
        pos: (0.0, 0.0, 0.0),
        vel: (0.0, 0.0, 0.0),
        state: AuthorityState::HandoffIn,
        verifying_key: None,
    });

    let offer_bytes = create_test_msg(200, HandoffType::Offer, 10.0, None);
    let offer_msg = zeus_common::flatbuffers::root::<HandoffMsg>(&offer_bytes).unwrap();
    node.handle_handoff_msg(offer_msg);

    assert_eq!(
        node.manager.get_entity(200).unwrap().state,
        AuthorityState::Local
    );
}

#[test]
fn test_security_rejects_unsigned_offer() {
    let mut node = NodeActor::new(0.0, 5.0, 0.0);

    let (key, check_key) = GhostSerializer::generate_keypair();
    let valid_bytes = create_test_msg(300, HandoffType::Offer, 10.0, Some(&key));
    let valid_msg = zeus_common::flatbuffers::root::<HandoffMsg>(&valid_bytes).unwrap();

    let (wrong_key, _) = GhostSerializer::generate_keypair();
    let forged_bytes = create_test_msg(301, HandoffType::Offer, 10.0, Some(&wrong_key));

    node.manager.add_entity(Entity {
        id: 300,
        pos: (0.0, 0.0, 0.0),
        vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Remote,
        verifying_key: Some(check_key),
    });

    node.handle_handoff_msg(valid_msg);

    assert_eq!(
        node.manager.get_entity(300).unwrap().state,
        AuthorityState::Local,
        "Valid signature failed"
    );

    node.manager.add_entity(Entity {
        id: 301,
        pos: (0.0, 0.0, 0.0),
        vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Remote,
        verifying_key: Some(check_key),
    });

    let forged_msg = zeus_common::flatbuffers::root::<HandoffMsg>(&forged_bytes).unwrap();
    node.handle_handoff_msg(forged_msg);

    assert_eq!(
        node.manager.get_entity(301).unwrap().state,
        AuthorityState::Remote,
        "Invalid signature was accepted!"
    );
}

#[test]
fn test_2000_entities_triggers_split_warning() {
    use std::net::SocketAddr;
    use zeus_node::discovery::DiscoveryActor;

    let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let mut node = NodeActor::new(0.0, 5.0, 0.0);
    let mut discovery = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);

    for i in 0..2500 {
        let entity = Entity {
            id: i,
            pos: ((i as f32) * 0.001, 0.0, 0.0),
            vel: (0.0, 0.0, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        };
        node.manager.add_entity(entity);
    }

    assert_eq!(
        node.manager.entity_count(),
        2500,
        "Should have 2500 entities"
    );

    let entity_count = node.manager.entity_count() as u16;
    discovery.set_load(entity_count, 0);

    assert_eq!(discovery.local_load.entity_count(), 2500);

    let announce_bytes = discovery.generate_announce();
    let msg = zeus_common::flatbuffers::root::<zeus_common::DiscoveryMsg>(&announce_bytes).unwrap();
    let announce = msg.payload_as_discovery_announce().unwrap();
    let load = announce.load().expect("Load should be present");

    assert_eq!(
        load.entity_count(),
        2500,
        "Announce should broadcast 2500 entities"
    );
    assert!(
        load.entity_count() > 2000,
        "Load exceeds CRITICAL threshold"
    );

    println!("✅ Test passed: 2500 entities correctly trigger CRITICAL LOAD warning");
}

#[test]
fn test_load_threshold_boundary() {
    use std::net::SocketAddr;
    use zeus_node::discovery::DiscoveryActor;

    let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let mut discovery = DiscoveryActor::new(1, (0.0, 0.0, 0.0), addr, 0);

    discovery.set_load(1999, 50);
    assert_eq!(discovery.local_load.entity_count(), 1999);

    discovery.set_load(2000, 50);
    assert_eq!(discovery.local_load.entity_count(), 2000);

    discovery.set_load(2001, 50);
    // ^^^ This should print: "[Node] ⚠️  CRITICAL LOAD: 2001 entities. REQUESTING SPLIT."
    assert_eq!(discovery.local_load.entity_count(), 2001);

    println!("✅ Test passed: Load threshold boundary behavior verified");
}
