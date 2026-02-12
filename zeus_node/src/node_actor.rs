use crate::entity_manager::{AuthorityState, Entity, EntityManager};
use std::collections::VecDeque;
use zeus_common::{HandoffMsg, HandoffType};

pub struct NodeActor {
    pub manager: EntityManager,
    pub outgoing_messages: VecDeque<(u64, HandoffType)>,
}

impl NodeActor {
    pub fn new(boundary: f32, margin: f32) -> Self {
        Self {
            manager: EntityManager::new(boundary, margin),
            outgoing_messages: VecDeque::new(),
        }
    }

    pub fn update(&mut self, dt: f32) {
        let candidates = self.manager.update(dt);

        for id in candidates {
            self.manager.set_state(id, AuthorityState::HandoffOut);
            self.outgoing_messages.push_back((id, HandoffType::Offer));
            // println!("[Node] Entity {} crossing boundary. Sending OFFER.", id);
        }
    }

    pub fn handle_handoff_msg(&mut self, msg: HandoffMsg) {
        let id = msg.entity_id();
        let (current_state, known_key) = if let Some(e) = self.manager.get_entity(id) {
            (Some(e.state.clone()), e.verifying_key)
        } else {
            (None, None)
        };

        match msg.type_() {
            HandoffType::Offer => {
                let is_new = self.manager.get_entity(id).is_none();
                let is_local = matches!(current_state, Some(AuthorityState::Local));

                if let Some(ghost) = msg.state() {
                    // VERIFY SIGNATURE logic (omitted for brevity in replacement, assuming strictly verifying if key exists)
                    if let Some(key) = known_key {
                        if !zeus_common::verify_signature(ghost, &key) {
                            if is_new {
                                println!(
                                    "[Node] SECURITY WARNING: Signature failed for {}. Dropping.",
                                    id
                                );
                            }
                            return;
                        }
                    } else {
                        // if is_new { println!("[Server] [+] Player {} JOINED (New Connection).", id); }
                    }

                    let pos = ghost.position().unwrap();
                    let vel = ghost.velocity().unwrap();

                    if is_local {
                        // CLIENT UPDATE: Just update physics, don't change state or ACK
                        let mut e = self.manager.get_entity(id).unwrap().clone();
                        e.pos = (pos.x(), pos.y(), pos.z());
                        e.vel = (vel.x(), vel.y(), vel.z());
                        self.manager.add_entity(e); // Updates existing
                    // No ACK for client updates
                    } else {
                        let entity = Entity {
                            id,
                            pos: (pos.x(), pos.y(), pos.z()),
                            vel: (vel.x(), vel.y(), vel.z()),
                            state: AuthorityState::Local, // Treat new TOFU inputs as Local immediately for this Demo
                            verifying_key: known_key,
                        };
                        self.manager.add_entity(entity);
                        // self.outgoing_messages.push_back((id, HandoffType::Ack)); // DISABLE ACK FOR DEMO to prevent blocking on Client stream
                    }
                } else {
                    println!("[Node] Received Offer for {} but no state attached", id);
                }
            }
            HandoffType::Ack => {
                // println!("[Node] Received ACK for Entity {}", id);
                if let Some(AuthorityState::HandoffOut) = current_state {
                    self.manager.set_state(id, AuthorityState::Remote);
                    self.outgoing_messages.push_back((id, HandoffType::Commit));
                    // println!("[Server] [-] Player {} DEPARTED (Handoff Complete).", id);
                }
            }
            HandoffType::Commit => {
                // println!("[Node] Received COMMIT for Entity {}", id);
                if let Some(AuthorityState::HandoffIn) = current_state {
                    self.manager.set_state(id, AuthorityState::Local);
                    // println!("[Server] [+] Player {} ARRIVED from Neighbor.", id);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeus_common::{GhostArgs, Vec3};

    // Helper to create dummy HandoffMsg
    fn _create_msg(id: u64, type_: HandoffType) -> HandoffMsg<'static> {
        // We can't easily construct a Flatbuffers table owning its data in a simple function return without leaking builder
        // or using a specific test setup.
        // Instead, we will Mock the message handling by manually triggering the logic if possible,
        // OR we just construct the bytes and parse it.
        // Let's assume we can construct bytes.
        let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
        let pos = Vec3::new(100.0, 0.0, 0.0);
        let vel = Vec3::new(0.0, 0.0, 0.0);
        let sig = builder.create_vector(&[0u8; 64]);
        let ghost = zeus_common::Ghost::create(
            &mut builder,
            &GhostArgs {
                entity_id: id,
                position: Some(&pos),
                velocity: Some(&vel),
                signature: Some(sig),
            },
        );

        let msg = zeus_common::HandoffMsg::create(
            &mut builder,
            &zeus_common::HandoffMsgArgs {
                entity_id: id,
                type_,
                state: Some(ghost),
            },
        );
        builder.finish(msg, None);
        let _bytes = builder.finished_data().to_vec();

        // This is tricky because HandoffMsg<'a> refers to buffer.
        // We need the test to hold the buffer.
        // So we will just do this inside the test.
        unreachable!()
    }

    #[test]
    fn test_node_actor_handoff_flow() {
        let mut node = NodeActor::new(0.0, 5.0);

        // 1. Receive OFFER for new Entity 99
        let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
        let pos = Vec3::new(100.0, 0.0, 0.0);
        let vel = Vec3::new(0.0, 0.0, 0.0);
        let sig = builder.create_vector(&[0u8; 64]);
        let ghost = zeus_common::Ghost::create(
            &mut builder,
            &GhostArgs {
                entity_id: 99,
                position: Some(&pos),
                velocity: Some(&vel),
                signature: Some(sig),
            },
        );
        let msg = zeus_common::HandoffMsg::create(
            &mut builder,
            &zeus_common::HandoffMsgArgs {
                entity_id: 99,
                type_: HandoffType::Offer,
                state: Some(ghost),
            },
        );
        builder.finish(msg, None);
        let buf = builder.finished_data();
        let msg = zeus_common::flatbuffers::root::<HandoffMsg>(buf).unwrap();

        node.handle_handoff_msg(msg);

        // Verify state is HandoffIn
        let e = node.manager.get_entity(99).unwrap();
        assert_eq!(e.state, AuthorityState::HandoffIn);
        // Verify Ack queued
        assert_eq!(node.outgoing_messages.len(), 1);
        assert_eq!(node.outgoing_messages[0], (99, HandoffType::Ack));
        node.outgoing_messages.clear();

        // 2. Simulate sending COMMIT (Node receives Commit)
        let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
        let msg = zeus_common::HandoffMsg::create(
            &mut builder,
            &zeus_common::HandoffMsgArgs {
                entity_id: 99,
                type_: HandoffType::Commit,
                state: None,
            },
        );
        builder.finish(msg, None);
        let buf = builder.finished_data();
        let msg = zeus_common::flatbuffers::root::<HandoffMsg>(buf).unwrap();

        node.handle_handoff_msg(msg);

        // Verify state is Local
        let e = node.manager.get_entity(99).unwrap();
        assert_eq!(e.state, AuthorityState::Local);
    }
}
