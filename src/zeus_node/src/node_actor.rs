use crate::cell::Cell;
use crate::entity_manager::{AuthorityState, Entity, EntityManager};
use std::collections::VecDeque;
use std::net::SocketAddr;
use zeus_common::{HandoffMsg, HandoffType};

pub struct NodeActor {
    pub manager: EntityManager,
    pub outgoing_messages: VecDeque<(u64, HandoffType, Option<SocketAddr>)>,
}

impl NodeActor {
    pub fn new(boundary: f32, margin: f32, lower_boundary: f32) -> Self {
        Self {
            manager: EntityManager::new(boundary, margin, lower_boundary),
            outgoing_messages: VecDeque::new(),
        }
    }

    pub fn new_3d(cell: Cell, margin: f32) -> Self {
        Self {
            manager: EntityManager::new_3d(cell, margin),
            outgoing_messages: VecDeque::new(),
        }
    }

    pub fn update(&mut self, dt: f32) -> Vec<(u64, crate::cell::Face)> {
        self.manager.update(dt)
    }

    pub fn set_boundary(&mut self, boundary: f32) {
        self.manager.set_boundary(boundary);
    }

    pub fn handle_handoff_msg(&mut self, msg: HandoffMsg, source: Option<SocketAddr>) {
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
                let is_handoff_in = matches!(current_state, Some(AuthorityState::HandoffIn));

                if is_handoff_in {
                    self.outgoing_messages.push_back((id, HandoffType::Ack, None));
                    return;
                }

                if let Some(ghost) = msg.state() {
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
                    }

                    let pos = ghost.position().unwrap();
                    let vel = ghost.velocity().unwrap();

                    let entity_pos = (pos.x(), pos.y(), pos.z());

                    if is_local {
                        let mut e = self.manager.get_entity(id).unwrap().clone();
                        e.pos = entity_pos;
                        e.vel = (vel.x(), vel.y(), vel.z());
                        self.manager.add_entity(e);
                    } else {
                        let cell = self.manager.cell();
                        let is_3d = cell.y_min.is_finite();
                        if is_3d && !cell.contains_with_margin(entity_pos, 0.5) {
                            return;
                        }
                        let clamped_pos = cell.clamp_inside(entity_pos, 0.5);
                        let was_remote = matches!(current_state, Some(AuthorityState::Remote));
                        let entity = Entity {
                            id,
                            pos: clamped_pos,
                            vel: (vel.x(), vel.y(), vel.z()),
                            state: AuthorityState::HandoffIn,
                            verifying_key: known_key,
                        };
                        self.manager.add_entity(entity);
                        if is_new || was_remote {
                            self.outgoing_messages.push_back((id, HandoffType::Ack, None));
                        }
                    }
                } else {
                    println!("[Node] Received Offer for {} but no state attached", id);
                }
            }
            HandoffType::Ack => {
                if let Some(AuthorityState::HandoffOut) = current_state {
                    self.manager.set_state(id, AuthorityState::Remote);
                    self.outgoing_messages.push_back((id, HandoffType::Commit, source));
                }
            }
            HandoffType::Commit => {
                if let Some(AuthorityState::HandoffIn) = current_state {
                    self.manager.set_state(id, AuthorityState::Local);
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

    fn _create_msg(id: u64, type_: HandoffType) -> HandoffMsg<'static> {
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
        unreachable!()
    }

    #[test]
    fn test_node_actor_handoff_flow() {
        let mut node = NodeActor::new(0.0, 5.0, 0.0);
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

        node.handle_handoff_msg(msg, None);
        let e = node.manager.get_entity(99).unwrap();
        assert_eq!(e.state, AuthorityState::HandoffIn);
        node.outgoing_messages.clear();
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

        node.handle_handoff_msg(msg, None);
        let e = node.manager.get_entity(99).unwrap();
        assert_eq!(e.state, AuthorityState::Local);
    }
}
