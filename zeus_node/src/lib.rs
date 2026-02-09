pub mod discovery;
pub mod engine;
pub mod entity_manager;
pub mod node_actor;

use zeus_common::HandoffMsg;

pub enum NetworkEvent {
    NewConnection(quinn::Connection),
    Handoff(u64, HandoffMsg<'static>),
    Payload(quinn::Connection, Vec<u8>, bool), // (Conn, Bytes, IsStream)
}
