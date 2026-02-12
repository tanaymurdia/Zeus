#[allow(dead_code, unused_imports)]
pub mod ghost_generated;

pub use ghost_generated::zeus::ghost::{
    DiscoveryAnnounce, DiscoveryAnnounceArgs, DiscoveryFindNode, DiscoveryFindNodeArgs,
    DiscoveryHeartbeat, DiscoveryHeartbeatArgs, DiscoveryMsg, DiscoveryMsgArgs, DiscoveryNodeInfo,
    DiscoveryNodeInfoArgs, DiscoveryPayload, Ghost, GhostArgs, HandoffMsg, HandoffMsgArgs,
    HandoffType, NodeLoad, StateUpdate, StateUpdateArgs, Vec3,
};

pub use ed25519_dalek::{self, Signature, Signer, SigningKey, Verifier, VerifyingKey};
use flatbuffers::FlatBufferBuilder;

pub extern crate flatbuffers;

pub struct GhostSerializer {
    builder: FlatBufferBuilder<'static>,
    signing_key: Option<SigningKey>,
}

impl GhostSerializer {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            signing_key: None,
        }
    }

    pub fn set_keypair(&mut self, key: SigningKey) {
        self.signing_key = Some(key);
    }

    pub fn generate_keypair() -> (SigningKey, VerifyingKey) {
        let mut bytes = [0u8; 32];
        use rand::RngCore;
        let mut rng = rand::rng();
        rng.fill_bytes(&mut bytes);
        let signing_key = SigningKey::from_bytes(&bytes);
        let verifying_key = signing_key.verifying_key();
        (signing_key, verifying_key)
    }

    pub fn serialize(&mut self, id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) -> &[u8] {
        self.builder.reset();

        let mut data = Vec::with_capacity(32);
        data.extend_from_slice(&id.to_le_bytes());
        data.extend_from_slice(&pos.0.to_le_bytes());
        data.extend_from_slice(&pos.1.to_le_bytes());
        data.extend_from_slice(&pos.2.to_le_bytes());
        data.extend_from_slice(&vel.0.to_le_bytes());
        data.extend_from_slice(&vel.1.to_le_bytes());
        data.extend_from_slice(&vel.2.to_le_bytes());

        let signature_bytes = if let Some(key) = &self.signing_key {
            key.sign(&data).to_bytes().to_vec()
        } else {
            vec![0u8; 64]
        };

        let pos_arg = Vec3::new(pos.0, pos.1, pos.2);
        let vel_arg = Vec3::new(vel.0, vel.1, vel.2);
        let sig = self.builder.create_vector(&signature_bytes);

        let ghost = Ghost::create(
            &mut self.builder,
            &GhostArgs {
                entity_id: id,
                position: Some(&pos_arg),
                velocity: Some(&vel_arg),
                signature: Some(sig),
            },
        );

        // Wrap in HandoffMsg (Offer) to match Node Protocol
        let msg = HandoffMsg::create(
            &mut self.builder,
            &HandoffMsgArgs {
                entity_id: id,
                type_: HandoffType::Offer,
                state: Some(ghost),
            },
        );

        self.builder.finish(msg, None);
        self.builder.finished_data()
    }
}

pub fn verify_signature(ghost: Ghost, public_key: &VerifyingKey) -> bool {
    let id = ghost.entity_id();
    let pos = ghost.position().unwrap();
    let vel = ghost.velocity().unwrap();

    let mut data = Vec::with_capacity(32);
    data.extend_from_slice(&id.to_le_bytes());
    data.extend_from_slice(&pos.x().to_le_bytes());
    data.extend_from_slice(&pos.y().to_le_bytes());
    data.extend_from_slice(&pos.z().to_le_bytes());
    data.extend_from_slice(&vel.x().to_le_bytes());
    data.extend_from_slice(&vel.y().to_le_bytes());
    data.extend_from_slice(&vel.z().to_le_bytes());

    let sig_bytes = ghost.signature().unwrap().bytes();
    if sig_bytes.len() != 64 {
        return false;
    }

    let signature = match Signature::from_slice(sig_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };

    public_key.verify(&data, &signature).is_ok()
}

pub fn serialize_ghost(id: u64, pos: (f32, f32, f32), vel: (f32, f32, f32)) -> Vec<u8> {
    let mut serializer = GhostSerializer::new();
    serializer.serialize(id, pos, vel).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ghost_serializer_roundtrip() {
        let mut serializer = GhostSerializer::new();
        let bytes = serializer.serialize(12345, (1.0, 2.0, 3.0), (0.1, 0.2, 0.3));

        // Verify deserialization
        // Verify deserialization
        let msg = flatbuffers::root::<HandoffMsg>(bytes).expect("Failed to parse HandoffMsg");
        assert_eq!(msg.type_(), HandoffType::Offer);
        let ghost = msg.state().unwrap();
        assert_eq!(ghost.entity_id(), 12345);
        assert_eq!(ghost.position().unwrap().x(), 1.0);
        assert_eq!(ghost.velocity().unwrap().z(), 0.3);
    }

    #[test]
    fn test_ghost_serializer_reuse() {
        let mut serializer = GhostSerializer::new();

        // First serialization
        let bytes1 = serializer.serialize(1, (0.0, 0.0, 0.0), (0.0, 0.0, 0.0));
        // Need to copy because buffer is reused
        let vec1 = bytes1.to_vec();
        let msg1 = flatbuffers::root::<HandoffMsg>(&vec1).unwrap();
        let ghost1 = msg1.state().unwrap();
        assert_eq!(ghost1.entity_id(), 1);

        // Second serialization (reset happens internally)
        let bytes2 = serializer.serialize(2, (10.0, 10.0, 10.0), (0.0, 0.0, 0.0));
        let msg2 = flatbuffers::root::<HandoffMsg>(bytes2).unwrap();
        let ghost2 = msg2.state().unwrap();
        assert_eq!(ghost2.entity_id(), 2);
        assert_eq!(ghost2.position().unwrap().x(), 10.0);
    }

    #[test]
    fn test_ghost_serializer_edge_cases() {
        let mut serializer = GhostSerializer::new();

        // Max values
        let bytes = serializer.serialize(u64::MAX, (f32::MAX, f32::MIN, 0.0), (0.0, 0.0, 0.0));
        let msg = flatbuffers::root::<HandoffMsg>(bytes).unwrap();
        let ghost = msg.state().unwrap();
        assert_eq!(ghost.entity_id(), u64::MAX);
        assert_eq!(ghost.position().unwrap().x(), f32::MAX);
        assert_eq!(ghost.position().unwrap().y(), f32::MIN);

        // NaN values (should serialize, though logic might break elsewhere. Serializer shouldn't panic)
        let bytes_nan = serializer.serialize(1, (f32::NAN, 0.0, 0.0), (0.0, 0.0, 0.0));
        let msg_nan = flatbuffers::root::<HandoffMsg>(bytes_nan).unwrap();
        let ghost_nan = msg_nan.state().unwrap();
        assert!(ghost_nan.position().unwrap().x().is_nan());
    }

    #[test]
    fn test_ghost_serializer_signature() {
        let (signing_key, verifying_key) = GhostSerializer::generate_keypair();
        let mut serializer = GhostSerializer::new();
        serializer.set_keypair(signing_key);

        let bytes = serializer.serialize(999, (1.0, 1.0, 1.0), (0.0, 0.0, 0.0));
        let msg = flatbuffers::root::<HandoffMsg>(bytes).unwrap();
        let ghost = msg.state().unwrap();

        // Verify valid signature
        assert!(verify_signature(ghost, &verifying_key));

        // Verify tampering fails
        let mut tampered_bytes = bytes.to_vec();
        tampered_bytes[40] ^= 1; // Corrupt data (approximate offset for payload)

        let mut tampered_bytes = bytes.to_vec();
        tampered_bytes[40] ^= 1; // Corrupt data (approximate offset for payload)

        let tampered_msg = flatbuffers::root::<HandoffMsg>(&tampered_bytes).unwrap();
        let tampered_ghost = tampered_msg.state().unwrap();
        assert!(!verify_signature(tampered_ghost, &verifying_key));
    }
}
