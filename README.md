# Zeus

> **Zero-Trust Physics for the Spatial Web.**
> Zeus is a serverless, spatially-partitioned game engine where entities migrate seamlessly between nodes using a cryptographically verified handoff protocol over a full-mesh QUIC network.

---

## Quick Start: Cosmic Drift Demo

Cosmic Drift is a real-time physics demo showcasing Zeus's full-mesh autoscaling. NPC balls spawn on Node 0, and as count thresholds are crossed (5, 10, 15), the orchestrator automatically spawns new nodes. All nodes form a full mesh — every node connects to every other node directly. The client connects to all nodes simultaneously, receiving entity state with exactly 1-hop latency from any source.

### 1. Launch the Server Orchestrator

```bash
cargo run -p cosmic_drift_server -- orchestrator --start-port 5000
```

The orchestrator spawns Node 0 on port 5000. As balls accumulate, it spawns Node 1 (5001), Node 2 (5002), Node 3 (5003) — each connecting to all existing nodes via `--peers`.

### 2. Launch the Client

```bash
cargo run -p cosmic_drift
```

The client connects to port 5000 initially. When it detects new nodes via 0xAA status broadcasts, it spawns additional reader tasks for ports 5001-5003, merging all entity state into a single view.

### 3. Controls
- **W/A/S/D**: Move player ball
- **M**: Spawn 500 NPC balls
- **N**: Spawn 100 NPC balls
- **Arrow Keys**: Move camera

---

## Architecture

### Full Mesh Topology

```
    Node 0 <----> Node 1
      ^  \       /  ^
      |   \     /   |
      |    \   /    |
      v     v v     v
    Node 3 <----> Node 2

    Client connects to ALL nodes
```

Every node connects to every other node directly. No daisy chain, no relay, no multi-hop latency. Entity data is always exactly 1 hop from source to any destination.

### The Wire (Data Plane)
- **Protocol**: QUIC datagrams for high-frequency physics state (unreliable, unordered, fast)
- **Format**: Custom compact binary — not FlatBuffers
  - **0xCC** (Node -> Client): `u64 id + u8 flags + i16 quantized pos [+ i16 vel]` = 15-21 bytes/entity
  - **0xCE** (Node -> Peer): `u64 id + 3×i16 pos + 3×i16 vel` = 20 bytes/entity + 64B Ed25519 batch signature
  - **0xAA** (Node -> Client): 6-byte status: entity count, node count, map width, ball radius
  - **0xBB** (Node -> Client): player entity ID list
- **Quantization**: Positions at 2mm precision (×500), velocities at 0.01 m/s (×100)
- **Delta encoding**: Only entities whose quantized position changed are broadcast to clients
- **128Hz physics / 32Hz broadcast**: 4:1 throttle ratio

### The Handoff (Control Plane)
Seamless entity migration between nodes via 3-way handshake over reliable QUIC streams (FlatBuffers):
1. **OFFER**: "I am sending you Entity X."
2. **ACK**: "I am ready to receive Entity X."
3. **COMMIT**: "I relinquish control. Entity X is yours."

Hysteresis prevents oscillation at boundaries.

### Zero-Trust Physics (Security)
- Every 0xCE peer gossip datagram includes a batch Ed25519 signature
- Receiving nodes verify the signature against the sender's `VerifyingKey` (exchanged via 0xCF datagrams alongside discovery announces)
- Unverified gossip is rejected
- Signing: ~13us per batch. Verification: ~32us per batch. At 32Hz × 4 nodes = ~4ms/sec total overhead.

### Dynamic Auto-Scaling
Auto-scaling is live, not a future feature:
- **Threshold-based**: 5 balls -> 2 nodes, 10 -> 3, 15 -> 4
- **Orchestrator-driven**: Monitors `REQUEST_SPLIT` from Node 0, spawns new nodes with `--peers` pointing to all existing nodes
- **Zone recomputation**: Each node dynamically recalculates its spatial zone based on total node count

### SDK Separation
The game server (`cosmic_drift_server`) is a pure physics source. It implements `GameWorld` (step, arrive, depart, update) and nothing else. All networking concerns live in `ZeusEngine`/`GameLoop`:
- `broadcast_status()` sends 0xAA + 0xBB to clients
- `should_split()` encapsulates autoscaling thresholds
- `broadcast_state_to_clients()` / `broadcast_state_to_peers()` handle all wire encoding

Any game using Zeus gets autoscaling, status broadcasts, handoff, and zero-trust for free.

---

## Benchmarks

Run with `cargo run -p zeus_common --bin benchmark --release`:

| Metric | Result | Notes |
| :--- | :--- | :--- |
| **Compact encode (5k entities)** | ~0.17ms | 4.5x faster than FlatBuffers |
| **Bytes/entity (moving)** | 21 bytes | vs ~60B FlatBuffers |
| **Bytes/entity (at rest)** | 15 bytes | At-rest flag omits velocity |
| **Delta encoding savings** | ~53% | Only changed entities broadcast |
| **Batch signing (5k entities)** | ~0.8ms | 16.8x faster than per-entity |
| **Ed25519 verify** | ~32us | Per batch, not per entity |
| **Discovery throughput** | ~3.3M ops/sec | Lightweight FlatBuffer announce |

*Benchmarks on Apple Silicon (M1 Pro).*

---

## Development

### Prerequisites
- Rust (stable)
- `flatc` (FlatBuffers compiler, only if modifying `ghost.fbs`)

### Building
```bash
cargo build --release
```

### Running Tests
```bash
cargo test --workspace
```

Includes: compact binary encode/decode, delta encoding, Ed25519 signing/verification, handoff protocol, multi-node e2e integration, SDK separation.

---

## Why This Is Novel

1. **Protocol-First**: The game server is just a physics source. It doesn't know about clients, networking, or protocols. It feeds `ZeusEngine`.
2. **Full Mesh**: Every node connects to every other node. Client connects to all nodes. No single point of failure. 1-hop latency everywhere.
3. **Zero-Trust Physics**: Unlike traditional MMOs with trusted authorities, Zeus verifies every state update with Ed25519 signatures. Byzantine Fault Tolerant by design.
4. **Atomic Handoff**: Seamless entity migration between servers without dropping a frame — usually reserved for proprietary engines (Star Citizen, SpatialOS). Zeus makes this open-source.
5. **SDK-Ready**: Any game engine (Bevy, Unity, Unreal) can implement the `GameWorld` trait and get distributed physics for free.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
