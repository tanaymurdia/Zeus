# Spatial Hypervisor (Project Zeus)

> **Zero-Trust Physics for the Spatial Web.**
> Zeus is a proof-of-concept for a serverless, spatially-partitioned game world where entities migrate seamlessly between nodes using a specialized handoff protocol.

---

## 🚀 Quick Start: Cosmic Drift Demo

Experience the seamless handoff protocol in action with the "Cosmic Drift" visualization. In this demo, you pilot a ship across a server boundary (`x=0`), triggering a real-time cryptographic authority swap between two distinct server nodes.

### 1. Launch the Server Orchestrator
The orchestrator automatically spawns and manages the physics nodes (Server A & Server B).

```bash
# Terminal 1
cargo run -p cosmic_drift_server -- orchestrator
```
*You will see logs indicating that Node 0 and Node 1 have launched and are simulating physics.*

### 2. Launch the Client
Visualizes the world and controls the ship.

```bash
# Terminal 2
cargo run -p cosmic_drift
```

### 3. Controls
-   **W/A/S/D**: Move Ship
-   **Arrow Keys**: Move Camera
-   **Space**: Boost

### 4. The Protocol in Action

When you cross the **Red Wall** (at `x=0`), the Zeus Handoff Protocol triggers automatically.

**Log Flows to Watch (Terminal 1):**

| Event | Log Message | Meaning |
|-------|-------------|---------|
| **Handoff** | `[+] Player <ID> ARRIVED` | Entity ownership successfully transferred to this node. |
| **Departure** | `[-] Player <ID> DEPARTED` | Entity successfully handed off to neighbor. |

---

## 🏗 Architecture

The Spatial Hypervisor consists of three core phases, built to minimize latency and maximize security.

### Phase 1: The Wire (Data Plane)
The foundation of the hypervisor. Focuses on raw performance and efficient state compression.
-   **Protocol**: QUIC Datagrams for high-frequency physics updates (unreliable, unordered, fast).
-   **Schema**: Optimized Flatbuffer schema (`Ghost.fbs`) with minimal footprint.
-   **Performance**: **~0.77ms** serialization time for 5k entities (M1 Mac Benchmark).

### Phase 2: The Handoff (Control Plane)
The logic layer managing authority transitions.
-   **Problem**: Preventing "teleportation" or jitter when an entity crosses from Server A to Server B.
-   **Solution**:
    1.  **Hysteresis**: Entities must cross `boundary + margin` to trigger a handoff, preventing oscillation.
    2.  **3-Way Handshake**: 
        -   `OFFER`: "I am sending you Entity X."
        -   `ACK`: "I am ready to receive Entity X."
        -   `COMMIT`: "I relinquish control. Entity X is yours."

### Phase 3: The Witness (Security)
Cryptographic verification of state provenance.
-   **Zero-Trust**: Every state update is signed by the authoritative node (or client).
-   **Ed25519 Signatures**: Fast signing (~13µs) and verification (~32µs) ensures tamper-proof entity state. **Active in Release v0.1**.

### Phase 4: The Mesh (Discovery)
Decentralized neighbor discovery.
-   **Discovery Protocol**: Nodes gossip their existence via lightweight UDP announcments (~0.3µs serialize).
-   **Spatial Mapping**: Nodes dynamically find peers responsible for adjacent spatial regions.

---

## 📊 Benchmarks

Run locally (Active Release Build):

| Metric | Target | Result | Status |
| :--- | :--- | :--- | :--- |
| **Serialization (5k entities)** | < 2.00ms | **~0.77ms** | ✅ Passed |
| **Transport Overhead** | < 1.00ms | **~0.90ms** | ✅ Passed |
| **Signature Verify** | < 50µs | **~32.3µs** | ✅ Passed |
| **Discovery Throughput** | > 1M ops/sec | **~3.3M ops/sec** | ✅ Passed |

*Benchmarks run on Apple Silicon (M1 Pro).*

---

## 🛠 Usage (Development)

### Prerequisites
-   Rust (stable)
-   `flatc` (Flatbuffers compiler)

### Building
```bash
cargo build --release
```

### Running Test Suite
```bash
cargo test --workspace
```
Includes serialization stress tests, hysteresis logic verification, and cryptographic integrity checks.

---

## 💡 Why This Is Novel (The Spatial Hypervisor)

Zeus is not just a game server; it is a **Middleware** that turns any simulation into a distributed system.

1.  **Protocol-First Architecture**: The "Game Server" (`cosmic_drift_server`) is just a *physics source*. It doesn't know about clients or networking protocols. It just feeds `ZeusEngine`.
2.  **Scaffolding**: `ZeusEngine` handles the *entire* distributed system logic (Handoffs, Discovery, Replication, Client Interest Management) invisibly.
3.  **Client-Agnostic**: The Client connects to the *Mesh*, not a specific server. The Mesh routes data dynamically.
4.  **Zero-Trust Physics**: Unlike traditional MMOs where servers are trusted authorities, Zeus assumes **Byzantine Fault Tolerance**. Every physics update verified with `Ed25519` signatures in <35µs, enabling a **Permissionless Metaverse**.
5.  **The "Atomic Handoff"**: Seamlessly transferring a moving entity between servers without dropping a single frame is a complex problem usually reserved for proprietary engines (e.g., Star Citizen). Zeus makes this democratized and open-source.

---

## 🔮 Future: Dynamic Auto-Scaling

The next frontier for Zeus is **Elastic Partitioning**:

-   **Fluid Boundaries**: Instead of fixed lines, server boundaries will "float" based on load.
-   **Flash Crowd Handling**: If 10,000 players gather in one spot, the Mesh will recursively split that region into a dense **Quadtree** of nodes.
-   **Cost Efficiency**: Empty regions effectively "turn off" (merge into larger idle nodes), minimizing infrastructure costs.

See [scaling_strategy.md](file:///Users/tanaymurdia/.gemini/antigravity/brain/a4edf8af-67b3-4d4b-88d0-c2c56cb646fb/scaling_strategy.md) for the detailed design.
