use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::process::Stdio;
use tokio::fs;

use zeus_common::{Ghost, HandoffMsg, HandoffType};
use zeus_transport::{make_client_endpoint, make_server_endpoint};

use zeus_node::entity_manager::{AuthorityState, Entity};
use zeus_node::node_actor::NodeActor;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    mode: Mode,
}

use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Subcommand)]
enum Mode {
    Source {
        // Node A
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        bind: SocketAddr,
    },
    Target {
        // Node B
        #[arg(short, long, default_value = "127.0.0.1:5001")]
        bind: SocketAddr,
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        peer: SocketAddr,
    },
    Mesh {
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        bind: SocketAddr,
        #[arg(short, long)]
        seed: Option<SocketAddr>,
    },
    /// Auto-scaling orchestrator - spawns nodes automatically when load is critical
    Orchestrator {
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        bind: SocketAddr,
        #[arg(short, long, default_value = "4")]
        max_nodes: u8,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    zeus_transport::init();
    let cli = Cli::parse();

    match cli.mode {
        Mode::Source { bind } => run_source(bind).await?,
        Mode::Target { bind, peer } => run_target(bind, peer).await?,
        Mode::Mesh { bind, seed } => run_mesh(bind, seed).await?,
        Mode::Orchestrator { bind, max_nodes } => run_orchestrator(bind, max_nodes).await?,
    }
    Ok(())
}

use tokio::sync::mpsc;
use zeus_node::discovery::DiscoveryActor;

use zeus_node::NetworkEvent;

async fn run_mesh(
    bind: SocketAddr,
    seed: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup Endpoint (Dual Stack, Promiscuous for Dev)
    let (endpoint, _) = zeus_transport::make_promiscuous_endpoint(bind)?;

    println!("Mesh Node listening on {}", endpoint.local_addr()?);

    let (tx, mut rx) = mpsc::channel(100);

    // 2. Accept Loop (Spawned)
    let endpoint_clone = endpoint.clone();
    let tx_accept = tx.clone();
    tokio::spawn(async move {
        while let Some(conn) = endpoint_clone.accept().await {
            if let Ok(connection) = conn.await {
                println!(
                    "[Mesh] Accepted connection from {}",
                    connection.remote_address()
                );
                let _ = tx_accept
                    .send(NetworkEvent::NewConnection(connection))
                    .await;
            }
        }
    });

    // 3. Connect to Seed
    if let Some(seed_addr) = seed {
        println!("[Mesh] Connecting to Seed {}...", seed_addr);
        // We need a client config to connect.
        // zeus_transport doesn't expose a helper to *add* client config to existing server endpoint easily?
        // Let's look at `zeus_transport` content later. For now assume `connect` works if we add default client config?
        // Actually, defaults might fail cert validation.
        // We'll trust "localhost" (simulated).
        let connection = endpoint.connect(seed_addr, "localhost")?.await?;
        println!("[Mesh] Connected to Seed.");
        tx.send(NetworkEvent::NewConnection(connection)).await?;
    }

    // 4. Main State
    let mut node = NodeActor::new(0.0, 5.0); // Default Config
    let mut discovery = DiscoveryActor::new(rand::random(), (0.0, 0.0, 0.0), bind); // Random ID, Origin

    // Track connections: Map<Addr, Connection>?
    // Discovery tracks ID -> Addr.
    // We need Addr -> Connection.
    let mut connections: Vec<quinn::Connection> = Vec::new();

    let dt = 0.050;

    loop {
        let loop_start = std::time::Instant::now();

        // A. Logic Update
        node.update(dt);
        discovery.update(dt);

        // A2. Broadcast Load (Entity Count for Scaling Decisions)
        // Count actual connected clients as entities
        let total_entities = (connections.len() * 100) as u16; // Each client = 100 entities
        discovery.set_load(total_entities, 0);

        // B. Handle Network Events (Non-blocking ideally, but recv is async)
        // We select on rx and timeout (for tick).
        let tick_duration = std::time::Duration::from_millis(50);

        // consume events until timeout
        while let Ok(event) = rx.try_recv() {
            match event {
                NetworkEvent::NewConnection(conn) => {
                    connections.push(conn.clone());
                    // Spawn Reader for this connection
                    let tx_reader = tx.clone();
                    let conn_reader = conn.clone();
                    tokio::spawn(async move {
                        // Loop reading streams/datagrams
                        loop {
                            tokio::select! {
                                // Streams (Handoff)
                                res = conn_reader.accept_uni() => {
                                    match res {
                                        Ok(mut recv) => {
                                            if let Ok(bytes) = recv.read_to_end(64*1024).await {
                                                let _ = tx_reader.send(NetworkEvent::Payload(conn_reader.clone(), bytes, true)).await;
                                            }
                                        }
                                        Err(_) => break, // Connection closed
                                    }
                                }
                                // Datagrams (Discovery)
                                res = conn_reader.read_datagram() => {
                                     match res {
                                        Ok(bytes) => {
                                             let _ = tx_reader.send(NetworkEvent::Payload(conn_reader.clone(), bytes.to_vec(), false)).await;
                                        }
                                        Err(_) => break,
                                     }
                                }
                            }
                        }
                    });
                }
                NetworkEvent::Payload(conn, bytes, is_stream) => {
                    if is_stream {
                        // HandoffMsg OR Client Update
                        if let Ok(msg) = zeus_common::flatbuffers::root::<HandoffMsg>(&bytes) {
                            node.handle_handoff_msg(msg);
                        }
                        // Real update received from client
                    } else {
                        // DiscoveryMsg
                        if let Ok(msg) =
                            zeus_common::flatbuffers::root::<zeus_common::DiscoveryMsg>(&bytes)
                        {
                            discovery.process_packet(msg, conn.remote_address());
                        }
                    }
                }
                _ => {}
            }
        }

        // C. Outgoing Handoffs (Stream)
        // Check `node.outgoing_messages`.
        // We need target connection.
        // NodeActor only knows EntityID.
        // We need: Entity -> ... Wait. NodeActor knows `AuthorityState`.
        // How do we know WHICH neighbor to send to?
        // Phase 2 logic: 1-on-1 hardcoded.
        // Phase 4 logic: Query Discovery for neighbor closest to Entity?
        // Or NodeActor just emits "Offer Entity X".
        // Main Loop queries `discovery.find_peer_for(entity_pos)`.
        // If peer found:
        //    Get Connection (by Addr).
        //    If no connection, Connect?
        //    Send Stream.

        // For now, let's just broadcast to ALL connections if we don't know? No, that's flooding.
        // Let's assume 1 neighbor for simplicity in initial Mesh test.
        // Send to ALL connected peers for Handoff? (Temporary Multicast Handoff)
        // Or pick first one.

        while let Some((id, msg_type)) = node.outgoing_messages.pop_front() {
            // Build Message
            let msg_bytes = build_handoff_msg(id, msg_type, &node);
            // Broadcast to all connections (Naive Mesh)
            for conn in &connections {
                if let Ok(mut stream) = conn.open_uni().await {
                    let _ = stream.write_all(&msg_bytes).await;
                    let _ = stream.finish();
                }
            }
        }

        // D. Outgoing Discovery Announce (Gossip)
        // Periodically (e.g. every 1 sec)
        // Let's prioritize Datagrams.
        if loop_start.elapsed().as_millis() % 1000 < 50 {
            let announce_bytes = discovery.generate_announce();
            for conn in &connections {
                let _ = conn.send_datagram(announce_bytes.clone().into());
            }
        }

        // E. Send Server Status to Clients (every 200ms)
        // Format: [0xAA, entity_count_high, entity_count_low, node_count]
        // 0xAA = magic byte to identify status message
        if loop_start.elapsed().as_millis() % 200 < 50 {
            let node_count = (discovery.peers.len() + 1) as u8; // +1 for self
            let entity_count = total_entities;
            let status_bytes: [u8; 4] = [
                0xAA, // Magic byte
                (entity_count >> 8) as u8,
                (entity_count & 0xFF) as u8,
                node_count,
            ];
            for conn in &connections {
                let _ = conn.send_datagram(status_bytes.to_vec().into());
            }
        }

        // Wait for Tick
        let elapsed = loop_start.elapsed();
        if elapsed < tick_duration {
            tokio::time::sleep(tick_duration - elapsed).await;
        }
    }
}

async fn run_source(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, cert) = make_server_endpoint(addr)?;
    fs::write("server.cert", &cert).await?;
    println!("Source Node listening on {}", endpoint.local_addr()?);

    // Initialize Node Actor with Entity 1 at x=-10
    let mut node = NodeActor::new(0.0, 5.0); // Boundary 0, Margin 5
    node.manager.add_entity(Entity {
        id: 1,
        pos: (-10.0, 0.0, 0.0),
        vel: (10.0, 0.0, 0.0), // Moving +X at 10 units/sec
        state: AuthorityState::Local,
        verifying_key: None,
    });

    println!("Waiting for Target execution...");

    // Accept connection
    if let Some(conn) = endpoint.accept().await {
        let connection = conn.await?;
        println!("Connected to Target: {}", connection.remote_address());

        // Simulation Loop
        let dt = 0.050; // 50ms tick
        loop {
            let start = std::time::Instant::now();

            // 1. Logic Update
            node.update(dt);

            // Print Status
            if let Some(e) = node.manager.get_entity(1) {
                println!("[Source] Entity 1: Pos={:.1}, State={:?}", e.pos.0, e.state);
            }

            // 2. Process Outgoing Handoffs (Send via Stream)
            while let Some((id, msg_type)) = node.outgoing_messages.pop_front() {
                let msg_bytes = build_handoff_msg(id, msg_type, &node);
                let mut send_stream = connection.open_uni().await?;
                send_stream.write_all(&msg_bytes).await?;
                send_stream.finish()?;
            }

            // 3. Process Incoming Handoffs (Read Stream)
            let elapsed = start.elapsed();
            let tick_duration = std::time::Duration::from_millis(50);
            if elapsed < tick_duration {
                let timeout = tick_duration - elapsed;
                tokio::select! {
                   res = connection.accept_uni() => {
                       if let Ok(mut recv) = res {
                           // Read up to 64KB
                           match recv.read_to_end(64 * 1024).await {
                               Ok(buf) => {
                                   if let Ok(msg) = zeus_common::flatbuffers::root::<HandoffMsg>(&buf) {
                                       node.handle_handoff_msg(msg);
                                   }
                               }
                               Err(e) => eprintln!("[Source] Read error: {}", e),
                           }
                       }
                   }
                   _ = tokio::time::sleep(timeout) => {}
                }
            }
        }
    }
    Ok(())
}

async fn run_target(bind: SocketAddr, peer: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    if !std::path::Path::new("server.cert").exists() {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let cert = fs::read("server.cert").await?;
    let endpoint = make_client_endpoint(bind, &cert)?;

    println!("Connecting to Source {}...", peer);
    let connection = endpoint.connect(peer, "localhost")?.await?;
    println!("Connected to Source");

    let mut node = NodeActor::new(0.0, 5.0); // Same world config

    let dt = 0.050;
    loop {
        let start = std::time::Instant::now();

        // 1. Logic Update
        node.update(dt);

        if let Some(e) = node.manager.get_entity(1) {
            println!("[Target] Entity 1: Pos={:.1}, State={:?}", e.pos.0, e.state);
        }

        // 2. Outgoing
        while let Some((id, msg_type)) = node.outgoing_messages.pop_front() {
            let msg_bytes = build_handoff_msg(id, msg_type, &node);
            let mut send_stream = connection.open_uni().await?;
            send_stream.write_all(&msg_bytes).await?;
            send_stream.finish()?;
        }

        // 3. Incoming
        let elapsed = start.elapsed();
        let tick_duration = std::time::Duration::from_millis(50);
        if elapsed < tick_duration {
            let timeout = tick_duration - elapsed;
            tokio::select! {
               res = connection.accept_uni() => {
                   if let Ok(mut recv) = res {
                       match recv.read_to_end(64 * 1024).await {
                           Ok(buf) => {
                               if let Ok(msg) = zeus_common::flatbuffers::root::<HandoffMsg>(&buf) {
                                   node.handle_handoff_msg(msg);
                               }
                           }
                           Err(e) => eprintln!("[Target] Read error: {}", e),
                       }
                   }
               }
               _ = tokio::time::sleep(timeout) => {}
            }
        }
    }
}

// Helper to build HandoffMsg buffer
fn build_handoff_msg(id: u64, msg_type: HandoffType, node: &NodeActor) -> Vec<u8> {
    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();

    let ghost_offset = if let Some(e) = node.manager.get_entity(id) {
        let pos = zeus_common::Vec3::new(e.pos.0, e.pos.1, e.pos.2);
        let vel = zeus_common::Vec3::new(e.vel.0, e.vel.1, e.vel.2);
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

    let msg = zeus_common::HandoffMsg::create(
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

/// Orchestrator mode - spawns and monitors mesh nodes, auto-scales on critical load
async fn run_orchestrator(
    bind: SocketAddr,
    max_nodes: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════╗");
    println!("║       ZEUS AUTO-SCALING ORCHESTRATOR                   ║");
    println!("╠════════════════════════════════════════════════════════╣");
    println!(
        "║  Max Nodes: {}                                          ║",
        max_nodes
    );
    println!("║  Starting Node 0 on {}                      ║", bind);
    println!("╚════════════════════════════════════════════════════════╝");

    let mut node_count: u8 = 0;
    let mut next_port = bind.port();

    // Channel to receive "scale up" signals
    let (scale_tx, mut scale_rx) = tokio::sync::mpsc::channel::<()>(10);

    // Spawn first node
    let node_id = node_count;
    let port = next_port;
    let scale_tx_clone = scale_tx.clone();

    tokio::spawn(async move {
        spawn_and_monitor_node(node_id, port, None, scale_tx_clone).await;
    });

    node_count += 1;
    next_port += 1;

    // Main orchestrator loop - wait for scale signals
    loop {
        tokio::select! {
            _ = scale_rx.recv() => {
                if node_count >= max_nodes {
                    println!("[Orchestrator] ⚠️  MAX NODES ({}) reached. Cannot scale further.", max_nodes);
                    continue;
                }

                println!("[Orchestrator] 🚀 SCALING UP! Spawning Node {} on 127.0.0.1:{}", node_count, next_port);

                let node_id = node_count;
                let port = next_port;
                let seed_port = bind.port(); // Connect to first node
                let scale_tx_clone = scale_tx.clone();

                tokio::spawn(async move {
                    spawn_and_monitor_node(node_id, port, Some(seed_port), scale_tx_clone).await;
                });

                node_count += 1;
                next_port += 1;

                println!("[Orchestrator] 📊 Mesh now has {} node(s)", node_count);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n[Orchestrator] Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

/// Spawn a mesh node and monitor its output for CRITICAL LOAD
async fn spawn_and_monitor_node(
    node_id: u8,
    port: u16,
    seed_port: Option<u16>,
    scale_tx: tokio::sync::mpsc::Sender<()>,
) {
    use tokio::process::Command;

    let bind_addr = format!("127.0.0.1:{}", port);

    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("-p")
        .arg("zeus_node")
        .arg("--")
        .arg("mesh")
        .arg("--bind")
        .arg(&bind_addr);

    if let Some(seed) = seed_port {
        cmd.arg("--seed").arg(format!("127.0.0.1:{}", seed));
    }

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    println!("[Orchestrator] Starting Node {} on {}", node_id, bind_addr);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[Orchestrator] Failed to spawn node {}: {}", node_id, e);
            return;
        }
    };

    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout).lines();

    let mut has_triggered_scale = false;

    // Monitor stdout for CRITICAL LOAD
    while let Ok(Some(line)) = reader.next_line().await {
        // Print with node prefix
        println!("[Node {}] {}", node_id, line);

        // Check for critical load signal (only trigger once per node)
        if !has_triggered_scale && line.contains("CRITICAL LOAD") {
            println!("[Orchestrator] 🔔 Node {} reports CRITICAL LOAD!", node_id);
            let _ = scale_tx.send(()).await;
            has_triggered_scale = true;
        }
    }

    println!("[Orchestrator] Node {} exited", node_id);
}
