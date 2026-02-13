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
        #[arg(short, long, default_value = "127.0.0.1:5000")]
        bind: SocketAddr,
    },
    Target {
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
    let (endpoint, _) = zeus_transport::make_promiscuous_endpoint(bind)?;

    println!("Mesh Node listening on {}", endpoint.local_addr()?);

    let (tx, mut rx) = mpsc::channel(100);

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

    if let Some(seed_addr) = seed {
        println!("[Mesh] Connecting to Seed {}...", seed_addr);
        let connection = endpoint.connect(seed_addr, "localhost")?.await?;
        println!("[Mesh] Connected to Seed.");
        tx.send(NetworkEvent::NewConnection(connection)).await?;
    }

    let mut node = NodeActor::new(0.0, 5.0, 0.0);
    let mut discovery = DiscoveryActor::new(rand::random(), (0.0, 0.0, 0.0), bind, 0);

    let mut connections: Vec<quinn::Connection> = Vec::new();

    let dt = 0.050;

    loop {
        let loop_start = std::time::Instant::now();

        node.update(dt);
        discovery.update(dt);

        let total_entities = (connections.len() * 100) as u16;
        discovery.set_load(total_entities, 0);

        let tick_duration = std::time::Duration::from_millis(50);

        while let Ok(event) = rx.try_recv() {
            match event {
                NetworkEvent::NewConnection(conn) => {
                    connections.push(conn.clone());
                    let tx_reader = tx.clone();
                    let conn_reader = conn.clone();
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                res = conn_reader.accept_uni() => {
                                    match res {
                                        Ok(mut recv) => {
                                            if let Ok(bytes) = recv.read_to_end(64*1024).await {
                                                let _ = tx_reader.send(NetworkEvent::Payload(conn_reader.clone(), bytes, true)).await;
                                            }
                                        }
                                        Err(_) => break,
                                    }
                                }
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
                        if let Ok(msg) = zeus_common::flatbuffers::root::<HandoffMsg>(&bytes) {
                            node.handle_handoff_msg(msg);
                        }
                    } else {
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

        while let Some((id, msg_type)) = node.outgoing_messages.pop_front() {
            let msg_bytes = build_handoff_msg(id, msg_type, &node);
            for conn in &connections {
                if let Ok(mut stream) = conn.open_uni().await {
                    let _ = stream.write_all(&msg_bytes).await;
                    let _ = stream.finish();
                }
            }
        }

        if loop_start.elapsed().as_millis() % 1000 < 50 {
            let announce_bytes = discovery.generate_announce();
            for conn in &connections {
                let _ = conn.send_datagram(announce_bytes.clone().into());
            }
        }

        if loop_start.elapsed().as_millis() % 200 < 50 {
            let node_count = (discovery.peers.len() + 1) as u8;
            let entity_count = total_entities;
            let status_bytes: [u8; 4] = [
                0xAA,
                (entity_count >> 8) as u8,
                (entity_count & 0xFF) as u8,
                node_count,
            ];
            for conn in &connections {
                let _ = conn.send_datagram(status_bytes.to_vec().into());
            }
        }

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

    let mut node = NodeActor::new(0.0, 5.0, 0.0);
    node.manager.add_entity(Entity {
        id: 1,
        pos: (-10.0, 0.0, 0.0),
        vel: (10.0, 0.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    println!("Waiting for Target execution...");

    if let Some(conn) = endpoint.accept().await {
        let connection = conn.await?;
        println!("Connected to Target: {}", connection.remote_address());

        let dt = 0.050;
        loop {
            let start = std::time::Instant::now();

            node.update(dt);

            if let Some(e) = node.manager.get_entity(1) {
                println!("[Source] Entity 1: Pos={:.1}, State={:?}", e.pos.0, e.state);
            }

            while let Some((id, msg_type)) = node.outgoing_messages.pop_front() {
                let msg_bytes = build_handoff_msg(id, msg_type, &node);
                let mut send_stream = connection.open_uni().await?;
                send_stream.write_all(&msg_bytes).await?;
                send_stream.finish()?;
            }

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

    let mut node = NodeActor::new(0.0, 5.0, 0.0);

    let dt = 0.050;
    loop {
        let start = std::time::Instant::now();

        node.update(dt);

        if let Some(e) = node.manager.get_entity(1) {
            println!("[Target] Entity 1: Pos={:.1}, State={:?}", e.pos.0, e.state);
        }

        while let Some((id, msg_type)) = node.outgoing_messages.pop_front() {
            let msg_bytes = build_handoff_msg(id, msg_type, &node);
            let mut send_stream = connection.open_uni().await?;
            send_stream.write_all(&msg_bytes).await?;
            send_stream.finish()?;
        }

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

    let (scale_tx, mut scale_rx) = tokio::sync::mpsc::channel::<()>(10);

    let node_id = node_count;
    let port = next_port;
    let scale_tx_clone = scale_tx.clone();

    tokio::spawn(async move {
        spawn_and_monitor_node(node_id, port, None, scale_tx_clone).await;
    });

    node_count += 1;
    next_port += 1;

    loop {
        tokio::select! {
            _ = scale_rx.recv() => {
                if node_count >= max_nodes {
                    println!("[Orchestrator] ⚠️  MAX NODES ({}) reached.", max_nodes);
                    continue;
                }

                println!("[Orchestrator] 🚀 SCALING UP! Node {} on 127.0.0.1:{}", node_count, next_port);

                let node_id = node_count;
                let port = next_port;
                let seed_port = bind.port();
                let scale_tx_clone = scale_tx.clone();

                tokio::spawn(async move {
                    spawn_and_monitor_node(node_id, port, Some(seed_port), scale_tx_clone).await;
                });

                node_count += 1;
                next_port += 1;

                println!("[Orchestrator] 📊 Mesh has {} node(s)", node_count);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n[Orchestrator] Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

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

    while let Ok(Some(line)) = reader.next_line().await {
        println!("[Node {}] {}", node_id, line);

        if !has_triggered_scale && line.contains("CRITICAL LOAD") {
            println!("[Orchestrator] 🔔 Node {} reports CRITICAL LOAD!", node_id);
            let _ = scale_tx.send(()).await;
            has_triggered_scale = true;
        }
    }

    println!("[Orchestrator] Node {} exited", node_id);
}
