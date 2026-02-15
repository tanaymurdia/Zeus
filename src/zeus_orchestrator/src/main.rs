use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use zeus_node::cell::Cell;
use zeus_orchestrator::protocol::{self, *};
use zeus_orchestrator::{Orchestrator, OrchestratorConfig};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4999")]
    bind: SocketAddr,
    #[arg(long, default_value = "0,100,0,100,0,100")]
    world: String,
    #[arg(long, default_value = "")]
    spawn_template: String,
    #[arg(long, default_value = "500")]
    split_threshold: u32,
    #[arg(long, default_value = "50")]
    merge_threshold: u32,
    #[arg(long, default_value = "30")]
    merge_hold_secs: u64,
    #[arg(long, default_value = "5000")]
    start_port: u16,
}

fn parse_world(s: &str) -> Cell {
    let parts: Vec<f32> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    if parts.len() == 6 {
        Cell::new(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
    } else {
        Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let world_cell = parse_world(&args.world);

    let config = OrchestratorConfig {
        bind_addr: args.bind,
        world_cell,
        spawn_template: args.spawn_template,
        split_threshold: args.split_threshold,
        merge_threshold: args.merge_threshold,
        merge_hold_secs: args.merge_hold_secs,
        start_port: args.start_port,
    };

    println!("[Orchestrator] Starting on {}", config.bind_addr);
    println!("[Orchestrator] World: {:?}", config.world_cell);
    println!("[Orchestrator] Split threshold: {}, Merge threshold: {}", config.split_threshold, config.merge_threshold);

    let (endpoint, _) = zeus_transport::make_promiscuous_endpoint(config.bind_addr)?;
    let orch = Arc::new(Mutex::new(Orchestrator::new(config)));

    let orch_eval = orch.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut o = orch_eval.lock().await;
            let splits = o.evaluate_splits();
            for decision in splits {
                println!("[Orchestrator] Split triggered for cell {:?} (node {})", decision.cell, decision.original_node_id);
                if let Some(assignments) = o.execute_split(&decision.cell) {
                    let topo_bytes = protocol::encode_topology_update(&o.topology_bytes());
                    for (cell, port) in &assignments[1..] {
                        if !o.config.spawn_template.is_empty() {
                            let bind = format!("127.0.0.1:{}", port);
                            let cmd = o.build_spawn_command(&bind, cell);
                            println!("[Orchestrator] Spawning: {}", cmd);
                            let parts: Vec<&str> = cmd.split_whitespace().collect();
                            if let Some((program, args)) = parts.split_first() {
                                let _ = tokio::process::Command::new(program)
                                    .args(args)
                                    .spawn();
                            }
                        }
                    }
                    drop(topo_bytes);
                }
            }

            let merges = o.evaluate_merges();
            for decision in merges {
                println!("[Orchestrator] Merge triggered: survivor={}, shutdowns={:?}", decision.survivor_id, decision.shutdown_ids);
                o.execute_merge(&decision.parent_cell, decision.survivor_id, &decision.shutdown_ids);
            }
        }
    });

    loop {
        if let Some(connecting) = endpoint.accept().await {
            let orch_conn = orch.clone();
            tokio::spawn(async move {
                match connecting.await {
                    Ok(conn) => {
                        let remote = conn.remote_address();
                        println!("[Orchestrator] Node connected from {}", remote);
                        handle_node_connection(conn, orch_conn).await;
                    }
                    Err(e) => {
                        eprintln!("[Orchestrator] Connection failed: {}", e);
                    }
                }
            });
        }
    }
}

async fn handle_node_connection(conn: quinn::Connection, orch: Arc<Mutex<Orchestrator>>) {
    loop {
        tokio::select! {
            result = conn.accept_uni() => {
                match result {
                    Ok(mut recv) => {
                        match recv.read_to_end(64 * 1024).await {
                            Ok(data) => {
                                if data.is_empty() { continue; }
                                match data[0] {
                                    MSG_LOAD_REPORT => {
                                        if let Some(report) = LoadReport::decode(&data) {
                                            let mut o = orch.lock().await;
                                            o.handle_load_report(report);
                                        }
                                    }
                                    MSG_REQUEST_SPLIT => {
                                        if let Some(req) = RequestSplit::decode(&data) {
                                            println!("[Orchestrator] Split requested by node {} for cell {:?}", req.node_id, req.cell);
                                            let mut o = orch.lock().await;
                                            if let Some(assignments) = o.execute_split(&req.cell) {
                                                let topo_bytes = protocol::encode_topology_update(&o.topology_bytes());
                                                let assign = CellAssign {
                                                    cell: assignments[0].0.clone(),
                                                    neighbors: vec![],
                                                };
                                                if let Ok(mut stream) = conn.open_uni().await {
                                                    let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, &assign.encode()).await;
                                                    let _ = stream.finish();
                                                }
                                                if let Ok(mut stream) = conn.open_uni().await {
                                                    let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, &topo_bytes).await;
                                                    let _ = stream.finish();
                                                }
                                            }
                                        }
                                    }
                                    MSG_REQUEST_MERGE => {
                                        if let Some(req) = RequestMerge::decode(&data) {
                                            println!("[Orchestrator] Merge requested by node {} for cell {:?}", req.node_id, req.cell);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Err(e) => {
                                eprintln!("[Orchestrator] Read error: {}", e);
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            result = conn.read_datagram() => {
                match result {
                    Ok(data) => {
                        if data.is_empty() { continue; }
                        match data[0] {
                            MSG_LOAD_REPORT => {
                                if let Some(report) = LoadReport::decode(&data) {
                                    let mut o = orch.lock().await;
                                    o.handle_load_report(report);
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}
