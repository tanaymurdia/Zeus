use zeus_node::cell::Cell;
use zeus_orchestrator::protocol::*;
use zeus_orchestrator::{Orchestrator, OrchestratorConfig};

fn test_config() -> OrchestratorConfig {
    OrchestratorConfig {
        world_cell: Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0),
        split_threshold: 10,
        merge_threshold: 2,
        merge_hold_secs: 0,
        spawn_template: "./test_server --bind {bind} --cell {cell} --orchestrator {orch}".to_string(),
        ..Default::default()
    }
}

#[test]
fn test_node_registers_with_orchestrator() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
    assert_eq!(orch.node_count(), 1);
    let leaf = orch.octree.find_leaf_by_id(1).unwrap();
    assert_eq!(leaf.cell(), &cell);
}

#[test]
fn test_load_report_received() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());

    let report = LoadReport {
        node_id: 1,
        entity_count: 500,
        cpu_pct: 50,
        cell,
    };
    orch.handle_load_report(report);
    assert_eq!(orch.nodes[&1].entity_count, 500);
    assert_eq!(orch.nodes[&1].cpu_pct, 50);
}

#[test]
fn test_split_triggered_by_load() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());

    orch.handle_load_report(LoadReport {
        node_id: 1,
        entity_count: 20,
        cpu_pct: 80,
        cell: cell.clone(),
    });

    let splits = orch.evaluate_splits();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].original_node_id, 1);
    assert_eq!(splits[0].cell, cell);

    let assignments = orch.execute_split(&splits[0].cell).unwrap();
    assert_eq!(assignments.len(), 8);
    assert_eq!(orch.leaf_count(), 8);
}

#[test]
fn test_merge_triggered_by_idle() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
    orch.execute_split(&cell);

    let octants = cell.split_octants();
    for (i, oct) in octants.iter().enumerate().skip(1) {
        let id = (i + 1) as u64;
        let port = 5000 + i as u16;
        orch.register_node(id, format!("127.0.0.1:{}", port).parse().unwrap(), oct.clone());
    }

    for id in 1..=8u64 {
        if orch.nodes.contains_key(&id) {
            orch.handle_load_report(LoadReport {
                node_id: id,
                entity_count: 1,
                cpu_pct: 5,
                cell: orch.nodes[&id].cell.clone(),
            });
        }
    }

    let merges = orch.evaluate_merges();
    assert_eq!(merges.len(), 1);
    assert_eq!(merges[0].survivor_id, 1);
    assert_eq!(merges[0].shutdown_ids.len(), 7);

    orch.execute_merge(&merges[0].parent_cell, merges[0].survivor_id, &merges[0].shutdown_ids);
    assert_eq!(orch.leaf_count(), 1);
    assert_eq!(orch.node_count(), 1);
}

#[test]
fn test_topology_update_broadcast() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
    orch.execute_split(&cell);

    let topo_bytes = orch.topology_bytes();
    let encoded = encode_topology_update(&topo_bytes);
    assert_eq!(encoded[0], MSG_TOPOLOGY_UPDATE);

    let decoded_payload = decode_topology_update(&encoded).unwrap();
    let (restored, _) = zeus_node::octree::OctreeNode::deserialize(decoded_payload).unwrap();
    assert_eq!(restored.leaf_count(), 8);
}

#[test]
fn test_spawn_command_execution() {
    let orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cmd = orch.build_spawn_command("127.0.0.1:5001", &cell);
    assert!(cmd.contains("./test_server"));
    assert!(cmd.contains("--bind 127.0.0.1:5001"));
    assert!(cmd.contains("--cell 0,50,0,50,0,50"));
    assert!(cmd.contains("--orchestrator 127.0.0.1:4999"));
}

#[test]
fn test_multiple_registrations_tracked() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
    orch.execute_split(&cell);

    let octants = cell.split_octants();
    for (i, oct) in octants.iter().enumerate().skip(1) {
        orch.register_node((i + 1) as u64, format!("127.0.0.1:{}", 5000 + i).parse().unwrap(), oct.clone());
    }
    assert_eq!(orch.node_count(), 8);
    assert_eq!(orch.leaf_count(), 8);
}

#[test]
fn test_recursive_split_flash_crowd() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
    orch.execute_split(&cell);

    let octant0 = cell.split_octants()[0].clone();
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), octant0.clone());
    orch.handle_load_report(LoadReport {
        node_id: 1,
        entity_count: 2000,
        cpu_pct: 90,
        cell: octant0.clone(),
    });

    let splits = orch.evaluate_splits();
    assert!(!splits.is_empty());
    orch.execute_split(&splits[0].cell);
    assert_eq!(orch.leaf_count(), 15);
    assert_eq!(orch.octree.depth(), 2);
}

#[test]
fn test_cell_assign_protocol_with_neighbors() {
    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let neighbor1 = NeighborInfo {
        addr: "127.0.0.1:5001".parse().unwrap(),
        cell: Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0),
    };
    let neighbor2 = NeighborInfo {
        addr: "127.0.0.1:5002".parse().unwrap(),
        cell: Cell::new(0.0, 50.0, 50.0, 100.0, 0.0, 50.0),
    };
    let assign = CellAssign {
        cell: cell.clone(),
        neighbors: vec![neighbor1, neighbor2],
    };
    let encoded = assign.encode();
    let decoded = CellAssign::decode(&encoded).unwrap();
    assert_eq!(decoded.cell, cell);
    assert_eq!(decoded.neighbors.len(), 2);
}

#[test]
fn test_biased_split_with_positions() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());

    let positions: Vec<(f32, f32, f32)> = (0..100).map(|i| {
        (i as f32 * 0.3, i as f32 * 0.5, 50.0)
    }).collect();

    let assignments = orch.execute_split_biased(&cell, &positions).unwrap();
    assert_eq!(assignments.len(), 8);
    assert_eq!(orch.leaf_count(), 8);
}

#[test]
fn test_load_report_encode_decode() {
    let report = LoadReport {
        node_id: 42,
        entity_count: 1000,
        cpu_pct: 75,
        cell: Cell::new(10.0, 50.0, 20.0, 60.0, 30.0, 70.0),
    };
    let encoded = report.encode();
    let decoded = LoadReport::decode(&encoded).unwrap();
    assert_eq!(decoded.node_id, 42);
    assert_eq!(decoded.entity_count, 1000);
    assert_eq!(decoded.cpu_pct, 75);
    assert_eq!(decoded.cell, report.cell);
}

#[test]
fn test_node_shutdown_with_target() {
    let shutdown = NodeShutdown {
        reason: 1,
        target_addr: Some("127.0.0.1:5000".parse().unwrap()),
    };
    let encoded = shutdown.encode();
    let decoded = NodeShutdown::decode(&encoded).unwrap();
    assert_eq!(decoded.reason, 1);
    assert!(decoded.target_addr.is_some());
}

#[test]
fn test_entity_conservation_during_split() {
    let mut orch = Orchestrator::new(test_config());
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
    orch.handle_load_report(LoadReport {
        node_id: 1,
        entity_count: 100,
        cpu_pct: 50,
        cell: cell.clone(),
    });

    orch.execute_split(&cell);
    let leaves = orch.octree.all_leaves();
    assert_eq!(leaves.len(), 8);
    let total_vol: f32 = leaves.iter().map(|l| l.cell().volume()).sum();
    assert!((total_vol - cell.volume()).abs() < 0.01);
}
