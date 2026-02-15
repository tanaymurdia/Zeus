pub mod protocol;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;
use zeus_node::cell::Cell;
use zeus_node::octree::OctreeNode;

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub bind_addr: SocketAddr,
    pub world_cell: Cell,
    pub spawn_template: String,
    pub split_threshold: u32,
    pub merge_threshold: u32,
    pub merge_hold_secs: u64,
    pub start_port: u16,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:4999".parse().unwrap(),
            world_cell: Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0),
            spawn_template: String::new(),
            split_threshold: 500,
            merge_threshold: 50,
            merge_hold_secs: 30,
            start_port: 5000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub node_id: u64,
    pub addr: SocketAddr,
    pub cell: Cell,
    pub entity_count: u32,
    pub cpu_pct: u8,
    pub last_report: Instant,
    pub below_merge_since: Option<Instant>,
}

pub struct Orchestrator {
    pub config: OrchestratorConfig,
    pub octree: OctreeNode,
    pub nodes: HashMap<u64, NodeRecord>,
    pub next_port: u16,
    pub pending_splits: Vec<Cell>,
    pub pending_merges: Vec<Cell>,
    pub pending_shutdowns: Vec<u64>,
}

impl Orchestrator {
    pub fn new(config: OrchestratorConfig) -> Self {
        let octree = OctreeNode::new_leaf(config.world_cell.clone(), 0);
        let next_port = config.start_port;
        Self {
            config,
            octree,
            nodes: HashMap::new(),
            next_port,
            pending_splits: Vec::new(),
            pending_merges: Vec::new(),
            pending_shutdowns: Vec::new(),
        }
    }

    pub fn register_node(&mut self, node_id: u64, addr: SocketAddr, cell: Cell) {
        self.octree.assign_node_id(&cell, node_id);
        self.nodes.insert(node_id, NodeRecord {
            node_id,
            addr,
            cell,
            entity_count: 0,
            cpu_pct: 0,
            last_report: Instant::now(),
            below_merge_since: None,
        });
    }

    pub fn handle_load_report(&mut self, report: protocol::LoadReport) {
        if let Some(record) = self.nodes.get_mut(&report.node_id) {
            record.entity_count = report.entity_count;
            record.cpu_pct = report.cpu_pct;
            record.last_report = Instant::now();
            self.octree.update_entity_count(report.node_id, report.entity_count);

            if report.entity_count < self.config.merge_threshold {
                if record.below_merge_since.is_none() {
                    record.below_merge_since = Some(Instant::now());
                }
            } else {
                record.below_merge_since = None;
            }
        }
    }

    pub fn evaluate_splits(&mut self) -> Vec<SplitDecision> {
        let mut decisions = Vec::new();
        let leaves: Vec<(Cell, u64, u32)> = self.octree.all_leaves().iter().filter_map(|l| {
            match l {
                OctreeNode::Leaf { cell, node_id, entity_count } => Some((cell.clone(), *node_id, *entity_count)),
                _ => None,
            }
        }).collect();

        for (cell, node_id, entity_count) in leaves {
            if entity_count > self.config.split_threshold && node_id != 0 {
                decisions.push(SplitDecision { cell, original_node_id: node_id });
            }
        }
        decisions
    }

    pub fn evaluate_merges(&mut self) -> Vec<MergeDecision> {
        let mut decisions = Vec::new();
        self.check_merge_candidates(&self.octree.clone(), &mut decisions);
        decisions
    }

    fn check_merge_candidates(&self, node: &OctreeNode, decisions: &mut Vec<MergeDecision>) {
        if let OctreeNode::Internal { cell, children } = node {
            let all_leaves = children.iter().all(|c| matches!(c, OctreeNode::Leaf { .. }));
            if all_leaves {
                let all_below = children.iter().all(|c| {
                    if let OctreeNode::Leaf { node_id, entity_count, .. } = c {
                        if *node_id == 0 { return true; }
                        if let Some(record) = self.nodes.get(node_id) {
                            if let Some(since) = record.below_merge_since {
                                return *entity_count < self.config.merge_threshold
                                    && since.elapsed().as_secs() >= self.config.merge_hold_secs;
                            }
                        }
                        *entity_count < self.config.merge_threshold
                    } else {
                        false
                    }
                });
                if all_below {
                    let survivor = children.iter().find_map(|c| {
                        if let OctreeNode::Leaf { node_id, .. } = c {
                            if *node_id != 0 { Some(*node_id) } else { None }
                        } else {
                            None
                        }
                    });
                    if let Some(survivor_id) = survivor {
                        let shutdown_ids: Vec<u64> = children.iter().filter_map(|c| {
                            if let OctreeNode::Leaf { node_id, .. } = c {
                                if *node_id != 0 && *node_id != survivor_id { Some(*node_id) } else { None }
                            } else {
                                None
                            }
                        }).collect();
                        if !shutdown_ids.is_empty() {
                            decisions.push(MergeDecision {
                                parent_cell: cell.clone(),
                                survivor_id,
                                shutdown_ids,
                            });
                        }
                    }
                }
            } else {
                for child in children.iter() {
                    self.check_merge_candidates(child, decisions);
                }
            }
        }
    }

    pub fn execute_split_biased(&mut self, cell: &Cell, entity_positions: &[(f32, f32, f32)]) -> Option<Vec<(Cell, u16)>> {
        if entity_positions.is_empty() {
            return self.execute_split(cell);
        }
        let center = cell.center();
        let total = entity_positions.len() as f32;
        let bias_x = entity_positions.iter().filter(|p| p.0 < center.0).count() as f32 / total;
        let bias_y = entity_positions.iter().filter(|p| p.1 < center.1).count() as f32 / total;
        let bias_z = entity_positions.iter().filter(|p| p.2 < center.2).count() as f32 / total;

        let leaf = self.octree.find_leaf(cell.center());
        let original_id = match leaf {
            Some(OctreeNode::Leaf { node_id, .. }) => *node_id,
            _ => return None,
        };

        let octants = cell.split_octants_biased((bias_x, bias_y, bias_z));

        fn find_and_split_biased(node: &mut OctreeNode, target: &Cell, octants: &[Cell; 8]) -> bool {
            match node {
                OctreeNode::Leaf { cell, node_id, .. } => {
                    if cell == target {
                        let nid = *node_id;
                        let parent_cell = cell.clone();
                        let children = Box::new([
                            OctreeNode::Leaf { cell: octants[0].clone(), node_id: nid, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[1].clone(), node_id: 0, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[2].clone(), node_id: 0, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[3].clone(), node_id: 0, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[4].clone(), node_id: 0, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[5].clone(), node_id: 0, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[6].clone(), node_id: 0, entity_count: 0 },
                            OctreeNode::Leaf { cell: octants[7].clone(), node_id: 0, entity_count: 0 },
                        ]);
                        *node = OctreeNode::Internal { cell: parent_cell, children };
                        true
                    } else {
                        false
                    }
                }
                OctreeNode::Internal { children, .. } => {
                    for child in children.iter_mut() {
                        if find_and_split_biased(child, target, octants) { return true; }
                    }
                    false
                }
            }
        }

        if !find_and_split_biased(&mut self.octree, cell, &octants) {
            return None;
        }

        if let Some(record) = self.nodes.get_mut(&original_id) {
            record.cell = octants[0].clone();
        }

        let mut new_assignments = Vec::new();
        new_assignments.push((octants[0].clone(), self.nodes.get(&original_id).map(|r| r.addr.port()).unwrap_or(self.next_port)));
        for octant in &octants[1..] {
            let port = self.next_port;
            self.next_port += 1;
            new_assignments.push((octant.clone(), port));
        }
        Some(new_assignments)
    }

    pub fn execute_split(&mut self, cell: &Cell) -> Option<Vec<(Cell, u16)>> {
        let leaf = self.octree.find_leaf(cell.center());
        let original_id = match leaf {
            Some(OctreeNode::Leaf { node_id, .. }) => *node_id,
            _ => return None,
        };

        fn find_and_split(node: &mut OctreeNode, target: &Cell) -> Option<[Cell; 8]> {
            match node {
                OctreeNode::Leaf { cell, .. } => {
                    if cell == target { node.split() } else { None }
                }
                OctreeNode::Internal { children, .. } => {
                    for child in children.iter_mut() {
                        if let Some(octants) = find_and_split(child, target) {
                            return Some(octants);
                        }
                    }
                    None
                }
            }
        }

        let octants = find_and_split(&mut self.octree, cell)?;

        self.octree.assign_node_id(&octants[0], original_id);
        if let Some(record) = self.nodes.get_mut(&original_id) {
            record.cell = octants[0].clone();
        }

        let mut new_assignments = Vec::new();
        new_assignments.push((octants[0].clone(), self.nodes.get(&original_id).map(|r| r.addr.port()).unwrap_or(self.next_port)));

        for octant in &octants[1..] {
            let port = self.next_port;
            self.next_port += 1;
            new_assignments.push((octant.clone(), port));
        }

        Some(new_assignments)
    }

    pub fn execute_merge(&mut self, parent_cell: &Cell, survivor_id: u64, shutdown_ids: &[u64]) {
        fn find_and_merge(node: &mut OctreeNode, target: &Cell) -> bool {
            match node {
                OctreeNode::Internal { cell, .. } => {
                    if cell == target {
                        node.merge_siblings().is_some()
                    } else {
                        if let OctreeNode::Internal { children, .. } = node {
                            for child in children.iter_mut() {
                                if find_and_merge(child, target) { return true; }
                            }
                        }
                        false
                    }
                }
                _ => false,
            }
        }

        find_and_merge(&mut self.octree, parent_cell);
        self.octree.assign_node_id(parent_cell, survivor_id);

        if let Some(record) = self.nodes.get_mut(&survivor_id) {
            record.cell = parent_cell.clone();
        }

        for id in shutdown_ids {
            self.pending_shutdowns.push(*id);
            self.nodes.remove(id);
        }
    }

    pub fn topology_bytes(&self) -> Vec<u8> {
        self.octree.serialize()
    }

    pub fn allocate_port(&mut self) -> u16 {
        let port = self.next_port;
        self.next_port += 1;
        port
    }

    pub fn build_spawn_command(&self, bind_addr: &str, cell: &Cell) -> String {
        self.config.spawn_template
            .replace("{bind}", bind_addr)
            .replace("{cell}", &format!("{},{},{},{},{},{}", cell.x_min, cell.x_max, cell.y_min, cell.y_max, cell.z_min, cell.z_max))
            .replace("{orch}", &self.config.bind_addr.to_string())
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn leaf_count(&self) -> usize {
        self.octree.leaf_count()
    }
}

#[derive(Debug, Clone)]
pub struct SplitDecision {
    pub cell: Cell,
    pub original_node_id: u64,
}

#[derive(Debug, Clone)]
pub struct MergeDecision {
    pub parent_cell: Cell,
    pub survivor_id: u64,
    pub shutdown_ids: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> OrchestratorConfig {
        OrchestratorConfig {
            world_cell: Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0),
            split_threshold: 10,
            merge_threshold: 2,
            merge_hold_secs: 0,
            ..Default::default()
        }
    }

    #[test]
    fn test_orchestrator_new() {
        let orch = Orchestrator::new(default_config());
        assert_eq!(orch.leaf_count(), 1);
        assert_eq!(orch.node_count(), 0);
    }

    #[test]
    fn test_register_node() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        assert_eq!(orch.node_count(), 1);
        let leaf = orch.octree.find_leaf_by_id(1).unwrap();
        assert_eq!(leaf.cell(), &cell);
    }

    #[test]
    fn test_load_report_updates_count() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        orch.handle_load_report(protocol::LoadReport {
            node_id: 1,
            entity_count: 500,
            cpu_pct: 50,
            cell,
        });
        assert_eq!(orch.nodes[&1].entity_count, 500);
    }

    #[test]
    fn test_split_decision_triggered() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        orch.handle_load_report(protocol::LoadReport {
            node_id: 1,
            entity_count: 20,
            cpu_pct: 50,
            cell,
        });
        let splits = orch.evaluate_splits();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].original_node_id, 1);
    }

    #[test]
    fn test_no_split_below_threshold() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        orch.handle_load_report(protocol::LoadReport {
            node_id: 1,
            entity_count: 5,
            cpu_pct: 10,
            cell,
        });
        let splits = orch.evaluate_splits();
        assert!(splits.is_empty());
    }

    #[test]
    fn test_execute_split() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        let assignments = orch.execute_split(&cell).unwrap();
        assert_eq!(assignments.len(), 8);
        assert_eq!(orch.leaf_count(), 8);
    }

    #[test]
    fn test_execute_merge() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        orch.execute_split(&cell);

        let octants = cell.split_octants();
        for (i, oct) in octants.iter().enumerate().skip(1) {
            let id = (i + 1) as u64;
            let port = 5000 + i as u16;
            orch.register_node(id, format!("127.0.0.1:{}", port).parse().unwrap(), oct.clone());
        }

        orch.execute_merge(&cell, 1, &[2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(orch.leaf_count(), 1);
        assert_eq!(orch.node_count(), 1);
    }

    #[test]
    fn test_build_spawn_command() {
        let mut config = default_config();
        config.spawn_template = "./server run-node --bind {bind} --cell {cell} --orchestrator {orch}".to_string();
        config.bind_addr = "127.0.0.1:4999".parse().unwrap();
        let orch = Orchestrator::new(config);
        let cmd = orch.build_spawn_command("127.0.0.1:5001", &Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0));
        assert!(cmd.contains("--bind 127.0.0.1:5001"));
        assert!(cmd.contains("--cell 0,50,0,50,0,50"));
        assert!(cmd.contains("--orchestrator 127.0.0.1:4999"));
    }

    #[test]
    fn test_topology_bytes_roundtrip() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        orch.execute_split(&cell);
        let bytes = orch.topology_bytes();
        let (restored, _) = OctreeNode::deserialize(&bytes).unwrap();
        assert_eq!(restored.leaf_count(), 8);
    }

    #[test]
    fn test_recursive_split() {
        let mut orch = Orchestrator::new(default_config());
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), cell.clone());
        orch.execute_split(&cell);
        let first_octant = cell.split_octants()[0].clone();
        orch.register_node(1, "127.0.0.1:5000".parse().unwrap(), first_octant.clone());
        let second_split = orch.execute_split(&first_octant);
        assert!(second_split.is_some());
        assert_eq!(orch.leaf_count(), 15);
    }

    #[test]
    fn test_allocate_port() {
        let mut orch = Orchestrator::new(default_config());
        assert_eq!(orch.allocate_port(), 5000);
        assert_eq!(orch.allocate_port(), 5001);
        assert_eq!(orch.allocate_port(), 5002);
    }
}
