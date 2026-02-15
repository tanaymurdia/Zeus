use crate::cell::{Cell, Face};

#[derive(Clone, Debug)]
pub enum OctreeNode {
    Leaf {
        cell: Cell,
        node_id: u64,
        entity_count: u32,
    },
    Internal {
        cell: Cell,
        children: Box<[OctreeNode; 8]>,
    },
}

impl OctreeNode {
    pub fn new_leaf(cell: Cell, node_id: u64) -> Self {
        OctreeNode::Leaf { cell, node_id, entity_count: 0 }
    }

    pub fn cell(&self) -> &Cell {
        match self {
            OctreeNode::Leaf { cell, .. } => cell,
            OctreeNode::Internal { cell, .. } => cell,
        }
    }

    pub fn find_leaf(&self, pos: (f32, f32, f32)) -> Option<&OctreeNode> {
        match self {
            OctreeNode::Leaf { cell, .. } => {
                if cell.contains(pos) { Some(self) } else { None }
            }
            OctreeNode::Internal { cell, children } => {
                if !cell.contains(pos) { return None; }
                for child in children.iter() {
                    if let Some(leaf) = child.find_leaf(pos) {
                        return Some(leaf);
                    }
                }
                None
            }
        }
    }

    pub fn find_leaf_by_id(&self, target_id: u64) -> Option<&OctreeNode> {
        match self {
            OctreeNode::Leaf { node_id, .. } => {
                if *node_id == target_id { Some(self) } else { None }
            }
            OctreeNode::Internal { children, .. } => {
                for child in children.iter() {
                    if let Some(found) = child.find_leaf_by_id(target_id) {
                        return Some(found);
                    }
                }
                None
            }
        }
    }

    pub fn neighbor_at(&self, query_cell: &Cell, face: Face) -> Option<&OctreeNode> {
        let probe = match face {
            Face::XPos => (query_cell.x_max + 0.001, (query_cell.y_min + query_cell.y_max) * 0.5, (query_cell.z_min + query_cell.z_max) * 0.5),
            Face::XNeg => (query_cell.x_min - 0.001, (query_cell.y_min + query_cell.y_max) * 0.5, (query_cell.z_min + query_cell.z_max) * 0.5),
            Face::YPos => ((query_cell.x_min + query_cell.x_max) * 0.5, query_cell.y_max + 0.001, (query_cell.z_min + query_cell.z_max) * 0.5),
            Face::YNeg => ((query_cell.x_min + query_cell.x_max) * 0.5, query_cell.y_min - 0.001, (query_cell.z_min + query_cell.z_max) * 0.5),
            Face::ZPos => ((query_cell.x_min + query_cell.x_max) * 0.5, (query_cell.y_min + query_cell.y_max) * 0.5, query_cell.z_max + 0.001),
            Face::ZNeg => ((query_cell.x_min + query_cell.x_max) * 0.5, (query_cell.y_min + query_cell.y_max) * 0.5, query_cell.z_min - 0.001),
        };
        self.find_leaf(probe)
    }

    pub fn split(&mut self) -> Option<[Cell; 8]> {
        let (cell, node_id) = match self {
            OctreeNode::Leaf { cell, node_id, .. } => (cell.clone(), *node_id),
            OctreeNode::Internal { .. } => return None,
        };
        let octants = cell.split_octants();
        let children = Box::new([
            OctreeNode::new_leaf(octants[0].clone(), node_id),
            OctreeNode::new_leaf(octants[1].clone(), 0),
            OctreeNode::new_leaf(octants[2].clone(), 0),
            OctreeNode::new_leaf(octants[3].clone(), 0),
            OctreeNode::new_leaf(octants[4].clone(), 0),
            OctreeNode::new_leaf(octants[5].clone(), 0),
            OctreeNode::new_leaf(octants[6].clone(), 0),
            OctreeNode::new_leaf(octants[7].clone(), 0),
        ]);
        *self = OctreeNode::Internal { cell, children };
        Some(octants)
    }

    pub fn merge_siblings(&mut self) -> Option<Cell> {
        let (cell, survivor_id) = match self {
            OctreeNode::Internal { cell, children } => {
                let first_id = match &children[0] {
                    OctreeNode::Leaf { node_id, .. } => *node_id,
                    OctreeNode::Internal { .. } => return None,
                };
                for child in children.iter() {
                    match child {
                        OctreeNode::Leaf { .. } => {}
                        OctreeNode::Internal { .. } => return None,
                    }
                }
                (cell.clone(), first_id)
            }
            OctreeNode::Leaf { .. } => return None,
        };
        *self = OctreeNode::Leaf { cell: cell.clone(), node_id: survivor_id, entity_count: 0 };
        Some(cell)
    }

    pub fn all_leaves(&self) -> Vec<&OctreeNode> {
        let mut result = Vec::new();
        self.collect_leaves(&mut result);
        result
    }

    fn collect_leaves<'a>(&'a self, out: &mut Vec<&'a OctreeNode>) {
        match self {
            OctreeNode::Leaf { .. } => out.push(self),
            OctreeNode::Internal { children, .. } => {
                for child in children.iter() {
                    child.collect_leaves(out);
                }
            }
        }
    }

    pub fn leaf_count(&self) -> usize {
        match self {
            OctreeNode::Leaf { .. } => 1,
            OctreeNode::Internal { children, .. } => {
                children.iter().map(|c| c.leaf_count()).sum()
            }
        }
    }

    pub fn depth(&self) -> usize {
        match self {
            OctreeNode::Leaf { .. } => 0,
            OctreeNode::Internal { children, .. } => {
                1 + children.iter().map(|c| c.depth()).max().unwrap_or(0)
            }
        }
    }

    pub fn update_entity_count(&mut self, target_id: u64, count: u32) -> bool {
        match self {
            OctreeNode::Leaf { node_id, entity_count, .. } => {
                if *node_id == target_id {
                    *entity_count = count;
                    true
                } else {
                    false
                }
            }
            OctreeNode::Internal { children, .. } => {
                for child in children.iter_mut() {
                    if child.update_entity_count(target_id, count) {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn assign_node_id(&mut self, cell_match: &Cell, new_id: u64) -> bool {
        match self {
            OctreeNode::Leaf { cell, node_id, .. } => {
                if cell == cell_match {
                    *node_id = new_id;
                    true
                } else {
                    false
                }
            }
            OctreeNode::Internal { children, .. } => {
                for child in children.iter_mut() {
                    if child.assign_node_id(cell_match, new_id) {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf);
        buf
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            OctreeNode::Leaf { cell, node_id, entity_count } => {
                buf.push(0x00);
                buf.extend_from_slice(&cell.serialize());
                buf.extend_from_slice(&node_id.to_le_bytes());
                buf.extend_from_slice(&entity_count.to_le_bytes());
            }
            OctreeNode::Internal { cell, children } => {
                buf.push(0x01);
                buf.extend_from_slice(&cell.serialize());
                for child in children.iter() {
                    child.serialize_into(buf);
                }
            }
        }
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.is_empty() { return None; }
        match data[0] {
            0x00 => {
                if data.len() < 1 + 24 + 8 + 4 { return None; }
                let cell = Cell::deserialize(&data[1..25])?;
                let node_id = u64::from_le_bytes(data[25..33].try_into().ok()?);
                let entity_count = u32::from_le_bytes(data[33..37].try_into().ok()?);
                Some((OctreeNode::Leaf { cell, node_id, entity_count }, 37))
            }
            0x01 => {
                if data.len() < 1 + 24 { return None; }
                let cell = Cell::deserialize(&data[1..25])?;
                let mut offset = 25;
                let mut children_vec = Vec::with_capacity(8);
                for _ in 0..8 {
                    let (child, consumed) = OctreeNode::deserialize(&data[offset..])?;
                    children_vec.push(child);
                    offset += consumed;
                }
                let children: [OctreeNode; 8] = children_vec.try_into().ok()?;
                Some((OctreeNode::Internal { cell, children: Box::new(children) }, offset))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_octree_single_leaf() {
        let root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        assert_eq!(root.leaf_count(), 1);
        assert_eq!(root.depth(), 0);
        let leaf = root.find_leaf((50.0, 50.0, 50.0)).unwrap();
        match leaf {
            OctreeNode::Leaf { node_id, .. } => assert_eq!(*node_id, 1),
            _ => panic!("Expected leaf"),
        }
    }

    #[test]
    fn test_octree_split() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        let octants = root.split().unwrap();
        assert_eq!(octants.len(), 8);
        assert_eq!(root.leaf_count(), 8);
        assert_eq!(root.depth(), 1);
        let leaves = root.all_leaves();
        assert_eq!(leaves.len(), 8);
    }

    #[test]
    fn test_octree_find_leaf_after_split() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        let leaf = root.find_leaf((25.0, 25.0, 25.0)).unwrap();
        assert!(leaf.cell().contains((25.0, 25.0, 25.0)));
        let leaf2 = root.find_leaf((75.0, 75.0, 75.0)).unwrap();
        assert!(leaf2.cell().contains((75.0, 75.0, 75.0)));
        assert_ne!(leaf.cell(), leaf2.cell());
    }

    #[test]
    fn test_octree_neighbor_same_level() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        let leaf_a = root.find_leaf((25.0, 25.0, 25.0)).unwrap();
        let neighbor = root.neighbor_at(leaf_a.cell(), Face::XPos).unwrap();
        let neighbor_center = neighbor.cell().center();
        assert!(neighbor_center.0 > 50.0);
    }

    #[test]
    fn test_octree_neighbor_different_level() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        if let OctreeNode::Internal { children, .. } = &mut root {
            children[1].split();
        }
        let small_leaf = root.find_leaf((51.0, 1.0, 1.0)).unwrap();
        let neighbor = root.neighbor_at(small_leaf.cell(), Face::XNeg);
        assert!(neighbor.is_some());
        let n = neighbor.unwrap();
        assert!(n.cell().x_max <= small_leaf.cell().x_min + 0.01);
    }

    #[test]
    fn test_octree_merge() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        assert_eq!(root.leaf_count(), 8);
        let merged_cell = root.merge_siblings().unwrap();
        assert_eq!(root.leaf_count(), 1);
        assert_eq!(merged_cell, Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0));
    }

    #[test]
    fn test_octree_recursive_split() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        if let OctreeNode::Internal { children, .. } = &mut root {
            children[0].split();
        }
        assert_eq!(root.leaf_count(), 15);
        assert_eq!(root.depth(), 2);
    }

    #[test]
    fn test_octree_serialize_roundtrip() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 42);
        root.split();
        let bytes = root.serialize();
        let (restored, consumed) = OctreeNode::deserialize(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(restored.leaf_count(), root.leaf_count());
        assert_eq!(restored.depth(), root.depth());
    }

    #[test]
    fn test_octree_all_leaves() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        let leaves = root.all_leaves();
        assert_eq!(leaves.len(), 8);
        let total_vol: f32 = leaves.iter().map(|l| l.cell().volume()).sum();
        assert!((total_vol - 100.0 * 100.0 * 100.0).abs() < 0.01);
    }

    #[test]
    fn test_octree_update_entity_count() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 5);
        assert!(root.update_entity_count(5, 42));
        match &root {
            OctreeNode::Leaf { entity_count, .. } => assert_eq!(*entity_count, 42),
            _ => panic!("Expected leaf"),
        }
        assert!(!root.update_entity_count(999, 10));
    }

    #[test]
    fn test_octree_assign_node_id() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        let target_cell = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);
        assert!(root.assign_node_id(&target_cell, 99));
        let leaf = root.find_leaf((75.0, 25.0, 25.0)).unwrap();
        match leaf {
            OctreeNode::Leaf { node_id, .. } => assert_eq!(*node_id, 99),
            _ => panic!("Expected leaf"),
        }
    }

    #[test]
    fn test_octree_find_leaf_outside() {
        let root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        assert!(root.find_leaf((200.0, 200.0, 200.0)).is_none());
    }

    #[test]
    fn test_octree_find_leaf_by_id() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        let target_cell = Cell::new(50.0, 100.0, 50.0, 100.0, 50.0, 100.0);
        root.assign_node_id(&target_cell, 42);
        let found = root.find_leaf_by_id(42).unwrap();
        assert_eq!(found.cell(), &target_cell);
        assert!(root.find_leaf_by_id(999).is_none());
    }

    #[test]
    fn test_octree_neighbor_boundary() {
        let root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        let result = root.neighbor_at(root.cell(), Face::XPos);
        assert!(result.is_none());
    }

    #[test]
    fn test_merge_fails_on_non_leaf_children() {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
        if let OctreeNode::Internal { children, .. } = &mut root {
            children[0].split();
        }
        assert!(root.merge_siblings().is_none());
    }
}
