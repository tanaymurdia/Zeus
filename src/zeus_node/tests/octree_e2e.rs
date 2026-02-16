use zeus_node::cell::{Cell, Face};
use zeus_node::entity_manager::{AuthorityState, Entity, EntityManager};
use zeus_node::octree::OctreeNode;

#[test]
fn test_client_sees_all_entities_across_octree() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
    root.split();

    let mut positions = vec![
        (25.0, 25.0, 25.0),
        (75.0, 25.0, 25.0),
        (25.0, 75.0, 25.0),
        (75.0, 75.0, 75.0),
    ];

    let mut entity_map: std::collections::HashMap<(i32, i32, i32), Vec<(f32, f32, f32)>> = std::collections::HashMap::new();
    for pos in &positions {
        if let Some(leaf) = root.find_leaf(*pos) {
            let key = (
                (leaf.cell().x_min as i32),
                (leaf.cell().y_min as i32),
                (leaf.cell().z_min as i32),
            );
            entity_map.entry(key).or_default().push(*pos);
        }
    }

    let total: usize = entity_map.values().map(|v| v.len()).sum();
    assert_eq!(total, positions.len());
}

#[test]
fn test_flash_crowd_triggers_recursive_split() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);

    root.split();
    assert_eq!(root.leaf_count(), 8);

    if let OctreeNode::Internal { children, .. } = &mut root {
        children[0].split();
    }
    assert_eq!(root.leaf_count(), 15);

    if let OctreeNode::Internal { children, .. } = &mut root {
        if let OctreeNode::Internal { children: sub, .. } = &mut children[0] {
            sub[0].split();
        }
    }
    assert_eq!(root.leaf_count(), 22);
    assert_eq!(root.depth(), 3);
}

#[test]
fn test_scale_down_after_crowd_disperses() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
    root.split();
    assert_eq!(root.leaf_count(), 8);

    root.merge_siblings().unwrap();
    assert_eq!(root.leaf_count(), 1);
    assert_eq!(root.depth(), 0);
    assert_eq!(root.cell(), &Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0));
}

#[test]
fn test_8_node_full_mesh_3d() {
    let root_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut root = OctreeNode::new_leaf(root_cell.clone(), 0);
    root.split();

    let leaves = root.all_leaves();
    assert_eq!(leaves.len(), 8);

    for leaf in &leaves {
        let center = leaf.cell().center();
        let found = root.find_leaf(center).unwrap();
        assert_eq!(found.cell(), leaf.cell());
    }

    for i in 0..8 {
        let leaf = &leaves[i];
        let has_xpos_neighbor = root.neighbor_at(leaf.cell(), Face::XPos).is_some();
        let has_xneg_neighbor = root.neighbor_at(leaf.cell(), Face::XNeg).is_some();
        let has_ypos_neighbor = root.neighbor_at(leaf.cell(), Face::YPos).is_some();
        let has_yneg_neighbor = root.neighbor_at(leaf.cell(), Face::YNeg).is_some();
        let has_zpos_neighbor = root.neighbor_at(leaf.cell(), Face::ZPos).is_some();
        let has_zneg_neighbor = root.neighbor_at(leaf.cell(), Face::ZNeg).is_some();
        let neighbor_count = [has_xpos_neighbor, has_xneg_neighbor, has_ypos_neighbor, has_yneg_neighbor, has_zpos_neighbor, has_zneg_neighbor].iter().filter(|&&x| x).count();
        assert!(neighbor_count >= 3, "Each corner octant should have at least 3 face neighbors, got {} for octant {}", neighbor_count, i);
    }
}

#[test]
fn test_client_handoff_seamless_3d() {
    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let _cell_b = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);

    let mut mgr_a = EntityManager::new_3d(cell_a.clone(), 1.0);
    mgr_a.add_entity(Entity {
        id: 1,
        pos: (52.0, 25.0, 25.0),
        vel: (5.0, 0.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    let candidates = mgr_a.update(1.0);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].0, 1);
    assert_eq!(candidates[0].1, Face::XPos);
}

#[test]
fn test_handoff_across_y_boundary_3d() {
    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut mgr = EntityManager::new_3d(cell, 1.0);
    mgr.add_entity(Entity {
        id: 1,
        pos: (25.0, 52.0, 25.0),
        vel: (0.0, 5.0, 0.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    let candidates = mgr.update(1.0);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].1, Face::YPos);
}

#[test]
fn test_handoff_across_z_boundary_3d() {
    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut mgr = EntityManager::new_3d(cell, 1.0);
    mgr.add_entity(Entity {
        id: 1,
        pos: (25.0, 25.0, 52.0),
        vel: (0.0, 0.0, 5.0),
        state: AuthorityState::Local,
        verifying_key: None,
    });

    let candidates = mgr.update(1.0);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].1, Face::ZPos);
}

#[test]
fn test_handoff_between_different_size_cells() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
    root.split();

    if let OctreeNode::Internal { children, .. } = &mut root {
        children[1].split();
    }

    let small_cell = root.find_leaf((75.0, 1.0, 1.0)).unwrap().cell().clone();
    let large_neighbor = root.neighbor_at(&small_cell, Face::XNeg);
    assert!(large_neighbor.is_some());
    let n = large_neighbor.unwrap();
    assert!(n.cell().volume() > small_cell.volume());
}

#[test]
fn test_entity_conservation_during_octree_split() {
    let entities = vec![
        (1, (10.0, 10.0, 10.0)),
        (2, (60.0, 10.0, 10.0)),
        (3, (10.0, 60.0, 10.0)),
        (4, (60.0, 60.0, 60.0)),
        (5, (90.0, 90.0, 90.0)),
    ];

    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
    root.split();

    let mut assigned = 0;
    for (id, pos) in &entities {
        if root.find_leaf(*pos).is_some() {
            assigned += 1;
        }
    }
    assert_eq!(assigned, entities.len());
}

#[test]
fn test_no_dual_ownership_3d() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
    root.split();

    let test_points = vec![
        (25.0, 25.0, 25.0),
        (75.0, 25.0, 25.0),
        (25.0, 75.0, 25.0),
        (75.0, 75.0, 75.0),
    ];

    for pos in &test_points {
        let mut containing_cells = 0;
        for leaf in root.all_leaves() {
            if leaf.cell().contains(*pos) {
                containing_cells += 1;
            }
        }
        assert_eq!(containing_cells, 1, "Position {:?} should be in exactly 1 cell", pos);
    }
}

#[test]
fn test_gossip_spatial_filtering_3d() {
    let cell_a = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let cell_b = Cell::new(50.0, 100.0, 0.0, 50.0, 0.0, 50.0);
    let cell_far = Cell::new(0.0, 50.0, 50.0, 100.0, 50.0, 100.0);

    assert!(cell_a.shares_face(&cell_b));
    assert!(!cell_a.shares_face(&cell_far));
}
