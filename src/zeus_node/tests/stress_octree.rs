use std::time::Instant;
use zeus_node::cell::Cell;
use zeus_node::entity_manager::{AuthorityState, Entity, EntityManager};
use zeus_node::octree::OctreeNode;

#[test]
fn test_10k_entities_octree_performance() {
    let cell = Cell::new(0.0, 1000.0, 0.0, 1000.0, 0.0, 1000.0);
    let mut mgr = EntityManager::new_3d(cell, 5.0);

    for i in 0..10_000u64 {
        let x = (i % 100) as f32 * 10.0;
        let y = ((i / 100) % 100) as f32 * 10.0;
        let z = (i / 10_000) as f32 * 100.0;
        mgr.add_entity(Entity {
            id: i,
            pos: (x, y, z),
            vel: (1.0, 0.5, 0.0),
            state: AuthorityState::Local,
            verifying_key: None,
        });
    }

    let start = Instant::now();
    let _candidates = mgr.update(1.0 / 128.0);
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 8,
        "10k entity tick should be under 8ms (128Hz), took {}ms",
        elapsed.as_millis()
    );
}

#[test]
fn test_rapid_split_merge_cycles() {
    let world_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    for _ in 0..20 {
        let mut root = OctreeNode::new_leaf(world_cell.clone(), 1);
        root.split();
        assert_eq!(root.leaf_count(), 8);

        if let OctreeNode::Internal { children, .. } = &mut root {
            children[0].split();
        }
        assert_eq!(root.leaf_count(), 15);

        if let OctreeNode::Internal { children, .. } = &mut root {
            children[0].merge_siblings();
        }
        assert_eq!(root.leaf_count(), 8);

        root.merge_siblings();
        assert_eq!(root.leaf_count(), 1);
    }
}

#[test]
fn test_100_nodes_topology_convergence() {
    let world_cell = Cell::new(0.0, 1000.0, 0.0, 1000.0, 0.0, 1000.0);
    let mut root = OctreeNode::new_leaf(world_cell.clone(), 0);
    root.split();

    fn recursive_split(node: &mut OctreeNode, depth: usize, max_depth: usize) {
        if depth >= max_depth { return; }
        if let OctreeNode::Internal { children, .. } = node {
            for child in children.iter_mut() {
                child.split();
                if depth + 1 < max_depth {
                    recursive_split(child, depth + 1, max_depth);
                }
            }
        }
    }

    recursive_split(&mut root, 0, 1);
    let leaves = root.all_leaves();
    assert!(leaves.len() >= 64);

    let start = Instant::now();
    let bytes = root.serialize();
    let elapsed_ser = start.elapsed();
    assert!(
        elapsed_ser.as_micros() < 500,
        "Serialization of {} leaves should take < 500us, took {}us",
        leaves.len(),
        elapsed_ser.as_micros()
    );

    let start = Instant::now();
    let (restored, _) = OctreeNode::deserialize(&bytes).unwrap();
    let elapsed_de = start.elapsed();
    assert!(
        elapsed_de.as_micros() < 500,
        "Deserialization should take < 500us, took {}us",
        elapsed_de.as_micros()
    );

    assert_eq!(restored.leaf_count(), root.leaf_count());
}

#[test]
fn bench_octree_neighbor_lookup() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
    root.split();
    if let OctreeNode::Internal { children, .. } = &mut root {
        children[0].split();
    }

    let leaf = root.find_leaf((25.0, 25.0, 25.0)).unwrap();
    let cell = leaf.cell().clone();

    let start = Instant::now();
    for _ in 0..10_000 {
        let _ = root.neighbor_at(&cell, zeus_node::cell::Face::XPos);
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() / 10_000;
    assert!(
        per_op_ns < 1000,
        "Neighbor lookup should take < 1us, took {}ns",
        per_op_ns
    );
}

#[test]
fn bench_octree_split_operation() {
    let start = Instant::now();
    for _ in 0..1000 {
        let mut root = OctreeNode::new_leaf(Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0), 1);
        root.split();
    }
    let elapsed = start.elapsed();
    let per_op_us = elapsed.as_micros() / 1000;
    assert!(
        per_op_us < 100,
        "Split operation should take < 100us, took {}us",
        per_op_us
    );
}

#[test]
fn bench_cell_containment_check() {
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let pos = (50.0, 50.0, 50.0);

    let start = Instant::now();
    for _ in 0..1_000_000 {
        let _ = cell.contains(pos);
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() / 1_000_000;
    assert!(
        per_op_ns < 10,
        "Cell containment check should take < 10ns, took {}ns",
        per_op_ns
    );
}

#[test]
fn bench_topology_serialization_100_leaves() {
    let mut root = OctreeNode::new_leaf(Cell::new(0.0, 1000.0, 0.0, 1000.0, 0.0, 1000.0), 0);
    root.split();
    if let OctreeNode::Internal { children, .. } = &mut root {
        for child in children.iter_mut() {
            child.split();
        }
    }
    assert!(root.leaf_count() >= 64);

    let start = Instant::now();
    for _ in 0..1000 {
        let _ = root.serialize();
    }
    let elapsed = start.elapsed();
    let per_op_us = elapsed.as_micros() / 1000;
    assert!(
        per_op_us < 100,
        "Topology serialization ({} leaves) should take < 100us, took {}us",
        root.leaf_count(),
        per_op_us
    );
}

#[test]
fn bench_control_protocol_encode_decode() {
    use zeus_orchestrator::protocol::*;

    let report = LoadReport {
        node_id: 42,
        entity_count: 1000,
        cpu_pct: 75,
        cell: Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0),
    };

    let start = Instant::now();
    for _ in 0..100_000 {
        let encoded = report.encode();
        let _ = LoadReport::decode(&encoded);
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() / 100_000;
    assert!(
        per_op_ns < 5000,
        "Control protocol encode/decode should take < 5us, took {}ns",
        per_op_ns
    );
}

#[test]
fn test_large_world_cell_relative_encoding() {
    use zeus_node::engine::*;

    let cell_origin = (100000.0, 50000.0, -200000.0);
    let entity_pos = (100005.5, 50002.3, -199998.7);

    let quantized = quantize_cell_relative(entity_pos, cell_origin);
    let restored = dequantize_cell_relative(quantized, cell_origin);

    assert!((restored.0 - entity_pos.0).abs() < 0.002);
    assert!((restored.1 - entity_pos.1).abs() < 0.002);
    assert!((restored.2 - entity_pos.2).abs() < 0.002);
}

#[test]
fn test_hierarchical_encoding_roundtrip() {
    use zeus_node::engine::*;

    let cell_id = 42u32;
    let offset = (1234i16, -5678i16, 100i16);
    let encoded = encode_hierarchical(cell_id, offset);
    let (decoded_id, decoded_offset) = decode_hierarchical(&encoded).unwrap();
    assert_eq!(decoded_id, cell_id);
    assert_eq!(decoded_offset, offset);
}

#[test]
fn test_i32_quantization_large_values() {
    use zeus_node::engine::*;

    let large_val = 1_000_000.0f32;
    let q = quantize_pos_i32(large_val);
    let dq = dequantize_pos_i32(q);
    assert!((dq - large_val).abs() < 1.0);

    let neg_val = -500_000.0f32;
    let q2 = quantize_pos_i32(neg_val);
    let dq2 = dequantize_pos_i32(q2);
    assert!((dq2 - neg_val).abs() < 1.0);
}
