#[allow(unused_imports)]
use super::helpers::*;
use std::collections::{HashMap, HashSet};
use zeus_node::autoscaler::{AutoScaleConfig, AutoScaler, ScaleEvent};
use zeus_node::cell::{Cell, Face};

#[tokio::test]
async fn test_autoscaler_triggers_split_at_threshold() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();

    let positions_below: Vec<(f32, f32, f32)> = (0..30).map(|i| (i as f32 * 0.5, 12.0, 0.0)).collect();
    let events = scaler.evaluate(&cell, 30, &peers, &peer_cells, 1, &positions_below);
    assert!(!events.iter().any(|e| matches!(e, ScaleEvent::SplitRecommended { .. })),
        "30 < 40 threshold should not split");

    let positions_above: Vec<(f32, f32, f32)> = (0..45).map(|i| (i as f32 * 0.5, 12.0, 0.0)).collect();
    let mut scaler2 = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });
    let events2 = scaler2.evaluate(&cell, 45, &peers, &peer_cells, 1, &positions_above);
    assert!(events2.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })),
        "45 >= 40 threshold should trigger warmup/split");
}

#[tokio::test]
async fn test_autoscaler_split_produces_non_uniform_cells() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut positions: Vec<(f32, f32, f32)> = Vec::new();
    for i in 0..35 {
        positions.push((2.0 + i as f32 * 0.3, 12.0, 0.0));
    }
    for i in 0..15 {
        positions.push((20.0 + i as f32 * 0.2, 12.0, 0.0));
    }

    let (keep, new, axis, _pos) = AutoScaler::compute_binary_split(&cell, &positions);
    assert!(keep.shares_face(&new), "Split halves must be adjacent");
    let union = keep.union(&new);
    assert!((union.x_min - cell.x_min).abs() < 1e-3);
    assert!((union.x_max - cell.x_max).abs() < 1e-3);
    assert!((union.y_min - cell.y_min).abs() < 1e-3);
    assert!((union.y_max - cell.y_max).abs() < 1e-3);
    assert!((union.z_min - cell.z_min).abs() < 1e-3);
    assert!((union.z_max - cell.z_max).abs() < 1e-3);

    let keep_count = positions.iter().filter(|p| keep.contains(**p)).count();
    let new_count = positions.iter().filter(|p| new.contains(**p)).count();
    assert!(keep_count > 0, "keep_cell should contain some entities");
    assert!(new_count > 0, "new_cell should contain some entities");
    let exclusive_keep = positions.iter().filter(|p| keep.contains(**p) && !new.contains(**p)).count();
    let exclusive_new = positions.iter().filter(|p| new.contains(**p) && !keep.contains(**p)).count();
    let on_boundary = positions.iter().filter(|p| keep.contains(**p) && new.contains(**p)).count();
    assert_eq!(exclusive_keep + exclusive_new + on_boundary, 50, "All entities accounted for");

    assert!(matches!(axis, Face::YPos | Face::YNeg),
        "Y is longest axis (26 vs 24 vs 24), should split there, got {:?}", axis);
}

#[tokio::test]
async fn test_autoscaler_merge_triggers_below_threshold() {
    let cell = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 1024,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();

    let events = scaler.evaluate(&cell, 3, &peers, &peer_cells, 2, &[]);
    assert!(events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "3 < 5 with 2 nodes should recommend merge");
}

#[tokio::test]
async fn test_autoscaler_no_merge_on_single_node() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 1024,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();

    let events = scaler.evaluate(&cell, 2, &peers, &peer_cells, 1, &[]);
    assert!(!events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "Single node should never merge");
}

#[tokio::test]
async fn test_autoscaler_cell_expansion_on_adjacent_peer_death() {
    let my_cell = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
    let dead_cell = Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 1024,
        merge_cooldown_ticks: 1024,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });

    let mut peers = HashSet::new();
    peers.insert(42);
    let mut peer_cells = HashMap::new();
    peer_cells.insert(42, dead_cell.clone());
    scaler.evaluate(&my_cell, 10, &peers, &peer_cells, 2, &[]);

    let empty = HashSet::new();
    let empty_cells = HashMap::new();
    let events = scaler.evaluate(&my_cell, 10, &empty, &empty_cells, 1, &[]);

    let expanded = events.iter().find(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
    assert!(expanded.is_some(), "Adjacent peer death should expand cell");
    if let Some(ScaleEvent::CellExpanded { new_cell, .. }) = expanded {
        assert!((new_cell.x_min - 0.0).abs() < 1e-3);
        assert!((new_cell.x_max - 24.0).abs() < 1e-3);
        assert!((new_cell.volume() - my_cell.union(&dead_cell).volume()).abs() < 1.0);
    }
}

#[tokio::test]
async fn test_autoscaler_no_expansion_non_adjacent_peer() {
    let my_cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
    let far_cell = Cell::new(20.0, 30.0, 0.0, 10.0, 0.0, 10.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig::default());

    let mut peers = HashSet::new();
    peers.insert(42);
    let mut peer_cells = HashMap::new();
    peer_cells.insert(42, far_cell);
    scaler.evaluate(&my_cell, 10, &peers, &peer_cells, 2, &[]);

    let empty = HashSet::new();
    let empty_cells = HashMap::new();
    let events = scaler.evaluate(&my_cell, 10, &empty, &empty_cells, 1, &[]);

    assert!(!events.iter().any(|e| matches!(e, ScaleEvent::CellExpanded { .. })),
        "Non-adjacent peer death should NOT expand cell");
}

#[tokio::test]
async fn test_autoscaler_peer_join_leave_detection() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig::default());

    let mut peers = HashSet::new();
    peers.insert(10);
    peers.insert(20);
    let peer_cells = HashMap::new();
    let events = scaler.evaluate(&cell, 10, &peers, &peer_cells, 3, &[]);
    let joined: Vec<u64> = events.iter().filter_map(|e| {
        if let ScaleEvent::PeerJoined { id } = e { Some(*id) } else { None }
    }).collect();
    assert_eq!(joined.len(), 2);
    assert!(joined.contains(&10));
    assert!(joined.contains(&20));

    peers.remove(&10);
    let events2 = scaler.evaluate(&cell, 10, &peers, &peer_cells, 2, &[]);
    let left: Vec<u64> = events2.iter().filter_map(|e| {
        if let ScaleEvent::PeerLeft { id, .. } = e { Some(*id) } else { None }
    }).collect();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0], 10);
}

#[tokio::test]
async fn test_autoscaler_split_cooldown_prevents_rapid_splits() {
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 10,
        warmup_threshold: 10,
        merge_threshold: 2,
        split_cooldown_ticks: 100,
        merge_cooldown_ticks: 100,
        max_nodes: 16,
        startup_grace_ticks: 0,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();
    let positions: Vec<(f32, f32, f32)> = (0..20).map(|i| (i as f32 * 5.0, 50.0, 50.0)).collect();

    let events1 = scaler.evaluate(&cell, 20, &peers, &peer_cells, 1, &positions);
    assert!(events1.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })));

    let events2 = scaler.evaluate(&cell, 20, &peers, &peer_cells, 2, &positions);
    assert!(!events2.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })),
        "Cooldown should prevent immediate re-split");

    for _ in 0..99 {
        scaler.evaluate(&cell, 5, &peers, &peer_cells, 2, &[]);
    }

    let events3 = scaler.evaluate(&cell, 20, &peers, &peer_cells, 2, &positions);
    assert!(events3.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })),
        "After cooldown, split should be allowed again");
}

#[tokio::test]
async fn test_binary_split_axis_selection_z_dominant() {
    let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 100.0);
    let positions = vec![(5.0, 5.0, 50.0)];
    let (_, _, axis, _) = AutoScaler::compute_binary_split(&cell, &positions);
    assert!(matches!(axis, Face::ZPos | Face::ZNeg),
        "Z-axis is longest (100), should split along Z, got {:?}", axis);
}

#[tokio::test]
async fn test_binary_split_axis_selection_y_dominant() {
    let cell = Cell::new(0.0, 10.0, 0.0, 50.0, 0.0, 10.0);
    let positions = vec![(5.0, 25.0, 5.0)];
    let (_, _, axis, _) = AutoScaler::compute_binary_split(&cell, &positions);
    assert!(matches!(axis, Face::YPos | Face::YNeg),
        "Y-axis is longest (50), should split along Y, got {:?}", axis);
}

#[tokio::test]
async fn test_startup_grace_prevents_early_merge() {
    let cell = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
    let mut scaler = AutoScaler::new(AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 40,
        merge_threshold: 5,
        split_cooldown_ticks: 1024,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 100,
    });
    let peers = HashSet::new();
    let peer_cells = HashMap::new();

    let events = scaler.evaluate(&cell, 2, &peers, &peer_cells, 2, &[]);
    assert!(!events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "Should NOT merge during startup grace period");

    for _ in 0..99 {
        scaler.evaluate(&cell, 2, &peers, &peer_cells, 2, &[]);
    }

    let events2 = scaler.evaluate(&cell, 2, &peers, &peer_cells, 2, &[]);
    assert!(events2.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "After startup grace, merge should be allowed");
}

#[tokio::test]
async fn test_autoscaler_merge_threshold_boundary() {
    let config = AutoScaleConfig {
        split_threshold: 40,
        warmup_threshold: 30,
        merge_threshold: 5,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    };
    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut peers = HashSet::new();
    peers.insert(42);
    let peer_cells = HashMap::new();

    let mut scaler_at = AutoScaler::new(config.clone());
    let events_at = scaler_at.evaluate(&cell, 5, &peers, &peer_cells, 2, &[]);
    assert!(!events_at.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "At exactly merge_threshold, should NOT recommend merge");

    let mut scaler_below = AutoScaler::new(config.clone());
    let events_below = scaler_below.evaluate(&cell, 4, &peers, &peer_cells, 2, &[]);
    assert!(events_below.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "Below merge_threshold, should recommend merge");
}

#[tokio::test]
async fn test_expand_toward_returns_union() {
    let cell_a = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let dead_cell = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);

    let expanded = cell_a.expand_toward(&dead_cell);
    assert!(expanded.is_some());
    let ea = expanded.unwrap();
    assert!((ea.x_min - 0.0).abs() < 1e-3);
    assert!((ea.x_max - 100.0).abs() < 1e-3);
    assert!((ea.y_min - 0.0).abs() < 1e-3);
    assert!((ea.y_max - 100.0).abs() < 1e-3);
    assert!((ea.z_min - 0.0).abs() < 1e-3);
    assert!((ea.z_max - 100.0).abs() < 1e-3);
}

#[tokio::test]
async fn test_autoscaler_split_then_merge_cycle() {
    let config = AutoScaleConfig {
        split_threshold: 10,
        warmup_threshold: 8,
        merge_threshold: 3,
        split_cooldown_ticks: 0,
        merge_cooldown_ticks: 0,
        max_nodes: 16,
        startup_grace_ticks: 0,
    };
    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut scaler = AutoScaler::new(config);

    let events = scaler.evaluate(&cell, 15, &HashSet::new(), &HashMap::new(), 1,
        &[(50.0, 50.0, 50.0); 15]);
    assert!(events.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })
        || matches!(e, ScaleEvent::SplitRecommended { .. })),
        "15 entities > threshold 10 should trigger split");

    let mut peers = HashSet::new();
    peers.insert(42);
    let split_cell = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut peer_cells = HashMap::new();
    peer_cells.insert(42, split_cell.clone());
    let keep_cell = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
    let events2 = scaler.evaluate(&keep_cell, 2, &peers, &peer_cells, 2, &[]);
    assert!(events2.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)),
        "2 entities < threshold 3 with 2 nodes should trigger merge");

    let events3 = scaler.evaluate(&keep_cell, 2, &HashSet::new(), &HashMap::new(), 1, &[]);
    let expanded = events3.iter().any(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
    assert!(expanded, "After peer leaves, should expand cell");
}
