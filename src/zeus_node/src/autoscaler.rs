use std::collections::{HashMap, HashSet};
use crate::cell::{Cell, Face};

#[derive(Clone, Debug)]
pub struct AutoScaleConfig {
    pub split_threshold: usize,
    pub merge_threshold: usize,
    pub warmup_threshold: usize,
    pub split_cooldown_ticks: u32,
    pub merge_cooldown_ticks: u32,
    pub max_nodes: usize,
    pub startup_grace_ticks: u32,
}

impl Default for AutoScaleConfig {
    fn default() -> Self {
        Self {
            split_threshold: 40,
            merge_threshold: 5,
            warmup_threshold: 30,
            split_cooldown_ticks: 512,
            merge_cooldown_ticks: 1024,
            max_nodes: 16,
            startup_grace_ticks: 256,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ScaleEvent {
    WarmupRecommended {
        projected_cell: Cell,
        projected_new_cell: Cell,
        split_axis: Face,
        split_pos: f32,
    },
    SplitRecommended {
        keep_cell: Cell,
        new_cell: Cell,
        split_axis: Face,
        split_pos: f32,
    },
    MergeRecommended,
    PeerJoined { id: u64 },
    PeerLeft { id: u64, cell: Option<Cell> },
    CellExpanded { new_cell: Cell, dead_peer_id: u64 },
}

pub struct AutoScaler {
    pub config: AutoScaleConfig,
    last_split_tick: u32,
    last_merge_tick: u32,
    tick_counter: u32,
    known_peer_ids: HashSet<u64>,
    known_peer_cells: HashMap<u64, Cell>,
    warmup_emitted: bool,
}

impl AutoScaler {
    pub fn new(config: AutoScaleConfig) -> Self {
        Self {
            config,
            last_split_tick: 0,
            last_merge_tick: 0,
            tick_counter: 0,
            known_peer_ids: HashSet::new(),
            known_peer_cells: HashMap::new(),
            warmup_emitted: false,
        }
    }

    pub fn evaluate(
        &mut self,
        my_cell: &Cell,
        local_entity_count: usize,
        current_peer_ids: &HashSet<u64>,
        current_peer_cells: &HashMap<u64, Cell>,
        total_nodes: usize,
        local_positions: &[(f32, f32, f32)],
    ) -> Vec<ScaleEvent> {
        self.tick_counter += 1;
        let mut events = Vec::new();

        let joined: Vec<u64> = current_peer_ids.difference(&self.known_peer_ids).copied().collect();
        let left: Vec<u64> = self.known_peer_ids.difference(current_peer_ids).copied().collect();

        for id in &joined {
            events.push(ScaleEvent::PeerJoined { id: *id });
        }

        for id in &left {
            let dead_cell = self.known_peer_cells.get(id).cloned();
            events.push(ScaleEvent::PeerLeft { id: *id, cell: dead_cell.clone() });
            if let Some(ref dc) = dead_cell {
                if my_cell.shares_face(dc) {
                    if let Some(expanded) = my_cell.expand_toward(dc) {
                        events.push(ScaleEvent::CellExpanded {
                            new_cell: expanded,
                            dead_peer_id: *id,
                        });
                    }
                }
            }
        }

        self.known_peer_ids = current_peer_ids.clone();
        self.known_peer_cells = current_peer_cells.clone();

        let split_cooldown_ok = self.last_split_tick == 0
            || self.tick_counter.saturating_sub(self.last_split_tick)
                >= self.config.split_cooldown_ticks;

        if split_cooldown_ok && self.last_split_tick > 0 {
            self.warmup_emitted = false;
        }

        if local_entity_count >= self.config.warmup_threshold
            && !self.warmup_emitted
            && split_cooldown_ok
            && total_nodes < self.config.max_nodes
        {
            let (keep, new, axis, pos) = Self::compute_binary_split(my_cell, local_positions);
            events.push(ScaleEvent::WarmupRecommended {
                projected_cell: keep,
                projected_new_cell: new,
                split_axis: axis,
                split_pos: pos,
            });
            self.warmup_emitted = true;
        }

        if local_entity_count >= self.config.split_threshold
            && self.warmup_emitted
            && total_nodes < self.config.max_nodes
        {
            let (keep, new, axis, pos) = Self::compute_binary_split(my_cell, local_positions);
            events.push(ScaleEvent::SplitRecommended {
                keep_cell: keep,
                new_cell: new,
                split_axis: axis,
                split_pos: pos,
            });
            self.warmup_emitted = false;
            self.last_split_tick = self.tick_counter;
        }

        let past_startup = self.tick_counter > self.config.startup_grace_ticks;
        let merge_cooldown_ok = self.last_merge_tick == 0
            || self.tick_counter.saturating_sub(self.last_merge_tick)
                >= self.config.merge_cooldown_ticks;
        if local_entity_count < self.config.merge_threshold
            && total_nodes > 1
            && merge_cooldown_ok
            && past_startup
        {
            events.push(ScaleEvent::MergeRecommended);
            self.last_merge_tick = self.tick_counter;
        }

        events
    }

    pub fn compute_binary_split(
        cell: &Cell,
        positions: &[(f32, f32, f32)],
    ) -> (Cell, Cell, Face, f32) {
        let size = cell.size();
        let (dx, dy, dz) = size;

        let axis = if dx >= dy && dx >= dz {
            Face::XPos
        } else if dy >= dx && dy >= dz {
            Face::YPos
        } else {
            Face::ZPos
        };

        let split_pos = if positions.is_empty() {
            match axis {
                Face::XPos | Face::XNeg => (cell.x_min + cell.x_max) * 0.5,
                Face::YPos | Face::YNeg => (cell.y_min + cell.y_max) * 0.5,
                _ => (cell.z_min + cell.z_max) * 0.5,
            }
        } else {
            let (min_bound, max_bound) = match axis {
                Face::XPos | Face::XNeg => (cell.x_min, cell.x_max),
                Face::YPos | Face::YNeg => (cell.y_min, cell.y_max),
                _ => (cell.z_min, cell.z_max),
            };
            let span = max_bound - min_bound;
            let clamp_lo = min_bound + span * 0.2;
            let clamp_hi = min_bound + span * 0.8;

            let mut sorted: Vec<f32> = positions.iter().map(|p| match axis {
                Face::XPos | Face::XNeg => p.0,
                Face::YPos | Face::YNeg => p.1,
                _ => p.2,
            }).collect();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let median = sorted[sorted.len() / 2];
            median.clamp(clamp_lo, clamp_hi)
        };

        let (a, b) = cell.split_binary(axis, split_pos);

        let count_a = positions.iter().filter(|p| a.contains(**p)).count();
        let count_b = positions.len().saturating_sub(count_a);

        if count_a >= count_b {
            (a, b, axis, split_pos)
        } else {
            (b, a, axis, split_pos)
        }
    }

    pub fn reset_split_cooldown(&mut self) {
        self.last_split_tick = self.tick_counter;
    }

    pub fn reset_merge_cooldown(&mut self) {
        self.last_merge_tick = self.tick_counter;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> AutoScaleConfig {
        AutoScaleConfig::default()
    }

    fn make_cell() -> Cell {
        Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0)
    }

    #[test]
    fn test_split_recommended_above_threshold() {
        let mut scaler = AutoScaler::new(default_config());
        let cell = make_cell();
        let peers = HashSet::new();
        let peer_cells = HashMap::new();
        let positions: Vec<(f32, f32, f32)> = (0..50).map(|i| (i as f32, 50.0, 50.0)).collect();
        let events = scaler.evaluate(&cell, 50, &peers, &peer_cells, 1, &positions);
        assert!(events.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })));
    }

    #[test]
    fn test_no_split_below_threshold() {
        let mut scaler = AutoScaler::new(default_config());
        let cell = make_cell();
        let peers = HashSet::new();
        let peer_cells = HashMap::new();
        let positions: Vec<(f32, f32, f32)> = (0..10).map(|i| (i as f32, 50.0, 50.0)).collect();
        let events = scaler.evaluate(&cell, 10, &peers, &peer_cells, 1, &positions);
        assert!(!events.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })));
    }

    #[test]
    fn test_no_split_during_cooldown() {
        let mut scaler = AutoScaler::new(default_config());
        let cell = make_cell();
        let peers = HashSet::new();
        let peer_cells = HashMap::new();
        let positions: Vec<(f32, f32, f32)> = (0..50).map(|i| (i as f32, 50.0, 50.0)).collect();
        let events = scaler.evaluate(&cell, 50, &peers, &peer_cells, 1, &positions);
        assert!(events.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })));
        let events2 = scaler.evaluate(&cell, 50, &peers, &peer_cells, 2, &positions);
        assert!(!events2.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })));
    }

    #[test]
    fn test_no_split_at_max_nodes() {
        let mut config = default_config();
        config.max_nodes = 2;
        let mut scaler = AutoScaler::new(config);
        let cell = make_cell();
        let peers = HashSet::new();
        let peer_cells = HashMap::new();
        let positions: Vec<(f32, f32, f32)> = (0..50).map(|i| (i as f32, 50.0, 50.0)).collect();
        let events = scaler.evaluate(&cell, 50, &peers, &peer_cells, 2, &positions);
        assert!(!events.iter().any(|e| matches!(e, ScaleEvent::WarmupRecommended { .. })));
    }

    #[test]
    fn test_merge_recommended_below_threshold() {
        let mut config = default_config();
        config.merge_cooldown_ticks = 0;
        config.startup_grace_ticks = 0;
        let mut scaler = AutoScaler::new(config);
        let cell = make_cell();
        let peers = HashSet::new();
        let peer_cells = HashMap::new();
        let events = scaler.evaluate(&cell, 3, &peers, &peer_cells, 2, &[]);
        assert!(events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)));
    }

    #[test]
    fn test_no_merge_single_node() {
        let mut config = default_config();
        config.merge_cooldown_ticks = 0;
        config.startup_grace_ticks = 0;
        let mut scaler = AutoScaler::new(config);
        let cell = make_cell();
        let peers = HashSet::new();
        let peer_cells = HashMap::new();
        let events = scaler.evaluate(&cell, 3, &peers, &peer_cells, 1, &[]);
        assert!(!events.iter().any(|e| matches!(e, ScaleEvent::MergeRecommended)));
    }

    #[test]
    fn test_peer_joined_detected() {
        let mut scaler = AutoScaler::new(default_config());
        let cell = make_cell();
        let mut peers = HashSet::new();
        peers.insert(42);
        let peer_cells = HashMap::new();
        let events = scaler.evaluate(&cell, 10, &peers, &peer_cells, 2, &[]);
        assert!(events.iter().any(|e| matches!(e, ScaleEvent::PeerJoined { id: 42 })));
    }

    #[test]
    fn test_peer_left_detected() {
        let mut scaler = AutoScaler::new(default_config());
        let cell = make_cell();
        let mut peers = HashSet::new();
        peers.insert(42);
        let peer_cells = HashMap::new();
        scaler.evaluate(&cell, 10, &peers, &peer_cells, 2, &[]);
        let empty_peers = HashSet::new();
        let events = scaler.evaluate(&cell, 10, &empty_peers, &peer_cells, 1, &[]);
        assert!(events.iter().any(|e| matches!(e, ScaleEvent::PeerLeft { id: 42, .. })));
    }

    #[test]
    fn test_cell_expanded_on_peer_death() {
        let mut scaler = AutoScaler::new(default_config());
        let my_cell = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
        let dead_cell = Cell::new(50.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        let mut peers = HashSet::new();
        peers.insert(42);
        let mut peer_cells = HashMap::new();
        peer_cells.insert(42, dead_cell);
        scaler.evaluate(&my_cell, 10, &peers, &peer_cells, 2, &[]);
        let empty_peers = HashSet::new();
        let empty_cells = HashMap::new();
        let events = scaler.evaluate(&my_cell, 10, &empty_peers, &empty_cells, 1, &[]);
        let expanded = events.iter().find(|e| matches!(e, ScaleEvent::CellExpanded { .. }));
        assert!(expanded.is_some());
        if let Some(ScaleEvent::CellExpanded { new_cell, .. }) = expanded {
            assert!((new_cell.x_min - 0.0).abs() < 1e-4);
            assert!((new_cell.x_max - 100.0).abs() < 1e-4);
        }
    }

    #[test]
    fn test_no_cell_expanded_non_adjacent_peer() {
        let mut scaler = AutoScaler::new(default_config());
        let my_cell = Cell::new(0.0, 50.0, 0.0, 100.0, 0.0, 100.0);
        let far_cell = Cell::new(80.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        let mut peers = HashSet::new();
        peers.insert(42);
        let mut peer_cells = HashMap::new();
        peer_cells.insert(42, far_cell);
        scaler.evaluate(&my_cell, 10, &peers, &peer_cells, 2, &[]);
        let empty_peers = HashSet::new();
        let empty_cells = HashMap::new();
        let events = scaler.evaluate(&my_cell, 10, &empty_peers, &empty_cells, 1, &[]);
        assert!(!events.iter().any(|e| matches!(e, ScaleEvent::CellExpanded { .. })));
    }

    #[test]
    fn test_compute_binary_split_picks_longest_axis() {
        let cell = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 30.0);
        let positions: Vec<(f32, f32, f32)> = vec![(50.0, 25.0, 15.0)];
        let (_, _, axis, _) = AutoScaler::compute_binary_split(&cell, &positions);
        assert!(matches!(axis, Face::XPos));
    }

    #[test]
    fn test_compute_binary_split_balances_entities() {
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        let mut positions = Vec::new();
        for i in 0..30 { positions.push((i as f32 * 2.0, 50.0, 50.0)); }
        for i in 0..10 { positions.push((70.0 + i as f32, 50.0, 50.0)); }
        let (keep, _new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);
        let keep_count = positions.iter().filter(|p| keep.contains(**p)).count();
        assert!(keep_count >= 20);
    }

    #[test]
    fn test_compute_binary_split_clamps_extreme_bias() {
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        let positions: Vec<(f32, f32, f32)> = (0..40).map(|_| (1.0, 50.0, 50.0)).collect();
        let (_, _, _, split_pos) = AutoScaler::compute_binary_split(&cell, &positions);
        assert!(split_pos >= 20.0, "Split pos {} should be clamped to >= 20%", split_pos);
    }

    #[test]
    fn test_compute_binary_split_empty_positions_uses_center() {
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        let (_, _, _, split_pos) = AutoScaler::compute_binary_split(&cell, &[]);
        assert!((split_pos - 50.0).abs() < 1e-4);
    }

    #[test]
    fn test_split_produces_valid_cells() {
        let cell = Cell::new(0.0, 100.0, -50.0, 50.0, 0.0, 200.0);
        let positions: Vec<(f32, f32, f32)> = (0..40).map(|i| {
            (25.0 + (i as f32 % 10.0), (i as f32 % 20.0) - 10.0, 100.0)
        }).collect();
        let (keep, new, _, _) = AutoScaler::compute_binary_split(&cell, &positions);
        let union = keep.union(&new);
        assert!((union.x_min - cell.x_min).abs() < 1e-4);
        assert!((union.x_max - cell.x_max).abs() < 1e-4);
        assert!((union.y_min - cell.y_min).abs() < 1e-4);
        assert!((union.y_max - cell.y_max).abs() < 1e-4);
        assert!((union.z_min - cell.z_min).abs() < 1e-4);
        assert!((union.z_max - cell.z_max).abs() < 1e-4);
        assert!(keep.volume() > 0.0);
        assert!(new.volume() > 0.0);
    }
}
