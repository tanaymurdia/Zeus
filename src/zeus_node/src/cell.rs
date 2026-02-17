#[derive(Clone, Debug, PartialEq)]
pub struct Cell {
    pub x_min: f32,
    pub x_max: f32,
    pub y_min: f32,
    pub y_max: f32,
    pub z_min: f32,
    pub z_max: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Face {
    XPos,
    XNeg,
    YPos,
    YNeg,
    ZPos,
    ZNeg,
}

impl Cell {
    pub fn new(x_min: f32, x_max: f32, y_min: f32, y_max: f32, z_min: f32, z_max: f32) -> Self {
        Self { x_min, x_max, y_min, y_max, z_min, z_max }
    }

    pub fn from_1d(lower_boundary: f32, upper_boundary: f32) -> Self {
        Self {
            x_min: lower_boundary,
            x_max: upper_boundary,
            y_min: f32::NEG_INFINITY,
            y_max: f32::INFINITY,
            z_min: f32::NEG_INFINITY,
            z_max: f32::INFINITY,
        }
    }

    pub fn contains(&self, pos: (f32, f32, f32)) -> bool {
        pos.0 >= self.x_min && pos.0 <= self.x_max
            && pos.1 >= self.y_min && pos.1 <= self.y_max
            && pos.2 >= self.z_min && pos.2 <= self.z_max
    }

    pub fn contains_with_margin(&self, pos: (f32, f32, f32), margin: f32) -> bool {
        pos.0 >= self.x_min - margin && pos.0 <= self.x_max + margin
            && pos.1 >= self.y_min - margin && pos.1 <= self.y_max + margin
            && pos.2 >= self.z_min - margin && pos.2 <= self.z_max + margin
    }

    pub fn clamp_inside(&self, pos: (f32, f32, f32), inset: f32) -> (f32, f32, f32) {
        let cx = (self.x_min + self.x_max) * 0.5;
        let cy = (self.y_min + self.y_max) * 0.5;
        let cz = (self.z_min + self.z_max) * 0.5;
        let (lo_x, hi_x) = if self.x_max - self.x_min > inset * 2.0 { (self.x_min + inset, self.x_max - inset) } else { (cx, cx) };
        let (lo_y, hi_y) = if self.y_max - self.y_min > inset * 2.0 { (self.y_min + inset, self.y_max - inset) } else { (cy, cy) };
        let (lo_z, hi_z) = if self.z_max - self.z_min > inset * 2.0 { (self.z_min + inset, self.z_max - inset) } else { (cz, cz) };
        (pos.0.clamp(lo_x, hi_x), pos.1.clamp(lo_y, hi_y), pos.2.clamp(lo_z, hi_z))
    }

    pub fn distance_to_point(&self, pos: (f32, f32, f32)) -> f32 {
        let dx = (self.x_min - pos.0).max(0.0).max(pos.0 - self.x_max);
        let dy = (self.y_min - pos.1).max(0.0).max(pos.1 - self.y_max);
        let dz = (self.z_min - pos.2).max(0.0).max(pos.2 - self.z_max);
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    pub fn center(&self) -> (f32, f32, f32) {
        (
            (self.x_min + self.x_max) * 0.5,
            (self.y_min + self.y_max) * 0.5,
            (self.z_min + self.z_max) * 0.5,
        )
    }

    pub fn size(&self) -> (f32, f32, f32) {
        (self.x_max - self.x_min, self.y_max - self.y_min, self.z_max - self.z_min)
    }

    pub fn volume(&self) -> f32 {
        let s = self.size();
        s.0 * s.1 * s.2
    }

    pub fn split_octants(&self) -> [Cell; 8] {
        let (cx, cy, cz) = self.center();
        [
            Cell::new(self.x_min, cx, self.y_min, cy, self.z_min, cz),
            Cell::new(cx, self.x_max, self.y_min, cy, self.z_min, cz),
            Cell::new(self.x_min, cx, cy, self.y_max, self.z_min, cz),
            Cell::new(cx, self.x_max, cy, self.y_max, self.z_min, cz),
            Cell::new(self.x_min, cx, self.y_min, cy, cz, self.z_max),
            Cell::new(cx, self.x_max, self.y_min, cy, cz, self.z_max),
            Cell::new(self.x_min, cx, cy, self.y_max, cz, self.z_max),
            Cell::new(cx, self.x_max, cy, self.y_max, cz, self.z_max),
        ]
    }

    pub fn exit_face(&self, pos: (f32, f32, f32), margin: f32) -> Option<Face> {
        if pos.0 > self.x_max + margin { return Some(Face::XPos); }
        if pos.0 < self.x_min - margin { return Some(Face::XNeg); }
        if pos.1 > self.y_max + margin { return Some(Face::YPos); }
        if pos.1 < self.y_min - margin { return Some(Face::YNeg); }
        if pos.2 > self.z_max + margin { return Some(Face::ZPos); }
        if pos.2 < self.z_min - margin { return Some(Face::ZNeg); }
        None
    }

    pub fn shares_face(&self, other: &Cell) -> bool {
        let x_touch = (self.x_max - other.x_min).abs() < 1e-4 || (self.x_min - other.x_max).abs() < 1e-4;
        let y_touch = (self.y_max - other.y_min).abs() < 1e-4 || (self.y_min - other.y_max).abs() < 1e-4;
        let z_touch = (self.z_max - other.z_min).abs() < 1e-4 || (self.z_min - other.z_max).abs() < 1e-4;

        let x_overlap = self.x_min < other.x_max && self.x_max > other.x_min;
        let y_overlap = self.y_min < other.y_max && self.y_max > other.y_min;
        let z_overlap = self.z_min < other.z_max && self.z_max > other.z_min;

        (x_touch && y_overlap && z_overlap)
            || (y_touch && x_overlap && z_overlap)
            || (z_touch && x_overlap && y_overlap)
    }

    pub fn near_any_face(&self, pos: (f32, f32, f32), margin: f32) -> bool {
        (pos.0 - self.x_max).abs() < margin
            || (pos.0 - self.x_min).abs() < margin
            || (pos.1 - self.y_max).abs() < margin
            || (pos.1 - self.y_min).abs() < margin
            || (pos.2 - self.z_max).abs() < margin
            || (pos.2 - self.z_min).abs() < margin
    }

    pub fn split_binary(&self, axis: Face, split_pos: f32) -> (Cell, Cell) {
        match axis {
            Face::XPos | Face::XNeg => (
                Cell::new(self.x_min, split_pos, self.y_min, self.y_max, self.z_min, self.z_max),
                Cell::new(split_pos, self.x_max, self.y_min, self.y_max, self.z_min, self.z_max),
            ),
            Face::YPos | Face::YNeg => (
                Cell::new(self.x_min, self.x_max, self.y_min, split_pos, self.z_min, self.z_max),
                Cell::new(self.x_min, self.x_max, split_pos, self.y_max, self.z_min, self.z_max),
            ),
            Face::ZPos | Face::ZNeg => (
                Cell::new(self.x_min, self.x_max, self.y_min, self.y_max, self.z_min, split_pos),
                Cell::new(self.x_min, self.x_max, self.y_min, self.y_max, split_pos, self.z_max),
            ),
        }
    }

    pub fn union(&self, other: &Cell) -> Cell {
        Cell::new(
            self.x_min.min(other.x_min),
            self.x_max.max(other.x_max),
            self.y_min.min(other.y_min),
            self.y_max.max(other.y_max),
            self.z_min.min(other.z_min),
            self.z_max.max(other.z_max),
        )
    }

    pub fn expand_toward(&self, dead: &Cell) -> Option<Cell> {
        let eps = 0.5;
        if (self.y_min - dead.y_max).abs() < eps {
            Some(Cell::new(self.x_min, self.x_max, dead.y_min, self.y_max, self.z_min, self.z_max))
        } else if (self.y_max - dead.y_min).abs() < eps {
            Some(Cell::new(self.x_min, self.x_max, self.y_min, dead.y_max, self.z_min, self.z_max))
        } else if (self.x_min - dead.x_max).abs() < eps {
            Some(Cell::new(dead.x_min, self.x_max, self.y_min, self.y_max, self.z_min, self.z_max))
        } else if (self.x_max - dead.x_min).abs() < eps {
            Some(Cell::new(self.x_min, dead.x_max, self.y_min, self.y_max, self.z_min, self.z_max))
        } else if (self.z_min - dead.z_max).abs() < eps {
            Some(Cell::new(self.x_min, self.x_max, self.y_min, self.y_max, dead.z_min, self.z_max))
        } else if (self.z_max - dead.z_min).abs() < eps {
            Some(Cell::new(self.x_min, self.x_max, self.y_min, self.y_max, self.z_min, dead.z_max))
        } else {
            Some(self.union(dead))
        }
    }

    pub fn split_octants_biased(&self, bias: (f32, f32, f32)) -> [Cell; 8] {
        let cx = self.x_min + (self.x_max - self.x_min) * bias.0.clamp(0.2, 0.8);
        let cy = self.y_min + (self.y_max - self.y_min) * bias.1.clamp(0.2, 0.8);
        let cz = self.z_min + (self.z_max - self.z_min) * bias.2.clamp(0.2, 0.8);
        [
            Cell::new(self.x_min, cx, self.y_min, cy, self.z_min, cz),
            Cell::new(cx, self.x_max, self.y_min, cy, self.z_min, cz),
            Cell::new(self.x_min, cx, cy, self.y_max, self.z_min, cz),
            Cell::new(cx, self.x_max, cy, self.y_max, self.z_min, cz),
            Cell::new(self.x_min, cx, self.y_min, cy, cz, self.z_max),
            Cell::new(cx, self.x_max, self.y_min, cy, cz, self.z_max),
            Cell::new(self.x_min, cx, cy, self.y_max, cz, self.z_max),
            Cell::new(cx, self.x_max, cy, self.y_max, cz, self.z_max),
        ]
    }

    pub fn clip_against(&self, keep_out: &Cell) -> Cell {
        let x_ol = self.x_min < keep_out.x_max && self.x_max > keep_out.x_min;
        let y_ol = self.y_min < keep_out.y_max && self.y_max > keep_out.y_min;
        let z_ol = self.z_min < keep_out.z_max && self.z_max > keep_out.z_min;
        if !(x_ol && y_ol && z_ol) {
            return self.clone();
        }
        let mut best = self.clone();
        let mut best_vol = 0.0f32;
        if self.x_max > keep_out.x_min && self.x_min < keep_out.x_min {
            let c = Cell::new(self.x_min, keep_out.x_min, self.y_min, self.y_max, self.z_min, self.z_max);
            let v = c.volume();
            if v > best_vol { best = c; best_vol = v; }
        }
        if self.x_min < keep_out.x_max && self.x_max > keep_out.x_max {
            let c = Cell::new(keep_out.x_max, self.x_max, self.y_min, self.y_max, self.z_min, self.z_max);
            let v = c.volume();
            if v > best_vol { best = c; best_vol = v; }
        }
        if self.y_max > keep_out.y_min && self.y_min < keep_out.y_min {
            let c = Cell::new(self.x_min, self.x_max, self.y_min, keep_out.y_min, self.z_min, self.z_max);
            let v = c.volume();
            if v > best_vol { best = c; best_vol = v; }
        }
        if self.y_min < keep_out.y_max && self.y_max > keep_out.y_max {
            let c = Cell::new(self.x_min, self.x_max, keep_out.y_max, self.y_max, self.z_min, self.z_max);
            let v = c.volume();
            if v > best_vol { best = c; best_vol = v; }
        }
        if self.z_max > keep_out.z_min && self.z_min < keep_out.z_min {
            let c = Cell::new(self.x_min, self.x_max, self.y_min, self.y_max, self.z_min, keep_out.z_min);
            let v = c.volume();
            if v > best_vol { best = c; best_vol = v; }
        }
        if self.z_min < keep_out.z_max && self.z_max > keep_out.z_max {
            let c = Cell::new(self.x_min, self.x_max, self.y_min, self.y_max, keep_out.z_max, self.z_max);
            let v = c.volume();
            if v > best_vol { best = c; best_vol = v; }
        }
        if best_vol <= 0.0 { return self.clone(); }
        best
    }

    pub fn serialize(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..4].copy_from_slice(&self.x_min.to_le_bytes());
        buf[4..8].copy_from_slice(&self.x_max.to_le_bytes());
        buf[8..12].copy_from_slice(&self.y_min.to_le_bytes());
        buf[12..16].copy_from_slice(&self.y_max.to_le_bytes());
        buf[16..20].copy_from_slice(&self.z_min.to_le_bytes());
        buf[20..24].copy_from_slice(&self.z_max.to_le_bytes());
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < 24 { return None; }
        Some(Self {
            x_min: f32::from_le_bytes(buf[0..4].try_into().ok()?),
            x_max: f32::from_le_bytes(buf[4..8].try_into().ok()?),
            y_min: f32::from_le_bytes(buf[8..12].try_into().ok()?),
            y_max: f32::from_le_bytes(buf[12..16].try_into().ok()?),
            z_min: f32::from_le_bytes(buf[16..20].try_into().ok()?),
            z_max: f32::from_le_bytes(buf[20..24].try_into().ok()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_contains() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        assert!(cell.contains((5.0, 5.0, 5.0)));
        assert!(cell.contains((0.0, 0.0, 0.0)));
        assert!(cell.contains((10.0, 10.0, 10.0)));
        assert!(!cell.contains((11.0, 5.0, 5.0)));
        assert!(!cell.contains((-1.0, 5.0, 5.0)));
        assert!(!cell.contains((5.0, -1.0, 5.0)));
        assert!(!cell.contains((5.0, 5.0, 11.0)));
    }

    #[test]
    fn test_cell_center_and_volume() {
        let cell = Cell::new(0.0, 10.0, 0.0, 20.0, 0.0, 30.0);
        assert_eq!(cell.center(), (5.0, 10.0, 15.0));
        assert!((cell.volume() - 6000.0).abs() < 0.01);
    }

    #[test]
    fn test_cell_split_octants() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let octants = cell.split_octants();
        assert_eq!(octants.len(), 8);
        let total_vol: f32 = octants.iter().map(|o| o.volume()).sum();
        assert!((total_vol - cell.volume()).abs() < 0.01);
        for o in &octants {
            assert!((o.size().0 - 5.0).abs() < 0.01);
            assert!((o.size().1 - 5.0).abs() < 0.01);
            assert!((o.size().2 - 5.0).abs() < 0.01);
        }
        for i in 0..8 {
            for j in (i + 1)..8 {
                let a = &octants[i];
                let b = &octants[j];
                let overlap_x = a.x_min.max(b.x_min) < a.x_max.min(b.x_max);
                let overlap_y = a.y_min.max(b.y_min) < a.y_max.min(b.y_max);
                let overlap_z = a.z_min.max(b.z_min) < a.z_max.min(b.z_max);
                assert!(!(overlap_x && overlap_y && overlap_z), "Octants {} and {} overlap", i, j);
            }
        }
    }

    #[test]
    fn test_cell_exit_face() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let margin = 1.0;
        assert_eq!(cell.exit_face((12.0, 5.0, 5.0), margin), Some(Face::XPos));
        assert_eq!(cell.exit_face((-2.0, 5.0, 5.0), margin), Some(Face::XNeg));
        assert_eq!(cell.exit_face((5.0, 12.0, 5.0), margin), Some(Face::YPos));
        assert_eq!(cell.exit_face((5.0, -2.0, 5.0), margin), Some(Face::YNeg));
        assert_eq!(cell.exit_face((5.0, 5.0, 12.0), margin), Some(Face::ZPos));
        assert_eq!(cell.exit_face((5.0, 5.0, -2.0), margin), Some(Face::ZNeg));
        assert_eq!(cell.exit_face((5.0, 5.0, 5.0), margin), None);
        assert_eq!(cell.exit_face((10.5, 5.0, 5.0), margin), None);
    }

    #[test]
    fn test_cell_shares_face() {
        let a = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let b = Cell::new(10.0, 20.0, 0.0, 10.0, 0.0, 10.0);
        assert!(a.shares_face(&b));
        let c = Cell::new(20.0, 30.0, 0.0, 10.0, 0.0, 10.0);
        assert!(!a.shares_face(&c));
        let d = Cell::new(0.0, 10.0, 10.0, 20.0, 0.0, 10.0);
        assert!(a.shares_face(&d));
    }

    #[test]
    fn test_cell_near_any_face() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        assert!(cell.near_any_face((0.5, 5.0, 5.0), 1.0));
        assert!(cell.near_any_face((9.5, 5.0, 5.0), 1.0));
        assert!(!cell.near_any_face((5.0, 5.0, 5.0), 1.0));
    }

    #[test]
    fn test_cell_serialize_roundtrip() {
        let cell = Cell::new(1.5, 10.3, -5.0, 20.7, 0.0, 100.0);
        let bytes = cell.serialize();
        let restored = Cell::deserialize(&bytes).unwrap();
        assert_eq!(cell, restored);
    }

    #[test]
    fn test_cell_split_octants_biased() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let octants = cell.split_octants_biased((0.3, 0.7, 0.5));
        assert_eq!(octants.len(), 8);
        let total_vol: f32 = octants.iter().map(|o| o.volume()).sum();
        assert!((total_vol - cell.volume()).abs() < 0.01);
        assert!((octants[0].x_max - 3.0).abs() < 0.01);
        assert!((octants[0].y_max - 7.0).abs() < 0.01);
        assert!((octants[0].z_max - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_cell_split_octants_biased_clamped() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let octants = cell.split_octants_biased((0.0, 1.0, 0.5));
        assert!((octants[0].x_max - 2.0).abs() < 0.01);
        assert!((octants[0].y_max - 8.0).abs() < 0.01);
    }

    #[test]
    fn test_cell_from_1d() {
        let cell = Cell::from_1d(5.0, 15.0);
        assert_eq!(cell.x_min, 5.0);
        assert_eq!(cell.x_max, 15.0);
        assert!(cell.contains((10.0, 999999.0, -999999.0)));
    }

    #[test]
    fn test_split_binary_x_axis() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let (a, b) = cell.split_binary(Face::XPos, 3.0);
        assert!((a.x_min - 0.0).abs() < 1e-4);
        assert!((a.x_max - 3.0).abs() < 1e-4);
        assert!((b.x_min - 3.0).abs() < 1e-4);
        assert!((b.x_max - 10.0).abs() < 1e-4);
        assert!((a.y_min - 0.0).abs() < 1e-4);
        assert!((a.y_max - 10.0).abs() < 1e-4);
        assert!((b.z_min - 0.0).abs() < 1e-4);
        assert!((b.z_max - 10.0).abs() < 1e-4);
        let union = a.union(&b);
        assert!((union.volume() - cell.volume()).abs() < 0.01);
    }

    #[test]
    fn test_split_binary_y_axis() {
        let cell = Cell::new(0.0, 10.0, 0.0, 20.0, 0.0, 10.0);
        let (a, b) = cell.split_binary(Face::YPos, 7.0);
        assert!((a.y_max - 7.0).abs() < 1e-4);
        assert!((b.y_min - 7.0).abs() < 1e-4);
        assert!((a.volume() + b.volume() - cell.volume()).abs() < 0.01);
    }

    #[test]
    fn test_split_binary_z_axis() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, -5.0, 15.0);
        let (a, b) = cell.split_binary(Face::ZNeg, 5.0);
        assert!((a.z_max - 5.0).abs() < 1e-4);
        assert!((b.z_min - 5.0).abs() < 1e-4);
        assert!((a.volume() + b.volume() - cell.volume()).abs() < 0.01);
    }

    #[test]
    fn test_split_binary_halves_share_face() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let (a, b) = cell.split_binary(Face::XPos, 6.0);
        assert!(a.shares_face(&b));
    }

    #[test]
    fn test_union_adjacent_cells() {
        let a = Cell::new(0.0, 5.0, 0.0, 10.0, 0.0, 10.0);
        let b = Cell::new(5.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let u = a.union(&b);
        assert!((u.x_min - 0.0).abs() < 1e-4);
        assert!((u.x_max - 10.0).abs() < 1e-4);
        assert!((u.y_min - 0.0).abs() < 1e-4);
        assert!((u.y_max - 10.0).abs() < 1e-4);
        assert!((u.volume() - 1000.0).abs() < 0.01);
    }

    #[test]
    fn test_union_non_adjacent_cells() {
        let a = Cell::new(0.0, 5.0, 0.0, 5.0, 0.0, 5.0);
        let b = Cell::new(10.0, 15.0, 10.0, 15.0, 10.0, 15.0);
        let u = a.union(&b);
        assert!((u.x_min - 0.0).abs() < 1e-4);
        assert!((u.x_max - 15.0).abs() < 1e-4);
        assert!((u.y_min - 0.0).abs() < 1e-4);
        assert!((u.y_max - 15.0).abs() < 1e-4);
        assert!((u.z_min - 0.0).abs() < 1e-4);
        assert!((u.z_max - 15.0).abs() < 1e-4);
    }

    #[test]
    fn test_split_binary_biased_position() {
        let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
        let (a, b) = cell.split_binary(Face::XPos, 20.0);
        assert!(a.volume() < b.volume());
        assert!((a.x_max - 20.0).abs() < 1e-4);
    }

    #[test]
    fn test_clip_against_no_overlap() {
        let a = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let b = Cell::new(20.0, 30.0, 0.0, 10.0, 0.0, 10.0);
        let clipped = a.clip_against(&b);
        assert_eq!(clipped, a);
    }

    #[test]
    fn test_clip_against_x_overlap() {
        let expanded = Cell::new(0.0, 24.0, -1.0, 25.0, -12.0, 12.0);
        let keep_out = Cell::new(12.0, 24.0, -1.0, 17.4, -12.0, 12.0);
        let clipped = expanded.clip_against(&keep_out);
        assert!(clipped.x_max <= keep_out.x_min + 0.01 || clipped.x_min >= keep_out.x_max - 0.01
            || clipped.y_max <= keep_out.y_min + 0.01 || clipped.y_min >= keep_out.y_max - 0.01
            || clipped.z_max <= keep_out.z_min + 0.01 || clipped.z_min >= keep_out.z_max - 0.01);
        assert!(clipped.volume() > 0.0);
    }

    #[test]
    fn test_clip_against_preserves_largest_volume() {
        let expanded = Cell::new(0.0, 20.0, 0.0, 10.0, 0.0, 10.0);
        let keep_out = Cell::new(15.0, 25.0, 0.0, 10.0, 0.0, 10.0);
        let clipped = expanded.clip_against(&keep_out);
        assert!((clipped.x_max - 15.0).abs() < 1e-4);
        assert!((clipped.x_min - 0.0).abs() < 1e-4);
    }

    #[test]
    fn test_clip_against_adjacent_cells() {
        let a = Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0);
        let b = Cell::new(0.0, 12.0, 12.0, 25.0, -12.0, 12.0);
        let clipped = a.clip_against(&b);
        assert!(clipped.volume() > 0.0);
    }

    #[test]
    fn test_distance_to_point_inside() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        assert!((cell.distance_to_point((5.0, 5.0, 5.0)) - 0.0).abs() < 1e-6);
        assert!((cell.distance_to_point((0.0, 0.0, 0.0)) - 0.0).abs() < 1e-6);
        assert!((cell.distance_to_point((10.0, 10.0, 10.0)) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_distance_to_point_outside_single_axis() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        assert!((cell.distance_to_point((15.0, 5.0, 5.0)) - 5.0).abs() < 1e-4);
        assert!((cell.distance_to_point((-3.0, 5.0, 5.0)) - 3.0).abs() < 1e-4);
        assert!((cell.distance_to_point((5.0, 12.0, 5.0)) - 2.0).abs() < 1e-4);
        assert!((cell.distance_to_point((5.0, 5.0, -4.0)) - 4.0).abs() < 1e-4);
    }

    #[test]
    fn test_distance_to_point_outside_corner() {
        let cell = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let d = cell.distance_to_point((13.0, 14.0, 10.0));
        let expected = (3.0_f32.powi(2) + 4.0_f32.powi(2)).sqrt();
        assert!((d - expected).abs() < 1e-4);
    }

    #[test]
    fn test_expand_toward_x_positive() {
        let a = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let dead = Cell::new(10.0, 20.0, 0.0, 10.0, 0.0, 10.0);
        let expanded = a.expand_toward(&dead).unwrap();
        assert!((expanded.x_min - 0.0).abs() < 1e-4);
        assert!((expanded.x_max - 20.0).abs() < 1e-4);
        assert!((expanded.y_min - 0.0).abs() < 1e-4);
        assert!((expanded.y_max - 10.0).abs() < 1e-4);
        assert!((expanded.z_min - 0.0).abs() < 1e-4);
        assert!((expanded.z_max - 10.0).abs() < 1e-4);
    }

    #[test]
    fn test_expand_toward_x_negative() {
        let a = Cell::new(10.0, 20.0, 0.0, 10.0, 0.0, 10.0);
        let dead = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let expanded = a.expand_toward(&dead).unwrap();
        assert!((expanded.x_min - 0.0).abs() < 1e-4);
        assert!((expanded.x_max - 20.0).abs() < 1e-4);
    }

    #[test]
    fn test_expand_toward_y_positive() {
        let a = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let dead = Cell::new(0.0, 10.0, 10.0, 25.0, 0.0, 10.0);
        let expanded = a.expand_toward(&dead).unwrap();
        assert!((expanded.y_min - 0.0).abs() < 1e-4);
        assert!((expanded.y_max - 25.0).abs() < 1e-4);
        assert!((expanded.x_min - 0.0).abs() < 1e-4);
        assert!((expanded.x_max - 10.0).abs() < 1e-4);
    }

    #[test]
    fn test_expand_toward_z_negative() {
        let a = Cell::new(0.0, 10.0, 0.0, 10.0, 0.0, 10.0);
        let dead = Cell::new(0.0, 10.0, 0.0, 10.0, -5.0, 0.0);
        let expanded = a.expand_toward(&dead).unwrap();
        assert!((expanded.z_min - (-5.0)).abs() < 1e-4);
        assert!((expanded.z_max - 10.0).abs() < 1e-4);
    }

    #[test]
    fn test_expand_toward_preserves_perpendicular_axes() {
        let a = Cell::new(0.0, 12.0, -1.0, 13.0, -12.0, -5.0);
        let dead = Cell::new(0.0, 24.0, 13.0, 25.0, -12.0, 12.0);
        let expanded = a.expand_toward(&dead).unwrap();
        assert!((expanded.x_min - 0.0).abs() < 1e-4);
        assert!((expanded.x_max - 12.0).abs() < 1e-4);
        assert!((expanded.y_min - (-1.0)).abs() < 1e-4);
        assert!((expanded.y_max - 25.0).abs() < 1e-4);
        assert!((expanded.z_min - (-12.0)).abs() < 1e-4);
        assert!((expanded.z_max - (-5.0)).abs() < 1e-4);
    }
}
