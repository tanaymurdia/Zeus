use bevy::prelude::*;
use crate::network::OctreeCells;

pub struct VisualsPlugin;

impl Plugin for VisualsPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Update, draw_octree_wireframes);
    }
}

pub const CELL_COLORS: [Color; 8] = [
    Color::srgb(0.1, 0.8, 0.3),
    Color::srgb(0.2, 0.4, 0.9),
    Color::srgb(0.8, 0.2, 0.7),
    Color::srgb(0.9, 0.6, 0.1),
    Color::srgb(0.0, 0.8, 0.8),
    Color::srgb(0.9, 0.9, 0.1),
    Color::srgb(0.9, 0.2, 0.2),
    Color::srgb(0.9, 0.9, 0.9),
];

pub fn cell_color_for_index(i: usize) -> Color {
    CELL_COLORS[i % CELL_COLORS.len()]
}

pub fn cell_color_for_position(pos: Vec3, cells: &[crate::network::CellBounds]) -> (Color, usize) {
    for (i, cell) in cells.iter().enumerate() {
        if pos.x >= cell.x_min && pos.x <= cell.x_max
            && pos.y >= cell.y_min && pos.y <= cell.y_max
            && pos.z >= cell.z_min && pos.z <= cell.z_max
        {
            return (cell_color_for_index(i), i);
        }
    }
    (CELL_COLORS[0], 0)
}

pub fn cell_color_for_position_with_hysteresis(
    pos: Vec3,
    cells: &[crate::network::CellBounds],
    current_cell: usize,
) -> (Color, usize) {
    let margin = 0.5;
    if current_cell < cells.len() {
        let cell = &cells[current_cell];
        if pos.x >= cell.x_min + margin && pos.x <= cell.x_max - margin
            && pos.y >= cell.y_min + margin && pos.y <= cell.y_max - margin
            && pos.z >= cell.z_min + margin && pos.z <= cell.z_max - margin
        {
            return (cell_color_for_index(current_cell), current_cell);
        }
        if pos.x >= cell.x_min && pos.x <= cell.x_max
            && pos.y >= cell.y_min && pos.y <= cell.y_max
            && pos.z >= cell.z_min && pos.z <= cell.z_max
        {
            return (cell_color_for_index(current_cell), current_cell);
        }
    }
    cell_color_for_position(pos, cells)
}

fn draw_octree_wireframes(
    octree_cells: Res<OctreeCells>,
    mut gizmos: Gizmos,
) {
    let cells = match octree_cells.cells.lock() {
        Ok(c) => c.clone(),
        Err(_) => return,
    };

    for (i, cell) in cells.iter().enumerate() {
        let color = cell_color_for_index(i).with_alpha(0.4);
        let center = Vec3::new(
            (cell.x_min + cell.x_max) / 2.0,
            (cell.y_min + cell.y_max) / 2.0,
            (cell.z_min + cell.z_max) / 2.0,
        );
        let size = Vec3::new(
            cell.x_max - cell.x_min,
            cell.y_max - cell.y_min,
            cell.z_max - cell.z_min,
        );
        gizmos.cuboid(
            Transform::from_translation(center).with_scale(size),
            color,
        );
    }
}
