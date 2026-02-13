use crate::network::ServerStatus;
use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

pub struct VisualsPlugin;

impl Plugin for VisualsPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Startup, setup_visuals)
            .add_systems(
                Update,
                (
                    update_zone_targets,
                    animate_floor_zones,
                    animate_dividers,
                    animate_labels,
                    sync_walls,
                ),
            );
    }
}

#[derive(Component)]
pub struct FloorZone {
    pub zone_index: u8,
    pub target_x: f32,
    pub target_y: f32,
    pub target_scale_x: f32,
    pub target_color: Color,
}

#[derive(Resource, Default)]
pub struct FloorZoneState {
    pub current_node_count: u8,
}

#[derive(Component)]
pub enum WallSide {
    Left,
    Right,
    Front,
    Back,
}

#[derive(Component)]
pub struct StaticWall;

pub const ZONE_COLORS: [(f32, f32, f32); 4] = [
    (0.1, 0.6, 0.3),
    (0.2, 0.3, 0.8),
    (0.7, 0.2, 0.6),
    (0.8, 0.5, 0.1),
];

#[derive(Component)]
pub struct ZoneDivider {
    pub divider_index: u8,
    pub target_x: f32,
    pub target_alpha: f32,
}

#[derive(Component)]
pub struct ZoneLabel {
    pub label_index: u8,
    pub target_x: f32,
    pub target_alpha: f32,
}

const ANIM_SPEED: f32 = 3.0;

fn setup_visuals(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    commands.insert_resource(FloorZoneState::default());

    let floor_depth = 30.0;

    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(24.0, 10.0, 0.3))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.05),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(12.0, 5.0, -12.0),
        RigidBody::Fixed,
        Collider::cuboid(12.0, 5.0, 0.15),
        WallSide::Back,
        StaticWall,
    ));
    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(24.0, 10.0, 0.3))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.05),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(12.0, 5.0, 12.0),
        RigidBody::Fixed,
        Collider::cuboid(12.0, 5.0, 0.15),
        WallSide::Front,
        StaticWall,
    ));

    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(0.3, 10.0, 30.0))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.05),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(0.0, 5.0, 0.0),
        RigidBody::Fixed,
        Collider::cuboid(0.15, 5.0, 15.0),
        WallSide::Left,
        StaticWall,
    ));
    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(0.3, 10.0, 30.0))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.05),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(24.0, 5.0, 0.0),
        RigidBody::Fixed,
        Collider::cuboid(0.15, 5.0, 15.0),
        WallSide::Right,
        StaticWall,
    ));

    commands.spawn((
        Mesh3d(
            meshes.add(
                Plane3d::default()
                    .mesh()
                    .size(26.0, floor_depth + 2.0),
            ),
        ),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::srgba(0.06, 0.06, 0.06, 1.0),
            perceptual_roughness: 0.9,
            metallic: 0.0,
            ..default()
        })),
        Transform::from_xyz(12.0, -1.05, 0.0),
        RigidBody::Fixed,
        Collider::cuboid(13.0, 0.1, (floor_depth + 2.0) / 2.0),
    ));

    let base_zone_width = 6.0;
    let (r0, g0, b0) = ZONE_COLORS[0];
    for i in 0..4u8 {
        let initial_x = if i == 0 { 12.0 } else { base_zone_width / 2.0 + (i as f32 * base_zone_width) };
        let initial_scale = if i == 0 { 24.0 / base_zone_width } else { 1.0 };
        let initial_y = if i == 0 { -1.0 } else { -3.0 };
        let initial_color = if i == 0 {
            Color::srgba(r0, g0, b0, 0.85)
        } else {
            Color::srgba(0.0, 0.0, 0.0, 0.0)
        };
        commands.spawn((
            Mesh3d(
                meshes.add(
                    Plane3d::default()
                        .mesh()
                        .size(base_zone_width, floor_depth),
                ),
            ),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: initial_color,
                alpha_mode: AlphaMode::Blend,
                perceptual_roughness: 0.3,
                metallic: 0.5,
                reflectance: 0.5,
                ..default()
            })),
            Transform::from_xyz(initial_x, initial_y, 0.0)
                .with_scale(Vec3::new(initial_scale, 1.0, 1.0)),
            FloorZone {
                zone_index: i,
                target_x: initial_x,
                target_y: initial_y,
                target_scale_x: initial_scale,
                target_color: initial_color,
            },
        ));
    }

    let divider_mesh = meshes.add(Cuboid::new(0.06, 12.0, floor_depth));
    for i in 0..3u8 {
        let x_pos = (i as f32 + 1.0) * base_zone_width;
        commands.spawn((
            Mesh3d(divider_mesh.clone()),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: Color::WHITE.with_alpha(0.0),
                emissive: LinearRgba::new(1.0, 1.0, 1.0, 0.0),
                alpha_mode: AlphaMode::Blend,
                unlit: true,
                double_sided: true,
                cull_mode: None,
                ..default()
            })),
            Transform::from_xyz(x_pos, 5.0, 0.0),
            ZoneDivider {
                divider_index: i,
                target_x: x_pos,
                target_alpha: 0.0,
            },
        ));
    }

    let camera_pos = Vec3::new(0.0, 50.0, 50.0);

    for i in 0..4u8 {
        let x_pos = base_zone_width / 2.0 + (i as f32 * base_zone_width);
        let label = format!("NODE {}", i);
        let (r, g, b) = ZONE_COLORS[i as usize];

        commands.spawn((
            Text2d::new(label),
            TextFont {
                font_size: 40.0,
                ..default()
            },
            TextColor(Color::srgba(
                r * 2.0,
                g * 2.0,
                b * 2.0,
                if i == 0 { 1.0 } else { 0.0 },
            )),
            Transform::from_xyz(x_pos, 10.0, 0.0).looking_at(camera_pos, Vec3::Y),
            ZoneLabel {
                label_index: i,
                target_x: x_pos,
                target_alpha: if i == 0 { 1.0 } else { 0.0 },
            },
        ));
    }
}

fn update_zone_targets(
    server_status: Res<ServerStatus>,
    mut zone_state: ResMut<FloorZoneState>,
    mut floor_query: Query<&mut FloorZone, (Without<ZoneDivider>, Without<ZoneLabel>)>,
    mut divider_query: Query<&mut ZoneDivider, (Without<FloorZone>, Without<ZoneLabel>)>,
    mut label_query: Query<&mut ZoneLabel, (Without<FloorZone>, Without<ZoneDivider>)>,
) {
    let node_count = server_status.get_node_count().max(1) as usize;

    if node_count == zone_state.current_node_count as usize {
        return;
    }
    zone_state.current_node_count = node_count as u8;

    let zone_width = 24.0 / node_count as f32;
    let base_zone_width = 6.0;
    let scale_x = zone_width / base_zone_width;

    for mut zone in floor_query.iter_mut() {
        let i = zone.zone_index as usize;
        if i < node_count {
            zone.target_x = zone_width * i as f32 + zone_width / 2.0;
            zone.target_y = -1.0;
            zone.target_scale_x = scale_x;
            let (r, g, b) = ZONE_COLORS[i.min(3)];
            zone.target_color = Color::srgba(r, g, b, 0.85);
        } else {
            zone.target_y = -3.0;
            zone.target_scale_x = 1.0;
            zone.target_color = Color::srgba(0.0, 0.0, 0.0, 0.0);
        }
    }

    for mut divider in divider_query.iter_mut() {
        let j = divider.divider_index as usize;
        if j < node_count.saturating_sub(1) {
            divider.target_x = zone_width * (j as f32 + 1.0);
            divider.target_alpha = 0.15;
        } else {
            divider.target_alpha = 0.0;
        }
    }

    for mut label in label_query.iter_mut() {
        let i = label.label_index as usize;
        if i < node_count {
            label.target_x = zone_width * i as f32 + zone_width / 2.0;
            label.target_alpha = 1.0;
        } else {
            label.target_alpha = 0.0;
        }
    }
}

fn animate_floor_zones(
    time: Res<Time>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut query: Query<(
        &FloorZone,
        &mut Transform,
        &MeshMaterial3d<StandardMaterial>,
    )>,
) {
    let dt = time.delta_secs();
    let t = (ANIM_SPEED * dt).min(1.0);

    for (zone, mut transform, material_handle) in query.iter_mut() {
        transform.translation.x += (zone.target_x - transform.translation.x) * t;
        transform.translation.y += (zone.target_y - transform.translation.y) * t;
        transform.scale.x += (zone.target_scale_x - transform.scale.x) * t;

        if let Some(material) = materials.get_mut(material_handle) {
            let cur = material.base_color.to_srgba();
            let tgt = zone.target_color.to_srgba();
            material.base_color = Color::srgba(
                cur.red + (tgt.red - cur.red) * t,
                cur.green + (tgt.green - cur.green) * t,
                cur.blue + (tgt.blue - cur.blue) * t,
                cur.alpha + (tgt.alpha - cur.alpha) * t,
            );
        }
    }
}

fn animate_dividers(
    time: Res<Time>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut query: Query<(
        &ZoneDivider,
        &mut Transform,
        &MeshMaterial3d<StandardMaterial>,
    )>,
) {
    let dt = time.delta_secs();
    let t = (ANIM_SPEED * dt).min(1.0);

    for (divider, mut transform, material_handle) in query.iter_mut() {
        transform.translation.x += (divider.target_x - transform.translation.x) * t;

        if let Some(material) = materials.get_mut(material_handle) {
            let cur_a = material.base_color.to_srgba().alpha;
            let new_a = cur_a + (divider.target_alpha - cur_a) * t;
            material.base_color = Color::WHITE.with_alpha(new_a);
            material.emissive = LinearRgba::new(1.0, 1.0, 1.0, new_a);
        }
    }
}

fn animate_labels(
    time: Res<Time>,
    mut query: Query<(&ZoneLabel, &mut Transform, &mut TextColor)>,
) {
    let dt = time.delta_secs();
    let t = (ANIM_SPEED * dt).min(1.0);
    let camera_pos = Vec3::new(0.0, 50.0, 50.0);

    for (label, mut transform, mut text_color) in query.iter_mut() {
        let cur_x = transform.translation.x;
        let new_x = cur_x + (label.target_x - cur_x) * t;
        *transform = Transform::from_xyz(new_x, 10.0, 0.0).looking_at(camera_pos, Vec3::Y);

        let i = label.label_index as usize;
        let (r, g, b) = ZONE_COLORS[i.min(3)];
        let cur_a = text_color.0.to_srgba().alpha;
        let new_a = cur_a + (label.target_alpha - cur_a) * t;
        *text_color = TextColor(Color::srgba(r * 2.0, g * 2.0, b * 2.0, new_a));
    }
}

fn sync_walls(
    server_status: Res<ServerStatus>,
    mut query: Query<(&WallSide, &mut Transform, &mut Collider)>,
) {
    let map_width = server_status.get_map_width();
    if map_width < 1.0 {
        return;
    }

    for (side, mut transform, mut collider) in query.iter_mut() {
        match side {
            WallSide::Left => {
                transform.translation.x = 0.0;
            }
            WallSide::Right => {
                transform.translation.x = map_width;
            }
            WallSide::Back => {
                transform.translation.x = map_width / 2.0;
                *collider = Collider::cuboid(map_width / 2.0, 5.0, 0.15);
            }
            WallSide::Front => {
                transform.translation.x = map_width / 2.0;
                *collider = Collider::cuboid(map_width / 2.0, 5.0, 0.15);
            }
        }
    }
}
