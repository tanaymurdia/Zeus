use crate::network::ServerStatus;
use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

pub struct VisualsPlugin;

impl Plugin for VisualsPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Startup, setup_visuals)
            .add_systems(Update, (animate_atmosphere, update_floor_zones, sync_walls));
    }
}

#[derive(Component)]
pub struct FloorZone {
    pub zone_index: u8,
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

fn setup_visuals(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    commands.insert_resource(FloorZoneState::default());

    let zone_width = 6.0;
    let floor_depth = 30.0;

    // Enclosing Walls (Visual)
    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(24.0, 10.0, 1.0))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.1),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(12.0, 5.0, -12.0),
        RigidBody::Fixed,
        Collider::cuboid(12.0, 5.0, 0.5), // Half-extents
        WallSide::Back,
        StaticWall,
    ));
    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(24.0, 10.0, 1.0))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.1),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(12.0, 5.0, 12.0),
        RigidBody::Fixed,
        Collider::cuboid(12.0, 5.0, 0.5),
        WallSide::Front,
        StaticWall,
    ));

    // Side Walls (Visual)
    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(1.0, 10.0, 200.0))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.1),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(0.0, 5.0, 0.0),
        RigidBody::Fixed,
        Collider::cuboid(0.5, 5.0, 100.0), // Side walls are long
        WallSide::Left,
        StaticWall,
    ));
    commands.spawn((
        Mesh3d(meshes.add(Cuboid::new(1.0, 10.0, 30.0))),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::WHITE.with_alpha(0.1),
            alpha_mode: AlphaMode::Blend,
            ..default()
        })),
        Transform::from_xyz(24.0, 5.0, 0.0),
        RigidBody::Fixed,
        Collider::cuboid(0.5, 5.0, 15.0),
        WallSide::Right,
        StaticWall,
    ));

    for i in 0..4u8 {
        let x_pos = zone_width / 2.0 + (i as f32 * zone_width);
        commands.spawn((
            Mesh3d(
                meshes.add(
                    Plane3d::default()
                        .mesh()
                        .size(zone_width - 2.0, floor_depth),
                ),
            ),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: if i == 0 {
                    let (r, g, b) = ZONE_COLORS[0];
                    Color::srgba(r, g, b, 1.0)
                } else {
                    Color::srgba(0.5, 0.5, 0.5, 0.2)
                },
                perceptual_roughness: 0.1,
                metallic: 0.7,
                reflectance: 0.8,
                alpha_mode: AlphaMode::Blend,
                ..default()
            })),
            Transform::from_xyz(x_pos, -1.0, 0.0),
            RigidBody::Fixed,
            Collider::cuboid(zone_width / 2.0, 0.1, floor_depth / 2.0),
            FloorZone { zone_index: i },
        ));
    }

    for i in 0..3u8 {
        let x_pos = (i as f32 + 1.0) * zone_width;
        commands.spawn((
            Mesh3d(meshes.add(Cuboid::new(0.5, 30.0, floor_depth))),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: LinearRgba::new(1.0, 1.0, 1.0, 0.0).into(),
                alpha_mode: AlphaMode::Blend,
                double_sided: true,
                cull_mode: None,
                unlit: true,
                ..default()
            })),
            Transform::from_xyz(x_pos, 15.0, 0.0),
            ZoneDivider { divider_index: i },
        ));
    }

    let camera_pos = Vec3::new(0.0, 50.0, 50.0);

    for i in 0..4u8 {
        let x_pos = zone_width / 2.0 + (i as f32 * zone_width);
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
            ZoneLabel { label_index: i },
        ));
    }
}

#[derive(Component)]
pub struct ZoneDivider {
    pub divider_index: u8,
}

#[derive(Component)]
pub struct ZoneLabel {
    pub label_index: u8,
}

fn update_floor_zones(
    server_status: Res<ServerStatus>,
    mut zone_state: ResMut<FloorZoneState>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut floor_query: Query<
        (&FloorZone, &mut Transform, &MeshMaterial3d<StandardMaterial>),
        (Without<ZoneDivider>, Without<ZoneLabel>),
    >,
    mut divider_query: Query<
        (&ZoneDivider, &mut Transform, &MeshMaterial3d<StandardMaterial>),
        (Without<FloorZone>, Without<ZoneLabel>),
    >,
    mut label_query: Query<
        (&ZoneLabel, &mut Transform, &mut TextColor),
        (Without<FloorZone>, Without<ZoneDivider>),
    >,
) {
    let node_count = server_status.get_node_count().max(1) as usize;

    if node_count == zone_state.current_node_count as usize {
        return;
    }
    zone_state.current_node_count = node_count as u8;

    let zone_width = 24.0 / node_count as f32;

    for (zone, mut transform, material_handle) in floor_query.iter_mut() {
        let i = zone.zone_index as usize;
        if let Some(material) = materials.get_mut(material_handle) {
            if i < node_count {
                let x_pos = zone_width * i as f32 + zone_width / 2.0;
                transform.translation.x = x_pos;
                let (r, g, b) = ZONE_COLORS[i.min(3)];
                material.base_color = Color::srgba(r, g, b, 1.0);
            } else {
                material.base_color = Color::srgba(0.5, 0.5, 0.5, 0.0);
            }
        }
    }

    for (divider, mut transform, material_handle) in divider_query.iter_mut() {
        let j = divider.divider_index as usize;
        if let Some(material) = materials.get_mut(material_handle) {
            if j < node_count.saturating_sub(1) {
                let x_pos = zone_width * (j as f32 + 1.0);
                transform.translation.x = x_pos;
                material.base_color = Color::srgba(1.0, 1.0, 1.0, 0.3);
            } else {
                material.base_color = Color::srgba(1.0, 1.0, 1.0, 0.0);
            }
        }
    }

    let camera_pos = Vec3::new(0.0, 50.0, 50.0);
    for (label, mut transform, mut text_color) in label_query.iter_mut() {
        let i = label.label_index as usize;
        if i < node_count {
            let x_pos = zone_width * i as f32 + zone_width / 2.0;
            transform.translation.x = x_pos;
            *transform = Transform::from_xyz(x_pos, 10.0, 0.0).looking_at(camera_pos, Vec3::Y);
            let (r, g, b) = ZONE_COLORS[i.min(3)];
            *text_color = TextColor(Color::srgba(r * 2.0, g * 2.0, b * 2.0, 1.0));
        } else {
            *text_color = TextColor(Color::srgba(0.0, 0.0, 0.0, 0.0));
        }
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
                *collider = Collider::cuboid(map_width / 2.0, 5.0, 0.5);
            }
            WallSide::Front => {
                transform.translation.x = map_width / 2.0;
                *collider = Collider::cuboid(map_width / 2.0, 5.0, 0.5);
            }
        }
    }
}

fn animate_atmosphere(_time: Res<Time>, mut _query: Query<&mut Transform, With<PointLight>>) {}
