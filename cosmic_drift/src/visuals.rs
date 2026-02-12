use crate::network::ServerStatus;
use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

pub struct VisualsPlugin;

impl Plugin for VisualsPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(Startup, setup_visuals)
            .add_systems(Update, (animate_atmosphere, update_floor_zones));
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

const ZONE_COLORS: [(f32, f32, f32); 4] = [
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

    let zone_width = 250.0;
    let floor_depth = 1000.0;

    for i in 0..4u8 {
        let x_pos = -375.0 + (i as f32 * zone_width);
        let (r, g, b) = ZONE_COLORS[i as usize];

        let alpha = if i == 0 { 1.0 } else { 0.1 };

        commands.spawn((
            Mesh3d(
                meshes.add(
                    Plane3d::default()
                        .mesh()
                        .size(zone_width - 2.0, floor_depth),
                ),
            ),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: Color::srgba(r, g, b, alpha),
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

    for i in 1..4u8 {
        let x_pos = -500.0 + (i as f32 * zone_width);
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
            FloorZone { zone_index: i },
        ));
    }

    let camera_pos = Vec3::new(0.0, 50.0, 50.0);

    for i in 0..4u8 {
        let x_pos = -375.0 + (i as f32 * zone_width);
        let label = format!("NODE {} ZONE", i);
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
            FloorZone { zone_index: i },
        ));
    }
}

fn update_floor_zones(
    server_status: Res<ServerStatus>,
    mut zone_state: ResMut<FloorZoneState>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut query: Query<(&FloorZone, &MeshMaterial3d<StandardMaterial>)>,
    mut text_query: Query<(&FloorZone, &mut TextColor), Without<MeshMaterial3d<StandardMaterial>>>,
) {
    let node_count = server_status.get_node_count();

    if node_count == zone_state.current_node_count {
        return;
    }
    zone_state.current_node_count = node_count;

    for (zone, material_handle) in query.iter_mut() {
        if let Some(material) = materials.get_mut(material_handle) {
            let (r, g, b) = ZONE_COLORS[zone.zone_index as usize];
            let alpha = if zone.zone_index < node_count {
                1.0
            } else {
                0.1
            };
            material.base_color = Color::srgba(r, g, b, alpha);
        }
    }

    for (zone, mut text_color) in text_query.iter_mut() {
        let (r, g, b) = ZONE_COLORS[zone.zone_index as usize];
        let alpha = if zone.zone_index < node_count {
            1.0
        } else {
            0.0
        };
        *text_color = TextColor(Color::srgba(r * 2.0, g * 2.0, b * 2.0, alpha));
    }

    if node_count > 1 {
        println!(
            "[Demo] 🎨 Floor split visualized: {} zones active",
            node_count
        );
    }
}

fn animate_atmosphere(_time: Res<Time>, mut _query: Query<&mut Transform, With<PointLight>>) {}
