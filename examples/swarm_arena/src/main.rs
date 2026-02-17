use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

mod network;
mod visuals;

use network::{ServerStatus, OctreeCells};

#[derive(Resource, Default)]
struct FrameCounter(u32);

#[derive(Component)]
pub struct PlayerShip;

#[derive(Component)]
struct ServerDrone {
    pub id: u64,
    pub cached_cell: usize,
    pub last_seen_frame: u32,
}

#[derive(Resource)]
pub struct GravityMode {
    pub mode: u8,
}

impl Default for GravityMode {
    fn default() -> Self {
        Self { mode: 0 }
    }
}

fn main() {
    App::new()
        .insert_resource(ClearColor(Color::srgb(0.02, 0.02, 0.06)))
        .insert_resource(GravityMode::default())
        .insert_resource(FrameCounter::default())
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: "Swarm Arena: Zeus 3D Octree Demo".into(),
                resolution: (1280.0, 720.0).into(),
                present_mode: bevy::window::PresentMode::AutoVsync,
                ..default()
            }),
            ..default()
        }))
        .add_plugins(RapierPhysicsPlugin::<NoUserData>::default())
        .add_plugins(visuals::VisualsPlugin)
        .add_plugins(network::NetworkPlugin)
        .add_systems(Startup, (setup_scene, setup_hud))
        .add_systems(
            Update,
            (
                fly_player,
                handle_gravity_input,
                update_hud,
                spawn_drones_on_keypress,
                camera_follow,
                render_server_drones,
            ),
        )
        .run();
}

#[derive(Component)]
struct MainCamera;

#[derive(Component)]
struct HudText;

fn setup_hud(mut commands: Commands) {
    commands.spawn((
        Text::new("Initializing..."),
        TextFont {
            font_size: 18.0,
            ..default()
        },
        TextColor(Color::WHITE),
        Node {
            position_type: PositionType::Absolute,
            top: Val::Px(10.0),
            left: Val::Px(10.0),
            ..default()
        },
        HudText,
    ));
}

fn update_hud(
    mut query: Query<&mut Text, With<HudText>>,
    player_query: Query<&Transform, With<PlayerShip>>,
    net: Res<network::NetworkResource>,
    server_status: Res<ServerStatus>,
    accumulated_state: Res<network::AccumulatedState>,
    gravity_mode: Res<GravityMode>,
    octree_cells: Res<OctreeCells>,
) {
    for mut text in query.iter_mut() {
        let status = if net.client.is_some() { "Connected" } else { "Connecting..." };
        let mut pos_str = String::from("Pos: N/A");
        if let Ok(transform) = player_query.get_single() {
            pos_str = format!(
                "Pos: {:.1}, {:.1}, {:.1}",
                transform.translation.x, transform.translation.y, transform.translation.z
            );
        }
        let nodes = server_status.get_node_count().max(1);
        let entities = accumulated_state
            .positions.lock().ok().map(|m| m.len() as u16).unwrap_or(0);
        let cell_count = octree_cells.cells.lock().ok().map(|c| c.len()).unwrap_or(0);
        let mode_str = match gravity_mode.mode {
            1 => "ATTRACT",
            2 => "REPEL",
            _ => "IDLE",
        };

        **text = format!(
            "Swarm Arena - Zeus 3D Octree Demo\nStatus: {}\n{}\n\nNodes: {} | Entities: {} | Cells: {}\nGravity: {} | Tick: 128Hz\n\n[LMB] Attract  [RMB] Repel\n[WASD] Move  [QE] Up/Down\n[N] +10 drones  [B] -10 drones",
            status, pos_str, nodes, entities, cell_count, mode_str
        );
    }
}

fn setup_scene(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    commands.spawn((
        Camera3d::default(),
        Transform::from_xyz(12.0, 30.0, 40.0).looking_at(Vec3::new(12.0, 12.0, 0.0), Vec3::Y),
        MainCamera,
    ));

    commands.spawn((
        Mesh3d(meshes.add(Sphere::new(1.5).mesh())),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::srgb(0.0, 1.0, 1.0),
            emissive: LinearRgba::rgb(0.0, 3.0, 3.0),
            ..default()
        })),
        Transform::from_xyz(12.0, 12.0, 0.0),
        RigidBody::Dynamic,
        Collider::ball(1.2),
        Velocity::default(),
        GravityScale(0.0),
        Damping {
            linear_damping: 0.8,
            angular_damping: 1.0,
        },
        ExternalImpulse::default(),
        PlayerShip,
    ));

    commands.spawn((
        PointLight {
            intensity: 4_000_000.0,
            shadows_enabled: true,
            range: 80.0,
            color: Color::srgb(0.9, 0.95, 1.0),
            ..default()
        },
        Transform::from_xyz(12.0, 30.0, 12.0),
    ));

    commands.spawn((
        DirectionalLight {
            color: Color::srgb(0.3, 0.3, 0.5),
            illuminance: 500.0,
            ..default()
        },
        Transform::from_rotation(Quat::from_euler(EulerRot::XYZ, -0.8, 0.3, 0.0)),
    ));
}

fn fly_player(
    input: Res<ButtonInput<KeyCode>>,
    time: Res<Time>,
    mut query: Query<&mut ExternalImpulse, With<PlayerShip>>,
) {
    let dt = time.delta_secs();
    for mut impulse in query.iter_mut() {
        let thrust = 120.0;
        let mut force = Vec3::ZERO;

        if input.pressed(KeyCode::KeyW) { force.z -= 1.0; }
        if input.pressed(KeyCode::KeyS) { force.z += 1.0; }
        if input.pressed(KeyCode::KeyA) { force.x -= 1.0; }
        if input.pressed(KeyCode::KeyD) { force.x += 1.0; }
        if input.pressed(KeyCode::KeyQ) { force.y += 1.0; }
        if input.pressed(KeyCode::KeyE) { force.y -= 1.0; }

        if force != Vec3::ZERO {
            impulse.impulse += force.normalize() * thrust * dt;
        }
    }
}

fn handle_gravity_input(
    mouse: Res<ButtonInput<MouseButton>>,
    mut gravity_mode: ResMut<GravityMode>,
) {
    if mouse.pressed(MouseButton::Left) {
        gravity_mode.mode = 1;
    } else if mouse.pressed(MouseButton::Right) {
        gravity_mode.mode = 2;
    } else {
        gravity_mode.mode = 0;
    }
}

fn spawn_drones_on_keypress(
    input: Res<ButtonInput<KeyCode>>,
    net: Res<network::NetworkResource>,
    player_query: Query<&Transform, With<PlayerShip>>,
) {
    let spawn_count: u16 = if input.just_pressed(KeyCode::KeyN) {
        10
    } else if input.just_pressed(KeyCode::KeyM) {
        50
    } else {
        0
    };

    let despawn_count: u16 = if input.just_pressed(KeyCode::KeyB) {
        10
    } else {
        0
    };

    if spawn_count > 0 {
        let pos = player_query.get_single().map(|t| t.translation).unwrap_or(Vec3::new(12.0, 12.0, 0.0));
        let mut buf = Vec::with_capacity(16);
        buf.push(0xDD);
        buf.push(0x02);
        buf.push((spawn_count >> 8) as u8);
        buf.push((spawn_count & 0xFF) as u8);
        buf.extend_from_slice(&pos.x.to_le_bytes());
        buf.extend_from_slice(&pos.y.to_le_bytes());
        buf.extend_from_slice(&pos.z.to_le_bytes());
        let payload: bytes::Bytes = buf.into();
        if let Ok(conns) = net.all_connections.lock() {
            for conn in conns.iter() {
                let _ = conn.send_datagram(payload.clone());
            }
        }
    }

    if despawn_count > 0 {
        let buf: bytes::Bytes = vec![0xDE, (despawn_count >> 8) as u8, (despawn_count & 0xFF) as u8].into();
        if let Ok(conns) = net.all_connections.lock() {
            for conn in conns.iter() {
                let _ = conn.send_datagram(buf.clone());
            }
        }
    }
}

fn camera_follow(
    player_query: Query<&Transform, With<PlayerShip>>,
    mut camera_query: Query<&mut Transform, (With<MainCamera>, Without<PlayerShip>)>,
) {
    let Ok(player_tf) = player_query.get_single() else { return; };
    let Ok(mut cam_tf) = camera_query.get_single_mut() else { return; };

    let offset = Vec3::new(0.0, 20.0, 35.0);
    let target = player_tf.translation + offset;
    cam_tf.translation = cam_tf.translation.lerp(target, 0.05);
    cam_tf.look_at(player_tf.translation, Vec3::Y);
}

fn render_server_drones(
    mut commands: Commands,
    drone_pos: Res<network::DronePositions>,
    accumulated_state: Res<network::AccumulatedState>,
    net: Res<network::NetworkResource>,
    octree_cells: Res<OctreeCells>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut drone_query: Query<(
        Entity,
        &mut Transform,
        &mut ServerDrone,
        &MeshMaterial3d<StandardMaterial>,
    )>,
    mut frame_counter: ResMut<FrameCounter>,
) {
    frame_counter.0 = frame_counter.0.wrapping_add(1);
    let current_frame = frame_counter.0;
    let despawn_grace_frames: u32 = 15;
    let now_instant = std::time::Instant::now();
    let acc_map = net
        .accumulated
        .as_ref()
        .and_then(|a| a.lock().ok())
        .map(|m| {
            m.iter()
                .map(|(&id, &(pos, vel, last_seen))| {
                    let dt = now_instant.duration_since(last_seen).as_secs_f32().min(0.05);
                    let extrapolated = (
                        pos.0 + vel.0 * dt,
                        pos.1 + vel.1 * dt,
                        pos.2 + vel.2 * dt,
                    );
                    (id, extrapolated)
                })
                .collect::<std::collections::HashMap<u64, (f32, f32, f32)>>()
        })
        .unwrap_or_default();

    let snapshots = match drone_pos.snapshots.lock() {
        Ok(s) => s,
        Err(_) => return,
    };

    let mut target_positions: std::collections::HashMap<u64, Vec3> = std::collections::HashMap::new();

    if snapshots.len() >= 2 {
        let delay = std::time::Duration::from_millis(16);
        let now = std::time::Instant::now();
        let render_time = if now > snapshots[0].timestamp + delay {
            now - delay
        } else {
            snapshots[0].timestamp
        };

        let mut prev: Option<&network::Snapshot> = None;
        let mut next: Option<&network::Snapshot> = None;

        for i in 0..snapshots.len() - 1 {
            let a = &snapshots[i];
            let b = &snapshots[i + 1];
            if a.timestamp <= render_time && b.timestamp >= render_time {
                prev = Some(a);
                next = Some(b);
                break;
            }
        }

        if let (Some(a), Some(b)) = (prev, next) {
            let dt = b.timestamp.duration_since(a.timestamp).as_secs_f32();
            let elapsed = render_time.duration_since(a.timestamp).as_secs_f32();
            let alpha = if dt > 0.0001 { (elapsed / dt).clamp(0.0, 1.0) } else { 0.0 };

            for (&id, &(pos_b, _)) in &b.entities {
                let p1 = Vec3::new(pos_b.0, pos_b.1, pos_b.2);
                let pos = if let Some(&(pos_a, _)) = a.entities.get(&id) {
                    let p0 = Vec3::new(pos_a.0, pos_a.1, pos_a.2);
                    p0.lerp(p1, alpha)
                } else {
                    p1
                };
                target_positions.insert(id, pos);
            }
        } else if let Some(last) = snapshots.back() {
            let elapsed_since_last = render_time
                .duration_since(last.timestamp)
                .as_secs_f32()
                .min(0.1);
            for (&id, &(pos_t, vel_t)) in &last.entities {
                let pos = Vec3::new(pos_t.0, pos_t.1, pos_t.2);
                let vel = Vec3::new(vel_t.0, vel_t.1, vel_t.2);
                target_positions.insert(id, pos + vel * elapsed_since_last);
            }
        }
    }

    for (&id, &pos) in &acc_map {
        target_positions.entry(id).or_insert(Vec3::new(pos.0, pos.1, pos.2));
    }

    drop(snapshots);

    let player_id = accumulated_state.player_id.lock().ok().and_then(|pid| *pid);
    if let Some(pid) = player_id {
        target_positions.remove(&pid);
    }

    let cells: Vec<network::CellBounds> = octree_cells.cells.lock().ok()
        .map(|c| c.clone())
        .unwrap_or_default();

    let remote_player_ids = accumulated_state
        .player_entity_ids.lock().ok()
        .map(|s| s.clone())
        .unwrap_or_default();

    let mut existing_drones = std::collections::HashMap::new();
    for (entity, _, server_drone, _) in drone_query.iter() {
        existing_drones.insert(server_drone.id, entity);
    }

    for (&id, &pos) in &target_positions {
        let is_player = remote_player_ids.contains(&id);

        if let Some(&entity) = existing_drones.get(&id) {
            if let Ok((_, mut transform, mut server_drone, material_handle)) = drone_query.get_mut(entity) {
                let dist = transform.translation.distance(pos);
                let alpha = if dist > 5.0 { 0.9 } else { 0.4 };
                transform.translation = transform.translation.lerp(pos, alpha);
                server_drone.last_seen_frame = current_frame;

                if !is_player && !cells.is_empty() {
                    let (_, cell_idx) = visuals::cell_color_for_position_with_hysteresis(
                        pos, &cells, server_drone.cached_cell,
                    );
                    server_drone.cached_cell = cell_idx;
                }

                let color = if is_player {
                    Color::srgb(1.0, 0.0, 1.0)
                } else {
                    visuals::cell_color_for_index(server_drone.cached_cell)
                };
                let srgba = color.to_srgba();
                if let Some(material) = materials.get_mut(material_handle) {
                    material.base_color = color;
                    material.emissive = LinearRgba::rgb(srgba.red * 2.0, srgba.green * 2.0, srgba.blue * 2.0);
                }
            }
        } else {
            let cell_idx = if is_player { 0 } else if !cells.is_empty() {
                visuals::cell_color_for_position(pos, &cells).1
            } else { 0 };
            let color = if is_player {
                Color::srgb(1.0, 0.0, 1.0)
            } else {
                visuals::cell_color_for_index(cell_idx)
            };
            let srgba = color.to_srgba();
            let radius = if is_player { 1.5 } else { 0.3 };

            commands.spawn((
                Mesh3d(meshes.add(Sphere::new(radius).mesh())),
                MeshMaterial3d(materials.add(StandardMaterial {
                    base_color: color,
                    emissive: LinearRgba::rgb(srgba.red * 2.0, srgba.green * 2.0, srgba.blue * 2.0),
                    ..default()
                })),
                Transform::from_translation(pos),
                ServerDrone { id, cached_cell: cell_idx, last_seen_frame: current_frame },
            ));
        }
    }

    for (id, entity) in existing_drones {
        if !target_positions.contains_key(&id) && !acc_map.contains_key(&id) {
            if let Ok((_, _, drone, _)) = drone_query.get(entity) {
                if current_frame.wrapping_sub(drone.last_seen_frame) > despawn_grace_frames {
                    commands.entity(entity).despawn();
                }
            }
        }
    }
}
