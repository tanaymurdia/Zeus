use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

mod network;
mod visuals;

use network::ServerStatus;
use visuals::ZONE_COLORS;

#[derive(Component)]
struct PlayerShip;

#[derive(Component)]
struct ServerBall {
    pub id: u64,
}

fn main() {
    App::new()
        .insert_resource(ClearColor(Color::srgb(0.1, 0.1, 0.1)))
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: "Cosmic Drift: Zeus Hypervisor Demo".into(),
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
                move_player,
                update_hud,
                spawn_entities_on_keypress,
                camera_follow,
                render_server_balls,
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
            font_size: 20.0,
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
    mut color_query: Query<&mut TextColor, With<HudText>>,
    player_query: Query<&Transform, With<PlayerShip>>,
    net: Res<network::NetworkResource>,
    server_status: Res<network::ServerStatus>,
    accumulated_state: Res<network::AccumulatedState>,
) {
    for mut text in query.iter_mut() {
        let status = if net.client.is_some() {
            "Connected"
        } else {
            "Connecting..."
        };
        let mut pos_str = String::from("Pos: N/A");

        let mesh_nodes = server_status.get_node_count().max(1) as usize;
        if let Ok(transform) = player_query.get_single() {
            pos_str = format!(
                "Pos: {:.1}, {:.1}, {:.1}",
                transform.translation.x, transform.translation.y, transform.translation.z
            );
        }
        let backend_entities = server_status.get_entity_count();

        let player_count = accumulated_state
            .player_entity_ids
            .lock()
            .ok()
            .map(|s| s.len())
            .unwrap_or(0);
        let zone_width = 24.0 / mesh_nodes as f32;
        let next_split = 5 * mesh_nodes;

        **text = format!(
            "Zeus Hypervisor Demo\nStatus: {}\n{}\n\n--- AUTOSCALING ---\nACTIVE NODES: {}\nZone Width: {:.1}\nEntities: {} | Next Split: >{}\n\nPlayers: {} | Tick: 128Hz\n\n[M] +500 balls  [N] +100 balls",
            status, pos_str,
            mesh_nodes, zone_width, backend_entities, next_split,
            player_count + 1
        );
    }

    let backend_entities = server_status.get_entity_count();
    for mut color in color_query.iter_mut() {
        if backend_entities > 2000 {
            *color = TextColor(Color::srgb(1.0, 0.2, 0.2));
        } else if backend_entities > 1500 {
            *color = TextColor(Color::srgb(1.0, 1.0, 0.2));
        } else {
            *color = TextColor(Color::WHITE);
        }
    }
}

fn move_player(
    input: Res<ButtonInput<KeyCode>>,
    mut query: Query<&mut ExternalImpulse, With<PlayerShip>>,
) {
    for mut impulse in query.iter_mut() {
        let thrust = 50.0;
        let mut force = Vec3::ZERO;

        if input.pressed(KeyCode::KeyW) {
            force.z -= 1.0;
        }
        if input.pressed(KeyCode::KeyS) {
            force.z += 1.0;
        }
        if input.pressed(KeyCode::KeyA) {
            force.x -= 1.0;
        }
        if input.pressed(KeyCode::KeyD) {
            force.x += 1.0;
        }

        if force != Vec3::ZERO {
            impulse.impulse += force.normalize() * thrust * 0.016;
        }
    }
}

fn setup_scene(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    commands.spawn((
        Camera3d::default(),
        Transform::from_xyz(10.0, 50.0, 50.0).looking_at(Vec3::new(10.0, 0.0, 0.0), Vec3::Y),
        MainCamera,
    ));

    commands.spawn((
        Mesh3d(meshes.add(Sphere::new(1.0).mesh())),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::srgb(0.0, 1.0, 1.0),
            emissive: LinearRgba::rgb(0.0, 2.0, 2.0),
            ..default()
        })),
        Transform::from_xyz(10.0, 0.0, 0.0),
        RigidBody::Dynamic,
        Collider::ball(0.8),
        Velocity::default(),
        Damping {
            linear_damping: 0.0,
            angular_damping: 0.0,
        },
        ExternalImpulse::default(),
        PlayerShip,
    ));

    commands.spawn((
        PointLight {
            intensity: 2_000_000.0,
            shadows_enabled: true,
            ..default()
        },
        Transform::from_xyz(4.0, 8.0, 4.0),
    ));
}

fn spawn_entities_on_keypress(
    input: Res<ButtonInput<KeyCode>>,
    net: Res<network::NetworkResource>,
) {
    let spawn_count: u16 = if input.just_pressed(KeyCode::KeyN) {
        100
    } else if input.just_pressed(KeyCode::KeyM) {
        500
    } else {
        0
    };

    if spawn_count == 0 || net.client.is_none() {
        return;
    }

    let buf = network::encode_0xdd(spawn_count);
    let client_lock = net.client.as_ref().unwrap().clone();
    let rt_handle = net.runtime.handle().clone();
    rt_handle.spawn(async move {
        let client = client_lock.lock().await;
        if let Some(conn) = client.connection() {
            let _ = conn.send_datagram(buf.into());
        }
    });
}

fn camera_follow(
    player_query: Query<&Transform, With<PlayerShip>>,
    mut camera_query: Query<&mut Transform, (With<MainCamera>, Without<PlayerShip>)>,
) {
    let Ok(player_transform) = player_query.get_single() else {
        return;
    };
    let Ok(mut camera_transform) = camera_query.get_single_mut() else {
        return;
    };

    let offset = Vec3::new(0.0, 40.0, 40.0);
    let target_pos = player_transform.translation + offset;

    camera_transform.translation = camera_transform.translation.lerp(target_pos, 0.05);

    camera_transform.look_at(player_transform.translation, Vec3::Y);
}

fn render_server_balls(
    server_status: Res<ServerStatus>,
    mut commands: Commands,
    ball_pos: Res<network::BallPositions>,
    accumulated_state: Res<network::AccumulatedState>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut ball_query: Query<(
        Entity,
        &mut Transform,
        &ServerBall,
        &MeshMaterial3d<StandardMaterial>,
    )>,
) {
    let snapshots = match ball_pos.snapshots.lock() {
        Ok(s) => s,
        Err(_) => return,
    };

    if snapshots.len() < 2 {
        return;
    }

    let delay = std::time::Duration::from_millis(15);

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

    let mut target_positions = std::collections::HashMap::new();

    if let (Some(a), Some(b)) = (prev, next) {
        let duration = b.timestamp.duration_since(a.timestamp).as_secs_f32();
        let elapsed = render_time.duration_since(a.timestamp).as_secs_f32();
        let alpha = if duration > 0.0001 {
            (elapsed / duration).clamp(0.0, 1.0)
        } else {
            0.0
        };

        for (&id, &(pos_b_tuple, _vel_b)) in &b.entities {
            let pos_b = Vec3::new(pos_b_tuple.0, pos_b_tuple.1, pos_b_tuple.2);

            let pos = if let Some(&(pos_a_tuple, _)) = a.entities.get(&id) {
                let pos_a = Vec3::new(pos_a_tuple.0, pos_a_tuple.1, pos_a_tuple.2);
                pos_a.lerp(pos_b, alpha)
            } else {
                pos_b
            };
            target_positions.insert(id, pos);
        }
    } else if let Some(last) = snapshots.back() {
        let elapsed_since_last = render_time
            .duration_since(last.timestamp)
            .as_secs_f32()
            .min(0.05);
        for (&id, &(pos_tuple, vel_tuple)) in &last.entities {
            let pos = Vec3::new(pos_tuple.0, pos_tuple.1, pos_tuple.2);
            let vel = Vec3::new(vel_tuple.0, vel_tuple.1, vel_tuple.2);
            target_positions.insert(id, pos + vel * elapsed_since_last);
        }
    }

    let player_id = accumulated_state.player_id.lock().ok().and_then(|pid| *pid);
    if let Some(pid) = player_id {
        target_positions.remove(&pid);
    }

    let mut existing_balls = std::collections::HashMap::new();
    for (entity, _, server_ball, _) in ball_query.iter() {
        existing_balls.insert(server_ball.id, entity);
    }

    let remote_player_ids = accumulated_state
        .player_entity_ids
        .lock()
        .ok()
        .map(|s| s.clone())
        .unwrap_or_default();

    let nc = server_status.get_node_count().max(1) as f32;
    let zw = 24.0 / nc;

    for (&id, &pos) in &target_positions {
        let is_player = remote_player_ids.contains(&id);
        let (r, g, b) = if is_player {
            (1.0, 0.0, 1.0)
        } else {
            let owner = (pos.x / zw).floor() as usize;
            let owner = owner.clamp(0, (nc as usize).saturating_sub(1));
            ZONE_COLORS[owner]
        };

        if let Some(&entity) = existing_balls.get(&id) {
            if let Ok((_, mut transform, _, material_handle)) = ball_query.get_mut(entity) {
                transform.translation = transform.translation.lerp(pos, 0.15);
                if let Some(material) = materials.get_mut(material_handle) {
                    material.base_color = Color::srgb(r, g, b);
                    material.emissive = LinearRgba::rgb(r * 1.5, g * 1.5, b * 1.5);
                }
            }
        } else {
            let radius = if is_player {
                1.0
            } else {
                let r = server_status.get_ball_radius();
                if r < 0.1 { 0.8 } else { r }
            };

            commands.spawn((
                Mesh3d(meshes.add(Sphere::new(radius).mesh())),
                MeshMaterial3d(materials.add(StandardMaterial {
                    base_color: Color::srgb(r, g, b),
                    emissive: LinearRgba::rgb(r * 1.5, g * 1.5, b * 1.5),
                    ..default()
                })),
                Transform::from_translation(pos),
                ServerBall { id },
            ));
        }
    }

    for (id, entity) in existing_balls {
        if !target_positions.contains_key(&id) {
            commands.entity(entity).despawn();
        }
    }
}
