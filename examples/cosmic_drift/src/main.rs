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
    pub cached_zone: usize,
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
                repel_player_from_npcs,
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
        let backend_entities = accumulated_state
            .positions
            .lock()
            .ok()
            .map(|m| m.len() as u16)
            .unwrap_or(0);

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

    let backend_entities = accumulated_state
        .positions
        .lock()
        .ok()
        .map(|m| m.len() as u16)
        .unwrap_or(0);
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
    time: Res<Time>,
    mut query: Query<&mut ExternalImpulse, With<PlayerShip>>,
) {
    let dt = time.delta_secs();
    for mut impulse in query.iter_mut() {
        let thrust = 80.0;
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
            impulse.impulse += force.normalize() * thrust * dt;
        }
    }
}

fn repel_player_from_npcs(
    mut player_query: Query<(&Transform, &mut ExternalImpulse), With<PlayerShip>>,
    npc_query: Query<&Transform, (With<ServerBall>, Without<PlayerShip>)>,
    time: Res<Time>,
) {
    let dt = time.delta_secs();
    let player_radius = 0.8_f32;
    let npc_radius = 0.8_f32;
    let min_dist = player_radius + npc_radius;
    let repel_strength = 15.0;

    for (player_tf, mut impulse) in player_query.iter_mut() {
        let pp = player_tf.translation;
        for npc_tf in npc_query.iter() {
            let np = npc_tf.translation;
            let diff = Vec3::new(pp.x - np.x, 0.0, pp.z - np.z);
            let dist = diff.length();
            if dist < min_dist && dist > 0.001 {
                let overlap = min_dist - dist;
                let push = diff.normalize() * overlap * repel_strength;
                impulse.impulse += push * dt;
            }
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
            linear_damping: 0.5,
            angular_damping: 0.5,
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

    if spawn_count == 0 {
        return;
    }

    let buf: bytes::Bytes = network::encode_0xdd(spawn_count).into();
    if let Ok(conns) = net.all_connections.lock() {
        for conn in conns.iter() {
            let _ = conn.send_datagram(buf.clone());
        }
    }
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
    net: Res<network::NetworkResource>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut ball_query: Query<(
        Entity,
        &mut Transform,
        &mut ServerBall,
        &MeshMaterial3d<StandardMaterial>,
    )>,
) {
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

    let snapshots = match ball_pos.snapshots.lock() {
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
            let alpha = if dt > 0.0001 {
                (elapsed / dt).clamp(0.0, 1.0)
            } else {
                0.0
            };

            for (&id, &(pos_b_tuple, _)) in &b.entities {
                let p1 = Vec3::new(pos_b_tuple.0, pos_b_tuple.1, pos_b_tuple.2);
                let pos = if let Some(&(pos_a_tuple, _)) = a.entities.get(&id) {
                    let p0 = Vec3::new(pos_a_tuple.0, pos_a_tuple.1, pos_a_tuple.2);
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
            for (&id, &(pos_tuple, vel_tuple)) in &last.entities {
                let pos = Vec3::new(pos_tuple.0, pos_tuple.1, pos_tuple.2);
                let vel = Vec3::new(vel_tuple.0, vel_tuple.1, vel_tuple.2);
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

    let nc = server_status.get_node_count().max(1) as usize;
    let zw = 24.0 / nc as f32;
    let hysteresis = 0.5_f32;

    for (&id, &pos) in &target_positions {
        let is_player = remote_player_ids.contains(&id);

        if let Some(&entity) = existing_balls.get(&id) {
            if let Ok((_, mut transform, mut server_ball, material_handle)) = ball_query.get_mut(entity) {
                transform.translation = transform.translation.lerp(pos, 0.7);

                if !is_player {
                    let raw_zone = (pos.x / zw).floor() as usize;
                    let raw_zone = raw_zone.clamp(0, nc.saturating_sub(1));
                    let cur = server_ball.cached_zone;
                    if raw_zone != cur {
                        let boundary = if raw_zone > cur {
                            cur as f32 * zw + zw
                        } else {
                            cur as f32 * zw
                        };
                        if (pos.x - boundary).abs() > hysteresis {
                            server_ball.cached_zone = raw_zone;
                        }
                    }
                }

                let (r, g, b) = if is_player {
                    (1.0, 0.0, 1.0)
                } else {
                    ZONE_COLORS[server_ball.cached_zone.min(3)]
                };
                if let Some(material) = materials.get_mut(material_handle) {
                    material.base_color = Color::srgb(r, g, b);
                    material.emissive = LinearRgba::rgb(r * 1.5, g * 1.5, b * 1.5);
                }
            }
        } else {
            let initial_zone = if is_player {
                0
            } else {
                let z = (pos.x / zw).floor() as usize;
                z.clamp(0, nc.saturating_sub(1))
            };
            let (r, g, b) = if is_player {
                (1.0, 0.0, 1.0)
            } else {
                ZONE_COLORS[initial_zone.min(3)]
            };

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
                ServerBall { id, cached_zone: initial_zone },
            ));
        }
    }

    for (id, entity) in existing_balls {
        if !target_positions.contains_key(&id) && !acc_map.contains_key(&id) {
            commands.entity(entity).despawn();
        }
    }
}
