use bevy::prelude::*;
use bevy_rapier3d::prelude::*;

mod network;
mod visuals;

#[derive(Component)]
struct PlayerShip;

#[derive(Component)]
struct NpcEntity {
    #[allow(dead_code)]
    id: u64,
}

#[derive(Component)]
struct ServerBall {
    pub id: u64,
}

#[derive(Resource, Default)]
pub struct SpawnCounter {
    pub count: u64,
    pub next_id: u64,
}

fn main() {
    App::new()
        .insert_resource(ClearColor(Color::srgb(0.1, 0.1, 0.1)))
        .init_resource::<SpawnCounter>()
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
        .add_plugins(RapierDebugRenderPlugin::default())
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
) {
    for mut text in query.iter_mut() {
        let status = if net.client.is_some() {
            "Connected"
        } else {
            "Connecting..."
        };
        let mut pos_str = String::from("Pos: N/A");

        if let Ok(transform) = player_query.get_single() {
            pos_str = format!(
                "Pos: {:.1}, {:.1}, {:.1}",
                transform.translation.x, transform.translation.y, transform.translation.z
            );
        }

        let backend_entities = server_status.get_entity_count();
        let mesh_nodes = server_status.get_node_count();

        let load_status = if backend_entities > 2000 {
            "⚠️ CRITICAL LOAD"
        } else if backend_entities > 1500 {
            "⚡ HIGH LOAD"
        } else {
            "✓ Normal"
        };

        **text = format!(
            "Zeus Hypervisor Demo\nStatus: {}\n{}\n\n━━━ BACKEND STATUS ━━━\nMesh Nodes: {}\nBackend Entities: {}\nLoad: {}\n\n[M] Add Load  [N] Add Load (small)",
            status, pos_str, mesh_nodes, backend_entities, load_status
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
        Transform::from_xyz(10.0, 2.0, 0.0),
        RigidBody::Dynamic,
        Collider::ball(1.0),
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
    mut commands: Commands,
    input: Res<ButtonInput<KeyCode>>,
    mut counter: ResMut<SpawnCounter>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    player_query: Query<&Transform, With<PlayerShip>>,
) {
    let spawn_count = if input.just_pressed(KeyCode::KeyN) {
        100
    } else if input.just_pressed(KeyCode::KeyM) {
        500
    } else {
        0
    };

    if spawn_count == 0 {
        return;
    }

    let base_pos = player_query
        .get_single()
        .map(|t| t.translation)
        .unwrap_or(Vec3::new(10.0, 2.0, 0.0));

    let mesh = meshes.add(Sphere::new(0.3).mesh());
    let material = materials.add(StandardMaterial {
        base_color: Color::srgb(1.0, 0.5, 0.0),
        emissive: LinearRgba::rgb(1.0, 0.3, 0.0),
        ..default()
    });
    let visual_limit = 50;
    let already_visual = counter.count.min(visual_limit as u64);
    let can_spawn_visual = (visual_limit as u64).saturating_sub(already_visual) as u64;
    let visual_to_spawn = (spawn_count as u64).min(can_spawn_visual);

    for _i in 0..visual_to_spawn {
        let id = counter.next_id;
        counter.next_id += 1;

        let offset = Vec3::new(
            (id as f32 * 0.7).sin() * 20.0,
            0.5,
            (id as f32 * 1.3).cos() * 20.0,
        );

        commands.spawn((
            Mesh3d(mesh.clone()),
            MeshMaterial3d(material.clone()),
            Transform::from_translation(base_pos + offset),
            RigidBody::Dynamic,
            Collider::ball(0.3),
            Velocity {
                linvel: Vec3::new(
                    (id as f32 * 0.3).sin() * 5.0,
                    0.0,
                    (id as f32 * 0.5).cos() * 5.0,
                ),
                ..default()
            },
            Damping {
                linear_damping: 0.3,
                angular_damping: 0.3,
            },
            NpcEntity { id },
        ));
    }

    let virtual_count = spawn_count as u64 - visual_to_spawn;
    counter.next_id += virtual_count;
    counter.count += spawn_count as u64;

    println!(
        "[Demo] Spawned {} entities ({} visual, {} virtual). Total: {}",
        spawn_count, visual_to_spawn, virtual_count, counter.count
    );

    if counter.count > 2000 {
        println!("[Demo] ⚠️  OVER 2000 ENTITIES - Check server for CRITICAL LOAD warning!");
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
    mut commands: Commands,
    ball_positions: Res<network::BallPositions>,
    accumulated_state: Res<network::AccumulatedState>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut ball_query: Query<(Entity, &mut Transform, &ServerBall)>,
) {
    let snapshots = match ball_positions.snapshots.lock() {
        Ok(s) => s,
        Err(_) => return,
    };

    if snapshots.len() < 2 {
        return;
    }

    let delay = std::time::Duration::from_millis(33);

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

        for (&id, &pos_b_tuple) in &b.entities {
            let pos_b = Vec3::new(pos_b_tuple.0, pos_b_tuple.1, pos_b_tuple.2);

            let pos = if let Some(&pos_a_tuple) = a.entities.get(&id) {
                let pos_a = Vec3::new(pos_a_tuple.0, pos_a_tuple.1, pos_a_tuple.2);
                pos_a.lerp(pos_b, alpha)
            } else {
                pos_b
            };
            target_positions.insert(id, pos);
        }
    } else {
        if let Some(last) = snapshots.back() {
            for (&id, &pos) in &last.entities {
                target_positions.insert(id, Vec3::new(pos.0, pos.1, pos.2));
            }
        }
    }

    let player_id = accumulated_state.player_id.lock().ok().and_then(|pid| *pid);
    if let Some(pid) = player_id {
        target_positions.remove(&pid);
    }

    if !target_positions.is_empty() {
        static RENDER_CTR: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let c = RENDER_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if c % 120 == 0 {
            let sample: Vec<_> = target_positions.iter().take(5).collect();
            println!(
                "[Render] {} balls. Sample: {:?}",
                target_positions.len(),
                sample
            );
        }
    }

    let mut existing_balls = std::collections::HashMap::new();
    for (entity, _, server_ball) in ball_query.iter() {
        existing_balls.insert(server_ball.id, entity);
    }

    for (&id, &pos) in &target_positions {
        if let Some(&entity) = existing_balls.get(&id) {
            if let Ok((_, mut transform, _)) = ball_query.get_mut(entity) {
                transform.translation = pos;
            }
        } else {
            commands.spawn((
                Mesh3d(meshes.add(Sphere::new(0.5).mesh())),
                MeshMaterial3d(materials.add(StandardMaterial {
                    base_color: Color::srgb(1.0, 0.5, 0.0),
                    emissive: LinearRgba::rgb(1.0, 0.3, 0.0),
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
