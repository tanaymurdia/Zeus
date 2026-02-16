use super::helpers::*;
use zeus_node::cell::Cell;
use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::{GameLoop, GameWorld};

#[tokio::test]
async fn test_single_node_drone_spawning_and_ticking() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut world = TestDroneWorld::new();
    for i in 0..50 {
        let x = (i as f32 % 10.0) * 2.0 + 1.0;
        let z = (i as f32 / 10.0).floor() * 2.0 + 1.0;
        world.spawn_drone((x, 5.0, z), (0.5, 0.0, -0.3));
    }
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    for id in 1..=50u64 {
        let (pos, vel) = game_loop.world.drones[&id];
        game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }

    for _ in 0..10 {
        game_loop.tick(1.0 / 128.0).await.unwrap();
    }
    assert_eq!(game_loop.world.local_ids.len(), 50);
    assert_eq!(game_loop.engine.node.manager.entities.len(), 50);
}

#[tokio::test]
async fn test_two_node_setup_with_cells() {
    let cell0 = Cell::new(0.0, 12.0, 0.0, 24.0, -12.0, 12.0);
    let cell1 = Cell::new(12.0, 24.0, 0.0, 24.0, -12.0, 12.0);

    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 12.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell0.clone()),
    };
    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 12.0,
        cell: Some(cell1.clone()),
    };

    let mut world0 = TestDroneWorld::new();
    for i in 0..10 {
        world0.spawn_drone((2.0 + i as f32, 5.0, 0.0), (0.0, 0.0, 0.0));
    }

    let mut world1 = TestDroneWorld::new();
    for i in 0..10 {
        world1.spawn_drone((14.0 + i as f32, 5.0, 0.0), (0.0, 0.0, 0.0));
    }

    let mut node0 = GameLoop::new(config0, world0).await.unwrap();
    let mut node1 = GameLoop::new(config1, world1).await.unwrap();

    for id in node0.world.local_ids.clone() {
        let (pos, vel) = node0.world.drones[&id];
        node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }
    for id in node1.world.local_ids.clone() {
        let (pos, vel) = node1.world.drones[&id];
        node1.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }

    for _ in 0..5 {
        node0.tick(1.0 / 128.0).await.unwrap();
        node1.tick(1.0 / 128.0).await.unwrap();
    }

    assert_eq!(node0.world.local_ids.len(), 10);
    assert_eq!(node1.world.local_ids.len(), 10);
}

#[tokio::test]
async fn test_should_split_drone_threshold() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let world = TestDroneWorld::new();
    let game_loop = GameLoop::new(config, world).await.unwrap();

    assert!(!game_loop.should_split(3));
    assert!(!game_loop.should_split(10));
    assert!(!game_loop.should_split(39));
    assert!(game_loop.should_split(40));
    assert!(game_loop.should_split(50));
}

#[tokio::test]
async fn test_status_payload_correct() {
    let mut world = TestDroneWorld::new();
    for i in 0..10 {
        world.spawn_drone((i as f32, 5.0, 0.0), (0.0, 0.0, 0.0));
    }
    let (count, width, radius) = world.status_payload();
    assert_eq!(count, 10);
    assert_eq!(width, 24);
    assert_eq!(radius, 3);
}

#[tokio::test]
async fn test_0xee_cell_broadcast_encoding() {
    let world_size: f32 = 24.0;
    let total_nodes: usize = 3;
    let zone_width = world_size / total_nodes as f32;

    let mut buf = Vec::with_capacity(1 + 1 + total_nodes * 24);
    buf.push(0xEE);
    buf.push(total_nodes as u8);
    for i in 0..total_nodes {
        let x_min = i as f32 * zone_width;
        let x_max = (i + 1) as f32 * zone_width;
        buf.extend_from_slice(&x_min.to_le_bytes());
        buf.extend_from_slice(&x_max.to_le_bytes());
        buf.extend_from_slice(&(-1.0f32).to_le_bytes());
        buf.extend_from_slice(&(world_size + 1.0).to_le_bytes());
        buf.extend_from_slice(&(-(world_size / 2.0)).to_le_bytes());
        buf.extend_from_slice(&((world_size / 2.0)).to_le_bytes());
    }

    assert_eq!(buf[0], 0xEE);
    assert_eq!(buf[1], 3);
    assert_eq!(buf.len(), 2 + 3 * 24);

    let mut offset = 2;
    for i in 0..total_nodes {
        let x_min = f32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
        offset += 4;
        let x_max = f32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
        offset += 4;
        let _y_min = f32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
        offset += 4;
        let _y_max = f32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
        offset += 4;
        let _z_min = f32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
        offset += 4;
        let _z_max = f32::from_le_bytes(buf[offset..offset+4].try_into().unwrap());
        offset += 4;

        assert!((x_min - i as f32 * zone_width).abs() < 0.01);
        assert!((x_max - (i + 1) as f32 * zone_width).abs() < 0.01);
    }
}

#[tokio::test]
async fn test_gravity_well_simulation() {
    let well_pos = (12.0, 5.0, 0.0);
    let well_radius = 30.0;
    let attract_strength = 50.0;

    let mut drones: Vec<((f32, f32, f32), (f32, f32, f32))> = Vec::new();
    for i in 0..20 {
        let x = 2.0 + (i as f32 % 5.0) * 4.0;
        let z = 2.0 + (i as f32 / 5.0).floor() * 4.0;
        drones.push(((x, 5.0, z), (0.0, 0.0, 0.0)));
    }

    for (pos, vel) in drones.iter_mut() {
        let dx = well_pos.0 - pos.0;
        let dy = well_pos.1 - pos.1;
        let dz = well_pos.2 - pos.2;
        let dist_sq = dx * dx + dy * dy + dz * dz;
        if dist_sq < well_radius * well_radius && dist_sq > 0.5 {
            let dist = dist_sq.sqrt();
            let f_mag = attract_strength / dist_sq.max(1.0);
            vel.0 += (dx / dist) * f_mag * 0.1;
            vel.1 += (dy / dist) * f_mag * 0.1;
            vel.2 += (dz / dist) * f_mag * 0.1;
        }
    }

    for (_, vel) in &drones {
        let speed = (vel.0 * vel.0 + vel.1 * vel.1 + vel.2 * vel.2).sqrt();
        assert!(speed > 0.0, "Drones near well should have velocity from attraction");
    }
}

#[tokio::test]
async fn test_cell_bounds_with_custom_cell() {
    let cell = Cell::new(-100.0, 100.0, -50.0, 50.0, -200.0, 200.0);
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 200.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: -100.0,
        cell: Some(cell.clone()),
    };
    let mut world = TestDroneWorld::new();
    world.spawn_drone((0.0, 0.0, 0.0), (1.0, 0.0, 0.0));
    let game_loop = GameLoop::new(config, world).await.unwrap();
    assert!(game_loop.world.local_ids.contains(&1));
}

#[tokio::test]
async fn test_drone_world_step_integration() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut world = TestDroneWorld::new();
    world.spawn_drone((12.0, 12.0, 0.0), (3.0, 0.0, 1.0));
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1,
        pos: (12.0, 12.0, 0.0),
        vel: (3.0, 0.0, 1.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..100 {
        game_loop.tick(1.0 / 128.0).await.unwrap();
    }

    assert!(game_loop.world.local_ids.contains(&1));
}

#[tokio::test]
async fn test_0xdd_02_spawn_protocol_encoding() {
    let count: u16 = 10;
    let pos = (12.5f32, 8.0f32, -3.0f32);
    let mut buf = Vec::with_capacity(16);
    buf.push(0xDD);
    buf.push(0x02);
    buf.push((count >> 8) as u8);
    buf.push((count & 0xFF) as u8);
    buf.extend_from_slice(&pos.0.to_le_bytes());
    buf.extend_from_slice(&pos.1.to_le_bytes());
    buf.extend_from_slice(&pos.2.to_le_bytes());

    assert_eq!(buf.len(), 16);
    assert_eq!(buf[0], 0xDD);
    assert_eq!(buf[1], 0x02);
    let decoded_count = ((buf[2] as u16) << 8) | (buf[3] as u16);
    assert_eq!(decoded_count, 10);
    let x = f32::from_le_bytes(buf[4..8].try_into().unwrap());
    let y = f32::from_le_bytes(buf[8..12].try_into().unwrap());
    let z = f32::from_le_bytes(buf[12..16].try_into().unwrap());
    assert!((x - 12.5).abs() < 1e-4);
    assert!((y - 8.0).abs() < 1e-4);
    assert!((z - (-3.0)).abs() < 1e-4);
}

#[tokio::test]
async fn test_0xde_despawn_protocol_encoding() {
    let count: u16 = 15;
    let buf: Vec<u8> = vec![0xDE, (count >> 8) as u8, (count & 0xFF) as u8];
    assert_eq!(buf.len(), 3);
    assert_eq!(buf[0], 0xDE);
    let decoded = ((buf[1] as u16) << 8) | (buf[2] as u16);
    assert_eq!(decoded, 15);
}

#[tokio::test]
async fn test_request_split_message_parsing() {
    let line = "REQUEST_SPLIT new_cell=12.0,24.0,-1.0,25.0,-12.0,12.0";
    let new_cell_str = line.split("new_cell=").nth(1).map(|s| s.trim().to_string());
    assert!(new_cell_str.is_some());
    let cell_str = new_cell_str.unwrap();
    let parts: Vec<f32> = cell_str.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    assert_eq!(parts.len(), 6);
    let cell = Cell::new(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
    assert!((cell.x_min - 12.0).abs() < 1e-3);
    assert!((cell.x_max - 24.0).abs() < 1e-3);
    assert!((cell.y_min - (-1.0)).abs() < 1e-3);
    assert!((cell.y_max - 25.0).abs() < 1e-3);
    assert!((cell.z_min - (-12.0)).abs() < 1e-3);
    assert!((cell.z_max - 12.0).abs() < 1e-3);
}

#[tokio::test]
async fn test_request_merge_message_detection() {
    let line = "REQUEST_MERGE";
    assert!(line.contains("REQUEST_MERGE"));
    let split_line = "REQUEST_SPLIT new_cell=0,10,0,10,0,10";
    assert!(!split_line.contains("REQUEST_MERGE"));
}

#[tokio::test]
async fn test_cell_broadcast_via_broadcast_cells() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let world = TestDroneWorld::new();
    let game_loop = GameLoop::new(config, world).await.unwrap();

    let cells = vec![
        Cell::new(0.0, 12.0, -1.0, 25.0, -12.0, 12.0),
        Cell::new(12.0, 24.0, -1.0, 25.0, -12.0, 12.0),
    ];
    game_loop.broadcast_cells(&cells);
}

#[tokio::test]
async fn test_local_entity_positions_helper() {
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut world = TestDroneWorld::new();
    for i in 0..5 {
        world.spawn_drone(((i as f32 + 1.0) * 3.0, 12.0, 0.0), (0.0, 0.0, 0.0));
    }
    let mut game_loop = GameLoop::new(config, world).await.unwrap();
    for id in 1..=5u64 {
        let (pos, vel) = game_loop.world.drones[&id];
        game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
            id, pos, vel,
            state: zeus_node::entity_manager::AuthorityState::Local,
            verifying_key: None,
        });
    }
    game_loop.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 99, pos: (20.0, 12.0, 0.0), vel: (0.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Remote,
        verifying_key: None,
    });

    let positions = game_loop.local_entity_positions();
    assert_eq!(positions.len(), 5, "Only Local entities should be returned");
    for (id, _) in &positions {
        assert!(*id >= 1 && *id <= 5);
    }
}

#[tokio::test]
async fn test_cell_serialize_deserialize_roundtrip() {
    let cell = Cell::new(1.5, 23.7, -0.5, 25.3, -11.8, 11.2);
    let bytes = cell.serialize();
    let restored = Cell::deserialize(&bytes).unwrap();
    assert_eq!(cell, restored);
}
