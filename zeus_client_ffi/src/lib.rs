use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use tokio::runtime::Runtime;
use zeus_client::ZeusClient;

pub struct FfiClient {
    client: ZeusClient,
    runtime: Runtime,
    local_id: u64,
}

#[unsafe(no_mangle)]
pub extern "C" fn zeus_client_create(local_id: u64) -> *mut FfiClient {
    let rt = match Runtime::new() {
        Ok(r) => r,
        Err(_) => return ptr::null_mut(),
    };
    let client = {
        let _guard = rt.enter();
        match ZeusClient::new(local_id) {
            Ok(c) => c,
            Err(_) => return ptr::null_mut(),
        }
    };
    Box::into_raw(Box::new(FfiClient {
        client,
        runtime: rt,
        local_id,
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_client_destroy(client: *mut FfiClient) {
    if !client.is_null() {
        unsafe { drop(Box::from_raw(client)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_client_local_id(client: *const FfiClient) -> u64 {
    if client.is_null() {
        return 0;
    }
    unsafe { (*client).local_id }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_client_connect(
    client: *mut FfiClient,
    address: *const c_char,
) -> i32 {
    if client.is_null() || address.is_null() {
        return -1;
    }
    let addr_str = match unsafe { CStr::from_ptr(address) }.to_str() {
        Ok(s) => s,
        Err(_) => return -2,
    };
    let addr: std::net::SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(_) => return -3,
    };
    let c = unsafe { &mut *client };
    match c.runtime.block_on(c.client.connect(addr)) {
        Ok(_) => 0,
        Err(_) => -4,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_client_send_state(
    client: *mut FfiClient,
    px: f32,
    py: f32,
    pz: f32,
    vx: f32,
    vy: f32,
    vz: f32,
) -> i32 {
    if client.is_null() {
        return -1;
    }
    let c = unsafe { &mut *client };
    match c
        .runtime
        .block_on(c.client.send_state((px, py, pz), (vx, vy, vz)))
    {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_client_poll(
    client: *mut FfiClient,
    buffer: *mut u8,
    buffer_len: i32,
) -> i32 {
    if client.is_null() || buffer.is_null() || buffer_len <= 0 {
        return -1;
    }
    let c = unsafe { &mut *client };
    let conn = c.client.connection();
    if conn.is_none() {
        return 0;
    }
    let conn = conn.unwrap();
    match c.runtime.block_on(async {
        tokio::select! {
            biased;
            result = conn.read_datagram() => Some(result),
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => None,
        }
    }) {
        Some(Ok(data)) => {
            let len = data.len().min(buffer_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buffer, len) };
            len as i32
        }
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_parse_status(
    data: *const u8,
    len: i32,
    entity_count: *mut u16,
    node_count: *mut u8,
    map_width: *mut u8,
    ball_radius: *mut u8,
) -> i32 {
    if data.is_null() || len < 6 {
        return -1;
    }
    let slice = unsafe { std::slice::from_raw_parts(data, len as usize) };
    if slice[0] != 0xAA {
        return -2;
    }
    if !entity_count.is_null() {
        unsafe { *entity_count = ((slice[1] as u16) << 8) | (slice[2] as u16) };
    }
    if !node_count.is_null() {
        unsafe { *node_count = slice[3] };
    }
    if !map_width.is_null() {
        unsafe { *map_width = slice[4] };
    }
    if !ball_radius.is_null() {
        unsafe { *ball_radius = slice[5] };
    }
    0
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_parse_player_ids(
    data: *const u8,
    len: i32,
    player_ids: *mut u64,
    max_players: i32,
) -> i32 {
    if data.is_null() || len < 3 {
        return -1;
    }
    let slice = unsafe { std::slice::from_raw_parts(data, len as usize) };
    if slice[0] != 0xBB {
        return -2;
    }
    let count = ((slice[1] as u16) << 8) | (slice[2] as u16);
    let mut offset = 3;
    let mut written = 0i32;
    for _ in 0..count {
        if offset + 8 > slice.len() {
            break;
        }
        if written >= max_players {
            break;
        }
        let id = u64::from_le_bytes(slice[offset..offset + 8].try_into().unwrap());
        if !player_ids.is_null() {
            unsafe { *player_ids.add(written as usize) = id };
        }
        offset += 8;
        written += 1;
    }
    written
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn zeus_parse_state_update(
    data: *const u8,
    len: i32,
    entity_ids: *mut u64,
    positions_xyz: *mut f32,
    velocities_xyz: *mut f32,
    max_entities: i32,
) -> i32 {
    if data.is_null() || len < 2 {
        return -1;
    }
    let slice = unsafe { std::slice::from_raw_parts(data, len as usize) };
    if slice[0] != 0xCC {
        return -2;
    }
    let fb_bytes = &slice[1..];
    let update = match zeus_common::flatbuffers::root::<zeus_common::StateUpdate>(fb_bytes) {
        Ok(u) => u,
        Err(_) => return -3,
    };
    let ghosts = match update.ghosts() {
        Some(g) => g,
        None => return 0,
    };
    let mut written = 0i32;
    for ghost in ghosts {
        if written >= max_entities {
            break;
        }
        let i = written as usize;
        if !entity_ids.is_null() {
            unsafe { *entity_ids.add(i) = ghost.entity_id() };
        }
        if !positions_xyz.is_null() {
            if let Some(pos) = ghost.position() {
                unsafe {
                    *positions_xyz.add(i * 3) = pos.x();
                    *positions_xyz.add(i * 3 + 1) = pos.y();
                    *positions_xyz.add(i * 3 + 2) = pos.z();
                }
            }
        }
        if !velocities_xyz.is_null() {
            if let Some(vel) = ghost.velocity() {
                unsafe {
                    *velocities_xyz.add(i * 3) = vel.x();
                    *velocities_xyz.add(i * 3 + 1) = vel.y();
                    *velocities_xyz.add(i * 3 + 2) = vel.z();
                }
            }
        }
        written += 1;
    }
    written
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_destroy() {
        let client = zeus_client_create(12345);
        assert!(!client.is_null());
        unsafe { zeus_client_destroy(client) };
    }

    #[test]
    fn test_local_id() {
        let client = zeus_client_create(99999);
        assert!(!client.is_null());
        unsafe {
            assert_eq!(zeus_client_local_id(client), 99999);
            zeus_client_destroy(client);
        }
    }

    #[test]
    fn test_poll_empty_returns_zero() {
        let client = zeus_client_create(1);
        assert!(!client.is_null());
        unsafe {
            let mut buf = [0u8; 1024];
            let result = zeus_client_poll(client, buf.as_mut_ptr(), 1024);
            assert_eq!(result, 0);
            zeus_client_destroy(client);
        }
    }

    #[test]
    fn test_parse_status() {
        let data: [u8; 6] = [0xAA, 0x01, 0xF4, 2, 24, 8];
        let mut entity_count: u16 = 0;
        let mut node_count: u8 = 0;
        let mut map_width: u8 = 0;
        let mut ball_radius: u8 = 0;
        unsafe {
            let result = zeus_parse_status(
                data.as_ptr(),
                6,
                &mut entity_count,
                &mut node_count,
                &mut map_width,
                &mut ball_radius,
            );
            assert_eq!(result, 0);
            assert_eq!(entity_count, 500);
            assert_eq!(node_count, 2);
            assert_eq!(map_width, 24);
            assert_eq!(ball_radius, 8);
        }
    }

    #[test]
    fn test_parse_player_ids() {
        let mut data = vec![0xBB, 0x00, 0x02];
        data.extend_from_slice(&100u64.to_le_bytes());
        data.extend_from_slice(&200u64.to_le_bytes());
        let mut ids = [0u64; 10];
        unsafe {
            let count =
                zeus_parse_player_ids(data.as_ptr(), data.len() as i32, ids.as_mut_ptr(), 10);
            assert_eq!(count, 2);
            assert_eq!(ids[0], 100);
            assert_eq!(ids[1], 200);
        }
    }

    #[test]
    fn test_parse_state_update() {
        use zeus_common::flatbuffers::FlatBufferBuilder;
        use zeus_common::{Ghost, GhostArgs, StateUpdate, StateUpdateArgs, Vec3};

        let mut builder = FlatBufferBuilder::new();
        let pos1 = Vec3::new(1.0, 2.0, 3.0);
        let vel1 = Vec3::new(0.1, 0.2, 0.3);
        let sig1 = builder.create_vector(&[0u8; 64]);
        let g1 = Ghost::create(
            &mut builder,
            &GhostArgs {
                entity_id: 10,
                position: Some(&pos1),
                velocity: Some(&vel1),
                signature: Some(sig1),
            },
        );

        let pos2 = Vec3::new(4.0, 5.0, 6.0);
        let vel2 = Vec3::new(0.4, 0.5, 0.6);
        let sig2 = builder.create_vector(&[0u8; 64]);
        let g2 = Ghost::create(
            &mut builder,
            &GhostArgs {
                entity_id: 20,
                position: Some(&pos2),
                velocity: Some(&vel2),
                signature: Some(sig2),
            },
        );

        let ghosts_vec = builder.create_vector(&[g1, g2]);
        let update = StateUpdate::create(
            &mut builder,
            &StateUpdateArgs {
                ghosts: Some(ghosts_vec),
            },
        );
        builder.finish(update, None);
        let fb_bytes = builder.finished_data();

        let mut payload = Vec::with_capacity(1 + fb_bytes.len());
        payload.push(0xCC);
        payload.extend_from_slice(fb_bytes);

        let mut ids = [0u64; 10];
        let mut positions = [0.0f32; 30];
        let mut velocities = [0.0f32; 30];

        unsafe {
            let count = zeus_parse_state_update(
                payload.as_ptr(),
                payload.len() as i32,
                ids.as_mut_ptr(),
                positions.as_mut_ptr(),
                velocities.as_mut_ptr(),
                10,
            );
            assert_eq!(count, 2);
            assert_eq!(ids[0], 10);
            assert_eq!(ids[1], 20);
            assert!((positions[0] - 1.0).abs() < 0.01);
            assert!((positions[4] - 5.0).abs() < 0.01);
            assert!((velocities[3] - 0.4).abs() < 0.01);
        }
    }
}
