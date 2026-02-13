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
    if slice.is_empty() || slice[0] != 0xCC {
        return -2;
    }
    if slice.len() < 3 {
        return -3;
    }
    let count = u16::from_le_bytes([slice[1], slice[2]]) as usize;
    let mut offset = 3usize;
    let mut written = 0i32;
    for _ in 0..count {
        if written >= max_entities {
            break;
        }
        if offset + 15 > slice.len() {
            break;
        }
        let id = u64::from_le_bytes([
            slice[offset], slice[offset+1], slice[offset+2], slice[offset+3],
            slice[offset+4], slice[offset+5], slice[offset+6], slice[offset+7],
        ]);
        offset += 8;
        let flags = slice[offset];
        offset += 1;
        let px = i16::from_le_bytes([slice[offset], slice[offset+1]]) as f32 / 500.0;
        offset += 2;
        let py = i16::from_le_bytes([slice[offset], slice[offset+1]]) as f32 / 500.0;
        offset += 2;
        let pz = i16::from_le_bytes([slice[offset], slice[offset+1]]) as f32 / 500.0;
        offset += 2;
        let (vx, vy, vz) = if flags & 1 == 0 {
            if offset + 6 > slice.len() { break; }
            let vx = i16::from_le_bytes([slice[offset], slice[offset+1]]) as f32 / 100.0;
            offset += 2;
            let vy = i16::from_le_bytes([slice[offset], slice[offset+1]]) as f32 / 100.0;
            offset += 2;
            let vz = i16::from_le_bytes([slice[offset], slice[offset+1]]) as f32 / 100.0;
            offset += 2;
            (vx, vy, vz)
        } else {
            (0.0, 0.0, 0.0)
        };
        let i = written as usize;
        if !entity_ids.is_null() {
            unsafe { *entity_ids.add(i) = id };
        }
        if !positions_xyz.is_null() {
            unsafe {
                *positions_xyz.add(i * 3) = px;
                *positions_xyz.add(i * 3 + 1) = py;
                *positions_xyz.add(i * 3 + 2) = pz;
            }
        }
        if !velocities_xyz.is_null() {
            unsafe {
                *velocities_xyz.add(i * 3) = vx;
                *velocities_xyz.add(i * 3 + 1) = vy;
                *velocities_xyz.add(i * 3 + 2) = vz;
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
        let mut payload = Vec::new();
        payload.push(0xCC);
        let count: u16 = 2;
        payload.extend_from_slice(&count.to_le_bytes());

        let id1: u64 = 10;
        let px1 = ((1.0f32 * 500.0).round() as i16).to_le_bytes();
        let py1 = ((2.0f32 * 500.0).round() as i16).to_le_bytes();
        let pz1 = ((3.0f32 * 500.0).round() as i16).to_le_bytes();
        let vx1 = ((0.1f32 * 100.0).round() as i16).to_le_bytes();
        let vy1 = ((0.2f32 * 100.0).round() as i16).to_le_bytes();
        let vz1 = ((0.3f32 * 100.0).round() as i16).to_le_bytes();

        payload.extend_from_slice(&id1.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&px1);
        payload.extend_from_slice(&py1);
        payload.extend_from_slice(&pz1);
        payload.extend_from_slice(&vx1);
        payload.extend_from_slice(&vy1);
        payload.extend_from_slice(&vz1);

        let id2: u64 = 20;
        let px2 = ((4.0f32 * 500.0).round() as i16).to_le_bytes();
        let py2 = ((5.0f32 * 500.0).round() as i16).to_le_bytes();
        let pz2 = ((6.0f32 * 500.0).round() as i16).to_le_bytes();
        let vx2 = ((0.4f32 * 100.0).round() as i16).to_le_bytes();
        let vy2 = ((0.5f32 * 100.0).round() as i16).to_le_bytes();
        let vz2 = ((0.6f32 * 100.0).round() as i16).to_le_bytes();

        payload.extend_from_slice(&id2.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(&px2);
        payload.extend_from_slice(&py2);
        payload.extend_from_slice(&pz2);
        payload.extend_from_slice(&vx2);
        payload.extend_from_slice(&vy2);
        payload.extend_from_slice(&vz2);

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
