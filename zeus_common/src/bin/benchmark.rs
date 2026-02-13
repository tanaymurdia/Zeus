use std::time::Instant;
use zeus_common::{
    GhostSerializer,
    ed25519_dalek::{Signer, Verifier},
};

fn main() {
    println!("=== Zeus Spatial Hypervisor Benchmarks (Local) ===");
    println!(
        "CPU: {}",
        std::thread::available_parallelism().unwrap().get()
    );

    benchmark_signing_overhead();
    benchmark_serialization_throughput();
    benchmark_discovery_overhead();
    benchmark_compact_vs_flatbuffers();
    benchmark_quantization_accuracy();
    benchmark_delta_encoding_hitrate();
    benchmark_batch_vs_perentity_signing();
}

fn benchmark_signing_overhead() {
    println!("\n--- Cryptographic Overhead (Ed25519) ---");
    let (signing_key, verify_key) = GhostSerializer::generate_keypair();
    let payload = [0u8; 32];

    let start = Instant::now();
    let iterations = 100_000;
    for _ in 0..iterations {
        signing_key.sign(&payload);
    }
    let duration = start.elapsed();
    println!(
        "Signing Latency: {:.4} µs/op ({:.1} ops/sec)",
        duration.as_micros() as f64 / iterations as f64,
        iterations as f64 / duration.as_secs_f64()
    );

    let signature = signing_key.sign(&payload);
    let start = Instant::now();
    for _ in 0..iterations {
        verify_key
            .verify(&payload, &signature)
            .expect("Verification failed");
    }
    let duration = start.elapsed();
    println!(
        "Verify Latency:  {:.4} µs/op ({:.1} ops/sec)",
        duration.as_micros() as f64 / iterations as f64,
        iterations as f64 / duration.as_secs_f64()
    );
}

fn benchmark_serialization_throughput() {
    println!("\n\n");
    let (signing_key, _) = GhostSerializer::generate_keypair();
    let mut serializer = GhostSerializer::new();
    serializer.set_keypair(signing_key);

    let iterations = 50_000;
    let start = Instant::now();

    for i in 0..iterations {
        let _bytes = serializer.serialize(i as u64, (100.0, 200.0, 300.0), (10.0, 20.0, 30.0));
    }

    let duration = start.elapsed();
    let throughput = iterations as f64 / duration.as_secs_f64();

    println!("Throughput:      {:.0} entities/sec", throughput);
    println!(
        "Per Entity:      {:.4} µs",
        duration.as_micros() as f64 / iterations as f64
    );
    println!(
        "Frame Budget (16ms): Can handle {:.0} entities/frame",
        throughput * 0.016
    );
}

fn benchmark_discovery_overhead() {
    println!("\n--- Discovery Protocol Overhead ---");
    use zeus_common::{
        DiscoveryAnnounce, DiscoveryAnnounceArgs, DiscoveryMsg, DiscoveryMsgArgs, Vec3,
        flatbuffers::FlatBufferBuilder,
    };

    let iterations = 100_000;
    let start = Instant::now();
    let mut builder = FlatBufferBuilder::new();
    let mut total_bytes = 0;

    for i in 0..iterations {
        builder.reset();
        let addr_str = "192.168.1.105:8080";
        let addr = builder.create_string(addr_str);
        let pos = Vec3::new(100.0, 200.0, 300.0);

        let announce = DiscoveryAnnounce::create(
            &mut builder,
            &DiscoveryAnnounceArgs {
                node_id: i as u64,
                address: Some(addr),
                position: Some(&pos),
                load: None,
                known_node_ids: None,
                ordinal: 0,
            },
        );

        let msg = DiscoveryMsg::create(
            &mut builder,
            &DiscoveryMsgArgs {
                payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
                payload: Some(announce.as_union_value()),
            },
        );

        builder.finish(msg, None);
        total_bytes += builder.finished_data().len();
    }

    let duration = start.elapsed();
    let avg_size = total_bytes as f64 / iterations as f64;

    println!("Announce Packet Size: {:.1} bytes", avg_size);
    println!(
        "Serialization Time:   {:.4} µs/op",
        duration.as_micros() as f64 / iterations as f64
    );
    println!(
        "Throughput:           {:.0} announces/sec",
        iterations as f64 / duration.as_secs_f64()
    );

    benchmark_load_broadcast();
}

fn quantize_pos(v: f32) -> i16 {
    (v * 500.0).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

fn dequantize_pos(v: i16) -> f32 {
    v as f32 / 500.0
}

fn quantize_vel(v: f32) -> i16 {
    (v * 100.0).round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

fn dequantize_vel(v: i16) -> f32 {
    v as f32 / 100.0
}

fn compact_encode(entries: &[(u64, (f32, f32, f32), (f32, f32, f32))]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3 + entries.len() * 21);
    buf.push(0xCC);
    let count = entries.len() as u16;
    buf.extend_from_slice(&count.to_le_bytes());
    for &(id, pos, vel) in entries {
        buf.extend_from_slice(&id.to_le_bytes());
        let vel_sq = vel.0 * vel.0 + vel.1 * vel.1 + vel.2 * vel.2;
        let at_rest = vel_sq < 0.001;
        buf.push(if at_rest { 1 } else { 0 });
        buf.extend_from_slice(&quantize_pos(pos.0).to_le_bytes());
        buf.extend_from_slice(&quantize_pos(pos.1).to_le_bytes());
        buf.extend_from_slice(&quantize_pos(pos.2).to_le_bytes());
        if !at_rest {
            buf.extend_from_slice(&quantize_vel(vel.0).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.1).to_le_bytes());
            buf.extend_from_slice(&quantize_vel(vel.2).to_le_bytes());
        }
    }
    buf
}

fn compact_decode(data: &[u8]) -> Vec<(u64, (f32, f32, f32), (f32, f32, f32))> {
    let mut result = Vec::new();
    if data.len() < 3 || data[0] != 0xCC {
        return result;
    }
    let count = u16::from_le_bytes([data[1], data[2]]) as usize;
    let mut offset = 3;
    for _ in 0..count {
        if offset + 15 > data.len() { break; }
        let id = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7],
        ]);
        offset += 8;
        let flags = data[offset];
        offset += 1;
        let px = dequantize_pos(i16::from_le_bytes([data[offset], data[offset+1]]));
        offset += 2;
        let py = dequantize_pos(i16::from_le_bytes([data[offset], data[offset+1]]));
        offset += 2;
        let pz = dequantize_pos(i16::from_le_bytes([data[offset], data[offset+1]]));
        offset += 2;
        let (vx, vy, vz) = if flags & 1 == 0 {
            if offset + 6 > data.len() { break; }
            let vx = dequantize_vel(i16::from_le_bytes([data[offset], data[offset+1]]));
            offset += 2;
            let vy = dequantize_vel(i16::from_le_bytes([data[offset], data[offset+1]]));
            offset += 2;
            let vz = dequantize_vel(i16::from_le_bytes([data[offset], data[offset+1]]));
            offset += 2;
            (vx, vy, vz)
        } else {
            (0.0, 0.0, 0.0)
        };
        result.push((id, (px, py, pz), (vx, vy, vz)));
    }
    result
}

fn benchmark_compact_vs_flatbuffers() {
    use zeus_common::{
        Ghost, GhostArgs, StateUpdate, StateUpdateArgs, Vec3,
        flatbuffers::FlatBufferBuilder,
    };

    println!("\n=== Compact Binary vs FlatBuffers ===");

    let entities: Vec<(u64, (f32, f32, f32), (f32, f32, f32))> = (0..20)
        .map(|i| (i as u64, (i as f32 * 1.2, 1.0, -(i as f32) * 0.5), (1.0 + i as f32 * 0.1, 0.0, -0.5)))
        .collect();

    let iterations = 50_000;

    let start = Instant::now();
    let mut fb_total_bytes = 0;
    let mut builder = FlatBufferBuilder::new();
    for _ in 0..iterations {
        builder.reset();
        let empty_sig = [0u8; 0];
        let mut ghosts = Vec::with_capacity(20);
        for &(id, pos, vel) in &entities {
            let p = Vec3::new(pos.0, pos.1, pos.2);
            let v = Vec3::new(vel.0, vel.1, vel.2);
            let sig = builder.create_vector(&empty_sig);
            ghosts.push(Ghost::create(&mut builder, &GhostArgs {
                entity_id: id,
                position: Some(&p),
                velocity: Some(&v),
                signature: Some(sig),
            }));
        }
        let gv = builder.create_vector(&ghosts);
        let msg = StateUpdate::create(&mut builder, &StateUpdateArgs { ghosts: Some(gv) });
        builder.finish(msg, None);
        fb_total_bytes += builder.finished_data().len() + 1;
    }
    let fb_duration = start.elapsed();

    let start = Instant::now();
    let mut compact_total_bytes = 0;
    for _ in 0..iterations {
        let buf = compact_encode(&entities);
        compact_total_bytes += buf.len();
    }
    let compact_duration = start.elapsed();

    let start = Instant::now();
    let sample_buf = compact_encode(&entities);
    for _ in 0..iterations {
        let _decoded = compact_decode(&sample_buf);
    }
    let compact_decode_duration = start.elapsed();

    let fb_bytes_per_entity = fb_total_bytes as f64 / (iterations as f64 * 20.0);
    let compact_bytes_per_entity = compact_total_bytes as f64 / (iterations as f64 * 20.0);
    let fb_us = fb_duration.as_micros() as f64 / (iterations as f64 * 20.0);
    let compact_us = compact_duration.as_micros() as f64 / (iterations as f64 * 20.0);

    println!("FlatBuffers encode:  {:.3} us/entity   {:.0} B/entity", fb_us, fb_bytes_per_entity);
    println!("Compact encode:      {:.3} us/entity   {:.0} B/entity", compact_us, compact_bytes_per_entity);
    println!("Compact decode:      {:.3} us/entity", compact_decode_duration.as_micros() as f64 / (iterations as f64 * 20.0));
    println!("Speedup: {:.1}x encode, {:.1}x size", fb_us / compact_us.max(0.001), fb_bytes_per_entity / compact_bytes_per_entity.max(0.001));
    println!("Bandwidth at 30 Hz for 20 entities:");
    println!("  FlatBuffers: {:.1} KB/s", fb_total_bytes as f64 / iterations as f64 * 30.0 / 1024.0);
    println!("  Compact:     {:.1} KB/s", compact_total_bytes as f64 / iterations as f64 * 30.0 / 1024.0);
}

fn benchmark_quantization_accuracy() {
    println!("\n=== Quantization Accuracy ===");
    let iterations = 100_000;

    let mut max_pos_err = 0.0f32;
    let mut total_pos_err = 0.0f64;
    let mut max_vel_err = 0.0f32;
    let mut total_vel_err = 0.0f64;
    let mut pos_errors: Vec<f32> = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let seed = i as f32 * 0.000137;
        let px = (seed * 7919.0).sin() * 12.0 + 12.0;
        let py = (seed * 6271.0).sin() * 12.5 + 7.5;
        let pz = (seed * 5387.0).sin() * 12.0;

        let qx = dequantize_pos(quantize_pos(px));
        let qy = dequantize_pos(quantize_pos(py));
        let qz = dequantize_pos(quantize_pos(pz));

        let err = ((px - qx).powi(2) + (py - qy).powi(2) + (pz - qz).powi(2)).sqrt();
        pos_errors.push(err);
        max_pos_err = max_pos_err.max(err);
        total_pos_err += err as f64;

        let vx = (seed * 3571.0).sin() * 15.0;
        let vy = (seed * 2909.0).sin() * 15.0;
        let vz = (seed * 4003.0).sin() * 15.0;

        let dvx = dequantize_vel(quantize_vel(vx));
        let dvy = dequantize_vel(quantize_vel(vy));
        let dvz = dequantize_vel(quantize_vel(vz));

        let verr = ((vx - dvx).powi(2) + (vy - dvy).powi(2) + (vz - dvz).powi(2)).sqrt();
        max_vel_err = max_vel_err.max(verr);
        total_vel_err += verr as f64;
    }

    pos_errors.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99 = pos_errors[(iterations as f64 * 0.99) as usize];

    println!("Position max error: {:.4}m  mean: {:.4}m  p99: {:.4}m", max_pos_err, total_pos_err / iterations as f64, p99);
    println!("Velocity max error: {:.4} m/s  mean: {:.4} m/s", max_vel_err, total_vel_err / iterations as f64);
}

fn benchmark_delta_encoding_hitrate() {
    println!("\n=== Delta Encoding (1000 ticks, 20 entities) ===");
    use std::collections::HashMap;

    let entity_count = 20;
    let ticks = 1000;

    let mut positions: Vec<(f32, f32, f32)> = (0..entity_count)
        .map(|i| (i as f32 * 1.2, 1.0, -(i as f32) * 0.5))
        .collect();
    let mut last_state: HashMap<u64, (i16, i16, i16)> = HashMap::new();

    let mut total_entities_sent = 0u64;
    let mut total_bytes_sent = 0u64;

    for tick in 0..ticks {
        for i in 0..entity_count {
            if i < 8 || tick % 10 == 0 {
                let seed = (tick * entity_count + i) as f32 * 0.01;
                positions[i].0 += seed.sin() * 0.05;
                positions[i].2 += seed.cos() * 0.03;
            }
        }

        let mut changed = 0usize;
        let mut batch_bytes = 3usize;
        for i in 0..entity_count {
            let qp = (quantize_pos(positions[i].0), quantize_pos(positions[i].1), quantize_pos(positions[i].2));
            if last_state.get(&(i as u64)) != Some(&qp) {
                last_state.insert(i as u64, qp);
                changed += 1;
                batch_bytes += 17;
            }
        }
        total_entities_sent += changed as u64;
        if changed > 0 {
            total_bytes_sent += batch_bytes as u64;
        }
    }

    let full_state_bytes = (3 + 17 * entity_count) * ticks;
    let avg_entities = total_entities_sent as f64 / ticks as f64;
    let avg_bytes = total_bytes_sent as f64 / ticks as f64;
    let savings = 1.0 - (total_bytes_sent as f64 / full_state_bytes as f64);

    println!("Avg entities/broadcast: {:.1} (vs {} full-state)", avg_entities, entity_count);
    println!("Avg bytes/broadcast: {:.0} B (vs {} B)", avg_bytes, 3 + 17 * entity_count);
    println!("Savings: {:.1}%", savings * 100.0);
}

fn benchmark_batch_vs_perentity_signing() {
    println!("\n=== Batch vs Per-Entity Signing ===");
    let (signing_key, _) = GhostSerializer::generate_keypair();

    let entity_payloads: Vec<[u8; 32]> = (0..20).map(|i| {
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        buf
    }).collect();

    let iterations = 10_000;

    let start = Instant::now();
    for _ in 0..iterations {
        for payload in &entity_payloads {
            let _ = signing_key.sign(payload);
        }
    }
    let per_entity_duration = start.elapsed();

    let mut batch_payload = Vec::with_capacity(640);
    for payload in &entity_payloads {
        batch_payload.extend_from_slice(payload);
    }
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = signing_key.sign(&batch_payload);
    }
    let batch_duration = start.elapsed();

    let per_entity_us = per_entity_duration.as_micros() as f64 / iterations as f64;
    let batch_us = batch_duration.as_micros() as f64 / iterations as f64;
    println!("Per-entity (20x): {:.1} us/batch", per_entity_us);
    println!("Batch (1x):       {:.1} us/batch", batch_us);
    println!("Speedup: {:.1}x", per_entity_us / batch_us.max(0.001));
}

fn benchmark_load_broadcast() {
    println!("\n--- Load Broadcasting Overhead ---");
    use zeus_common::{
        DiscoveryAnnounce, DiscoveryAnnounceArgs, DiscoveryMsg, DiscoveryMsgArgs, NodeLoad, Vec3,
        flatbuffers::FlatBufferBuilder,
    };

    let iterations = 100_000;
    let start = std::time::Instant::now();
    let mut builder = FlatBufferBuilder::new();
    let mut total_bytes = 0;

    for i in 0..iterations {
        builder.reset();
        let addr_str = "192.168.1.105:8080";
        let addr = builder.create_string(addr_str);
        let pos = Vec3::new(100.0, 200.0, 300.0);
        let load = NodeLoad::new((i % 3000) as u16, 75);

        let announce = DiscoveryAnnounce::create(
            &mut builder,
            &DiscoveryAnnounceArgs {
                node_id: i as u64,
                address: Some(addr),
                position: Some(&pos),
                load: Some(&load),
                known_node_ids: None,
                ordinal: 0,
            },
        );

        let msg = DiscoveryMsg::create(
            &mut builder,
            &DiscoveryMsgArgs {
                payload_type: zeus_common::DiscoveryPayload::DiscoveryAnnounce,
                payload: Some(announce.as_union_value()),
            },
        );

        builder.finish(msg, None);
        total_bytes += builder.finished_data().len();
    }

    let duration = start.elapsed();
    let avg_size = total_bytes as f64 / iterations as f64;

    println!(
        "Announce+Load Size:   {:.1} bytes (+4 bytes for NodeLoad)",
        avg_size
    );
    println!(
        "Serialization Time:   {:.4} µs/op",
        duration.as_micros() as f64 / iterations as f64
    );
    println!(
        "Throughput:           {:.0} load-aware announces/sec",
        iterations as f64 / duration.as_secs_f64()
    );
}
