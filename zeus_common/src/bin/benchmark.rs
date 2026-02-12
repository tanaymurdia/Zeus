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
