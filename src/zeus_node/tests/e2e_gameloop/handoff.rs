use super::helpers::*;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::sleep;
use zeus_client::ZeusClient;
use zeus_node::engine::ZeusConfig;
use zeus_node::game_loop::GameLoop;

#[tokio::test]
async fn test_multinode_entity_handoff() {
    let config_node0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 10.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config_node0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config_node1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 20.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let world1 = TestWorld::new();
    let mut node1 = GameLoop::new(config_node1, world1).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let initial_conn_0 = node0.engine.connections.len();
    let initial_conn_1 = node1.engine.connections.len();
    assert!(
        initial_conn_0 >= 1 || initial_conn_1 >= 1,
        "Nodes should be connected to each other (node0 conns: {}, node1 conns: {})",
        initial_conn_0,
        initial_conn_1
    );

    node0.world.spawn_local(42, (9.0, 0.0, 0.0), (5.0, 0.0, 0.0));

    for i in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();

        if let Some(e) = node0.engine.node.manager.get_entity(42) {
            if i % 10 == 0 {
                println!(
                    "[tick {}] Entity 42 on node0: pos=({:.1},{:.1},{:.1}) state={:?}",
                    i, e.pos.0, e.pos.1, e.pos.2, e.state
                );
            }
        }

        let n1_has = node1.engine.node.manager.get_entity(42).is_some();
        if n1_has {
            println!("[tick {}] Entity 42 arrived on node1!", i);
            break;
        }

        sleep(Duration::from_millis(5)).await;
    }

    let entity_on_node0 = node0.engine.node.manager.get_entity(42);
    let entity_on_node1 = node1.engine.node.manager.get_entity(42);

    let departed_from_0 = entity_on_node0
        .map(|e| {
            e.state == zeus_node::entity_manager::AuthorityState::Remote
                || e.state == zeus_node::entity_manager::AuthorityState::HandoffOut
        })
        .unwrap_or(false);
    let arrived_on_1 = entity_on_node1.is_some();

    assert!(
        departed_from_0 || arrived_on_1,
        "Entity 42 should have been handed off: node0 departed={}, node1 arrived={} (node0 entity: {:?}, node1 entity: {:?})",
        departed_from_0,
        arrived_on_1,
        entity_on_node0.map(|e| format!("state={:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
        entity_on_node1.map(|e| format!("state={:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
    );
}

#[tokio::test]
async fn test_multinode_client_receives_cross_node_entities() {
    let config_node0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 5.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let world0 = TestWorld::new();
    let mut node0 = GameLoop::new(config_node0, world0).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    node0.world.spawn_local(77, (3.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node0.world.spawn_local(78, (6.0, 0.0, 0.0), (0.0, 0.0, 0.0));

    let mut client = ZeusClient::new(5001).unwrap();
    client.connect(node0_addr).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let conn = client.connection().unwrap();
    let mut seen_ids = HashSet::new();
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(10), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        seen_ids.insert(id);
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        seen_ids.contains(&77),
        "Client should see entity 77 from node0. Seen: {:?}",
        seen_ids
    );
    assert!(
        seen_ids.contains(&78),
        "Client should see entity 78 from node0. Seen: {:?}",
        seen_ids
    );
}

#[tokio::test]
async fn test_e2e_handoff_with_gossip() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 10.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let mut client = ZeusClient::new(9001).unwrap();
    client.connect(node0_addr).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    node0.world.spawn_local(42, (9.0, 1.0, 0.0), (5.0, 0.0, 0.0));

    let mut client_saw_42 = false;
    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let conn = client.connection().unwrap();
        match tokio::time::timeout(Duration::from_millis(2), conn.read_datagram()).await {
            Ok(Ok(data)) => {
                if !data.is_empty() && data[0] == 0xCC {
                    for (id, _, _) in parse_0xcc_datagram(&data) {
                        if id == 42 {
                            client_saw_42 = true;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    assert!(client_saw_42, "Client should see entity 42 through handoff (either from node0 directly or via gossip)");
}

#[tokio::test]
async fn test_handoff_with_preexisting_gossip_proxy() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 5.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(42, (3.0, 0.0, 0.0), (10.0, 0.0, 0.0));

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let gossip_proxy = node1.engine.remote_entity_states.contains_key(&42);
    assert!(gossip_proxy, "Node 1 should have entity 42 as gossip proxy before handoff");

    for _ in 0..100 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let on_node1 = node1.engine.node.manager.get_entity(42);
    let on_node0 = node0.engine.node.manager.get_entity(42);
    let handed_off = on_node1.is_some()
        || on_node0
            .map(|e| e.state == zeus_node::entity_manager::AuthorityState::HandoffOut
                   || e.state == zeus_node::entity_manager::AuthorityState::Remote)
            .unwrap_or(false);
    assert!(
        handed_off,
        "Entity 42 should have been handed off after drifting past boundary. node0={:?} node1={:?}",
        on_node0.map(|e| format!("{:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
        on_node1.map(|e| format!("{:?} pos=({:.1},{:.1},{:.1})", e.state, e.pos.0, e.pos.1, e.pos.2)),
    );

    if let Some(e) = on_node1 {
        assert!(
            e.pos.0 > 0.0 && e.pos.0 < 200.0,
            "Entity 42 position on Node 1 should be reasonable, got x={:.1}",
            e.pos.0
        );
    }
}

#[tokio::test]
async fn test_boundary_shift_position_continuity() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for i in 0..5 {
        let x = 3.0 + (i as f32) * 12.0;
        node0.world.spawn_local(100 + i, (x, 0.0, 0.0), (0.5, 0.0, 0.0));
    }

    let mut prev_positions: HashMap<u64, f32> = HashMap::new();
    let mut max_delta: f32 = 0.0;

    for tick in 0..120 {
        if tick == 30 {
            node0.set_boundary(15.0);
        }

        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        for id in 100..105u64 {
            let pos_x = node0.engine.node.manager.get_entity(id)
                .map(|e| e.pos.0)
                .or_else(|| node1.engine.node.manager.get_entity(id).map(|e| e.pos.0));

            if let Some(x) = pos_x {
                if let Some(prev_x) = prev_positions.get(&id) {
                    let delta = (x - prev_x).abs();
                    if delta > max_delta {
                        max_delta = delta;
                    }
                }
                prev_positions.insert(id, x);
            }
        }
    }

    assert!(
        max_delta < 5.0,
        "Max position delta between ticks should be < 5.0, got {:.2}",
        max_delta
    );
}

#[tokio::test]
async fn test_cross_node_collision_after_handoff() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(10, (5.0, 0.0, 0.0), (0.0, 0.0, 0.0));
    node0.world.spawn_local(20, (9.0, 0.0, 0.0), (2.0, 0.0, 0.0));

    for _ in 0..20 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.set_boundary(7.0);

    for _ in 0..60 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let entity_20_on_node1 = node1.engine.node.manager.get_entity(20);
    let entity_20_state_on_node0 = node0.engine.node.manager.get_entity(20);
    let entity_20_migrated = entity_20_on_node1.is_some()
        || entity_20_state_on_node0
            .map(|e| e.state != zeus_node::entity_manager::AuthorityState::Local)
            .unwrap_or(false);

    let entity_10_on_node0 = node0.engine.node.manager.get_entity(10);
    assert!(
        entity_10_on_node0.is_some(),
        "Entity 10 should remain on node0"
    );

    if entity_20_migrated {
        let node0_sees_20 = node0.engine.remote_entity_states.contains_key(&20)
            || node0.engine.node.manager.get_entity(20).is_some();
        assert!(
            node0_sees_20,
            "After entity 20 migrated to node1, node0 should still see it via gossip or manager"
        );
    }

    let entity_10_exists = node0.engine.node.manager.get_entity(10).is_some()
        || node1.engine.node.manager.get_entity(10).is_some();
    let entity_20_exists = node0.engine.node.manager.get_entity(20).is_some()
        || node1.engine.node.manager.get_entity(20).is_some();
    assert!(entity_10_exists, "Entity 10 should still exist somewhere");
    assert!(entity_20_exists, "Entity 20 should still exist somewhere");
}

#[tokio::test]
async fn test_3node_boundary_convergence() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 24.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 2,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..400 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(3)).await;
    }

    let count0 = node0.engine.discovery.total_node_count();
    let count1 = node1.engine.discovery.total_node_count();
    let count2 = node2.engine.discovery.total_node_count();

    assert!(count1 >= 2, "Node 1 (middle) should know at least 2 nodes, got {}", count1);

    let any_leaf_converged = count0 >= 2 || count2 >= 2;
    assert!(any_leaf_converged,
        "At least one leaf node should know 2+ nodes after gossip convergence (n0={}, n2={})",
        count0, count2
    );
}

#[tokio::test]
async fn test_leftward_handoff() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 12.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 8.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node1.world.spawn_local(50, (7.0, 0.0, 0.0), (-3.0, 0.0, 0.0));
    node1.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 50,
        pos: (7.0, 0.0, 0.0),
        vel: (-3.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let on_node0 = node0.engine.node.manager.get_entity(50)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);
    let on_node1_local = node1.engine.node.manager.get_entity(50)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);

    let entity_exists = node0.engine.node.manager.get_entity(50).is_some()
        || node1.engine.node.manager.get_entity(50).is_some();
    assert!(entity_exists, "Entity 50 should still exist on at least one node");

    if on_node0 {
        assert!(!on_node1_local, "Entity should not be Local on both nodes");
    }
}

#[tokio::test]
async fn test_no_dual_ownership_3node_targeted() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 8.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 16.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 8.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 2,
        lower_boundary: 16.0,
        cell: None,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..15 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(77, (6.0, 0.0, 0.0), (5.0, 0.0, 0.0));
    node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 77,
        pos: (6.0, 0.0, 0.0),
        vel: (5.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..80 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let local_on_0 = node0.engine.node.manager.get_entity(77)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);
    let local_on_1 = node1.engine.node.manager.get_entity(77)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);
    let local_on_2 = node2.engine.node.manager.get_entity(77)
        .map(|e| e.state == zeus_node::entity_manager::AuthorityState::Local)
        .unwrap_or(false);

    let local_count = [local_on_0, local_on_1, local_on_2].iter().filter(|&&x| x).count();
    assert!(
        local_count <= 1,
        "Entity 77 should be Local on at most 1 node, found Local on {} nodes (n0={}, n1={}, n2={})",
        local_count, local_on_0, local_on_1, local_on_2
    );

    let exists = node0.engine.node.manager.get_entity(77).is_some()
        || node1.engine.node.manager.get_entity(77).is_some()
        || node2.engine.node.manager.get_entity(77).is_some();
    assert!(exists, "Entity 77 should exist on at least one node");
}

#[tokio::test]
async fn test_entity_conservation_bidirectional() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 12.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 24.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 12.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();

    sleep(Duration::from_millis(100)).await;

    for _ in 0..10 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    node0.world.spawn_local(1, (10.0, 0.0, 0.0), (3.0, 0.0, 0.0));
    node0.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1,
        pos: (10.0, 0.0, 0.0),
        vel: (3.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    node1.world.spawn_local(2, (13.0, 0.0, 0.0), (-3.0, 0.0, 0.0));
    node1.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 2,
        pos: (13.0, 0.0, 0.0),
        vel: (-3.0, 0.0, 0.0),
        state: zeus_node::entity_manager::AuthorityState::Local,
        verifying_key: None,
    });

    for _ in 0..100 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let e1_exists = node0.engine.node.manager.get_entity(1).is_some()
        || node1.engine.node.manager.get_entity(1).is_some();
    let e2_exists = node0.engine.node.manager.get_entity(2).is_some()
        || node1.engine.node.manager.get_entity(2).is_some();

    assert!(e1_exists, "Entity 1 should be conserved across nodes");
    assert!(e2_exists, "Entity 2 should be conserved across nodes");
}

#[tokio::test]
async fn test_membership_convergence_daisy_chain() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let _node0_id = node0.engine.discovery.local_id;

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_addr = node1.engine.endpoint.local_addr().unwrap();
    let node1_id = node1.engine.discovery.local_id;

    let config2 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node1_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 2,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node2 = GameLoop::new(config2, TestWorld::new()).await.unwrap();
    let _node2_id = node2.engine.discovery.local_id;

    sleep(Duration::from_millis(100)).await;

    for _ in 0..400 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        node2.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(3)).await;
    }

    let n0_knows = &node0.engine.discovery.known_node_ids;
    let n1_knows = &node1.engine.discovery.known_node_ids;
    let n2_knows = &node2.engine.discovery.known_node_ids;

    assert!(
        n1_knows.len() >= 2,
        "Node 1 (middle, directly connected to both) should know at least 2 nodes. Known: {:?}",
        n1_knows
    );

    let n0_n1_overlap = n0_knows.contains(&node1_id);
    let n2_n1_overlap = n2_knows.contains(&node1_id);
    assert!(n0_n1_overlap, "Node 0 should at least know Node 1");
    assert!(n2_n1_overlap, "Node 2 should at least know Node 1");
}

#[tokio::test]
async fn test_ed25519_verifying_key_exchange() {
    let config0 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 2.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node0 = GameLoop::new(config0, TestWorld::new()).await.unwrap();
    let node0_addr = node0.engine.endpoint.local_addr().unwrap();
    let node0_id = node0.engine.discovery.local_id;
    let node0_vk = node0.engine.verifying_key;

    let config1 = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![node0_addr],
        boundary: 100.0,
        margin: 2.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: None,
    };
    let mut node1 = GameLoop::new(config1, TestWorld::new()).await.unwrap();
    let node1_id = node1.engine.discovery.local_id;
    let node1_vk = node1.engine.verifying_key;

    sleep(Duration::from_millis(100)).await;

    for _ in 0..200 {
        node0.tick(0.016).await.unwrap();
        node1.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    let node1_has_node0_vk = node1.engine.peer_verifying_keys.get(&node0_id);
    assert!(
        node1_has_node0_vk.is_some(),
        "Node 1 should have Node 0's verifying key after discovery exchange"
    );
    assert_eq!(
        node1_has_node0_vk.unwrap().as_bytes(),
        node0_vk.as_bytes(),
        "Exchanged verifying key should match"
    );

    let node0_has_node1_vk = node0.engine.peer_verifying_keys.get(&node1_id);
    assert!(
        node0_has_node1_vk.is_some(),
        "Node 0 should have Node 1's verifying key after discovery exchange"
    );
    assert_eq!(
        node0_has_node1_vk.unwrap().as_bytes(),
        node1_vk.as_bytes(),
        "Exchanged verifying key should match"
    );
}

#[tokio::test]
async fn test_handoff_uses_handoff_in_then_commit_promotes() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;
    use zeus_node::node_actor::NodeActor;

    let cell = Cell::new(0.0, 25.0, 0.0, 25.0, 0.0, 25.0);
    let mut receiver = NodeActor::new_3d(cell, 1.0);

    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let pos = zeus_common::Vec3::new(12.0, 12.0, 12.0);
    let vel = zeus_common::Vec3::new(1.0, 0.5, -0.3);
    let sig = builder.create_vector(&[0u8; 64]);
    let ghost = zeus_common::Ghost::create(
        &mut builder,
        &zeus_common::GhostArgs {
            entity_id: 42,
            position: Some(&pos),
            velocity: Some(&vel),
            signature: Some(sig),
        },
    );
    let msg = zeus_common::HandoffMsg::create(
        &mut builder,
        &zeus_common::HandoffMsgArgs {
            entity_id: 42,
            type_: zeus_common::HandoffType::Offer,
            state: Some(ghost),
        },
    );
    builder.finish(msg, None);
    let buf = builder.finished_data().to_vec();
    let msg = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf).unwrap();
    receiver.handle_handoff_msg(msg, None);

    let e = receiver.manager.get_entity(42).unwrap();
    assert_eq!(e.state, AuthorityState::HandoffIn, "Offer should create entity as HandoffIn");
    assert_eq!(receiver.outgoing_messages.len(), 1, "Should queue Ack");
    assert_eq!(receiver.outgoing_messages[0].1, zeus_common::HandoffType::Ack);

    let mut builder2 = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let commit_msg = zeus_common::HandoffMsg::create(
        &mut builder2,
        &zeus_common::HandoffMsgArgs {
            entity_id: 42,
            type_: zeus_common::HandoffType::Commit,
            state: None,
        },
    );
    builder2.finish(commit_msg, None);
    let buf2 = builder2.finished_data().to_vec();
    let commit = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf2).unwrap();
    receiver.handle_handoff_msg(commit, None);

    let e2 = receiver.manager.get_entity(42).unwrap();
    assert_eq!(e2.state, AuthorityState::Local, "Commit should promote to Local");
}

#[tokio::test]
async fn test_entity_arrives_as_dynamic_not_kinematic() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell_a = Cell::new(0.0, 50.0, 0.0, 25.0, 0.0, 50.0);
    let cell_b = Cell::new(0.0, 50.0, 25.0, 50.0, 0.0, 50.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 50.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell_a.clone()),
    };
    let mut world_a = TestWorld::new();
    world_a.spawn_local(1, (25.0, 24.0, 25.0), (0.0, 10.0, 0.0));
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (25.0, 24.0, 25.0), vel: (0.0, 10.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 50.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let world_b = TestWorld::new();
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for _ in 0..80 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let b_arrived = &node_b.world.arrived;
    assert!(b_arrived.contains(&1), "Entity 1 should arrive on node B via EntityArrived (for dynamic body creation). Arrived: {:?}", b_arrived);

    assert!(node_b.world.local_ids.contains(&1),
        "Entity 1 should be in locally_simulated_ids (dynamic body). local_ids: {:?}", node_b.world.local_ids);
}

#[tokio::test]
async fn test_handoff_in_entity_not_moved_by_entity_manager() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity, EntityManager};

    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut em = EntityManager::new_3d(cell, 1.0);

    em.add_entity(Entity {
        id: 1, pos: (10.0, 10.0, 10.0), vel: (100.0, 100.0, 100.0),
        state: AuthorityState::HandoffIn, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 2, pos: (10.0, 10.0, 10.0), vel: (100.0, 100.0, 100.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    em.update(1.0);

    let e1 = em.get_entity(1).unwrap();
    assert!((e1.pos.0 - 10.0).abs() < 0.01, "HandoffIn entity should NOT be moved. pos.x={}", e1.pos.0);

    let e2 = em.get_entity(2).unwrap();
    assert!((e2.pos.0 - 10.0).abs() < 0.01, "entity_manager.update no longer moves entities (physics is source of truth). pos.x={}", e2.pos.0);
}

#[tokio::test]
async fn test_resync_creates_dynamic_not_kinematic() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(cell.clone()),
    };
    let world = TestWorld::new();
    let mut gl = GameLoop::new(config, world).await.unwrap();

    gl.engine.node.manager.add_entity(Entity {
        id: 42, pos: (50.0, 50.0, 50.0), vel: (1.0, 2.0, 3.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    gl.tick(0.016).await.unwrap();

    assert!(gl.world.local_ids.contains(&42),
        "Re-sync should add entity via on_entity_arrived, placing it in local_ids (dynamic)");
    assert!(gl.world.states.contains_key(&42),
        "Entity state should exist after re-sync");
}

#[tokio::test]
async fn test_handoff_out_not_moved_by_entity_manager_update() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity, EntityManager};

    let cell = Cell::new(0.0, 50.0, 0.0, 50.0, 0.0, 50.0);
    let mut em = EntityManager::new_3d(cell, 1.0);

    em.add_entity(Entity {
        id: 1, pos: (10.0, 30.0, 10.0), vel: (0.0, -100.0, 0.0),
        state: AuthorityState::HandoffOut, verifying_key: None,
    });
    em.add_entity(Entity {
        id: 2, pos: (10.0, 30.0, 10.0), vel: (0.0, -100.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    em.update(1.0);

    let e1 = em.get_entity(1).unwrap();
    assert!((e1.pos.1 - 30.0).abs() < 0.01,
        "HandoffOut entity should NOT be moved by update(). y={}", e1.pos.1);

    let e2 = em.get_entity(2).unwrap();
    assert!((e2.pos.1 - 30.0).abs() < 0.01,
        "entity_manager.update no longer moves entities (physics is source of truth). y={}", e2.pos.1);
}

#[tokio::test]
async fn test_handoff_retry_fires_immediately_after_eviction() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let keep_cell = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let config = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world = TestWorld::new();
    world.spawn_local(1, (50.0, 70.0, 50.0), (0.0, 0.0, 0.0));
    let mut gl = GameLoop::new(config, world).await.unwrap();
    gl.engine.node.manager.add_entity(Entity {
        id: 1, pos: (50.0, 70.0, 50.0), vel: (0.0, 0.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    gl.set_cell(keep_cell);
    gl.evict_out_of_cell_from_physics();

    assert_eq!(gl.engine.handoff_retry_counter, 127,
        "After eviction, retry counter should be set to 127 so next tick triggers offers");
}

#[tokio::test]
async fn test_boundary_entity_does_not_pingpong() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let full_cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let keep_cell = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let new_cell = Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0);

    let config_a = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 0,
        lower_boundary: 0.0,
        cell: Some(full_cell.clone()),
    };
    let mut world_a = TestWorld::new();
    let id_boundary = 1u64;
    world_a.spawn_local(id_boundary, (50.0, 50.2, 50.0), (0.0, -0.5, 0.0));
    let mut node_a = GameLoop::new(config_a, world_a).await.unwrap();
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    node_a.engine.node.manager.add_entity(Entity {
        id: id_boundary, pos: (50.0, 50.2, 50.0), vel: (0.0, -0.5, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: vec![addr_a],
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(new_cell.clone()),
    };
    let mut node_b = GameLoop::new(config_b, TestWorld::new()).await.unwrap();

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;
    }

    node_a.set_cell(keep_cell.clone());
    node_a.evict_out_of_cell_from_physics();

    let mut handoff_transitions = 0u32;
    let mut last_owner = 'A';

    for _ in 0..300 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let on_a = node_a.engine.node.manager.get_entity(id_boundary)
            .is_some_and(|e| e.state == AuthorityState::Local);
        let on_b = node_b.engine.node.manager.get_entity(id_boundary)
            .is_some_and(|e| e.state == AuthorityState::Local);

        let current = if on_a { 'A' } else if on_b { 'B' } else { last_owner };
        if current != last_owner {
            handoff_transitions += 1;
            last_owner = current;
        }
    }

    assert!(handoff_transitions <= 4,
        "Entity near boundary should not ping-pong. Transitions: {} (expected <= 4)", handoff_transitions);
}

#[tokio::test]
async fn test_clamp_inside_prevents_immediate_rehandoff() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::{AuthorityState, Entity};

    let _cell_a = Cell::new(0.0, 100.0, 0.0, 50.0, 0.0, 100.0);
    let cell_b = Cell::new(0.0, 100.0, 50.0, 100.0, 0.0, 100.0);

    let config_b = ZeusConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seed_addrs: Vec::new(),
        boundary: 100.0,
        margin: 1.0,
        ordinal: 1,
        lower_boundary: 0.0,
        cell: Some(cell_b.clone()),
    };
    let mut world_b = TestWorld::new();
    world_b.spawn_local(42, (50.0, 50.5, 50.0), (0.0, -2.0, 0.0));
    let mut node_b = GameLoop::new(config_b, world_b).await.unwrap();

    node_b.engine.node.manager.add_entity(Entity {
        id: 42, pos: (50.0, 50.5, 50.0), vel: (0.0, -2.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let mut exit_detected = false;
    for _ in 0..10 {
        node_b.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(5)).await;

        let e = node_b.engine.node.manager.get_entity(42);
        if e.is_some_and(|e| e.state == AuthorityState::HandoffOut) {
            exit_detected = true;
            break;
        }
    }

    assert!(!exit_detected,
        "Entity at y=50.5 with vel=-2 should NOT immediately exit cell [50,100] due to hysteresis margin");
}

#[tokio::test]
async fn test_3node_commit_targeted_to_single_peer() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;

    let cell_a = Cell::new(0.0, 100.0, 0.0, 33.0, 0.0, 100.0);
    let cell_b = Cell::new(0.0, 100.0, 33.0, 66.0, 0.0, 100.0);
    let cell_c = Cell::new(0.0, 100.0, 66.0, 100.0, 0.0, 100.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    node_a.world.spawn_local(1, (50.0, 32.0, 50.0), (0.0, 5.0, 0.0));
    node_a.engine.node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 1, pos: (50.0, 32.0, 50.0), vel: (0.0, 5.0, 0.0),
        state: AuthorityState::Local, verifying_key: None,
    });

    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;
    let addr_b = node_b.engine.endpoint.local_addr().unwrap();
    let mut node_c = make_node(cell_c.clone(), vec![addr_a, addr_b]).await;

    for _ in 0..20 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    for _ in 0..100 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let b_local = node_b.engine.node.manager.get_entity(1).is_some_and(|e| e.state == AuthorityState::Local);
    let c_local = node_c.engine.node.manager.get_entity(1).is_some_and(|e| e.state == AuthorityState::Local);

    assert!(b_local || c_local, "Entity 1 should be Local on node B or C");
    assert!(!(b_local && c_local), "Entity 1 must NOT be Local on both B and C (dual ownership)");
}

#[tokio::test]
async fn test_3node_losing_peer_handoffin_removed() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;
    use zeus_node::node_actor::NodeActor;

    let cell_b = Cell::new(0.0, 100.0, 33.0, 66.0, 0.0, 100.0);
    let cell_c = Cell::new(0.0, 100.0, 66.0, 100.0, 0.0, 100.0);

    let mut node_b = NodeActor::new_3d(cell_b.clone(), 1.0);
    let mut node_c = NodeActor::new_3d(cell_c.clone(), 1.0);

    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let pos = zeus_common::Vec3::new(50.0, 50.0, 50.0);
    let vel = zeus_common::Vec3::new(0.0, 0.0, 0.0);
    let sig = builder.create_vector(&[0u8; 64]);
    let ghost = zeus_common::Ghost::create(&mut builder, &zeus_common::GhostArgs {
        entity_id: 42, position: Some(&pos), velocity: Some(&vel), signature: Some(sig),
    });
    let msg = zeus_common::HandoffMsg::create(&mut builder, &zeus_common::HandoffMsgArgs {
        entity_id: 42, type_: zeus_common::HandoffType::Offer, state: Some(ghost),
    });
    builder.finish(msg, None);
    let buf = builder.finished_data().to_vec();

    let msg_b = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf).unwrap();
    node_b.handle_handoff_msg(msg_b, None);
    assert_eq!(node_b.manager.get_entity(42).unwrap().state, AuthorityState::HandoffIn);

    let msg_c = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf).unwrap();
    node_c.handle_handoff_msg(msg_c, None);
    assert!(node_c.manager.get_entity(42).is_none(), "Node C should reject offer (pos outside cell_c)");
}

#[tokio::test]
async fn test_3node_cell_exchange_auto() {
    use zeus_node::cell::Cell;

    let cell_a = Cell::new(0.0, 100.0, 0.0, 33.0, 0.0, 100.0);
    let cell_b = Cell::new(0.0, 100.0, 33.0, 66.0, 0.0, 100.0);
    let cell_c = Cell::new(0.0, 100.0, 66.0, 100.0, 0.0, 100.0);

    let mut node_a = make_node(cell_a.clone(), vec![]).await;
    let addr_a = node_a.engine.endpoint.local_addr().unwrap();
    let mut node_b = make_node(cell_b.clone(), vec![addr_a]).await;
    let addr_b = node_b.engine.endpoint.local_addr().unwrap();
    let mut node_c = make_node(cell_c.clone(), vec![addr_a, addr_b]).await;

    for _ in 0..30 {
        node_a.tick(0.016).await.unwrap();
        node_b.tick(0.016).await.unwrap();
        node_c.tick(0.016).await.unwrap();
        sleep(Duration::from_millis(10)).await;
    }

    let b_peer = node_a.engine.discovery.find_peer_containing((50.0, 50.0, 50.0));
    assert!(b_peer.is_some(), "Node A should know about Node B's cell via 0xD6 exchange");

    let c_peer = node_a.engine.discovery.find_peer_containing((50.0, 80.0, 50.0));
    assert!(c_peer.is_some(), "Node A should know about Node C's cell via 0xD6 exchange");
}

#[tokio::test]
async fn test_reject_duplicate_offer_handoffin() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;
    use zeus_node::node_actor::NodeActor;

    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut node = NodeActor::new_3d(cell, 1.0);

    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let pos = zeus_common::Vec3::new(50.0, 50.0, 50.0);
    let vel = zeus_common::Vec3::new(1.0, 0.0, 0.0);
    let sig = builder.create_vector(&[0u8; 64]);
    let ghost = zeus_common::Ghost::create(&mut builder, &zeus_common::GhostArgs {
        entity_id: 99, position: Some(&pos), velocity: Some(&vel), signature: Some(sig),
    });
    let msg = zeus_common::HandoffMsg::create(&mut builder, &zeus_common::HandoffMsgArgs {
        entity_id: 99, type_: zeus_common::HandoffType::Offer, state: Some(ghost),
    });
    builder.finish(msg, None);
    let buf = builder.finished_data().to_vec();

    let msg1 = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf).unwrap();
    node.handle_handoff_msg(msg1, None);
    assert_eq!(node.manager.get_entity(99).unwrap().state, AuthorityState::HandoffIn);
    assert_eq!(node.outgoing_messages.len(), 1);

    let msg2 = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf).unwrap();
    node.handle_handoff_msg(msg2, None);
    assert_eq!(node.outgoing_messages.len(), 2, "Duplicate offer for HandoffIn should re-send Ack for reliability");
}

#[tokio::test]
async fn test_targeted_commit_has_address() {
    use zeus_node::cell::Cell;
    use zeus_node::entity_manager::AuthorityState;
    use zeus_node::node_actor::NodeActor;

    let cell = Cell::new(0.0, 100.0, 0.0, 100.0, 0.0, 100.0);
    let mut node = NodeActor::new_3d(cell, 1.0);
    node.manager.add_entity(zeus_node::entity_manager::Entity {
        id: 42,
        pos: (50.0, 50.0, 50.0),
        vel: (0.0, 0.0, 0.0),
        state: AuthorityState::HandoffOut,
        verifying_key: None,
    });

    let fake_addr: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();

    let mut builder = zeus_common::flatbuffers::FlatBufferBuilder::new();
    let msg = zeus_common::HandoffMsg::create(&mut builder, &zeus_common::HandoffMsgArgs {
        entity_id: 42, type_: zeus_common::HandoffType::Ack, state: None,
    });
    builder.finish(msg, None);
    let buf = builder.finished_data().to_vec();
    let ack = zeus_common::flatbuffers::root::<zeus_common::HandoffMsg>(&buf).unwrap();

    node.handle_handoff_msg(ack, Some(fake_addr));

    assert_eq!(node.manager.get_entity(42).unwrap().state, AuthorityState::Remote);
    assert_eq!(node.outgoing_messages.len(), 1);
    let (id, msg_type, target) = &node.outgoing_messages[0];
    assert_eq!(*id, 42);
    assert_eq!(*msg_type, zeus_common::HandoffType::Commit);
    assert_eq!(*target, Some(fake_addr), "Commit should be targeted to the Ack sender's address");
}
