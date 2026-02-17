use super::helpers::*;
use zeus_node::entity_manager::AuthorityState;
use zeus_node::game_loop::GameLoop;

#[tokio::test]
async fn test_scale_up_then_down_full_lifecycle() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 30, (12.0, 12.0, 0.0), 1);
    nodes.push(node0);
    tick_all(&mut nodes, 5).await;

    let total_spawned = 30usize;
    assert_eq!(total_local(&nodes), total_spawned);

    do_split(&mut nodes, 0).await;
    let after_scale_up = total_local(&nodes);
    assert!(after_scale_up >= total_spawned - 2, "Scale-up: {} vs {} | {}", after_scale_up, total_spawned, entity_state_summary(&nodes));
    assert_single_ownership(&nodes, "scale-up");

    let node0_local: Vec<u64> = nodes[0].engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id).collect();
    let remove_count = node0_local.len().min(10);
    for id in &node0_local[..remove_count] {
        nodes[0].world.drones.remove(id);
        nodes[0].world.local_ids.remove(id);
        nodes[0].engine.node.manager.remove_entity(*id);
    }

    let node1_local: Vec<u64> = nodes[1].engine.node.manager.entities.iter()
        .filter(|(_, e)| e.state == AuthorityState::Local)
        .map(|(id, _)| *id).collect();
    let remove_count2 = node1_local.len().min(10);
    for id in &node1_local[..remove_count2] {
        nodes[1].world.drones.remove(id);
        nodes[1].world.local_ids.remove(id);
        nodes[1].engine.node.manager.remove_entity(*id);
    }

    let remaining = total_local(&nodes);
    let expected_remaining = after_scale_up - remove_count - remove_count2;
    assert_eq!(remaining, expected_remaining);

    tick_all(&mut nodes, 200).await;
    let final_count = total_local(&nodes);
    assert_eq!(final_count, expected_remaining, "After settle: {} vs {}", final_count, expected_remaining);
    assert_single_ownership(&nodes, "post-removal");
}

#[tokio::test]
async fn test_rapid_spawn_despawn_cycles_no_entity_leak() {
    let full_cell = WORLD.clone();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();

    let mut running_total: usize = 0;
    let mut next_id: u64 = 1;

    for cycle in 0..5 {
        let batch_size = 10;
        spawn_stationary(&mut node0, batch_size, (12.0, 12.0, (cycle as f32) * 2.0 - 4.0), next_id);
        next_id += batch_size as u64;
        running_total += batch_size;

        for _ in 0..20 { node0.tick(DT).await.unwrap(); }

        let local = local_count_for_single(&node0);
        assert_eq!(local, running_total, "Cycle {} spawn: {} vs {}", cycle, running_total, local);

        let remove_n = 5.min(running_total);
        remove_entities(&mut node0, remove_n);
        running_total -= remove_n;

        for _ in 0..20 { node0.tick(DT).await.unwrap(); }

        let local_after = local_count_for_single(&node0);
        assert_eq!(local_after, running_total, "Cycle {} despawn: {} vs {}", cycle, running_total, local_after);
    }

    let physics = node0.world.local_ids.len();
    let manager = local_count_for_single(&node0);
    assert_eq!(physics, manager, "Physics ({}) vs manager ({})", physics, manager);
}

#[tokio::test]
async fn test_scale_up_scale_down_round_trip() {
    let full_cell = WORLD.clone();
    let mut nodes: Vec<GameLoop<BoundedPhysicsWorld>> = Vec::new();
    let mut node0 = GameLoop::new(make_config(full_cell.clone(), vec![]), new_world()).await.unwrap();
    spawn_stationary(&mut node0, 40, (12.0, 12.0, 0.0), 1);
    nodes.push(node0);
    tick_all(&mut nodes, 10).await;

    let total_spawned = 40usize;

    do_split(&mut nodes, 0).await;
    let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
    do_split(&mut nodes, biggest).await;
    let biggest = (0..nodes.len()).max_by_key(|i| local_count_for(&nodes[*i])).unwrap();
    do_split(&mut nodes, biggest).await;

    let after_up = total_local(&nodes);
    assert!(after_up >= total_spawned - 6, "Scaled to {} nodes: ≥{}, got {}", nodes.len(), total_spawned - 6, after_up);
    assert_single_ownership(&nodes, "scaled-up");

    for (i, node) in nodes.iter().enumerate() {
        let local = local_count_for(node);
        if local > 0 {
            let physics = node.world.local_ids.len();
            assert!((local as i64 - physics as i64).abs() <= 2, "Node {}: mgr={} vs phys={}", i, local, physics);
        }
    }
}

fn local_count_for_single(node: &GameLoop<BoundedPhysicsWorld>) -> usize {
    node.engine.node.manager.entities.values()
        .filter(|e| e.state == AuthorityState::Local).count()
}
