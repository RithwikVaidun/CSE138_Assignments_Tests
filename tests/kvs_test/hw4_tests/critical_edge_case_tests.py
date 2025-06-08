# critical_edge_case_tests.py
import time
import threading
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_key_assignment_consistency(conductor: ClusterConductor, fx: KvsFixture):
    """Same key always maps to same shard across all nodes."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Key Assignment Consistency")
    
    # Create 3-shard setup
    view = {
        "ConsistentA": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "ConsistentB": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "ConsistentC": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    import requests
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Test key assignment consistency
    test_keys = [f"consistency_key_{i:03d}" for i in range(30)]
    
    # Put keys and record which shard each ends up in
    shard_assignments = {}
    
    for key in test_keys:
        client.put(nodes[0], key, f"value_{key}")
    
    time.sleep(2)
    
    # Check each key's assignment from each shard's perspective
    for key in test_keys:
        found_shards = []
        
        # Check which shards have this key
        for shard_idx in [0, 2, 4]:  # One node from each shard
            result = client.get_all(nodes[shard_idx])
            if result["ok"] and key in result["values"]:
                shard_name = ["ConsistentA", "ConsistentB", "ConsistentC"][shard_idx // 2]
                found_shards.append(shard_name)
        
        # Each key should be in exactly one shard
        assert len(found_shards) == 1, f"Key {key} found in {len(found_shards)} shards: {found_shards}"
        shard_assignments[key] = found_shards[0]
    
    # Verify assignment is deterministic by checking from different nodes
    for key in test_keys[:5]:  # Sample check
        for node_idx in range(6):
            get_response = client.get(nodes[node_idx], key)
            assert get_response["ok"], f"Key {key} should be accessible from any node"
    
    return True, "OK"

def test_deep_cross_shard_causal_chains(conductor: ClusterConductor, fx: KvsFixture):
    """Long causal chains across multiple shards."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Deep Cross-Shard Causal Chains")
    
    view = {
        "ChainA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                   {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "ChainB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                   {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}],
        "ChainC": [{"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
                   {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}]
    }
    
    import requests
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Create a long causal chain: A1→B1→C1→A2→B2→C2→A3
    chain_ops = [
        ("chain_a1", "value_a1", 0),  # Shard A
        ("chain_b1", "value_b1", 2),  # Shard B  
        ("chain_c1", "value_c1", 4),  # Shard C
        ("chain_a2", "value_a2", 1),  # Shard A
        ("chain_b2", "value_b2", 3),  # Shard B
        ("chain_c2", "value_c2", 5),  # Shard C
        ("chain_a3", "value_a3", 0),  # Shard A
    ]
    
    # Build causal chain by reading previous before writing next
    for i, (key, value, node_idx) in enumerate(chain_ops):
        if i > 0:
            # Read previous key to establish causality
            prev_key = chain_ops[i-1][0]
            get_response = client.get(nodes[node_idx], prev_key)
            assert get_response["ok"], f"Failed to read {prev_key} for causality"
        
        # Write current key
        put_response = client.put(nodes[node_idx], key, value)
        assert put_response["ok"], f"Failed to write {key}"
        
        time.sleep(0.1)  # Small delay for replication
    
    # Verify entire chain is readable from different nodes
    for key, expected_value, _ in chain_ops:
        for test_node_idx in [1, 3, 5]:  # Different nodes
            get_response = client.get(nodes[test_node_idx], key)
            assert get_response["ok"], f"Chain key {key} not accessible from node {test_node_idx}"
            assert get_response["value"] == expected_value, f"Wrong value for {key}"
    
    return True, "OK"

def test_partial_shard_recovery(conductor: ClusterConductor, fx: KvsFixture):
    """Some nodes in a shard recover, others don't."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Partial Shard Recovery")
    
    view = {
        "RecoverA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                     {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "RecoverB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                     {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}],
        "RecoverC": [{"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
                     {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}]
    }
    
    import requests
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Add initial data
    initial_keys = [f"recovery_key_{i:02d}" for i in range(12)]
    for key in initial_keys:
        client.put(nodes[0], key, f"value_{key}")
    
    time.sleep(2)
    
    # Fail all nodes in RecoverB shard
    conductor.simulate_kill_node(nodes[2], conductor.base_net)
    conductor.simulate_kill_node(nodes[3], conductor.base_net)
    
    time.sleep(1)
    
    # Verify RecoverB keys are unavailable
    for key in initial_keys:
        get_response = client.get(nodes[0], key)
        if not get_response["ok"]:
            # This key was in RecoverB shard
            assert get_response["status_code"] == 503, f"Expected 503 for failed shard key {key}"
    
    # Recover only one node from RecoverB
    conductor.simulate_revive_node(nodes[2], conductor.base_net)
    
    time.sleep(2)
    
    # RecoverB should be partially functional
    accessible_count = 0
    for key in initial_keys:
        get_response = client.get(nodes[0], key)
        if get_response["ok"]:
            accessible_count += 1
    
    # Should have recovered some keys
    assert accessible_count > 0, "No keys recovered after partial shard recovery"
    
    # Recover second node
    conductor.simulate_revive_node(nodes[3], conductor.base_net)
    
    time.sleep(3)
    
    # All keys should now be accessible
    for key in initial_keys:
        get_response = client.get(nodes[0], key)
        assert get_response["ok"], f"Key {key} should be accessible after full recovery"
    
    return True, "OK"

def test_cross_shard_concurrent_write_conflicts(conductor: ClusterConductor, fx: KvsFixture):
    """Concurrent writes to different shards with causal dependencies."""
    nodes = conductor.spawn_cluster(node_count=4)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    
    log("\n> TEST: Cross-Shard Concurrent Write Conflicts")
    
    view = {
        "ConflictA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                      {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "ConflictB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                      {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}]
    }
    
    import requests
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Initial setup
    client1.put(nodes[0], "conflict_base", "initial_value")
    time.sleep(1)
    
    # Both clients read the base value (establishing causality)
    base_response1 = client1.get(nodes[0], "conflict_base")
    base_response2 = client2.get(nodes[0], "conflict_base")
    assert base_response1["ok"] and base_response2["ok"]
    
    # Concurrent writes to different shards, both depending on base
    def client1_work():
        client1.put(nodes[0], "conflict_result_a", "client1_result")
    
    def client2_work():
        client2.put(nodes[2], "conflict_result_b", "client2_result")
    
    # Execute concurrently
    t1 = threading.Thread(target=client1_work)
    t2 = threading.Thread(target=client2_work)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    time.sleep(2)
    
    # Both results should be visible and causally consistent
    result_a = client1.get(nodes[2], "conflict_result_a")  # Cross-shard read
    result_b = client2.get(nodes[0], "conflict_result_b")  # Cross-shard read
    
    assert result_a["ok"], "Client1 result should be accessible cross-shard"
    assert result_b["ok"], "Client2 result should be accessible cross-shard"
    assert result_a["value"] == "client1_result"
    assert result_b["value"] == "client2_result"
    
    return True, "OK"

def test_shard_failure_during_data_migration(conductor: ClusterConductor, fx: KvsFixture):
    """Shard fails while resharding is in progress."""
    nodes = conductor.spawn_cluster(node_count=8)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Shard Failure During Data Migration")
    
    # Start with 2 shards
    initial_view = {
        "MigrationA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                       {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "MigrationB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                       {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}]
    }
    
    import requests
    for node in nodes[:4]:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": initial_view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Add data
    test_keys = [f"migration_key_{i:02d}" for i in range(20)]
    for key in test_keys:
        client.put(nodes[0], key, f"value_{key}")
    
    time.sleep(2)
    
    # Start resharding (add third shard)
    new_view = {
        "MigrationA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                       {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "MigrationB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                       {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}],
        "MigrationC": [{"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
                       {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}]
    }
    
    # Apply new view to trigger resharding
    def apply_view():
        for node in nodes[:6]:
            requests.put(f"http://localhost:{node.external_port}/view", json={"view": new_view}, timeout=10)
    
    # Start resharding in background
    migration_thread = threading.Thread(target=apply_view)
    migration_thread.start()
    
    # Immediately fail one of the existing shards during migration
    time.sleep(0.5)  # Let migration start
    conductor.simulate_kill_node(nodes[2], conductor.base_net)
    
    migration_thread.join()
    time.sleep(3)
    
    # System should still function despite failure during migration
    accessible_keys = 0
    for key in test_keys:
        get_response = client.get(nodes[0], key)
        if get_response["ok"]:
            accessible_keys += 1
    
    # Should have most keys still accessible
    assert accessible_keys >= len(test_keys) * 0.7, f"Too many keys lost: {accessible_keys}/{len(test_keys)}"
    
    return True, "OK"

def test_many_shards_performance(conductor: ClusterConductor, fx: KvsFixture):
    """Performance with many shards (8 shards)."""
    nodes = conductor.spawn_cluster(node_count=8)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Many Shards Performance")
    
    # Create 8 shards (1 node each for simplicity)
    view = {}
    for i in range(8):
        view[f"PerfShard{i}"] = [{"address": f"{nodes[i].ip}:8081", "id": nodes[i].index}]
    
    import requests
    start_time = time.time()
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": view}, timeout=10)
        assert resp.status_code == 200
    
    view_time = time.time() - start_time
    log(f"View setup time: {view_time:.2f}s")
    
    time.sleep(2)
    
    # Performance test: many operations
    num_ops = 40
    start_time = time.time()
    
    for i in range(num_ops):
        key = f"perf_key_{i:03d}"
        value = f"perf_value_{i}"
        client.put(nodes[i % 8], key, value)
    
    write_time = time.time() - start_time
    write_throughput = num_ops / write_time
    
    log(f"Write performance: {write_throughput:.1f} ops/sec")
    
    time.sleep(2)
    
    # Read performance
    start_time = time.time()
    
    for i in range(num_ops):
        key = f"perf_key_{i:03d}"
        get_response = client.get(nodes[(i + 4) % 8], key)  # Different nodes
        assert get_response["ok"], f"Read failed for {key}"
    
    read_time = time.time() - start_time
    read_throughput = num_ops / read_time
    
    log(f"Read performance: {read_throughput:.1f} ops/sec")
    
    # Performance thresholds
    assert write_throughput > 2.0, f"Write throughput too low: {write_throughput:.1f}"
    assert read_throughput > 4.0, f"Read throughput too low: {read_throughput:.1f}"
    
    return True, "OK"

def test_proxy_request_failures(conductor: ClusterConductor, fx: KvsFixture):
    """Proxy requests fail, system handles gracefully."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Proxy Request Failures")
    
    view = {
        "ProxyA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                   {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "ProxyB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                   {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}],
        "ProxyC": [{"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
                   {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}]
    }
    
    import requests
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Add data to different shards
    test_keys = [f"proxy_key_{i:02d}" for i in range(15)]
    for key in test_keys:
        client.put(nodes[0], key, f"value_{key}")
    
    time.sleep(2)
    
    # Determine key distribution
    proxy_a_keys = set(client.get_all(nodes[0])["values"].keys())
    proxy_b_keys = set(client.get_all(nodes[2])["values"].keys())
    proxy_c_keys = set(client.get_all(nodes[4])["values"].keys())
    
    # Fail one shard completely
    conductor.simulate_kill_node(nodes[2], conductor.base_net)
    conductor.simulate_kill_node(nodes[3], conductor.base_net)
    
    time.sleep(1)
    
    # Requests for ProxyB keys should fail gracefully
    failed_requests = 0
    successful_requests = 0
    
    for key in test_keys:
        get_response = client.get(nodes[0], key)  # From ProxyA
        
        if key in proxy_b_keys:
            # Should get 503 (service unavailable) for ProxyB keys
            if not get_response["ok"]:
                assert get_response["status_code"] == 503, f"Expected 503 for {key}, got {get_response['status_code']}"
                failed_requests += 1
        else:
            # Should succeed for ProxyA and ProxyC keys
            assert get_response["ok"], f"Key {key} should be accessible"
            successful_requests += 1
    
    log(f"Failed requests (expected): {failed_requests}")
    log(f"Successful requests: {successful_requests}")
    
    assert failed_requests > 0, "Should have some failed proxy requests"
    assert successful_requests > 0, "Should have some successful requests"
    
    return True, "OK"

def test_large_dataset_resharding(conductor: ClusterConductor, fx: KvsFixture):
    """Performance with larger dataset during resharding."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Large Dataset Resharding")
    
    # Start with 2 shards
    initial_view = {
        "LargeA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                   {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "LargeB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                   {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}]
    }
    
    import requests
    for node in nodes[:4]:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": initial_view}, timeout=10)
        assert resp.status_code == 200
    
    time.sleep(2)
    
    # Add larger dataset (use threading for speed)
    num_keys = 80
    keys_per_thread = 20
    
    def insert_batch(start_idx, batch_size):
        for i in range(start_idx, start_idx + batch_size):
            key = f"large_key_{i:04d}"
            value = f"large_value_{i:04d}_{'x' * 20}"  # Longer values
            client.put(nodes[i % 4], key, value)
    
    # Parallel insertion
    threads = []
    for i in range(0, num_keys, keys_per_thread):
        thread = threading.Thread(target=insert_batch, args=(i, keys_per_thread))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    time.sleep(3)
    
    # Measure resharding time
    new_view = {
        "LargeA": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                   {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
        "LargeB": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                   {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}],
        "LargeC": [{"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
                   {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}]
    }
    
    reshard_start = time.time()
    
    for node in nodes:
        resp = requests.put(f"http://localhost:{node.external_port}/view", json={"view": new_view}, timeout=15)
        assert resp.status_code == 200
    
    time.sleep(5)  # Wait for resharding
    
    reshard_time = time.time() - reshard_start
    log(f"Large dataset resharding time: {reshard_time:.2f}s for {num_keys} keys")
    
    # Verify data integrity
    accessible_keys = 0
    for i in range(num_keys):
        key = f"large_key_{i:04d}"
        get_response = client.get(nodes[0], key)
        if get_response["ok"]:
            accessible_keys += 1
    
    data_integrity = (accessible_keys / num_keys) * 100
    log(f"Data integrity: {data_integrity:.1f}% ({accessible_keys}/{num_keys})")
    
    assert data_integrity >= 95.0, f"Poor data integrity: {data_integrity:.1f}%"
    assert reshard_time < 30.0, f"Resharding too slow: {reshard_time:.2f}s"
    
    return True, "OK"

# Test collection
CRITICAL_EDGE_CASE_TESTS = [
    TestCase("test_key_assignment_consistency", test_key_assignment_consistency),
    TestCase("test_deep_cross_shard_causal_chains", test_deep_cross_shard_causal_chains),
    TestCase("test_partial_shard_recovery", test_partial_shard_recovery),
    TestCase("test_cross_shard_concurrent_write_conflicts", test_cross_shard_concurrent_write_conflicts),
    TestCase("test_shard_failure_during_data_migration", test_shard_failure_during_data_migration),
    TestCase("test_many_shards_performance", test_many_shards_performance),
    TestCase("test_proxy_request_failures", test_proxy_request_failures),
    TestCase("test_large_dataset_resharding", test_large_dataset_resharding),
]