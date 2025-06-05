# advanced_sharding_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_complex_resharding_scenario(conductor: ClusterConductor, fx: KvsFixture):
    """Test complex resharding with multiple view changes."""
    nodes = conductor.spawn_cluster(node_count=8)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Complex Resharding Scenario")
    
    # Start with 2 shards
    view1 = {
        "Shard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Shard2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    # Apply first view
    import requests
    for node in nodes[:4]:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view1},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set view1: {e}"
    
    time.sleep(2)
    
    # Add initial data set
    initial_keys = [f"data_{i:03d}" for i in range(30)]
    for key in initial_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"Initial PUT failed for {key}"
    
    time.sleep(2)
    
    # Expand to 4 shards
    view2 = {
        "Shard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Shard2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "Shard3": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ],
        "Shard4": [
            {"address": f"{nodes[6].ip}:8081", "id": nodes[6].index},
            {"address": f"{nodes[7].ip}:8081", "id": nodes[7].index}
        ]
    }
    
    log("Expanding to 4 shards...")
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view2},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set view2: {e}"
    
    time.sleep(5)  # More time for complex resharding
    
    # Verify all data still accessible
    for key in initial_keys:
        get_response = client.get(nodes[0], key)
        assert get_response["ok"], f"Data lost during expansion: {key}"
    
    # Contract back to 3 shards (remove Shard4)
    view3 = {
        "Shard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Shard2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "Shard3": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    log("Contracting to 3 shards...")
    for node in nodes[:6]:  # Only nodes in the new view
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view3},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set view3: {e}"
    
    time.sleep(5)
    
    # Final verification
    for key in initial_keys:
        get_response = client.get(nodes[0], key)
        assert get_response["ok"], f"Data lost during contraction: {key}"
    
    return True, "OK"

def test_shard_failure_handling(conductor: ClusterConductor, fx: KvsFixture):
    """Test behavior when an entire shard becomes unavailable."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Shard Failure Handling")
    
    # Create 3-shard setup
    view = {
        "Alpha": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Beta": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "Gamma": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    import requests
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set view: {e}"
    
    time.sleep(2)
    
    # Add data across all shards
    test_keys = [f"key_{i:02d}" for i in range(15)]
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(2)
    
    # Determine which keys are in which shards
    alpha_keys = set(client.get_all(nodes[0])["values"].keys())
    beta_keys = set(client.get_all(nodes[2])["values"].keys())
    gamma_keys = set(client.get_all(nodes[4])["values"].keys())
    
    log(f"Alpha shard has: {sorted(alpha_keys)}")
    log(f"Beta shard has: {sorted(beta_keys)}")
    log(f"Gamma shard has: {sorted(gamma_keys)}")
    
    # Simulate Beta shard failure
    log("Simulating Beta shard failure...")
    conductor.simulate_kill_node(nodes[2], conductor.base_net)
    conductor.simulate_kill_node(nodes[3], conductor.base_net)
    
    time.sleep(2)
    
    # Keys in Alpha and Gamma should still be accessible
    for key in alpha_keys:
        get_response = client.get(nodes[0], key)
        assert get_response["ok"], f"Alpha key {key} should still be accessible"
    
    for key in gamma_keys:
        get_response = client.get(nodes[4], key)
        assert get_response["ok"], f"Gamma key {key} should still be accessible"
    
    # Keys in Beta should be unavailable (503 Service Unavailable)
    for key in beta_keys:
        get_response = client.get(nodes[0], key)  # Should try to proxy to Beta
        # Should get 503 since Beta shard is down
        if not get_response["ok"]:
            assert get_response["status_code"] == 503, \
                f"Expected 503 for Beta key {key}, got {get_response['status_code']}"
    
    return True, "OK"

def test_large_scale_sharding(conductor: ClusterConductor, fx: KvsFixture):
    """Test sharding with many keys to verify distribution."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Large Scale Sharding")
    
    # Create 3 shards
    view = {
        "DataShard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "DataShard2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "DataShard3": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    import requests
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set view: {e}"
    
    time.sleep(2)
    
    # Add a large number of keys
    num_keys = 100
    test_keys = [f"large_key_{i:04d}" for i in range(num_keys)]
    
    log(f"Adding {num_keys} keys...")
    for key in test_keys:
        value = f"large_value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(3)
    
    # Check distribution across shards
    shard1_data = client.get_all(nodes[0])["values"]
    shard2_data = client.get_all(nodes[2])["values"]
    shard3_data = client.get_all(nodes[4])["values"]
    
    shard1_count = len(shard1_data)
    shard2_count = len(shard2_data)
    shard3_count = len(shard3_data)
    
    log(f"Distribution: Shard1={shard1_count}, Shard2={shard2_count}, Shard3={shard3_count}")
    
    # Verify total count
    total_distributed = shard1_count + shard2_count + shard3_count
    assert total_distributed == num_keys, \
        f"Key count mismatch: {total_distributed} vs {num_keys}"
    
    # Verify reasonable distribution (no shard should have < 20% or > 50% of keys)
    min_expected = num_keys * 0.2
    max_expected = num_keys * 0.5
    
    for shard_name, count in [("Shard1", shard1_count), ("Shard2", shard2_count), ("Shard3", shard3_count)]:
        assert count >= min_expected, f"{shard_name} has too few keys: {count}"
        assert count <= max_expected, f"{shard_name} has too many keys: {count}"
    
    # Verify all keys are still accessible via proxying
    log("Verifying all keys accessible via proxying...")
    missing_keys = []
    for key in test_keys[:20]:  # Check a sample
        get_response = client.get(nodes[1], key)  # Use different node
        if not get_response["ok"]:
            missing_keys.append(key)
    
    assert len(missing_keys) == 0, f"Missing keys: {missing_keys}"
    
    return True, "OK"

def test_concurrent_resharding_and_operations(conductor: ClusterConductor, fx: KvsFixture):
    """Test operations during resharding process."""
    nodes = conductor.spawn_cluster(node_count=6)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    
    log("\n> TEST: Concurrent Resharding and Operations")
    
    # Start with 2 shards
    initial_view = {
        "ShardA": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "ShardB": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    import requests
    for node in nodes[:4]:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": initial_view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set initial view: {e}"
    
    time.sleep(2)
    
    # Add some initial data
    for i in range(10):
        key = f"concurrent_key_{i:02d}"
        value = f"initial_value_{i}"
        client1.put(nodes[0], key, value)
    
    time.sleep(1)
    
    # Start resharding (add third shard)
    new_view = {
        "ShardA": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "ShardB": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "ShardC": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    # Apply new view
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": new_view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set new view: {e}"
    
    # Immediately start doing operations while resharding might be happening
    log("Performing operations during resharding...")
    
    # Add new data
    for i in range(10, 20):
        key = f"concurrent_key_{i:02d}"
        value = f"during_reshard_value_{i}"
        put_response = client2.put(nodes[1], key, value)
        if put_response["ok"]:
            log(f"Successfully added {key} during resharding")
    
    # Read existing data
    successful_reads = 0
    for i in range(10):
        key = f"concurrent_key_{i:02d}"
        get_response = client2.get(nodes[2], key)
        if get_response["ok"]:
            successful_reads += 1
    
    log(f"Successfully read {successful_reads}/10 keys during resharding")
    
    # Wait for resharding to complete
    time.sleep(5)
    
    # Verify all data is accessible after resharding
    log("Verifying data integrity after resharding...")
    for i in range(20):
        key = f"concurrent_key_{i:02d}"
        get_response = client1.get(nodes[0], key)
        assert get_response["ok"], f"Key {key} lost after concurrent resharding"
    
    return True, "OK"

def test_cross_shard_causal_dependencies(conductor: ClusterConductor, fx: KvsFixture):
    """Test complex causal dependencies that span multiple shards."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Cross-Shard Causal Dependencies")
    
    # Create 2-shard setup
    view = {
        "Left": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Right": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    import requests
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set view: {e}"
    
    time.sleep(2)
    
    # Create a complex causal chain that likely spans shards
    operations = [
        ("step1", "value1"),
        ("step2", "value2"),
        ("step3", "value3"),
        ("step4", "value4"),
        ("step5", "value5")
    ]
    
    log("Building cross-shard causal chain...")
    
    # Each operation depends on reading the previous one
    for i, (key, value) in enumerate(operations):
        # Write current step
        put_response = client.put(nodes[i % len(nodes)], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
        
        # Read current step to establish causal dependency for next step
        get_response = client.get(nodes[(i + 1) % len(nodes)], key)
        assert get_response["ok"], f"GET failed for {key}"
        assert get_response["value"] == value, f"Wrong value for {key}"
        
        # Small delay to ensure operations are ordered
        time.sleep(0.1)
    
    # Now read the entire chain from different nodes to verify causality
    log("Verifying causal chain from different nodes...")
    
    for i, (key, expected_value) in enumerate(operations):
        # Read from a different node each time
        node_idx = (len(operations) - 1 - i) % len(nodes)
        get_response = client.get(nodes[node_idx], key)
        assert get_response["ok"], f"Causal read failed for {key} from node {node_idx}"
        assert get_response["value"] == expected_value, \
            f"Causal value wrong for {key}: expected {expected_value}, got {get_response['value']}"
    
    return True, "OK"

ADVANCED_SHARDING_TESTS = [
    TestCase("test_complex_resharding_scenario", test_complex_resharding_scenario),
    TestCase("test_shard_failure_handling", test_shard_failure_handling),
    TestCase("test_large_scale_sharding", test_large_scale_sharding),
    TestCase("test_concurrent_resharding_and_operations", test_concurrent_resharding_and_operations),
    TestCase("test_cross_shard_causal_dependencies", test_cross_shard_causal_dependencies),
]