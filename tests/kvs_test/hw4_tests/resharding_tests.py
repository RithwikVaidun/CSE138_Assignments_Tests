# resharding_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_shard_add_resharding(conductor: ClusterConductor, fx: KvsFixture):
    """Test adding a new shard and verify data resharding."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Shard Addition and Resharding")
    
    # Start with 2 shards
    initial_view = {
        "Shard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Shard2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    # Send initial view
    import requests
    for node in nodes[:4]:  # Only first 4 nodes initially
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
    
    # Add initial data
    initial_keys = [f"initial_key_{i:02d}" for i in range(10)]
    for key in initial_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(2)
    
    # Record initial distribution
    shard1_before = client.get_all(nodes[0])["values"]
    shard2_before = client.get_all(nodes[2])["values"]
    
    log(f"Before resharding - Shard1: {len(shard1_before)} keys, Shard2: {len(shard2_before)} keys")
    
    # Add a third shard
    new_view = {
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
    
    # Send new view to all nodes (including new ones)
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
    
    # Wait for resharding to complete
    time.sleep(5)
    
    # Verify all data is still accessible
    for key in initial_keys:
        get_response = client.get(nodes[0], key)  # Should proxy if needed
        assert get_response["ok"], f"GET failed for {key} after resharding"
        expected_value = f"value_{key}"
        assert get_response["value"] == expected_value, \
            f"Expected '{expected_value}' for {key}, got '{get_response['value']}'"
    
    # Check new distribution
    shard1_after = client.get_all(nodes[0])["values"]
    shard2_after = client.get_all(nodes[2])["values"]
    shard3_after = client.get_all(nodes[4])["values"]
    
    log(f"After resharding - Shard1: {len(shard1_after)} keys, Shard2: {len(shard2_after)} keys, Shard3: {len(shard3_after)} keys")
    
    # Verify no key overlap
    all_keys_after = set(shard1_after.keys()) | set(shard2_after.keys()) | set(shard3_after.keys())
    assert all_keys_after == set(initial_keys), "Some keys were lost during resharding"
    
    # Verify all shards have some data (though distribution might be uneven)
    total_keys_after = len(shard1_after) + len(shard2_after) + len(shard3_after)
    assert total_keys_after == len(initial_keys), f"Key count mismatch: {total_keys_after} vs {len(initial_keys)}"
    
    return True, "OK"

def test_shard_removal_resharding(conductor: ClusterConductor, fx: KvsFixture):
    """Test removing a shard and verify data migration."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Shard Removal and Resharding")
    
    # Start with 3 shards
    initial_view = {
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
                json={"view": initial_view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set initial view: {e}"
    
    time.sleep(2)
    
    # Add data across all shards
    test_keys = [f"removal_key_{i:02d}" for i in range(15)]
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(2)
    
    # Record distribution before removal
    alpha_before = client.get_all(nodes[0])["values"]
    beta_before = client.get_all(nodes[2])["values"]
    gamma_before = client.get_all(nodes[4])["values"]
    
    log(f"Before removal - Alpha: {len(alpha_before)}, Beta: {len(beta_before)}, Gamma: {len(gamma_before)}")
    
    # Remove Beta shard
    new_view = {
        "Alpha": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Gamma": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    # Send new view (only to remaining nodes in the assignment)
    remaining_nodes = [nodes[0], nodes[1], nodes[4], nodes[5]]
    for node in remaining_nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": new_view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set removal view: {e}"
    
    # Wait for resharding
    time.sleep(5)
    
    # Verify all data is still accessible from remaining shards
    for key in test_keys:
        get_response = client.get(nodes[0], key)  # Try from Alpha
        if not get_response["ok"]:
            get_response = client.get(nodes[4], key)  # Try from Gamma
        assert get_response["ok"], f"Key {key} became inaccessible after shard removal"
    
    # Check final distribution
    alpha_after = client.get_all(nodes[0])["values"]
    gamma_after = client.get_all(nodes[4])["values"]
    
    log(f"After removal - Alpha: {len(alpha_after)}, Gamma: {len(gamma_after)}")
    
    # Verify all keys are distributed between remaining shards
    all_keys_after = set(alpha_after.keys()) | set(gamma_after.keys())
    assert all_keys_after == set(test_keys), "Keys lost during shard removal"
    
    return True, "OK"

def test_minimal_data_movement(conductor: ClusterConductor, fx: KvsFixture):
    """Test that resharding moves minimal amount of data."""
    nodes = conductor.spawn_cluster(node_count=8)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Minimal Data Movement During Resharding")
    
    # Start with 2 shards and many keys
    initial_view = {
        "Shard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Shard2": [
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
    
    # Add many keys to test distribution
    num_keys = 50
    test_keys = [f"minimal_key_{i:03d}" for i in range(num_keys)]
    
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(2)
    
    # Record initial distribution
    shard1_initial = set(client.get_all(nodes[0])["values"].keys())
    shard2_initial = set(client.get_all(nodes[2])["values"].keys())
    
    log(f"Initial distribution: Shard1={len(shard1_initial)}, Shard2={len(shard2_initial)}")
    
    # Add two more shards (should redistribute to ~25% each)
    new_view = {
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
    
    time.sleep(5)
    
    # Check final distribution
    shard1_final = set(client.get_all(nodes[0])["values"].keys())
    shard2_final = set(client.get_all(nodes[2])["values"].keys())
    shard3_final = set(client.get_all(nodes[4])["values"].keys())
    shard4_final = set(client.get_all(nodes[6])["values"].keys())
    
    log(f"Final distribution: Shard1={len(shard1_final)}, Shard2={len(shard2_final)}, Shard3={len(shard3_final)}, Shard4={len(shard4_final)}")
    
    # Calculate how many keys moved from original shards
    shard1_stayed = len(shard1_initial.intersection(shard1_final))
    shard1_moved = len(shard1_initial) - shard1_stayed
    
    shard2_stayed = len(shard2_initial.intersection(shard2_final))
    shard2_moved = len(shard2_initial) - shard2_stayed
    
    total_moved = shard1_moved + shard2_moved
    movement_percentage = (total_moved / num_keys) * 100
    
    log(f"Data movement: {total_moved}/{num_keys} keys moved ({movement_percentage:.1f}%)")
    
    # For going from 2 to 4 shards, we expect roughly 50% of keys to move
    # (each original shard should keep about half its keys)
    assert movement_percentage <= 70, f"Too much data movement: {movement_percentage}%"
    
    # Verify all keys are still present
    all_final_keys = shard1_final | shard2_final | shard3_final | shard4_final
    assert all_final_keys == set(test_keys), "Keys lost during resharding"
    
    return True, "OK"

def test_resharding_preserves_causality(conductor: ClusterConductor, fx: KvsFixture):
    """Test that causal relationships are preserved during resharding."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Resharding Preserves Causality")
    
    # Start with 2 shards
    initial_view = {
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
    
    # Create a causal chain before resharding
    causal_keys = ["cause1", "effect1", "cause2", "effect2"]
    
    # Build causal dependencies
    client.put(nodes[0], "cause1", "initial_cause")
    
    get_response = client.get(nodes[1], "cause1")
    assert get_response["ok"], "Failed to read cause1"
    
    client.put(nodes[1], "effect1", "depends_on_cause1")
    
    get_response = client.get(nodes[0], "effect1")
    assert get_response["ok"], "Failed to read effect1"
    
    client.put(nodes[0], "cause2", "second_cause")
    
    get_response = client.get(nodes[1], "cause2")
    assert get_response["ok"], "Failed to read cause2"
    
    client.put(nodes[1], "effect2", "depends_on_cause2")
    
    time.sleep(1)
    
    # Trigger resharding by adding a third shard
    new_view = {
        "Left": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Right": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "Center": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ]
    }
    
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": new_view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set resharding view: {e}"
    
    time.sleep(5)
    
    # Verify causal relationships are preserved after resharding
    log("Verifying causal chain after resharding...")
    
    # Read the causal chain in order from different nodes
    cause1_response = client.get(nodes[4], "cause1")
    assert cause1_response["ok"], "cause1 not accessible after resharding"
    
    effect1_response = client.get(nodes[5], "effect1")
    assert effect1_response["ok"], "effect1 not accessible after resharding"
    
    cause2_response = client.get(nodes[0], "cause2")
    assert cause2_response["ok"], "cause2 not accessible after resharding"
    
    effect2_response = client.get(nodes[2], "effect2")
    assert effect2_response["ok"], "effect2 not accessible after resharding"
    
    # Verify values are correct
    assert cause1_response["value"] == "initial_cause"
    assert effect1_response["value"] == "depends_on_cause1"
    assert cause2_response["value"] == "second_cause"
    assert effect2_response["value"] == "depends_on_cause2"
    
    return True, "OK"

def test_no_operation_resharding(conductor: ClusterConductor, fx: KvsFixture):
    """Test view changes that don't require resharding."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: No-Op Resharding (Same Shard Names)")
    
    # Initial view
    initial_view = {
        "Alpha": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Beta": [
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
    
    # Add test data
    test_keys = [f"noop_key_{i:02d}" for i in range(10)]
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(1)
    
    # Record distribution before "resharding"
    alpha_before = set(client.get_all(nodes[0])["values"].keys())
    beta_before = set(client.get_all(nodes[2])["values"].keys())
    
    log(f"Before no-op: Alpha={len(alpha_before)}, Beta={len(beta_before)}")
    
    # Change view but keep same shard names (just different nodes)
    new_view = {
        "Alpha": [
            {"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
            {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}
        ],
        "Beta": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    # Apply new view to all relevant nodes
    all_relevant_nodes = [nodes[2], nodes[3], nodes[4], nodes[5]]
    for node in all_relevant_nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": new_view},
                timeout=10
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set no-op view: {e}"
    
    time.sleep(3)
    
    # Check that data distribution should be the same (no resharding needed)
    # Note: Alpha shard moved to different physical nodes but logically same shard
    alpha_after = set(client.get_all(nodes[4])["values"].keys())  # New Alpha node
    beta_after = set(client.get_all(nodes[2])["values"].keys())   # Same Beta node
    
    log(f"After no-op: Alpha={len(alpha_after)}, Beta={len(beta_after)}")
    
    # Verify all data is still accessible
    for key in test_keys:
        get_response = client.get(nodes[4], key)  # Try new Alpha first
        if not get_response["ok"]:
            get_response = client.get(nodes[2], key)  # Try Beta
        assert get_response["ok"], f"Key {key} became inaccessible after view change"
    
    # All keys should still be present
    all_keys_after = alpha_after | beta_after
    assert all_keys_after == set(test_keys), "Keys lost during no-op view change"
    
    return True, "OK"

RESHARDING_TESTS = [
    TestCase("test_shard_add_resharding", test_shard_add_resharding),
    TestCase("test_shard_removal_resharding", test_shard_removal_resharding),
    TestCase("test_minimal_data_movement", test_minimal_data_movement),
    TestCase("test_resharding_preserves_causality", test_resharding_preserves_causality),
    TestCase("test_no_operation_resharding", test_no_operation_resharding),
]