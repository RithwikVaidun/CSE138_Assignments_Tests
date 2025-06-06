# basic_sharding_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_single_shard_backwards_compatibility(conductor: ClusterConductor, fx: KvsFixture):
    """Test that single shard works like Assignment 3 (backwards compatibility)."""
    nodes = conductor.spawn_cluster(node_count=3)
    client = fx.create_client(name="client1")
    
    # Use legacy view format (should work as single shard)
    client.broadcast_view(nodes)
    
    log("\n> TEST: Single Shard Backwards Compatibility")
    
    # Put a value
    put_response = client.put(nodes[0], "test_key", "test_value")
    assert put_response["ok"], f"PUT failed with status code {put_response['status_code']}"
    
    # Get the value from different nodes
    for i, node in enumerate(nodes):
        get_response = client.get(node, "test_key")
        assert get_response["ok"], f"GET from node {i} failed with status code {get_response['status_code']}"
        assert get_response["value"] == "test_value", \
            f"Expected 'test_value' from node {i}, got '{get_response['value']}'"
    
    return True, "OK"

def test_basic_two_shard_setup(conductor: ClusterConductor, fx: KvsFixture):
    """Test basic two-shard setup with key distribution."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Basic Two Shard Setup")
    
    # Create sharded view: 2 nodes per shard
    view = {
        "Shard1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Shard2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    # Send sharded view to all nodes
    for node in nodes:
        response = client.send_view(node, [])  # Empty for direct call
        # We need to manually send the sharded view
        import requests
        import json
        
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view},
                timeout=10
            )
            assert resp.status_code == 200, f"View update failed for node {node.index}: {resp.status_code}"
        except Exception as e:
            log(f"Error updating view for node {node.index}: {e}")
            return False, f"Failed to update view: {e}"
    
    # Wait for view changes to propagate
    time.sleep(2)
    
    # Test that we can put and get values (they might go to different shards)
    test_keys = ["key1", "key2", "key3", "key4", "key5"]
    
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key} with status code {put_response['status_code']}"
        
        # Try to get from different nodes (some might proxy)
        get_response = client.get(nodes[1], key)
        assert get_response["ok"], f"GET failed for {key} with status code {get_response['status_code']}"
        assert get_response["value"] == value, f"Expected '{value}' for {key}, got '{get_response['value']}'"
    
    return True, "OK"

def test_key_distribution_across_shards(conductor: ClusterConductor, fx: KvsFixture):
    """Test that keys are distributed across shards and GET /data only returns local keys."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Key Distribution Across Shards")
    
    # Create sharded view
    view = {
        "ShardA": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "ShardB": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    # Send sharded view to all nodes
    import requests
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": view},
                timeout=10
            )
            assert resp.status_code == 200, f"View update failed for node {node.index}"
        except Exception as e:
            return False, f"Failed to update view: {e}"
    
    time.sleep(2)
    
    # Put many keys
    test_keys = [f"key_{i:03d}" for i in range(20)]
    
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    # Wait for replication
    time.sleep(2)
    
    # Get all data from each shard separately
    shard_a_data = client.get_all(nodes[0])  # ShardA
    shard_b_data = client.get_all(nodes[2])  # ShardB
    
    assert shard_a_data["ok"], "GET /data failed for ShardA"
    assert shard_b_data["ok"], "GET /data failed for ShardB"
    
    shard_a_keys = set(shard_a_data["values"].keys())
    shard_b_keys = set(shard_b_data["values"].keys())
    
    log(f"ShardA has {len(shard_a_keys)} keys: {sorted(shard_a_keys)}")
    log(f"ShardB has {len(shard_b_keys)} keys: {sorted(shard_b_keys)}")
    
    # Verify no overlap between shards
    assert len(shard_a_keys.intersection(shard_b_keys)) == 0, \
        f"Key overlap between shards: {shard_a_keys.intersection(shard_b_keys)}"
    
    # Verify all keys are present across both shards
    all_returned_keys = shard_a_keys.union(shard_b_keys)
    assert all_returned_keys == set(test_keys), \
        f"Missing keys: {set(test_keys) - all_returned_keys}"
    
    # Verify both shards have some keys (distribution should be roughly even)
    assert len(shard_a_keys) > 0, "ShardA should have some keys"
    assert len(shard_b_keys) > 0, "ShardB should have some keys"
    
    return True, "OK"

def test_causal_consistency_across_shards(conductor: ClusterConductor, fx: KvsFixture):
    """Test that causal consistency is maintained across shard boundaries."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Causal Consistency Across Shards")
    
    # Create sharded view
    view = {
        "ShardX": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "ShardY": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ]
    }
    
    # Send sharded view
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
            return False, f"Failed to update view: {e}"
    
    time.sleep(2)
    
    # Create a causal chain that might span shards
    # Write key1
    put1_response = client.put(nodes[0], "causal_key1", "value1")
    assert put1_response["ok"], "PUT 1 failed"
    
    # Read key1, then write key2 (causal dependency)
    get1_response = client.get(nodes[1], "causal_key1")
    assert get1_response["ok"], "GET 1 failed"
    assert get1_response["value"] == "value1", "GET 1 wrong value"
    
    put2_response = client.put(nodes[0], "causal_key2", "value2")
    assert put2_response["ok"], "PUT 2 failed"
    
    # Read key2, then write key3 (extending the chain)
    get2_response = client.get(nodes[1], "causal_key2")
    assert get2_response["ok"], "GET 2 failed"
    assert get2_response["value"] == "value2", "GET 2 wrong value"
    
    put3_response = client.put(nodes[0], "causal_key3", "value3")
    assert put3_response["ok"], "PUT 3 failed"
    
    # Now read the chain in order from different nodes
    # This should work regardless of which shard the keys ended up in
    final_get1 = client.get(nodes[2], "causal_key1")
    assert final_get1["ok"], "Final GET 1 failed"
    assert final_get1["value"] == "value1", "Final GET 1 wrong value"
    
    final_get2 = client.get(nodes[3], "causal_key2")
    assert final_get2["ok"], "Final GET 2 failed"
    assert final_get2["value"] == "value2", "Final GET 2 wrong value"
    
    final_get3 = client.get(nodes[2], "causal_key3")
    assert final_get3["ok"], "Final GET 3 failed"
    assert final_get3["value"] == "value3", "Final GET 3 wrong value"
    
    return True, "OK"

def test_transparent_proxying(conductor: ClusterConductor, fx: KvsFixture):
    """Test that request proxying is transparent to clients."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Transparent Request Proxying")
    
    # Create 3-shard view
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
    
    # Send sharded view
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
            return False, f"Failed to update view: {e}"
    
    time.sleep(2)
    
    # Put keys from different nodes (some will require proxying)
    test_data = [
        ("user:alice", "Alice Data"),
        ("user:bob", "Bob Data"), 
        ("user:carol", "Carol Data"),
        ("config:theme", "dark"),
        ("config:language", "en"),
        ("session:123", "active")
    ]
    
    # Put data from different nodes
    for i, (key, value) in enumerate(test_data):
        node_idx = i % len(nodes)
        put_response = client.put(nodes[node_idx], key, value)
        assert put_response["ok"], f"PUT failed for {key} from node {node_idx}"
    
    time.sleep(2)
    
    # Get data from different nodes (will require proxying for some)
    for i, (key, expected_value) in enumerate(test_data):
        # Use a different node than we used for PUT
        node_idx = (i + 3) % len(nodes)
        get_response = client.get(nodes[node_idx], key)
        assert get_response["ok"], f"GET failed for {key} from node {node_idx}"
        assert get_response["value"] == expected_value, \
            f"Expected '{expected_value}' for {key}, got '{get_response['value']}'"
    
    return True, "OK"

BASIC_SHARDING_TESTS = [
    TestCase("test_basic_two_shard_setup", test_basic_two_shard_setup),
    TestCase("test_key_distribution_across_shards", test_key_distribution_across_shards),
    TestCase("test_causal_consistency_across_shards", test_causal_consistency_across_shards),
    TestCase("test_transparent_proxying", test_transparent_proxying),
]

# Backwards compatibility test - only run when explicitly filtered
COMPATIBILITY_TESTS = [
    TestCase("test_single_shard_backwards_compatibility", test_single_shard_backwards_compatibility),
]