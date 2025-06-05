# eventual_consistency_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_partition_healing_convergence(conductor: ClusterConductor, fx: KvsFixture):
    """Test that partitioned nodes converge after healing."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Partition Healing Convergence")
    
    # Create a partition with [0,1] and [2,3]
    conductor.create_partition(nodes[0:2], "p1")
    conductor.create_partition(nodes[2:], "p2")
    
    # Update views
    p1_client = fx.create_client(name="p1_client")
    p1_client.broadcast_view(nodes[0:2])
    
    p2_client = fx.create_client(name="p2_client")
    p2_client.broadcast_view(nodes[2:])
    
    # Write different values to the same key in both partitions
    p1_client.put(nodes[0], "shared_key", "p1_value")
    p2_client.put(nodes[2], "shared_key", "p2_value")
    
    # Verify different values in each partition
    p1_response = p1_client.get(nodes[1], "shared_key")
    assert p1_response["value"] == "p1_value", \
        f"Expected 'p1_value' in p1, got '{p1_response['value']}'"
    
    p2_response = p2_client.get(nodes[3], "shared_key")
    assert p2_response["value"] == "p2_value", \
        f"Expected 'p2_value' in p2, got '{p2_response['value']}'"
    
    # Heal the partition
    conductor.create_partition(nodes, "healed")
    
    # Update view to include all nodes
    healed_client = fx.create_client(name="healed_client")
    healed_client.broadcast_view(nodes)
    
    # Wait for convergence (10 seconds as per spec)
    time.sleep(10)
    
    # Check that all nodes have converged to the same value
    values = []
    for node in nodes:
        response = healed_client.get(node, "shared_key")
        if response["ok"]:
            values.append(response["value"])
    
    # All nodes should have the same value
    assert len(set(values)) == 1, f"Nodes have not converged: {values}"
    
    # The value should be one of the two partition values
    assert values[0] in ["p1_value", "p2_value"], \
        f"Unexpected value after healing: {values[0]}"
    
    return True, "OK"

def test_multiple_concurrent_updates(conductor: ClusterConductor, fx: KvsFixture):
    """Test that multiple concurrent updates eventually converge."""
    nodes = conductor.spawn_cluster(node_count=5)
    clients = [fx.create_client(f"client{i}") for i in range(5)]
    
    for client in clients:
        client.broadcast_view(nodes)
        client.reset_causal_metadata()  # Ensure independent updates
    
    log("\n> TEST: Multiple Concurrent Updates")
    
    # Each client updates the same key through a different node
    for i, client in enumerate(clients):
        client.put(nodes[i], "concurrent_key", f"value_from_client{i}")
    
    # Wait for convergence
    time.sleep(10)
    
    # Check that all nodes have converged to the same value
    check_client = fx.create_client("check_client")
    check_client.broadcast_view(nodes)
    
    values = []
    for node in nodes:
        response = check_client.get(node, "concurrent_key")
        if response["ok"]:
            values.append(response["value"])
    
    # All nodes should have the same value
    assert len(set(values)) == 1, f"Nodes have not converged: {values}"
    
    # The value should be one of the five client values
    expected_values = [f"value_from_client{i}" for i in range(5)]
    assert values[0] in expected_values, \
        f"Unexpected value after convergence: {values[0]}"
    
    return True, "OK"

def test_node_joining_after_partition(conductor: ClusterConductor, fx: KvsFixture):
    """Test that a node joining after a partition heals correctly gets up to date."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    # Start with only 3 nodes in the view
    client.broadcast_view(nodes[0:3])
    
    log("\n> TEST: Node Joining After Partition")
    
    # Put some data with the initial 3 nodes
    client.put(nodes[0], "key1", "value1")
    client.put(nodes[1], "key2", "value2")
    client.put(nodes[2], "key3", "value3")
    
    # Now add the 4th node to the view
    new_view_client = fx.create_client(name="new_view_client")
    new_view_client.broadcast_view(nodes)
    
    # Wait for the new node to catch up
    time.sleep(10)
    
    # The new node should have all the data
    for key, expected in [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]:
        response = new_view_client.get(nodes[3], key)
        assert response["ok"], f"GET for {key} failed with status {response['status_code']}"
        assert response["value"] == expected, \
            f"Expected '{expected}' for {key}, got '{response['value']}'"
    
    return True, "OK"

def test_repeated_partition_and_healing(conductor: ClusterConductor, fx: KvsFixture):
    """Test convergence after repeated partitioning and healing."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="main_client")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Repeated Partition and Healing")
    
    # Initial data
    client.put(nodes[0], "shared_key", "initial_value")
    
    # Scenario 1: Create partitions [0,1,2] and [3,4,5]
    conductor.create_partition(nodes[0:3], "p1a")
    conductor.create_partition(nodes[3:], "p1b")
    
    # Update views
    p1a_client = fx.create_client(name="p1a_client")
    p1a_client.broadcast_view(nodes[0:3])
    
    p1b_client = fx.create_client(name="p1b_client")
    p1b_client.broadcast_view(nodes[3:])
    
    # Update in both partitions
    p1a_client.put(nodes[0], "shared_key", "p1a_value")
    p1b_client.put(nodes[3], "shared_key", "p1b_value")
    
    # Heal partition
    conductor.create_partition(nodes, "healed1")
    
    # Update view
    healed1_client = fx.create_client(name="healed1_client")
    healed1_client.broadcast_view(nodes)
    
    # Wait for convergence
    time.sleep(10)
    
    # Scenario 2: Create new partitions [0,2,4] and [1,3,5]
    conductor.create_partition([nodes[0], nodes[2], nodes[4]], "p2a")
    conductor.create_partition([nodes[1], nodes[3], nodes[5]], "p2b")
    
    # Update views
    p2a_nodes = [nodes[0], nodes[2], nodes[4]]
    p2b_nodes = [nodes[1], nodes[3], nodes[5]]
    
    p2a_client = fx.create_client(name="p2a_client")
    p2a_client.broadcast_view(p2a_nodes)
    
    p2b_client = fx.create_client(name="p2b_client")
    p2b_client.broadcast_view(p2b_nodes)
    
    # Update in both new partitions
    p2a_client.put(nodes[0], "shared_key", "p2a_value")
    p2b_client.put(nodes[1], "shared_key", "p2b_value")
    
    # Heal partition again
    conductor.create_partition(nodes, "healed2")
    
    # Update view
    healed2_client = fx.create_client(name="healed2_client")
    healed2_client.broadcast_view(nodes)
    
    # Wait for convergence
    time.sleep(10)
    
    # Check that all nodes have converged to the same value
    values = []
    for node in nodes:
        response = healed2_client.get(node, "shared_key")
        if response["ok"]:
            values.append(response["value"])
    
    # All nodes should have the same value
    assert len(set(values)) == 1, f"Nodes have not converged: {values}"
    
    # The value should be one of the expected values
    expected_values = ["p1a_value", "p1b_value", "p2a_value", "p2b_value"]
    assert values[0] in expected_values, \
        f"Unexpected value after repeated healing: {values[0]}"
    
    return True, "OK"

EVENTUAL_CONSISTENCY_TESTS = [
    TestCase("test_partition_healing_convergence", test_partition_healing_convergence),
    TestCase("test_multiple_concurrent_updates", test_multiple_concurrent_updates),
    TestCase("test_node_joining_after_partition", test_node_joining_after_partition),
    TestCase("test_repeated_partition_and_healing", test_repeated_partition_and_healing),
]