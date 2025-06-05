# availability_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_availability_during_partition(conductor: ClusterConductor, fx: KvsFixture):
    """Test that nodes remain available during a network partition."""
    nodes = conductor.spawn_cluster(node_count=5)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Availability During Partition")
    
    # Put some initial data
    client.put(nodes[0], "test_key", "initial_value")
    
    # Create a partition: nodes[0:2] in partition p1, nodes[2:] in partition p2
    conductor.create_partition(nodes[0:2], "p1")
    conductor.create_partition(nodes[2:], "p2")
    
    # Update view for partition p1
    p1_client = fx.create_client(name="p1_client")
    p1_client.broadcast_view(nodes[0:2])
    
    # Update view for partition p2
    p2_client = fx.create_client(name="p2_client")
    p2_client.broadcast_view(nodes[2:])
    
    # Nodes in p1 should be able to serve requests
    p1_put = p1_client.put(nodes[0], "p1_key", "p1_value")
    assert p1_put["ok"], f"PUT in p1 failed with status {p1_put['status_code']}"
    
    p1_get = p1_client.get(nodes[1], "p1_key")
    assert p1_get["ok"], f"GET in p1 failed with status {p1_get['status_code']}"
    assert p1_get["value"] == "p1_value", f"Expected 'p1_value', got '{p1_get['value']}'"
    
    # Nodes in p2 should also be able to serve requests
    p2_put = p2_client.put(nodes[2], "p2_key", "p2_value")
    assert p2_put["ok"], f"PUT in p2 failed with status {p2_put['status_code']}"
    
    p2_get = p2_client.get(nodes[3], "p2_key")
    assert p2_get["ok"], f"GET in p2 failed with status {p2_get['status_code']}"
    assert p2_get["value"] == "p2_value", f"Expected 'p2_value', got '{p2_get['value']}'"
    
    return True, "OK"

def test_isolated_node_unavailable(conductor: ClusterConductor, fx: KvsFixture):
    """Test that an isolated node (not in view) returns 503."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    # Set view to include only the first 3 nodes
    client.broadcast_view(nodes[0:3])
    
    log("\n> TEST: Isolated Node Unavailable")
    
    # The fourth node (not in view) should return 503
    response = client.put(nodes[3], "test_key", "test_value")
    assert response["status_code"] == 503, \
        f"Expected 503 from isolated node, got {response['status_code']}"
    
    # Other nodes should be available
    response = client.put(nodes[0], "test_key", "test_value")
    assert response["ok"], f"PUT failed with status {response['status_code']}"
    
    return True, "OK"

def test_multiple_sequential_partitions(conductor: ClusterConductor, fx: KvsFixture):
    """Test availability through multiple sequential partitions."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Multiple Sequential Partitions")
    
    # Initial data
    client.put(nodes[0], "shared_key", "initial_value")
    
    # Partition 1: [0,1,2] and [3,4,5]
    conductor.create_partition(nodes[0:3], "p1a")
    conductor.create_partition(nodes[3:], "p1b")
    
    # Update views
    client_p1a = fx.create_client(name="client_p1a")
    client_p1a.broadcast_view(nodes[0:3])
    
    client_p1b = fx.create_client(name="client_p1b")
    client_p1b.broadcast_view(nodes[3:])
    
    # Both partitions should be available
    client_p1a.put(nodes[0], "p1a_key", "p1a_value")
    client_p1b.put(nodes[3], "p1b_key", "p1b_value")
    
    # Partition 2: [0,3,4] and [1,2,5]
    conductor.create_partition(nodes[0:1] + nodes[3:5], "p2a")
    conductor.create_partition(nodes[1:3] + nodes[5:], "p2b")
    
    # Update views
    p2a_nodes = [nodes[0], nodes[3], nodes[4]]
    p2b_nodes = [nodes[1], nodes[2], nodes[5]]
    
    client_p2a = fx.create_client(name="client_p2a")
    client_p2a.broadcast_view(p2a_nodes)
    
    client_p2b = fx.create_client(name="client_p2b")
    client_p2b.broadcast_view(p2b_nodes)
    
    # Both new partitions should be available
    client_p2a.put(nodes[0], "p2a_key", "p2a_value")
    client_p2b.put(nodes[1], "p2b_key", "p2b_value")
    
    # Verify reads still work in both partitions
    response = client_p2a.get(nodes[3], "p2a_key")
    assert response["ok"], f"GET in p2a failed with status {response['status_code']}"
    assert response["value"] == "p2a_value", f"Expected 'p2a_value', got '{response['value']}'"
    
    response = client_p2b.get(nodes[5], "p2b_key")
    assert response["ok"], f"GET in p2b failed with status {response['status_code']}"
    assert response["value"] == "p2b_value", f"Expected 'p2b_value', got '{response['value']}'"
    
    return True, "OK"

def test_majority_partition_failure(conductor: ClusterConductor, fx: KvsFixture):
    """Test that a system remains available even when majority of nodes fail."""
    nodes = conductor.spawn_cluster(node_count=5)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Majority Partition Failure")
    
    # Initial data
    client.put(nodes[0], "key", "initial_value")
    
    # Simulate failure of majority (3) nodes
    for i in range(3):
        conductor.simulate_kill_node(nodes[i], conductor.base_net)
    
    # Update view to only include the remaining nodes
    remaining_nodes = nodes[3:]
    remaining_client = fx.create_client(name="remaining_client")
    remaining_client.broadcast_view(remaining_nodes)
    
    # Remaining nodes should still be available
    response = remaining_client.put(nodes[3], "new_key", "new_value")
    assert response["ok"], f"PUT failed with status {response['status_code']}"
    
    response = remaining_client.get(nodes[4], "new_key")
    assert response["ok"], f"GET failed with status {response['status_code']}"
    assert response["value"] == "new_value", f"Expected 'new_value', got '{response['value']}'"
    
    return True, "OK"

AVAILABILITY_TESTS = [
    TestCase("test_availability_during_partition", test_availability_during_partition),
    TestCase("test_isolated_node_unavailable", test_isolated_node_unavailable),
    TestCase("test_multiple_sequential_partitions", test_multiple_sequential_partitions),
    TestCase("test_majority_partition_failure", test_majority_partition_failure),
]