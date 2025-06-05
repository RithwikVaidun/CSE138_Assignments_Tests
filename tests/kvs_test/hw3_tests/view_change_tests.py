# view_change_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_add_node(conductor: ClusterConductor, fx: KvsFixture):
    """Test adding a node to the view."""
    # Start with 3 nodes
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    # Set view with just the first 3 nodes
    client.broadcast_view(nodes[0:3])
    
    log("\n> TEST: Add Node")
    
    # Add some data
    client.put(nodes[0], "key1", "value1")
    client.put(nodes[1], "key2", "value2")
    
    # Now add the 4th node to the view
    new_view = nodes  # All 4 nodes
    new_client = fx.create_client(name="new_client")
    new_client.broadcast_view(new_view)
    
    # Wait for the new node to catch up
    time.sleep(10)
    
    # The new node should have all the data
    for key, expected in [("key1", "value1"), ("key2", "value2")]:
        response = new_client.get(nodes[3], key)
        assert response["ok"], f"GET for {key} failed with status {response['status_code']}"
        assert response["value"] == expected, \
            f"Expected '{expected}' for {key}, got '{response['value']}'"
    
    # Should be able to write to the new node
    new_client.put(nodes[3], "key3", "value3")
    
    # Other nodes should see the new data
    for i in range(3):
        response = new_client.get(nodes[i], "key3")
        assert response["ok"], f"GET from node {i} failed with status {response['status_code']}"
        assert response["value"] == "value3", \
            f"Expected 'value3' from node {i}, got '{response['value']}'"
    
    return True, "OK"

def test_remove_node(conductor: ClusterConductor, fx: KvsFixture):
    """Test removing a node from the view."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Remove Node")
    
    # Add some data
    client.put(nodes[0], "key1", "value1")
    
    # Tell ALL nodes about the new view (including node 3 that's being removed)
    reduced_view = nodes[0:3]
    reduced_client = fx.create_client(name="reduced_client")
    
    # Send new view to all original nodes, not just the remaining ones
    for node in nodes:  # All 4 nodes
        reduced_client.send_view(node, reduced_view)
    
    # Now node 3 knows it's not in the view and should return 503
    response = reduced_client.put(nodes[3], "key2", "value2")
    assert response["status_code"] == 503, \
        f"Expected 503 from removed node, got {response['status_code']}"
    
    # Other nodes should still work
    reduced_client.put(nodes[0], "key2", "value2")
    
    for i in range(3):
        response = reduced_client.get(nodes[i], "key2")
        assert response["ok"], f"GET from node {i} failed with status {response['status_code']}"
        assert response["value"] == "value2", \
            f"Expected 'value2' from node {i}, got '{response['value']}'"
    
    return True, "OK"

def test_view_change_with_partition(conductor: ClusterConductor, fx: KvsFixture):
    """Test view changes during a partition."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: View Change with Partition")
    
    # Initial data
    client.put(nodes[0], "key1", "value1")
    
    # Create a partition: [0,1,2] and [3,4,5]
    p1 = conductor.create_partition(nodes[0:3], "p1")
    p2 = conductor.create_partition(nodes[3:], "p2")
    
    # Update views to match partitions
    p1_client = fx.create_client(name="p1_client")
    p1_client.broadcast_view(nodes[0:3])
    
    p2_client = fx.create_client(name="p2_client")
    p2_client.broadcast_view(nodes[3:])
    
    # Both partitions should be able to serve requests
    p1_client.put(nodes[0], "p1_key", "p1_value")
    p2_client.put(nodes[3], "p2_key", "p2_value")
    
    # Now simulate node failure in p1 - use correct network reference
    conductor.simulate_kill_node(nodes[2], p1)
    reduced_client = fx.create_client(name="reduced_client")
    reduced_client.send_view(nodes[2], [])  # Reset removed node
    
    # Update view to exclude failed node
    p1_reduced_client = fx.create_client(name="p1_reduced_client")
    p1_reduced_client.broadcast_view(nodes[0:2])
    
    # Partition p1 should still work
    p1_reduced_client.put(nodes[0], "p1_key2", "p1_value2")
    
    response = p1_reduced_client.get(nodes[1], "p1_key2")
    assert response["ok"], f"GET failed with status {response['status_code']}"
    assert response["value"] == "p1_value2", \
        f"Expected 'p1_value2', got '{response['value']}'"
    
    return True, "OK"

def test_complex_view_changes(conductor: ClusterConductor, fx: KvsFixture):
    """Test a complex series of view changes."""
    nodes = conductor.spawn_cluster(node_count=8)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Complex View Changes")
    
    # Start with nodes 0-3
    initial_view = nodes[0:4]
    client.broadcast_view(initial_view)
    
    # Put some data
    client.put(nodes[0], "key1", "value1")
    
    # Add nodes 4-5
    view2 = nodes[0:6]
    client2 = fx.create_client(name="client2")
    client2.broadcast_view(view2)
    
    # Wait for new nodes to catch up
    time.sleep(3)
    
    # Put more data
    client2.put(nodes[4], "key2", "value2")
    
    # Remove nodes 0-1, add nodes 6-7
    view3 = nodes[2:8]
    client3 = fx.create_client(name="client3")
    client3.broadcast_view(view3)
    
    # Properly remove nodes 0 and 1 - inform them they're removed
    conductor.simulate_kill_node(nodes[0], conductor.base_net)
    client3.send_view(nodes[0], [])  # Reset to no view
    
    conductor.simulate_kill_node(nodes[1], conductor.base_net)
    client3.send_view(nodes[1], [])  # Reset to no view
    
    # Wait for new nodes to catch up
    time.sleep(3)
    
    # All nodes in view3 should have all data
    for key, expected in [("key1", "value1"), ("key2", "value2")]:
        for i in range(2, 8):
            response = client3.get(nodes[i], key)
            assert response["ok"], f"GET for {key} from node {i} failed with status {response['status_code']}"
            assert response["value"] == expected, \
                f"Expected '{expected}' for {key} from node {i}, got '{response['value']}'"
    
    # Put more data through new nodes
    client3.put(nodes[6], "key3", "value3")
    
    # Check that all view3 nodes have the new data
    for i in range(2, 8):
        response = client3.get(nodes[i], "key3")
        assert response["ok"], f"GET from node {i} failed with status {response['status_code']}"
        assert response["value"] == "value3", \
            f"Expected 'value3' from node {i}, got '{response['value']}'"
    
    # Removed nodes should return 503
    response = client3.put(nodes[0], "key4", "value4")
    assert response["status_code"] == 503, \
        f"Expected 503 from removed node 0, got {response['status_code']}"
    
    return True, "OK"

VIEW_CHANGE_TESTS = [
    TestCase("test_add_node", test_add_node),
    TestCase("test_remove_node", test_remove_node),
    TestCase("test_view_change_with_partition", test_view_change_with_partition),
    TestCase("test_complex_view_changes", test_complex_view_changes),
]