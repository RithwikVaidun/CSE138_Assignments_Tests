# causal_consistency_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_write_read_causality(conductor: ClusterConductor, fx: KvsFixture):
    """Test write-read causality: if client writes A then reads A, it should see its write."""
    nodes = conductor.spawn_cluster(node_count=3)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Write-Read Causality")
    
    # Client writes to key1 on node0
    client.put(nodes[0], "key1", "value1")
    
    # Client reads key1 from different node (node1)
    response = client.get(nodes[1], "key1")
    
    # Should see its own write
    assert response["value"] == "value1", \
        f"Expected 'value1', got '{response['value']}'"
    
    return True, "OK"

def test_read_write_causality(conductor: ClusterConductor, fx: KvsFixture):
    """Test read-write causality: if client reads A then writes B, the system should 
    maintain the causal dependency between A and B."""
    nodes = conductor.spawn_cluster(node_count=4)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    
    log("\n> TEST: Read-Write Causality")
    
    # Client1 writes key1
    client1.put(nodes[0], "key1", "value1")
    
    # Client2 reads key1
    response = client2.get(nodes[1], "key1")
    assert response["value"] == "value1", \
        f"Expected 'value1', got '{response['value']}'"
    
    # Client2 writes key2 (causally dependent on reading key1)
    client2.put(nodes[2], "key2", "value2")
    
    # Client1 writes key1 again
    client1.put(nodes[3], "key1", "value1_updated")
    
    # Client2 reads key1 again
    # Since client2 has a causal dependency on key1=value1, it should
    # either see value1 or value1_updated, but not an older version
    response = client2.get(nodes[0], "key1")
    assert response["value"] in ["value1", "value1_updated"], \
        f"Expected 'value1' or 'value1_updated', got '{response['value']}'"
    
    return True, "OK"

def test_causal_dependency_chain(conductor: ClusterConductor, fx: KvsFixture):
    """Test a multi-step causal dependency chain."""
    nodes = conductor.spawn_cluster(node_count=5)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Causal Dependency Chain")
    
    # Create a causal chain: A -> B -> C -> D
    # Put key A
    client.put(nodes[0], "key_A", "value_A")
    
    # Read A, then put B
    response = client.get(nodes[1], "key_A")
    assert response["value"] == "value_A"
    client.put(nodes[1], "key_B", "value_B")
    
    # Read B, then put C
    response = client.get(nodes[2], "key_B")
    assert response["value"] == "value_B"
    client.put(nodes[2], "key_C", "value_C")
    
    # Read C, then put D
    response = client.get(nodes[3], "key_C")
    assert response["value"] == "value_C"
    client.put(nodes[3], "key_D", "value_D")
    
    # Now verify the chain on a different node
    # Reading D should guarantee that A, B, C are visible
    response = client.get(nodes[4], "key_D")
    assert response["value"] == "value_D"
    
    response = client.get(nodes[4], "key_C")
    assert response["value"] == "value_C"
    
    response = client.get(nodes[4], "key_B")
    assert response["value"] == "value_B"
    
    response = client.get(nodes[4], "key_A")
    assert response["value"] == "value_A"
    
    return True, "OK"

def test_concurrent_writes(conductor: ClusterConductor, fx: KvsFixture):
    """Test that concurrent writes to the same key eventually converge."""
    nodes = conductor.spawn_cluster(node_count=3)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    
    log("\n> TEST: Concurrent Writes")
    
    # Reset causal metadata for both clients
    client1.reset_causal_metadata()
    client2.reset_causal_metadata()
    
    # Both clients write to the same key "concurrently"
    client1.put(nodes[0], "concurrent_key", "value_from_client1")
    client2.put(nodes[1], "concurrent_key", "value_from_client2")
    
    # Allow time for anti-entropy
    time.sleep(2)
    
    # Check that all nodes have converged to the same value
    values = []
    for node in nodes:
        response = client1.get(node, "concurrent_key")
        if response["ok"]:
            values.append(response["value"])
    
    # All nodes should have the same value
    assert len(set(values)) == 1, f"Nodes have not converged: {values}"
    
    # The value should be one of the two writes
    assert values[0] in ["value_from_client1", "value_from_client2"], \
        f"Unexpected value: {values[0]}"
    
    return True, "OK"

def test_causal_visibility(conductor: ClusterConductor, fx: KvsFixture):
    """Test that if a client sees a write, it also sees all causally preceding writes."""
    nodes = conductor.spawn_cluster(node_count=4)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    
    log("\n> TEST: Causal Visibility")
    
    # Client1 writes key1
    client1.put(nodes[0], "key1", "value1")
    
    # Client1 reads key1, then writes key2 (causally dependent)
    response = client1.get(nodes[1], "key1")
    assert response["value"] == "value1"
    client1.put(nodes[1], "key2", "value2")
    
    # Client2 reads key2
    for _ in range(10):  # Retry with delay to account for anti-entropy
        response = client2.get(nodes[2], "key2")
        if response["ok"] and response["value"] == "value2":
            break
        time.sleep(0.5)
    
    assert response["value"] == "value2", f"Client2 should see key2=value2, got {response['value']}"
    
    # Client2 should now be able to see key1 as well (causal dependency)
    response = client2.get(nodes[3], "key1")
    assert response["value"] == "value1", f"Client2 should see key1=value1, got {response['value']}"
    
    return True, "OK"

CAUSAL_CONSISTENCY_TESTS = [
    TestCase("test_write_read_causality", test_write_read_causality),
    TestCase("test_read_write_causality", test_read_write_causality),
    TestCase("test_causal_dependency_chain", test_causal_dependency_chain),
    TestCase("test_concurrent_writes", test_concurrent_writes),
    TestCase("test_causal_visibility", test_causal_visibility),
]