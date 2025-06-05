# basic_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def basic_put_get_with_metadata(conductor: ClusterConductor, fx: KvsFixture):
    """Test basic put and get operations with causal metadata."""
    nodes = conductor.spawn_cluster(node_count=3)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Basic Put/Get with Causal Metadata")
    
    # Put a value
    put_response = client.put(nodes[0], "test_key", "test_value")
    assert put_response["ok"], f"PUT failed with status code {put_response['status_code']}"
    
    # Get the value
    get_response = client.get(nodes[0], "test_key")
    assert get_response["ok"], f"GET failed with status code {get_response['status_code']}"
    assert get_response["value"] == "test_value", \
        f"Expected 'test_value', got '{get_response['value']}'"
    
    return True, "OK"

def basic_causal_chain(conductor: ClusterConductor, fx: KvsFixture):
    """Test a simple causal chain: put A -> get A -> put B -> get B."""
    nodes = conductor.spawn_cluster(node_count=3)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Basic Causal Chain")
    
    # Put key1 -> value1
    put1_response = client.put(nodes[0], "key1", "value1")
    assert put1_response["ok"], f"PUT 1 failed with status code {put1_response['status_code']}"
    
    # Get key1
    get1_response = client.get(nodes[1], "key1")
    assert get1_response["ok"], f"GET 1 failed with status code {get1_response['status_code']}"
    assert get1_response["value"] == "value1", \
        f"Expected 'value1', got '{get1_response['value']}'"
    
    # Put key2 -> value2 (causally after reading key1)
    put2_response = client.put(nodes[2], "key2", "value2")
    assert put2_response["ok"], f"PUT 2 failed with status code {put2_response['status_code']}"
    
    # Get key2
    get2_response = client.get(nodes[0], "key2")
    assert get2_response["ok"], f"GET 2 failed with status code {get2_response['status_code']}"
    assert get2_response["value"] == "value2", \
        f"Expected 'value2', got '{get2_response['value']}'"
    
    return True, "OK"

def basic_availability(conductor: ClusterConductor, fx: KvsFixture):
    """Test basic availability: each node should be able to serve requests."""
    nodes = conductor.spawn_cluster(node_count=5)
    client = fx.create_client(name="client1")
    client.broadcast_view(nodes)
    
    log("\n> TEST: Basic Availability")
    
    # Put a value through node 0
    put_response = client.put(nodes[0], "avail_key", "avail_value")
    assert put_response["ok"], f"PUT failed with status code {put_response['status_code']}"
    
    # Try to get the value from all nodes
    for i, node in enumerate(nodes):
        get_response = client.get(node, "avail_key")
        assert get_response["ok"], f"GET from node {i} failed with status code {get_response['status_code']}"
        assert get_response["value"] == "avail_value", \
            f"Expected 'avail_value' from node {i}, got '{get_response['value']}'"
    
    return True, "OK"

def initial_view_empty(conductor: ClusterConductor, fx: KvsFixture):
    """Test that nodes with empty views respond with 503."""
    nodes = conductor.spawn_cluster(node_count=1)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Initial View Empty")
    
    # Try to put a value (should fail with 503)
    put_response = client.put(nodes[0], "test_key", "test_value")
    assert put_response["status_code"] == 503, \
        f"Expected status code 503, got {put_response['status_code']}"
    
    # Now set the view and the request should succeed
    client.send_view(nodes[0], [nodes[0]])
    
    put_response = client.put(nodes[0], "test_key", "test_value")
    assert put_response["ok"], f"PUT failed with status code {put_response['status_code']}"
    
    return True, "OK"

def multiple_clients_causal_metadata(conductor: ClusterConductor, fx: KvsFixture):
    """Test that causal metadata is correctly handled between multiple clients."""
    nodes = conductor.spawn_cluster(node_count=3)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")
    client1.broadcast_view(nodes)
    
    log("\n> TEST: Multiple Clients with Causal Metadata")
    
    # Client 1 puts a value
    client1.put(nodes[0], "shared_key", "value1")
    
    # Client 1 gets the value
    get_response = client1.get(nodes[1], "shared_key")
    assert get_response["value"] == "value1", \
        f"Client 1 expected 'value1', got '{get_response['value']}'"
    
    # Client 1 updates the value
    client1.put(nodes[2], "shared_key", "value2")
    
    # Client 2 tries to get the value (should get value2 eventually)
    # might need to retry a few times due to anti-entropy process
    max_retries = 10
    for i in range(max_retries):
        get_response = client2.get(nodes[0], "shared_key")
        if get_response["value"] == "value2":
            break
        time.sleep(0.5)  # Wait for anti-entropy
    
    assert get_response["value"] == "value2", \
        f"Client 2 expected 'value2', got '{get_response['value']}'"
    
    # Now client 2 updates the value
    client2.put(nodes[1], "shared_key", "value3")
    
    # Client 1 should see the update eventually
    for i in range(max_retries):
        get_response = client1.get(nodes[2], "shared_key")
        if get_response["value"] == "value3":
            break
        time.sleep(0.5)  # Wait for anti-entropy
    
    assert get_response["value"] == "value3", \
        f"Client 1 expected 'value3', got '{get_response['value']}'"
    
    return True, "OK"

BASIC_TESTS = [
    TestCase("basic_put_get_with_metadata", basic_put_get_with_metadata),
    TestCase("basic_causal_chain", basic_causal_chain),
    TestCase("basic_availability", basic_availability),
    TestCase("initial_view_empty", initial_view_empty),
    TestCase("multiple_clients_causal_metadata", multiple_clients_causal_metadata),
]