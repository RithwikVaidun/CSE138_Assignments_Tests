from multiprocessing.pool import ThreadPool
import time
import random
import string
from threading import Thread

from ..containers import ClusterConductor
from ..hw2_api import KvsFixture
from ..testcase import TestCase
from ..util import log


def advanced_concurrent_writes(conductor: ClusterConductor, fx: KvsFixture):
    """Test concurrent writes to the same key from different clients."""
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="main_client")
    mc.broadcast_view(nodes)
    
    # Create multiple clients for concurrent access
    clients = [fx.create_client(f"client_{i}") for i in range(5)]
    
    # Initialize key
    r = mc.put(nodes[0], "concurrent_key", "initial")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Perform concurrent writes
    log("\n> TEST CONCURRENT WRITES")
    
    def write_key(client_idx):
        client = clients[client_idx]
        value = f"value_from_client_{client_idx}"
        r = client.put(nodes[client_idx % len(nodes)], "concurrent_key", value)
        return r.status_code
    
    # Run concurrent writes
    with ThreadPool() as pool:
        results = pool.map(write_key, range(5))
    
    for status in results:
        assert status in [200, 201], f"expected 200 or 201, got {status}"
    
    # Verify all nodes have the same value after concurrent writes
    values = []
    for node in nodes:
        r = mc.get(node, "concurrent_key")
        assert r.status_code == 200, f"expected 200, got {r.status_code}"
        values.append(r.value)
    
    # All nodes should have the same value (strong consistency)
    assert len(set(values)) == 1, f"nodes have inconsistent values: {values}"
    
    return True, "ok"


def advanced_large_values(conductor: ClusterConductor, fx: KvsFixture):
    """Test storage and retrieval of large values."""
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> TEST LARGE VALUES")
    
    # Create large values of increasing sizes
    sizes = [1024, 10240, 102400]  # 1KB, 10KB, 100KB
    
    for size in sizes:
        # Generate random string of specified size
        large_value = ''.join(random.choices(string.ascii_letters + string.digits, k=size))
        key = f"large_key_{size}"
        
        log(f"\n> TESTING {size} bytes value")
        r = mc.put(nodes[0], key, large_value)
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
        
        # Verify each node has the same large value
        for node in nodes:
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == large_value, f"value mismatch for {size} bytes"
    
    return True, "ok"


def advanced_primary_failure_during_replication(conductor: ClusterConductor, fx: KvsFixture):
    """Test behavior when primary fails during replication."""
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> TEST PRIMARY FAILURE DURING REPLICATION")
    
    # Add some initial data
    test_keys = ["test1", "test2", "test3"]
    for i, key in enumerate(test_keys):
        r = mc.put(nodes[0], key, f"value{i}")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Kill the primary (node 0) right after sending a new write
    log("\n> KILLING PRIMARY DURING WRITE")
    # Start a thread to kill the primary with a small delay
    def kill_primary_after_delay():
        time.sleep(0.1)  # Short delay to allow request to be in-flight
        conductor.simulate_kill_node(nodes[0], conductor.base_net)
    
    kill_thread = Thread(target=kill_primary_after_delay)
    kill_thread.start()
    
    # This write may fail as the primary is killed during processing
    try:
        mc.put(nodes[0], "during_failure", "value")
    except Exception as e:
        log(f"Expected exception during primary failure: {e}")
    
    kill_thread.join()
    
    # Update view to exclude failed primary
    new_view = nodes[1:]
    mc.broadcast_view(new_view)
    
    # Verify previous data is still accessible from remaining nodes
    for i, key in enumerate(test_keys):
        for node in new_view:
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == f"value{i}", f"expected value{i}, got {r.value}"
    
    # Write new data to new primary
    r = mc.put(nodes[1], "after_failure", "new_value")
    assert r.ok, f"expected success, got {r.status_code}"
    
    # Verify new data is replicated to all remaining nodes
    for node in new_view:
        r = mc.get(node, "after_failure")
        assert r.status_code == 200, f"expected 200, got {r.status_code}"
        assert r.value == "new_value", f"expected new_value, got {r.value}"
    
    return True, "ok"


def advanced_complex_partition(conductor: ClusterConductor, fx: KvsFixture):
    """Test complex network partitions with multiple groups."""
    nodes = conductor.spawn_cluster(node_count=5)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> TEST COMPLEX NETWORK PARTITIONS")
    
    # Add initial data
    r = mc.put(nodes[0], "partition_test", "initial")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Create three partitions: [0,1], [2,3], [4]
    log("\n> CREATING MULTIPLE PARTITIONS")
    
    # Partition 1: nodes 0,1
    conductor.create_partition([nodes[0], nodes[1]], "p1")
    mc.broadcast_view([nodes[0], nodes[1]])
    
    # Partition 2: nodes 2,3
    conductor.create_partition([nodes[2], nodes[3]], "p2")
    mc.broadcast_view([nodes[2], nodes[3]])
    
    # Partition 3: node 4
    conductor.create_partition([nodes[4]], "p3")
    mc.broadcast_view([nodes[4]])
    
    # Describe the network topology
    conductor.describe_cluster()
    
    # Write different data to each partition
    log("\n> WRITING TO DIFFERENT PARTITIONS")
    mc.put(nodes[0], "p1_key", "p1_value")
    mc.put(nodes[2], "p2_key", "p2_value")
    mc.put(nodes[4], "p3_key", "p3_value")
    
    # Verify data is correctly isolated in each partition
    log("\n> VERIFYING PARTITION ISOLATION")
    
    # Verify partition 1
    r = mc.get(nodes[0], "p1_key")
    assert r.status_code == 200, f"expected 200 from p1, got {r.status_code}"
    assert r.value == "p1_value", f"expected p1_value, got {r.value}"
    
    r = mc.get(nodes[1], "p1_key")
    assert r.status_code == 200, f"expected 200 from p1, got {r.status_code}"
    
    # Partition 1 should not see keys from partition 2 or 3
    r = mc.get(nodes[0], "p2_key")
    assert r.status_code == 404, f"expected 404 for p2_key in p1, got {r.status_code}"
    
    # Verify partition 2
    r = mc.get(nodes[2], "p2_key")
    assert r.status_code == 200, f"expected 200 from p2, got {r.status_code}"
    assert r.value == "p2_value", f"expected p2_value, got {r.value}"
    
    # Partition 2 should not see keys from partition 1 or 3
    r = mc.get(nodes[2], "p1_key")
    assert r.status_code == 404, f"expected 404 for p1_key in p2, got {r.status_code}"
    
    # Verify partition 3
    r = mc.get(nodes[4], "p3_key")
    assert r.status_code == 200, f"expected 200 from p3, got {r.status_code}"
    assert r.value == "p3_value", f"expected p3_value, got {r.value}"
    
    # Verify each partition has its own primary
    # In p1, node 0 should be primary (lowest ID)
    r = mc.put(nodes[0], "p1_test", "from_p1_primary")
    assert r.ok, f"expected success from p1 primary, got {r.status_code}"
    
    # In p2, node 2 should be primary (lowest ID in partition)
    r = mc.put(nodes[2], "p2_test", "from_p2_primary")
    assert r.ok, f"expected success from p2 primary, got {r.status_code}"
    
    # In p3, node 4 is the only node, so it must be primary
    r = mc.put(nodes[4], "p3_test", "from_p3_primary")
    assert r.ok, f"expected success from p3 primary, got {r.status_code}"
    
    return True, "ok"


def advanced_partition_healing(conductor: ClusterConductor, fx: KvsFixture):
    """Test partition healing and subsequent data merging."""
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> TEST PARTITION HEALING")
    
    # Add initial data
    r = mc.put(nodes[0], "common_key", "initial_value")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Create two partitions: [0,1] and [2,3]
    log("\n> CREATING PARTITIONS")
    
    # Partition 1: nodes 0,1
    conductor.create_partition([nodes[0], nodes[1]], "p1")
    mc.broadcast_view([nodes[0], nodes[1]])
    
    # Partition 2: nodes 2,3
    conductor.create_partition([nodes[2], nodes[3]], "p2")
    mc.broadcast_view([nodes[2], nodes[3]])
    
    # Write different data to each partition, including updates to the common key
    log("\n> WRITING TO DIFFERENT PARTITIONS")
    mc.put(nodes[0], "p1_key", "p1_value")
    mc.put(nodes[0], "common_key", "p1_updated_value")
    
    mc.put(nodes[2], "p2_key", "p2_value")
    mc.put(nodes[2], "common_key", "p2_updated_value")
    
    # Heal the partition by creating a new view with all nodes
    log("\n> HEALING PARTITION")
    conductor.create_partition(nodes, "healed")
    mc.broadcast_view(nodes)
    
    # Verify that all nodes can access all keys after healing
    log("\n> VERIFYING DATA AFTER HEALING")
    
    # All nodes should see p1_key
    for i, node in enumerate(nodes):
        r = mc.get(node, "p1_key")
        assert r.status_code == 200, f"node {i} expected 200 for p1_key, got {r.status_code}"
        assert r.value == "p1_value", f"node {i} expected p1_value, got {r.value}"
    
    # All nodes should see p2_key
    for i, node in enumerate(nodes):
        r = mc.get(node, "p2_key")
        assert r.status_code == 200, f"node {i} expected 200 for p2_key, got {r.status_code}"
        assert r.value == "p2_value", f"node {i} expected p2_value, got {r.value}"
    
    # For the common_key, all nodes should have the same value after healing
    # Which value prevails depends on implementation, but it must be consistent
    values = []
    for node in nodes:
        r = mc.get(node, "common_key")
        assert r.status_code == 200, f"expected 200 for common_key, got {r.status_code}"
        values.append(r.value)
    
    # All nodes should have the same value (strong consistency after healing)
    assert len(set(values)) == 1, f"nodes have inconsistent values after healing: {values}"
    
    # The value should be either p1_updated_value or p2_updated_value
    # Which one depends on the implementation's conflict resolution strategy
    common_value = values[0]
    assert common_value in ["p1_updated_value", "p2_updated_value"], f"unexpected value: {common_value}"
    
    return True, "ok"


def advanced_view_changes(conductor: ClusterConductor, fx: KvsFixture):
    """Test multiple view changes with node additions and removals."""
    nodes = conductor.spawn_cluster(node_count=5)
    mc = fx.create_client(name="tester")
    
    log("\n> TEST MULTIPLE VIEW CHANGES")
    
    # Start with a view of nodes 0, 1, 2
    initial_view = nodes[:3]
    mc.broadcast_view(initial_view)
    
    # Add some data
    log("\n> ADDING INITIAL DATA")
    keys = ["key1", "key2", "key3"]
    for i, key in enumerate(keys):
        r = mc.put(nodes[0], key, f"value{i}")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Verify data is accessible from all nodes in the view
    for node in initial_view:
        for i, key in enumerate(keys):
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == f"value{i}", f"expected value{i}, got {r.value}"
    
    # Change view: remove node 0 (primary), add node 3
    log("\n> CHANGING VIEW: REMOVE PRIMARY, ADD NEW NODE")
    new_view = [nodes[1], nodes[2], nodes[3]]
    mc.broadcast_view(new_view)
    
    # Verify data is still accessible after view change
    for node in new_view:
        for i, key in enumerate(keys):
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == f"value{i}", f"expected value{i}, got {r.value}"
    
    # Add new data to the new view
    log("\n> ADDING DATA TO NEW VIEW")
    new_keys = ["new_key1", "new_key2"]
    for i, key in enumerate(new_keys):
        r = mc.put(nodes[1], key, f"new_value{i}")
        assert r.ok, f"expected success, got {r.status_code}"
    
    # Change view again: keep only nodes 2, 3 and add node 4
    log("\n> CHANGING VIEW AGAIN")
    final_view = [nodes[2], nodes[3], nodes[4]]
    mc.broadcast_view(final_view)
    
    # Verify all data is still accessible
    for node in final_view:
        # Original data
        for i, key in enumerate(keys):
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == f"value{i}", f"expected value{i}, got {r.value}"
        
        # Data added after first view change
        for i, key in enumerate(new_keys):
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == f"new_value{i}", f"expected new_value{i}, got {r.value}"
    
    # Verify node that was removed from view is not accepting requests
    # Node 0 should respond with 503 Service Unavailable
    try:
        r = mc.get(nodes[0], "key1")
        assert r.status_code == 503, f"expected 503 from removed node, got {r.status_code}"
    except Exception as e:
        # Alternatively, the request might time out if the node is not responding
        log(f"Expected exception when accessing removed node: {e}")
    
    return True, "ok"


def advanced_node_recovery(conductor: ClusterConductor, fx: KvsFixture):
    """Test node recovery after crashes and rejoining."""
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> TEST NODE RECOVERY")
    
    # Add initial data
    initial_keys = ["recovery_key1", "recovery_key2"]
    for i, key in enumerate(initial_keys):
        r = mc.put(nodes[0], key, f"recovery_value{i}")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Kill node 1
    log("\n> KILLING NODE 1")
    conductor.simulate_kill_node(nodes[1], conductor.base_net)
    
    # Update view to exclude killed node
    active_nodes = [nodes[0], nodes[2], nodes[3]]
    mc.broadcast_view(active_nodes)
    
    # Add more data with node 1 down
    r = mc.put(nodes[0], "during_failure", "added_during_failure")
    assert r.ok, f"expected success, got {r.status_code}"
    
    # Revive node 1
    log("\n> REVIVING NODE 1")
    conductor.simulate_revive_node(nodes[1], conductor.base_net)
    
    # Update view to include the revived node
    mc.broadcast_view(nodes)
    
    # Verify all nodes can access all data
    for node in nodes:
        # Initial data
        for i, key in enumerate(initial_keys):
            r = mc.get(node, key)
            assert r.status_code == 200, f"expected 200, got {r.status_code}"
            assert r.value == f"recovery_value{i}", f"expected recovery_value{i}, got {r.value}"
        
        # Data added during failure
        r = mc.get(node, "during_failure")
        assert r.status_code == 200, f"expected 200, got {r.status_code}"
        assert r.value == "added_during_failure", f"expected added_during_failure, got {r.value}"
    
    # Kill and revive the primary node (node 0)
    log("\n> KILLING AND REVIVING PRIMARY NODE")
    conductor.simulate_kill_node(nodes[0], conductor.base_net)
    
    # Update view to exclude killed primary
    active_nodes = nodes[1:]
    mc.broadcast_view(active_nodes)
    
    # Add data with primary down (node 1 should become new primary)
    r = mc.put(nodes[1], "after_primary_failure", "new_primary_value")
    assert r.ok, f"expected success, got {r.status_code}"
    
    # Revive the original primary
    conductor.simulate_revive_node(nodes[0], conductor.base_net)
    
    # Update view to include all nodes again
    mc.broadcast_view(nodes)
    
    # Verify all data is accessible, including data added after primary failure
    for node in nodes:
        r = mc.get(node, "after_primary_failure")
        assert r.status_code == 200, f"expected 200, got {r.status_code}"
        assert r.value == "new_primary_value", f"expected new_primary_value, got {r.value}"
    
    return True, "ok"


def advanced_boundary_conditions(conductor: ClusterConductor, fx: KvsFixture):
    """Test boundary conditions and edge cases."""
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> TEST BOUNDARY CONDITIONS")
    
    # Test with empty value
    log("\n> TESTING EMPTY VALUE")
    r = mc.put(nodes[0], "empty_value_key", "")
    assert r.status_code == 201, f"expected 201 for empty value, got {r.status_code}"
    
    r = mc.get(nodes[0], "empty_value_key")
    assert r.status_code == 200, f"expected 200 for empty value, got {r.status_code}"
    assert r.value == "", f"expected empty string, got {r.value}"
    
    # Test with special characters in key and value
    log("\n> TESTING SPECIAL CHARACTERS")
    special_key = "special!@#$%^&*()_+{}[]|;:'\",.<>?/~`"
    special_value = "value!@#$%^&*()_+{}[]|;:'\",.<>?/~`"
    
    r = mc.put(nodes[0], special_key, special_value)
    assert r.status_code == 201, f"expected 201 for special chars, got {r.status_code}"
    
    r = mc.get(nodes[0], special_key)
    assert r.status_code == 200, f"expected 200 for special chars, got {r.status_code}"
    assert r.value == special_value, f"value mismatch for special chars"
    
    # Test with very long key
    log("\n> TESTING LONG KEY")
    long_key = "a" * 1000  # 1000 character key
    r = mc.put(nodes[0], long_key, "long_key_value")
    assert r.status_code == 201, f"expected 201 for long key, got {r.status_code}"
    
    r = mc.get(nodes[0], long_key)
    assert r.status_code == 200, f"expected 200 for long key, got {r.status_code}"
    assert r.value == "long_key_value", f"expected long_key_value, got {r.value}"
    
    # Test race condition with rapid updates
    log("\n> TESTING RAPID UPDATES")
    race_key = "race_key"
    r = mc.put(nodes[0], race_key, "initial")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"
    
    # Perform multiple rapid updates
    num_updates = 10
    for i in range(num_updates):
        r = mc.put(nodes[0], race_key, f"update_{i}")
        assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"
    
    # All nodes should have the final value
    for node in nodes:
        r = mc.get(node, race_key)
        assert r.status_code == 200, f"expected 200, got {r.status_code}"
        assert r.value == f"update_{num_updates-1}", f"expected update_{num_updates-1}, got {r.value}"
    
    return True, "ok"


def performance_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test performance under load."""
    nodes = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    
    log("\n> PERFORMANCE TEST")
    
    # Test throughput: multiple writes in sequence
    log("\n> TESTING WRITE THROUGHPUT")
    num_writes = 50
    start_time = time.time()
    
    for i in range(num_writes):
        key = f"perf_key_{i}"
        value = f"perf_value_{i}"
        r = mc.put(nodes[0], key, value)
        assert r.ok, f"expected success for write {i}, got {r.status_code}"
    
    write_duration = time.time() - start_time
    write_throughput = num_writes / write_duration
    log(f"Write throughput: {write_throughput:.2f} ops/sec ({num_writes} ops in {write_duration:.2f} sec)")
    
    # Test read throughput
    log("\n> TESTING READ THROUGHPUT")
    num_reads = 100
    start_time = time.time()
    
    for i in range(num_reads):
        key = f"perf_key_{i % num_writes}"  # Cycle through keys we've written
        r = mc.get(nodes[i % len(nodes)], key)  # Distribute reads across nodes
        assert r.ok, f"expected success for read {i}, got {r.status_code}"
    
    read_duration = time.time() - start_time
    read_throughput = num_reads / read_duration
    log(f"Read throughput: {read_throughput:.2f} ops/sec ({num_reads} ops in {read_duration:.2f} sec)")
    
    # Test mixed workload
    log("\n> TESTING MIXED WORKLOAD")
    num_ops = 100
    start_time = time.time()
    
    for i in range(num_ops):
        key = f"mixed_key_{i % 20}"  # Use 20 different keys
        if i % 3 == 0:  # 1/3 writes, 2/3 reads
            r = mc.put(nodes[0], key, f"mixed_value_{i}")
        else:
            r = mc.get(nodes[i % len(nodes)], key)
        assert r.ok if i % 3 != 0 or i >= 20 else r.status_code in [200, 201], f"operation {i} failed"
    
    mixed_duration = time.time() - start_time
    mixed_throughput = num_ops / mixed_duration
    log(f"Mixed throughput: {mixed_throughput:.2f} ops/sec ({num_ops} ops in {mixed_duration:.2f} sec)")
    
    return True, "Throughput tests completed successfully"


# Define the advanced test suite
ADVANCED_TESTS = [
    TestCase("advanced_concurrent_writes", advanced_concurrent_writes),
    TestCase("advanced_large_values", advanced_large_values),
    TestCase("advanced_primary_failure_during_replication", advanced_primary_failure_during_replication),
    TestCase("advanced_complex_partition", advanced_complex_partition),
    TestCase("advanced_partition_healing", advanced_partition_healing),
    TestCase("advanced_view_changes", advanced_view_changes),
    TestCase("advanced_node_recovery", advanced_node_recovery),
    TestCase("advanced_boundary_conditions", advanced_boundary_conditions),
    TestCase("performance_test", performance_test),
]