# performance_tests.py
import time
from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log

def test_resharding_performance(conductor: ClusterConductor, fx: KvsFixture):
    """Test that resharding performance meets requirements (fast enough)."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Resharding Performance")
    
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
    
    # Add a substantial number of keys to test performance
    num_keys = 100
    test_keys = [f"perf_key_{i:04d}" for i in range(num_keys)]
    
    log(f"Adding {num_keys} keys for performance test...")
    start_time = time.time()
    
    for key in test_keys:
        value = f"performance_value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    insert_time = time.time() - start_time
    log(f"Data insertion took {insert_time:.2f} seconds ({num_keys/insert_time:.1f} ops/sec)")
    
    time.sleep(2)
    
    # Measure resharding time (add third shard)
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
    
    log("Starting resharding performance measurement...")
    reshard_start = time.time()
    
    for node in nodes:
        try:
            resp = requests.put(
                f"http://localhost:{node.external_port}/view",
                json={"view": new_view},
                timeout=15
            )
            assert resp.status_code == 200
        except Exception as e:
            return False, f"Failed to set new view: {e}"
    
    # Wait for resharding to complete and measure time
    time.sleep(1)  # Brief pause to let resharding start
    
    # Verify all keys are accessible (resharding should be done)
    verification_start = time.time()
    accessible_keys = 0
    
    for key in test_keys:
        get_response = client.get(nodes[0], key)
        if get_response["ok"]:
            accessible_keys += 1
    
    verification_time = time.time() - verification_start
    reshard_total_time = time.time() - reshard_start
    
    log(f"Resharding took {reshard_total_time:.2f} seconds")
    log(f"Verification of {accessible_keys}/{num_keys} keys took {verification_time:.2f} seconds")
    
    # Performance requirement: resharding should be "fast enough"
    # For 100 keys going from 2 to 3 shards, should complete within reasonable time
    assert reshard_total_time < 15.0, f"Resharding too slow: {reshard_total_time:.2f}s"
    assert accessible_keys == num_keys, f"Keys lost during resharding: {accessible_keys}/{num_keys}"
    
    # Check data distribution efficiency
    shard1_keys = len(client.get_all(nodes[0])["values"])
    shard2_keys = len(client.get_all(nodes[2])["values"])
    shard3_keys = len(client.get_all(nodes[4])["values"])
    
    log(f"Final distribution: Shard1={shard1_keys}, Shard2={shard2_keys}, Shard3={shard3_keys}")
    
    # Should have reasonable distribution (no shard with > 60% of keys)
    max_keys_per_shard = max(shard1_keys, shard2_keys, shard3_keys)
    max_percentage = (max_keys_per_shard / num_keys) * 100
    
    assert max_percentage <= 60, f"Poor key distribution: one shard has {max_percentage:.1f}% of keys"
    
    return True, "OK"

def test_throughput_with_sharding(conductor: ClusterConductor, fx: KvsFixture):
    """Test throughput performance with multiple shards."""
    nodes = conductor.spawn_cluster(node_count=6)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Throughput with Sharding")
    
    # Create 3-shard setup
    view = {
        "Fast1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Fast2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "Fast3": [
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
    
    # Test write throughput
    num_writes = 50
    write_keys = [f"throughput_write_{i:03d}" for i in range(num_writes)]
    
    log(f"Testing write throughput with {num_writes} operations...")
    write_start = time.time()
    
    for i, key in enumerate(write_keys):
        node_idx = i % len(nodes)  # Distribute writes across all nodes
        value = f"throughput_value_{i}"
        put_response = client.put(nodes[node_idx], key, value)
        assert put_response["ok"], f"Write failed for {key}"
    
    write_duration = time.time() - write_start
    write_throughput = num_writes / write_duration
    
    log(f"Write throughput: {write_throughput:.1f} ops/sec ({num_writes} ops in {write_duration:.2f}s)")
    
    time.sleep(1)
    
    # Test read throughput
    num_reads = 100
    
    log(f"Testing read throughput with {num_reads} operations...")
    read_start = time.time()
    
    for i in range(num_reads):
        key = write_keys[i % len(write_keys)]  # Cycle through written keys
        node_idx = i % len(nodes)  # Distribute reads across all nodes
        get_response = client.get(nodes[node_idx], key)
        assert get_response["ok"], f"Read failed for {key}"
    
    read_duration = time.time() - read_start
    read_throughput = num_reads / read_duration
    
    log(f"Read throughput: {read_throughput:.1f} ops/sec ({num_reads} ops in {read_duration:.2f}s)")
    
    # Performance expectations for sharded system
    assert write_throughput > 5.0, f"Write throughput too low: {write_throughput:.1f} ops/sec"
    assert read_throughput > 10.0, f"Read throughput too low: {read_throughput:.1f} ops/sec"
    
    return True, "OK"

def test_proxy_overhead(conductor: ClusterConductor, fx: KvsFixture):
    """Test the overhead of request proxying."""
    nodes = conductor.spawn_cluster(node_count=4)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Proxy Overhead")
    
    # Setup 2-shard system
    view = {
        "Local": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Remote": [
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
    
    # Add keys and determine their shard assignment
    test_keys = [f"proxy_key_{i:02d}" for i in range(20)]
    
    for key in test_keys:
        value = f"value_{key}"
        put_response = client.put(nodes[0], key, value)
        assert put_response["ok"], f"PUT failed for {key}"
    
    time.sleep(1)
    
    # Identify which keys are local vs remote from node 0's perspective
    local_keys = set(client.get_all(nodes[0])["values"].keys())
    remote_keys = set(test_keys) - local_keys
    
    log(f"From node 0: {len(local_keys)} local keys, {len(remote_keys)} remote keys")
    
    if len(local_keys) == 0 or len(remote_keys) == 0:
        log("Warning: All keys ended up in one shard, cannot measure proxy overhead")
        return True, "OK - Cannot measure proxy overhead with current distribution"
    
    # Measure local access time
    local_sample = list(local_keys)[:5]
    local_start = time.time()
    
    for key in local_sample:
        get_response = client.get(nodes[0], key)
        assert get_response["ok"], f"Local GET failed for {key}"
    
    local_duration = time.time() - local_start
    local_avg = local_duration / len(local_sample)
    
    # Measure remote access time (proxied)
    remote_sample = list(remote_keys)[:5]
    remote_start = time.time()
    
    for key in remote_sample:
        get_response = client.get(nodes[0], key)  # Should be proxied
        assert get_response["ok"], f"Remote GET failed for {key}"
    
    remote_duration = time.time() - remote_start
    remote_avg = remote_duration / len(remote_sample)
    
    proxy_overhead = remote_avg - local_avg
    overhead_percentage = (proxy_overhead / local_avg) * 100 if local_avg > 0 else 0
    
    log(f"Local access avg: {local_avg*1000:.1f}ms")
    log(f"Remote access avg: {remote_avg*1000:.1f}ms") 
    log(f"Proxy overhead: {proxy_overhead*1000:.1f}ms ({overhead_percentage:.1f}%)")
    
    # Proxy overhead should be reasonable (less than 10x slower)
    assert overhead_percentage < 1000, f"Proxy overhead too high: {overhead_percentage:.1f}%"
    
    return True, "OK"

def test_scalability_with_more_shards(conductor: ClusterConductor, fx: KvsFixture):
    """Test system behavior with increasing number of shards."""
    nodes = conductor.spawn_cluster(node_count=8)
    client = fx.create_client(name="client1")
    
    log("\n> TEST: Scalability with More Shards")
    
    # Test with 2, 4, and 8 shards to see scaling behavior
    shard_configs = [
        # 2 shards
        {
            "Alpha": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                     {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
            "Beta": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                    {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}]
        },
        # 4 shards  
        {
            "Alpha": [{"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
                     {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}],
            "Beta": [{"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
                    {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}],
            "Gamma": [{"address": f"{nodes[4].ip}:8081", "id": nodes[4].index},
                     {"address": f"{nodes[5].ip}:8081", "id": nodes[5].index}],
            "Delta": [{"address": f"{nodes[6].ip}:8081", "id": nodes[6].index},
                     {"address": f"{nodes[7].ip}:8081", "id": nodes[7].index}]
        }
    ]
    
    import requests
    
    for config_idx, view_config in enumerate(shard_configs):
        num_shards = len(view_config)
        log(f"\nTesting with {num_shards} shards...")
        
        # Apply configuration
        for node in nodes:
            try:
                resp = requests.put(
                    f"http://localhost:{node.external_port}/view",
                    json={"view": view_config},
                    timeout=10
                )
                assert resp.status_code == 200
            except Exception as e:
                return False, f"Failed to set {num_shards}-shard view: {e}"
        
        time.sleep(3)
        
        # Add test data for this configuration
        num_keys = 20
        config_keys = [f"scale_{num_shards}s_key_{i:02d}" for i in range(num_keys)]
        
        # Measure write performance
        write_start = time.time()
        for key in config_keys:
            value = f"scale_value_{key}"
            put_response = client.put(nodes[0], key, value)
            assert put_response["ok"], f"PUT failed for {key} in {num_shards}-shard config"
        
        write_duration = time.time() - write_start
        write_throughput = num_keys / write_duration
        
        log(f"  Write throughput: {write_throughput:.1f} ops/sec")
        
        time.sleep(1)
        
        # Measure read performance
        read_start = time.time()
        for key in config_keys:
            get_response = client.get(nodes[1], key)  # Different node to test proxying
            assert get_response["ok"], f"GET failed for {key} in {num_shards}-shard config"
        
        read_duration = time.time() - read_start
        read_throughput = num_keys / read_duration
        
        log(f"  Read throughput: {read_throughput:.1f} ops/sec")
        
        # Check distribution across shards
        shard_sizes = []
        node_indices = [0, 2, 4, 6]  # First node of each potential shard
        
        for i in range(num_shards):
            if i < len(node_indices):
                shard_data = client.get_all(nodes[node_indices[i]])["values"]
                shard_sizes.append(len(shard_data))
        
        log(f"  Distribution: {shard_sizes}")
        
        # Verify reasonable distribution
        if len(shard_sizes) > 1:
            max_size = max(shard_sizes)
            min_size = min(shard_sizes)
            imbalance = (max_size - min_size) / num_keys if num_keys > 0 else 0
            
            log(f"  Load imbalance: {imbalance:.2f}")
            assert imbalance <= 0.6, f"Too much load imbalance with {num_shards} shards: {imbalance:.2f}"
    
    return True, "OK"

def test_concurrent_load_performance(conductor: ClusterConductor, fx: KvsFixture):
    """Test performance under concurrent load from multiple clients."""
    nodes = conductor.spawn_cluster(node_count=6)
    clients = [fx.create_client(f"load_client_{i}") for i in range(3)]
    
    log("\n> TEST: Concurrent Load Performance")
    
    # Setup 3-shard system
    view = {
        "Load1": [
            {"address": f"{nodes[0].ip}:8081", "id": nodes[0].index},
            {"address": f"{nodes[1].ip}:8081", "id": nodes[1].index}
        ],
        "Load2": [
            {"address": f"{nodes[2].ip}:8081", "id": nodes[2].index},
            {"address": f"{nodes[3].ip}:8081", "id": nodes[3].index}
        ],
        "Load3": [
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
    
    # Each client performs operations concurrently
    operations_per_client = 20
    
    def client_workload(client_idx):
        client = clients[client_idx]
        client_keys = [f"concurrent_load_{client_idx}_{i:02d}" for i in range(operations_per_client)]
        
        # Write phase
        write_start = time.time()
        for key in client_keys:
            value = f"load_value_{client_idx}_{key}"
            node_idx = (client_idx + hash(key)) % len(nodes)
            put_response = client.put(nodes[node_idx], key, value)
            if not put_response["ok"]:
                return False, f"PUT failed for {key}"
        
        write_duration = time.time() - write_start
        
        # Read phase  
        read_start = time.time()
        for key in client_keys:
            node_idx = (client_idx + hash(key) + 1) % len(nodes)  # Different node
            get_response = client.get(nodes[node_idx], key)
            if not get_response["ok"]:
                return False, f"GET failed for {key}"
        
        read_duration = time.time() - read_start
        
        return True, {
            "write_duration": write_duration,
            "read_duration": read_duration,
            "write_throughput": operations_per_client / write_duration,
            "read_throughput": operations_per_client / read_duration
        }
    
    # Run concurrent workloads
    log("Starting concurrent workloads...")
    concurrent_start = time.time()
    
    import threading
    results = {}
    
    def run_client(client_idx):
        results[client_idx] = client_workload(client_idx)
    
    threads = []
    for i in range(len(clients)):
        thread = threading.Thread(target=run_client, args=(i,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    concurrent_duration = time.time() - concurrent_start
    
    # Analyze results
    successful_clients = 0
    total_write_throughput = 0
    total_read_throughput = 0
    
    for client_idx, result in results.items():
        if result[0]:  # Success
            successful_clients += 1
            stats = result[1]
            total_write_throughput += stats["write_throughput"]
            total_read_throughput += stats["read_throughput"]
            log(f"  Client {client_idx}: Write {stats['write_throughput']:.1f} ops/sec, Read {stats['read_throughput']:.1f} ops/sec")
    
    log(f"Concurrent execution took {concurrent_duration:.2f} seconds")
    log(f"Successful clients: {successful_clients}/{len(clients)}")
    log(f"Aggregate write throughput: {total_write_throughput:.1f} ops/sec")
    log(f"Aggregate read throughput: {total_read_throughput:.1f} ops/sec")
    
    # Performance expectations
    assert successful_clients == len(clients), f"Some clients failed: {successful_clients}/{len(clients)}"
    assert total_write_throughput > 10.0, f"Aggregate write throughput too low: {total_write_throughput:.1f}"
    assert total_read_throughput > 20.0, f"Aggregate read throughput too low: {total_read_throughput:.1f}"
    
    return True, "OK"

PERFORMANCE_TESTS = [
    TestCase("test_resharding_performance", test_resharding_performance),
    TestCase("test_throughput_with_sharding", test_throughput_with_sharding),
    TestCase("test_proxy_overhead", test_proxy_overhead),
    TestCase("test_scalability_with_more_shards", test_scalability_with_more_shards),
    TestCase("test_concurrent_load_performance", test_concurrent_load_performance),
]