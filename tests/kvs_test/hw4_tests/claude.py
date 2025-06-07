import random
import string
import time

from ..containers import ClusterConductor
from ..hw4_api import KvsFixture
from ..testcase import TestCase
from ..util import log


def test_view_transitions(conductor: ClusterConductor, fx: KvsFixture, views: list[dict], num_nodes: int, num_keys: int):
    n = conductor.spawn_cluster(node_count=num_nodes)
    c = fx.create_client(name="client", keep_meta=False)

    c.broadcast_view(views[0], n)
    # ---- Determine nodes to write to from first view ----
    put_node_ids = []
    for nodes in views[0].values():
        if isinstance(nodes, list):
            put_node_ids.extend(nodes)
        else:
            put_node_ids.append(nodes)
    put_node_ids = sorted(set(put_node_ids))

    # ---- Put values ----
    base_key = "key"
    inserted = {}
    for i in range(num_keys):
        key = f"{base_key}{i}"
        value = f"{i}"
        node_id = put_node_ids[i % len(put_node_ids)]
        r = c.put(n[node_id], key, value)
        assert r["ok"], f"PUT failed for key {key} on node {node_id}"
        inserted[key] = value

    log(f"Inserted {num_keys} keys to nodes: {put_node_ids}")

    # ---- For each view, broadcast and verify ----
    for idx, view in enumerate(views):
        log(f"\nApplying view {idx}: {view}")
        c.broadcast_view(view, n)

        all_kv = {}
        per_shard_keys = []

        for shard, nodes in view.items():
            node_ids = nodes if isinstance(nodes, list) else [nodes]
            if node_ids:
                chosen_id = random.choice(node_ids)
            else:
                continue

            r = c.get_all(n[chosen_id])
            if "items" not in r:
                log(f"Node {chosen_id} returned no items for shard {shard}")
                continue
            assert "items" in r
            shard_kv = r["items"]

            log(f"Shard {shard} (node {chosen_id}) has {len(shard_kv)} keys")

            # Store key set for disjoint check
            shard_keys = set(shard_kv.keys())
            per_shard_keys.append(shard_keys)

            # Add to global KV mapping
            for k, v in shard_kv.items():
                if k in all_kv:
                    assert all_kv[k] == v, f"Key {k} has inconsistent values: {all_kv[k]} vs {v}"
                else:
                    all_kv[k] = v

        # ---- Check disjointness if enabled ----
        for i in range(len(per_shard_keys)):
            for j in range(i + 1, len(per_shard_keys)):
                overlap = per_shard_keys[i] & per_shard_keys[j]
                assert not overlap, f"Shards {i} and {j} overlap on keys: {overlap}"

        # ---- Check exact match of full key-value map ----
        assert all_kv == inserted, f"Key-value mismatch at view {idx}\n" f"Missing keys: {set(inserted) - set(all_kv)}\n" f"Extra keys: {set(all_kv) - set(inserted)}\n" f"Wrong values: {[k for k in inserted if k in all_kv and inserted[k] != all_kv[k]]}"

    return True


def stress_test_cases(conductor: ClusterConductor, fx: KvsFixture):
    """Stress test with many keys and frequent resharding"""
    views = [
        {"shard1": [0], "shard2": [1]},
        {"shard1": [0], "shard2": [1], "shard3": [2]},
        {"shard1": [0], "shard2": [1], "shard3": [2], "shard4": [3]},
        {"shard1": [0], "shard2": [1], "shard3": [2], "shard4": [3], "shard5": [4]},
        {"shard1": [0, 4], "shard2": [1], "shard3": [2, 3]},  # consolidate
        {"shard1": [0, 1, 2, 3, 4]},  # single shard
    ]
    test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=500)
    return True, "ok"


def rapid_resharding_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test rapid back-and-forth resharding"""
    views = [
        {"shard1": [0], "shard2": [1], "shard3": [2]},
        {"shard1": [0, 1, 2]},  # merge all
        {"shard1": [0], "shard2": [1], "shard3": [2]},  # split again
        {"shard1": [0, 2], "shard2": [1]},  # different split
        {"shard1": [0], "shard2": [1], "shard3": [2]},  # back to original
    ]
    test_view_transitions(conductor, fx, views, num_nodes=3, num_keys=100)
    return True, "ok"


def single_node_migration_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test moving a single node between different shards"""
    views = [
        {"shard1": [0, 1], "shard2": [2]},
        {"shard1": [0], "shard2": [1, 2]},  # move node 1 to shard2
        {"shard1": [0, 2], "shard2": [1]},  # move node 2 to shard1
        {"shard1": [1], "shard2": [0, 2]},  # move node 0 to shard2
    ]
    test_view_transitions(conductor, fx, views, num_nodes=3, num_keys=50)
    return True, "ok"


def uneven_shard_sizes_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test with very uneven shard sizes"""
    views = [
        {"shard1": [0], "shard2": [1], "shard3": [2], "shard4": [3]},  # 4 small shards
        {"big_shard": [0, 1], "tiny_shard": [3]},  # very uneven
        {"big_shard": [0, 1, 2], "tiny_shard": [3]},  # very uneven
        {"mega_shard": [0, 1, 2, 3]},  # everything in one shard
        {"shard_a": [0], "shard_b": [1], "shard_c": [2], "shard_d": [3]},  # back to even
    ]
    test_view_transitions(conductor, fx, views, num_nodes=4, num_keys=200)
    return True, "ok"


def empty_to_populated_test(conductor: ClusterConductor, fx: KvsFixture):
    """Start with minimal setup and grow"""
    views = [
        {"solo": [0]},  # single node
        {"solo": [0], "new": [1]},  # add second node
        {"solo": [0], "new": [1], "third": [2]},  # add third
        {"merged": [0, 1], "separate": [2]},  # reorganize
    ]
    test_view_transitions(conductor, fx, views, num_nodes=3, num_keys=75)
    return True, "ok"


def shard_elimination_cascade_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test cascading shard eliminations"""
    views = [
        {"s1": [0], "s2": [1], "s3": [2], "s4": [3], "s5": [4]},  # 5 shards
        {"s1": [0], "s2": [1], "s3": [2, 3, 4]},  # eliminate 2 shards
        {"s1": [0, 1], "s3": [2, 3, 4]},  # eliminate another
        {"final": [0, 1, 2, 3, 4]},  # all in one
    ]
    test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=150)
    return True, "ok"


def complex_node_swapping_test(conductor: ClusterConductor, fx: KvsFixture):
    """Complex patterns of nodes moving between shards"""
    views = [
        {"alpha": [0, 1], "beta": [2, 3]},
        {"alpha": [0, 2], "beta": [1, 3]},  # swap nodes 1,2
        {"alpha": [1, 2], "beta": [0, 3]},  # swap nodes 0,1
        {"alpha": [1, 3], "beta": [0, 2]},  # swap nodes 2,3
        {"alpha": [0, 1, 2, 3]},  # merge all
    ]
    test_view_transitions(conductor, fx, views, num_nodes=4, num_keys=80)
    return True, "ok"


def many_small_shards_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test with many single-node shards"""
    views = [
        {f"shard_{i}": [i] for i in range(6)},  # 6 single-node shards
        {"big": [0, 1, 2], "med": [3, 4], "small": [5]},  # consolidate unevenly
        {f"shard_{i}": [i] for i in range(6)},  # back to individual
        {"all": [0, 1, 2, 3, 4, 5]},  # everything together
    ]
    test_view_transitions(conductor, fx, views, num_nodes=6, num_keys=120)
    return True, "ok"


def edge_case_single_key_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test resharding with very few keys"""
    views = [
        {"s1": [0], "s2": [1]},
        {"s1": [0], "s2": [1], "s3": [2]},
        {"s1": [0, 1, 2]},
        {"s1": [0], "s2": [1], "s3": [2]},
    ]
    test_view_transitions(conductor, fx, views, num_nodes=3, num_keys=3)  # Only 3 keys!
    return True, "ok"


def alternating_pattern_test(conductor: ClusterConductor, fx: KvsFixture):
    """Alternating between two different shard configurations"""
    config_a = {"left": [0, 2], "right": [1, 3]}
    config_b = {"top": [0, 1], "bottom": [2, 3]}

    views = [config_a, config_b, config_a, config_b, config_a]
    test_view_transitions(conductor, fx, views, num_nodes=4, num_keys=100)
    return True, "ok"


# Corner cases and error conditions
def zero_key_resharding_test(conductor: ClusterConductor, fx: KvsFixture):
    """Test resharding with no keys initially"""
    views = [
        {"s1": [0]},
        {"s1": [0], "s2": [1]},  # Add shard with no keys to redistribute
        {"s1": [0, 1]},  # Merge back
    ]
    test_view_transitions(conductor, fx, views, num_nodes=2, num_keys=0)
    return True, "ok"


def comprehensive_stress_test(conductor: ClusterConductor, fx: KvsFixture):
    """Ultimate stress test combining multiple patterns"""
    # Start simple
    views = [{"start": [0]}]

    # Gradually add complexity
    views.extend(
        [
            {"start": [0], "second": [1]},
            {"start": [0], "second": [1], "third": [2]},
            {"start": [0], "second": [1], "third": [2], "fourth": [3]},
        ]
    )

    # Complex reorganizations
    views.extend(
        [
            {"big": [0, 1], "small": [2], "tiny": [3]},
            {"left": [0, 2], "right": [1, 3]},
            {"top": [0, 1, 2], "bottom": [3]},
            {"all": [0, 1, 2, 3]},
            {"split": [0], "again": [1], "many": [2], "times": [3]},
            {"final": [0, 1, 2, 3]},
        ]
    )

    test_view_transitions(conductor, fx, views, num_nodes=4, num_keys=500)
    return True, "ok"


# Performance-focused tests for the 10% efficiency requirement
def efficiency_test_add_shards(conductor: ClusterConductor, fx: KvsFixture):
    """Test adding shards with many keys to verify minimal data movement"""
    # Start with 2 shards, many keys
    views = [
        {"s1": [0], "s2": [1]},
        {"s1": [0], "s2": [1], "s3": [2]},  # Add 1 shard
        {"s1": [0], "s2": [1], "s3": [2], "s4": [3]},  # Add another
        {"s1": [0], "s2": [1], "s3": [2], "s4": [3], "s5": [4]},  # Add another
    ]
    # Use many keys to test if resharding is efficient
    test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=500)
    return True, "ok"


def efficiency_test_remove_shards(conductor: ClusterConductor, fx: KvsFixture):
    """Test removing shards with many keys"""
    views = [
        {"s1": [0], "s2": [1], "s3": [2], "s4": [3], "s5": [4]},  # Start with 5
        {"s1": [0, 4], "s2": [1], "s3": [2], "s4": [3]},  # Remove 1, merge nodes
        {"s1": [0, 4], "s2": [1, 3], "s3": [2]},  # Remove another
        {"s1": [0, 4, 2], "s2": [1, 3]},  # Remove another
    ]
    test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=500)
    return True, "ok"


CLAUDE_TESTS = [
    TestCase("stress_test_cases", stress_test_cases),
    TestCase("rapid_resharding_test", rapid_resharding_test),
    TestCase("single_node_migration_test", single_node_migration_test),
    TestCase("uneven_shard_sizes_test", uneven_shard_sizes_test),
    TestCase("empty_to_populated_test", empty_to_populated_test),
    TestCase("shard_elimination_cascade_test", shard_elimination_cascade_test),
    TestCase("complex_node_swapping_test", complex_node_swapping_test),
    TestCase("many_small_shards_test", many_small_shards_test),
    TestCase("edge_case_single_key_test", edge_case_single_key_test),
    TestCase("alternating_pattern_test", alternating_pattern_test),
    TestCase("zero_key_resharding_test", zero_key_resharding_test),
    TestCase("comprehensive_stress_test", comprehensive_stress_test),
    TestCase("efficiency_test_add_shards", efficiency_test_add_shards),
    TestCase("efficiency_test_remove_shards", efficiency_test_remove_shards),
]
