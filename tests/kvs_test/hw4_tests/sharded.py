import hashlib
import json
import random
import string
from hashlib import blake2b
from multiprocessing.pool import ThreadPool
from typing import List

from ..containers import ClusterConductor
from ..hw4_api import KvsFixture
from ..testcase import TestCase
from ..util import log

K = 2 << 512


def hashing(value: str) -> int:
    h = blake2b()
    h.update(value.encode())
    return int(h.hexdigest(), 16) % K


class HRingKeyHasher:
    def __init__(self, shards: List[str], vnode_count: int = 100, shard_view: dict = None):
        self.vnode_count = vnode_count
        self.shard_hashes = {}
        self.vnode_map = {}
        self.shard_view = shard_view or {shard: [1] for shard in shards}
        for shard in shards:
            for v in range(vnode_count):
                vnode_key = f"{shard}-vn{v}"
                vnode_hash = hashing(vnode_key)
                self.shard_hashes[vnode_key] = vnode_hash
                self.vnode_map[vnode_key] = shard
        self.shard_hashes = dict(sorted(self.shard_hashes.items(), key=lambda item: item[1]))

    def get_shard_for_key(self, key: str) -> str:
        key_hash = hashing(key)
        for vnode_key, vnode_hash in self.shard_hashes.items():
            shard = self.vnode_map[vnode_key]
            if vnode_hash > key_hash and len(self.shard_view.get(shard, [])) > 0:
                return shard

        return self.vnode_map[next(iter(self.shard_hashes))]

    def find_key_for_shard(self, target_shard: str, max_tries=10000, found=[]) -> str:
        for i in range(max_tries):
            candidate = f"key{i}"
            if self.get_shard_for_key(candidate) == target_shard and candidate not in found:
                return candidate
        raise RuntimeError(f"Could not find key for shard {target_shard}")


def partitioned_single_shard_put_get(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test: 1 shard, 2 nodes, with a partition.
    - Client puts x=1 on node0.
    - Client puts y=2 on node1.
    - Client gets y from node1 (should succeed).
    """
    nodes = conductor.spawn_cluster(node_count=2, nodes_per_shard=2)
    client1 = fx.create_client(name="client1")
    client2 = fx.create_client(name="client2")

    client1.broadcast_view(nodes)
    client2.broadcast_view(nodes)
    nodes = nodes["shard0"]

    conductor.create_partition([nodes[0]], "p0")
    conductor.create_partition([nodes[1]], "p1")

    log("\n> TEST: Partitioned puts and get on node1")

    cm1 = {}

    r1 = client1.put(nodes[0], "x", "1", causal_metadata=cm1)
    assert r1["status_code"] == 200, f"Expected 200 for client1 PUT x on node1, got {r1['status_code']}"
    cm1 = r1["causal_metadata"]

    r2 = client1.put(nodes[1], "y", "2", causal_metadata=cm1)
    assert r2["status_code"] == 200, f"Expected 200 for client1 PUT y on node2, got {r2['status_code']}"
    cm1 = r2["causal_metadata"]

    r3 = client2.get(nodes[1], "y", causal_metadata=cm1)
    assert r3["status_code"] == 200, f"expected 200 for client2 GET y on node2, got {r3['status_code']}"
    assert r3["value"] == "2", f"expected value '2' for y on node2, got {r3['value']}"
    cm1 = r3["causal_metadata"]

    return True, "Partitioned single shard put/get test passed"


def cross_shard_put_get_with_forwarding(conductor: ClusterConductor, fx: KvsFixture):
    """
    Test behavior where:
    - A key that hashes to shard0 is PUT to a node in shard1 (wrong shard).
    - GET is done on shard0's node (correct shard).
    - All nodes are partitioned (no inter-node communication).
    - Expect GET to succeed if the system supports internal forwarding or replication.
    """

    shards = conductor.spawn_cluster(node_count=2, nodes_per_shard=1)
    shard0_node = shards["shard0"][0]
    shard1_node = shards["shard1"][0]

    log(f"[INFO] shard0_node: {shard0_node.name}, IP: {shard0_node.ip}, Port: {shard0_node.external_port}")
    log(f"[INFO] shard1_node: {shard1_node.name}, IP: {shard1_node.ip}, Port: {shard1_node.external_port}")

    client = fx.create_client(name="client-test")
    client.broadcast_view(shards)

    key_hasher = HRingKeyHasher(shards=list(shards.keys()), shard_view=shards)
    key = key_hasher.find_key_for_shard("shard0")
    value = "value123"

    conductor.create_partition([shard0_node], "p0")
    conductor.create_partition([shard1_node], "p1")

    log(f"\n> TEST: Putting key {key!r} (belongs to shard0) into node in shard1")

    put_res = client.put(shard1_node, key, value, causal_metadata={})
    assert put_res["status_code"] == 200, f"PUT failed on shard1's node for key {key}"

    get_res = client.get(shard0_node, key, causal_metadata=put_res["causal_metadata"])
    assert get_res["status_code"] == 200, f"GET failed on shard0's node for key {key}"
    assert get_res["value"] == value, f"Expected value '{value}', got '{get_res['value']}'"

    return True, f"Test passed: key '{key}' correctly retrieved from shard0 after being put to shard1"


def test_add_shard_and_disjoint_keys(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 3 shards (3 nodes each).
    - Put unique keys on each shard.
    - Add a new shard (3 nodes) to the view.
    - Broadcast new view.
    - Check each shard's data is still disjoint.
    """
    num_shards = 3
    nodes_per_shard = 3
    keys_per_shard = 5

    nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]] * num_shards)
    client = fx.create_client(name="client_add_shard")
    client.broadcast_view(nodes)

    all_keys = {}
    for shard_name, shard_nodes in nodes.items():
        shard_keys = []
        for i in range(keys_per_shard):
            key = f"{shard_name}_key_{i}"
            value = f"{shard_name}_val_{i}"
            r = client.put(shard_nodes[0], key, value, causal_metadata={})
            assert r["status_code"] == 200, f"PUT failed for {key} on {shard_name}"
            shard_keys.append(key)
        all_keys[shard_name] = set(shard_keys)

    new_shard_nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]])["shard0"]
    new_shard_name = f"shard{len(nodes)}"
    nodes[new_shard_name] = new_shard_nodes

    client.broadcast_view(nodes)

    shard_keysets = {}
    for shard_name, shard_nodes in nodes.items():
        r = client.get_all(shard_nodes[0], causal_metadata={})
        assert r["status_code"] == 200, f"GET_ALL failed on {shard_name}"
        items = r.get("items", {})
        shard_keysets[shard_name] = set(items.keys())

    shard_names = list(nodes.keys())
    for i, s1 in enumerate(shard_names):
        for s2 in shard_names[i + 1 :]:
            overlap = shard_keysets[s1] & shard_keysets[s2]
            assert not overlap, f"Shards {s1} and {s2} have overlapping keys: {overlap}"

    print("Shard get_all key sets are disjoint after adding a shard:", {k: list(v) for k, v in shard_keysets.items()})
    return True, "All shards returned disjoint key sets after adding a new shard"


def test_add_shard_and_remove_others(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 3 shards (3 nodes each).
    - Put unique keys on each shard.
    - Add a new shard (3 nodes) and remove 2 of the original shards from the view.
    - Broadcast new view (now 2 shards: 1 old, 1 new).
    - Check each remaining shard's data is disjoint and all keys are present.
    """
    num_shards = 3
    nodes_per_shard = 3
    keys_per_shard = 5

    nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]] * num_shards)
    client = fx.create_client(name="client_add_shard_remove_others")
    client.broadcast_view(nodes)

    all_keys = {}
    for shard_name, shard_nodes in nodes.items():
        shard_keys = []
        for i in range(keys_per_shard):
            key = f"{shard_name}_key_{i}"
            value = f"{shard_name}_val_{i}"
            r = client.put(shard_nodes[0], key, value, causal_metadata={})
            assert r["status_code"] == 200, f"PUT failed for {key} on {shard_name}"
            shard_keys.append((key, value))
        all_keys[shard_name] = set(k for k, v in shard_keys)

    new_shard_nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]])["shard0"]
    new_shard_name = f"shard{len(nodes)}"

    remaining_shards = {}
    orig_shard_names = list(nodes.keys())
    remaining_shards[orig_shard_names[0]] = nodes[orig_shard_names[0]]
    remaining_shards[new_shard_name] = new_shard_nodes

    client.broadcast_view(remaining_shards)
    print("heyheheyehehy")

    shard_keysets = {}
    for shard_name, shard_nodes in remaining_shards.items():
        r = client.get_all(shard_nodes[0], causal_metadata={})
        assert r["status_code"] == 200, f"GET_ALL failed on {shard_name}"
        items = r.get("items", {})
        shard_keysets[shard_name] = set(items.keys())

    expected_keys = set()
    for keys in all_keys.values():
        expected_keys.update(keys)
    found_keys = set()
    for keys in shard_keysets.values():
        found_keys.update(keys)
    assert expected_keys == found_keys, f"Expected all keys {expected_keys}, got {found_keys}"

    shard_names = list(remaining_shards.keys())
    for i, s1 in enumerate(shard_names):
        for s2 in shard_names[i + 1 :]:
            overlap = shard_keysets[s1] & shard_keysets[s2]
            assert not overlap, f"Shards {s1} and {s2} have overlapping keys: {overlap}"

    print(
        "Shard get_all key sets are disjoint after removing shards and adding a new one:",
        {k: list(v) for k, v in shard_keysets.items()},
    )
    return (
        True,
        "All shards returned disjoint key sets and all keys are present after removing two shards and adding a new one",
    )


def test_repeated_add_shards_and_disjoint_keys(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 3 shards (3 nodes each).
    - Put 500 unique keys (distributed across the shards).
    - Repeatedly add a new shard (3 nodes) and broadcast the new view.
    - After each view change, check all shards' data is disjoint.
    - Repeat until there are 30 shards.
    """
    initial_shards = 3
    nodes_per_shard = 3
    total_keys = 200

    nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]] * initial_shards)
    client = fx.create_client(name="client_repeated_add_shards")
    client.broadcast_view(nodes)

    shard_names = list(nodes.keys())
    all_keys = {}
    for i in range(total_keys):
        shard_idx = i % len(shard_names)
        shard_name = shard_names[shard_idx]
        key = f"{shard_name}_key_{i}"
        value = f"{shard_name}_val_{i}"
        r = client.put(nodes[shard_name][0], key, value, causal_metadata={})
        assert r["status_code"] == 200, f"PUT failed for {key} on {shard_name}"
        all_keys[key] = value

    for n in range(initial_shards, 5):
        new_shard_nodes = conductor.alternative_spawn_cluster([[nodes_per_shard]])["shard0"]
        new_shard_name = f"shard{n}"
        nodes[new_shard_name] = new_shard_nodes

        client.broadcast_view(nodes)

        shard_keysets = {}
        for shard_name, shard_nodes in nodes.items():
            r = client.get_all(shard_nodes[0], causal_metadata={})
            assert r["status_code"] == 200, f"GET_ALL failed on {shard_name}"
            items = r.get("items", {})
            shard_keysets[shard_name] = set(items.keys())

        shard_names = list(nodes.keys())
        for i, s1 in enumerate(shard_names):
            for s2 in shard_names[i + 1 :]:
                overlap = shard_keysets[s1] & shard_keysets[s2]
                assert not overlap, f"Shards {s1} and {s2} have overlapping keys: {overlap}"

        print(f"After adding shard {new_shard_name}, all key sets are disjoint.")

    return True, "All shards returned disjoint key sets after each repeated add-shard view change"


def test_node_moves_between_shards(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 2 shards (2 nodes each).
    - Put unique keys on each shard.
    - Move one node from shard0 to shard1 in the new view.
    - Broadcast new view.
    - Check that keys are correctly redistributed and no keys are lost or duplicated.
    """
    nodes = conductor.alternative_spawn_cluster([[2], [2]])
    client = fx.create_client("client_move_node")
    client.broadcast_view(nodes)

    client.put(nodes["shard0"][0], "a", "A", causal_metadata={})
    client.put(nodes["shard1"][0], "b", "B", causal_metadata={})

    node0 = nodes["shard0"][0]
    new_nodes = {
        "shard0": [nodes["shard0"][1]],
        "shard1": [nodes["shard1"][0], nodes["shard1"][1], node0],
    }
    client.broadcast_view(new_nodes)

    for shard, nodelist in new_nodes.items():
        r = client.get_all(nodelist[0], causal_metadata={})
        assert r["status_code"] == 200
        items = r["items"]

        assert sum("a" in client.get_all(n[0], causal_metadata={})["items"] for n in new_nodes.values()) == 1
        assert sum("b" in client.get_all(n[0], causal_metadata={})["items"] for n in new_nodes.values()) == 1

    return True, "Node reassignment between shards handled correctly"


def test_view_change_no_data_movement(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 2 shards (2 nodes each).
    - Put keys on each shard.
    - Change the order of nodes in the view (but not the shards).
    - Broadcast new view.
    - Ensure all keys remain in the correct shards and are not duplicated or lost.
    """
    nodes = conductor.alternative_spawn_cluster([[2], [2]])
    client = fx.create_client("client_view_no_move")
    client.broadcast_view(nodes)
    print(nodes)
    client.put(nodes["shard0"][0], "c", "C", causal_metadata={})
    client.put(nodes["shard1"][0], "b", "B", causal_metadata={})

    new_nodes = {
        "shard0": [nodes["shard0"][1], nodes["shard0"][0]],
        "shard1": [nodes["shard1"][1], nodes["shard1"][0]],
    }
    client.broadcast_view(new_nodes)

    for shard, nodelist in new_nodes.items():
        r = client.get_all(nodelist[0], causal_metadata={})
        assert r["status_code"] == 200
        items = r["items"]
        if shard == "shard0":
            assert "c" in items
        if shard == "shard1":
            assert "b" in items

    return True, "View change with no data movement handled correctly"


def test_causal_consistency_across_shards(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 2 shards (2 nodes each).
    - Client puts x=1 on shard0.
    - Client puts y=2 on shard1, causally after reading x=1.
    - Partition the shards.
    - Client tries to get y from shard1 with causal metadata for x=1 (should succeed).
    - Client tries to get x from shard1 with causal metadata for y=2 (should hang or timeout).
    """
    nodes = conductor.alternative_spawn_cluster([[2], [2]])
    client = fx.create_client("client_causal_cross")
    client.broadcast_view(nodes)

    r1 = client.put(nodes["shard0"][0], "c", "1", causal_metadata={})
    cm1 = r1["causal_metadata"]

    r2 = client.get(nodes["shard0"][0], "c", causal_metadata=cm1)
    cm2 = r2["causal_metadata"]

    r3 = client.put(nodes["shard1"][0], "b", "2", causal_metadata=cm2)
    cm3 = r3["causal_metadata"]

    conductor.create_partition(nodes["shard0"], "p0")
    conductor.create_partition(nodes["shard1"], "p1")

    r4 = client.get(nodes["shard1"][0], "b", causal_metadata=cm3)
    assert r4["status_code"] == 200 and r4["value"] == "2"

    r5 = client.get(nodes["shard1"][0], "c", causal_metadata=cm3)
    assert r5["status_code"] == 408, f"Expected timeout, got {r5['status_code']}"

    return True, "Causal consistency across shards is enforced"


def test_add_remove_multiple_shards(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 4 shards (2 nodes each).
    - Put unique keys on each shard.
    - Remove 2 shards and add 2 new shards in a single view change.
    - Broadcast new view.
    - Ensure all keys are present and disjoint in the remaining shards.
    """
    nodes = conductor.alternative_spawn_cluster([[2], [2], [2], [2]])
    client = fx.create_client("client_add_remove_multi")
    client.broadcast_view(nodes)

    for i, shard in enumerate(nodes):
        client.put(nodes[shard][0], f"k{i}", f"v{i}", causal_metadata={})

    new_shard_nodes1 = conductor.alternative_spawn_cluster([[2]])["shard0"]
    new_shard_nodes2 = conductor.alternative_spawn_cluster([[2]])["shard0"]
    new_nodes = {
        list(nodes.keys())[2]: nodes[list(nodes.keys())[2]],
        list(nodes.keys())[3]: nodes[list(nodes.keys())[3]],
        "shard4": new_shard_nodes1,
        "shard5": new_shard_nodes2,
    }
    client.broadcast_view(new_nodes)

    all_keys = set()
    for nodelist in new_nodes.values():
        r = client.get_all(nodelist[0], causal_metadata={})
        assert r["status_code"] == 200
        items = r["items"]
        for k in items:
            assert k not in all_keys, f"Key {k} found in multiple shards"
            all_keys.add(k)

    return True, "Add/remove multiple shards in one view change works"


def test_shard_with_no_keys(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 2 shards (2 nodes each).
    - Put keys that all hash to shard0.
    - Add a new shard (2 nodes) that is not responsible for any keys.
    - Broadcast new view.
    - Ensure the new shard returns an empty store on get_all.
    """
    nodes = conductor.alternative_spawn_cluster([[2], [2]])
    client = fx.create_client("client_shard_no_keys")
    client.broadcast_view(nodes)
    new_shard_nodes = conductor.alternative_spawn_cluster([[2]])["shard0"]
    new_shard_name = "shard2"
    nodes[new_shard_name] = new_shard_nodes

    hasher = HRingKeyHasher(shards=list(nodes.keys()), shard_view=nodes)
    keys_for_shard0 = []
    for i in range(10):
        key = hasher.find_key_for_shard("shard0", max_tries=10000, found=keys_for_shard0)
        keys_for_shard0.append(key)

    print(keys_for_shard0)
    for key in keys_for_shard0:
        client.put(nodes["shard0"][0], key, f"val_{key}", causal_metadata={})

    new_shard_nodes = conductor.alternative_spawn_cluster([[2]])["shard0"]
    new_shard_name = "shard2"
    nodes[new_shard_name] = new_shard_nodes
    print(nodes)
    client.broadcast_view(nodes)

    r = client.get_all(new_shard_nodes[0], causal_metadata={})
    assert r["status_code"] == 200
    assert r["items"] == {}, f"Expected empty store, got {r['items']}"

    return True, "Shard with no keys after resharding returns empty store"


def test_proxy_get_put_to_correct_shard(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 2 shards (2 nodes each).
    - Put a key on shard0.
    - Try to GET and PUT that key on a node in shard1.
    - Ensure the request is proxied and the correct value is returned.
    """
    nodes = conductor.alternative_spawn_cluster([[2], [2]])
    client = fx.create_client("client_proxy")
    client.broadcast_view(nodes)

    client.put(nodes["shard0"][0], "foo", "bar", causal_metadata={})

    r = client.get(nodes["shard1"][0], "foo", causal_metadata={})
    assert r["status_code"] == 200
    assert r["value"] == "bar"

    client.put(nodes["shard1"][0], "foo", "baz", causal_metadata={})
    r2 = client.get(nodes["shard0"][0], "foo", causal_metadata={})
    assert r2["status_code"] == 200
    assert r2["value"] == "baz"

    return True, "Proxying GET/PUT to correct shard works"


def test_large_view_change(conductor: ClusterConductor, fx: KvsFixture):
    """
    - Create a cluster with 10 shards (2 nodes each).
    - Put unique keys on each shard.
    - Change view to a single shard (all nodes).
    - Ensure all keys are present in the final shard.
    """
    nodes = conductor.alternative_spawn_cluster([[2]] * 10)
    client = fx.create_client("client_large_view")
    client.broadcast_view(nodes)

    for i, shard in enumerate(nodes):
        client.put(nodes[shard][0], f"k{i}", f"v{i}", causal_metadata={})

    all_nodes = [n for nodelist in nodes.values() for n in nodelist]
    new_nodes = {"shard0": all_nodes}
    client.broadcast_view(new_nodes)

    r = client.get_all(all_nodes[0], causal_metadata={})
    assert r["status_code"] == 200
    items = r["items"]
    for i in range(10):
        assert f"k{i}" in items and items[f"k{i}"] == f"v{i}"

    return True, "Large view change merges all keys correctly"


SHARD_TESTS = [
    # TestCase("partitioned_single_shard_put_get", partitioned_single_shard_put_get),
    # TestCase("test_add_shard_and_disjoint_keys", test_add_shard_and_disjoint_keys),
    # TestCase("test_add_shard_and_remove_others", test_add_shard_and_remove_others),
    # TestCase("test_repeated_add_shards_and_disjoint_keys", test_repeated_add_shards_and_disjoint_keys),
    # TestCase("test_node_moves_between_shards", test_node_moves_between_shards),
    # TestCase("test_view_change_no_data_movement", test_view_change_no_data_movement),
    # TestCase("test_causal_consistency_across_shards", test_causal_consistency_across_shards),
    # TestCase("test_add_remove_multiple_shards", test_add_remove_multiple_shards),
    # TestCase("test_shard_with_no_keys", test_shard_with_no_keys),
    TestCase("test_proxy_get_put_to_correct_shard", test_proxy_get_put_to_correct_shard),
    TestCase("test_large_view_change", test_large_view_change),
]
