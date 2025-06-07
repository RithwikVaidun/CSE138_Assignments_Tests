import random
import string
import time

from ..containers import ClusterConductor
from ..hw3_api import KvsFixture
from ..testcase import TestCase
from ..util import log


def basic_shuffle_add_remove(conductor: ClusterConductor, fx: KvsFixture):
    n = conductor.spawn_cluster(node_count=3)
    c = fx.create_client(name="client", keep_meta=False)

    shards = {"shard0": 0, "shard1": 1}
    c.broadcast_view(shards, n)

    node_to_put = 0
    base_key = "key"
    NUM_KEYS = 15
    for i in range(NUM_KEYS):
        r = c.put(n[node_to_put], f"{base_key}{i}", f"{i}")
        assert r["ok"], f"expected ok for new key, got {r.status_code}"
        node_to_put += 1
        node_to_put = node_to_put % 2

    log(f"Put {NUM_KEYS} keys\n")

    r = c.get_all(n[0])
    shard0 = r["items"]

    r = c.get_all(n[1])
    shard1 = r["items"]

    log(f"Shard 0 keys: {shard0}")
    log(f"Shard 1 keys: {shard1}\n")

    # Total number of keys should matched number of keys put
    total_keys = len(shard0) + len(shard1)
    assert total_keys == NUM_KEYS, f"expected {NUM_KEYS} keys, got {total_keys}"

    # # Add a shard, causing a shuffle.
    shards = {"shard0": 0, "shard1": 1, "shard2": 2}
    c.broadcast_view(shards, n)

    # Get the keys on shard 0
    r = c.get_all(n[0])
    shard0 = r["items"]

    # get the keys on shard 1
    r = c.get_all(n[1])
    shard1 = r["items"]

    # get the keys on shard 2
    r = c.get_all(n[2])
    shard2 = r["items"]

    log(f"Shard 0 keys: {shard0}")
    log(f"Shard 1 keys: {shard1}")
    log(f"Shard 2 keys: {shard2}\n")

    total_keys = len(shard0) + len(shard1) + len(shard2)
    assert total_keys == NUM_KEYS, f"expected {NUM_KEYS} keys, got {total_keys}"

    # Remove shard 2, causing a shuffle. Move Node 2 to shard 1 so the keys should still exist, and be shuffled

    log("removing 2nd shard\n")
    # shards = {"shard0": 0, "shard1": 1, "shard2": 2}
    shards = {"shard0": 0, "shard1": 1}
    c.broadcast_view(shards, n)

    r = c.get_all(n[0])
    shard0 = r["items"]

    r = c.get_all(n[1])
    shard1 = r["items"]

    total_keys = len(shard0) + len(shard1)
    assert total_keys == NUM_KEYS, f"expected {NUM_KEYS} keys, got {total_keys}"

    # Remove shard 1. This loses keys.
    # shards = {"shard0": 0, "shard1": 1}
    shards = {"shard0": 0}
    c.broadcast_view(shards, n)

    r = c.get_all(n[0])
    shard0 = r["items"]
    assert len(shard0) == NUM_KEYS, f"expected {NUM_KEYS} keys, got {len(shard0)}"
    return True, "ok"


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


def generate_random_views(max_shards: int, num_nodes: int, num_views: int):
    assert num_nodes >= 1, "At least one node is required"
    assert max_shards >= 1, "At least one shard is required"

    def random_shard_name(existing):
        while True:
            name = "".join(random.choices(string.ascii_lowercase, k=6))
            if name not in existing:
                return name

    views = []

    # Generate first view
    all_node_ids = list(range(num_nodes))
    num_shards = random.randint(1, max_shards)
    node_pool = all_node_ids[:]
    random.shuffle(node_pool)

    view = {}
    for _ in range(num_shards):
        shard_name = random_shard_name(view)
        shard_size = random.randint(1, max(1, len(node_pool) // (num_shards - len(view)) or 1))
        assigned_nodes = [node_pool.pop() for _ in range(min(shard_size, len(node_pool)))]
        view[shard_name] = assigned_nodes if len(assigned_nodes) > 1 else assigned_nodes[0]
    views.append(view)

    # Generate subsequent views
    for _ in range(1, num_views):
        prev_view = views[-1]
        next_view = {}

        # Reuse one random shard name (with fresh random nodes)
        kept_shard_name = random.choice(list(prev_view.keys()))
        next_view[kept_shard_name] = None  # we'll assign it below

        used_nodes = set()
        remaining_nodes = list(range(num_nodes))
        random.shuffle(remaining_nodes)

        # Assign new nodes to the kept shard
        num_nodes_for_shard = random.randint(1, len(remaining_nodes))
        shard_nodes = [remaining_nodes.pop() for _ in range(num_nodes_for_shard)]
        used_nodes.update(shard_nodes)
        next_view[kept_shard_name] = shard_nodes if len(shard_nodes) > 1 else shard_nodes[0]

        # Randomly decide how many total shards in this view
        total_shards = random.randint(1, max_shards)
        while len(next_view) < total_shards and remaining_nodes:
            shard_name = random_shard_name(next_view)
            num_nodes_for_shard = random.randint(1, len(remaining_nodes))
            shard_nodes = [remaining_nodes.pop() for _ in range(num_nodes_for_shard)]
            used_nodes.update(shard_nodes)
            next_view[shard_name] = shard_nodes if len(shard_nodes) > 1 else shard_nodes[0]

        views.append(next_view)

    return views


def basic_shuffles(conductor: ClusterConductor, fx: KvsFixture):
    views = [
        {"shard1": 0, "shard2": 1},
        {"shard1": 0, "shard2": 1, "shard3": 2},
        {"shard1": [0, 2], "shard2": 1},
        {"shard1": 0, "shard2": 1},
    ]
    test_view_transitions(conductor, fx, views, num_nodes=3, num_keys=15)

    views = [
        {"shard1": [0, 1]},
        {"shard1": [0, 1], "shard2": [2]},
    ]
    test_view_transitions(conductor=conductor, fx=fx, views=views, num_nodes=3, num_keys=300)

    views = [
        {"shard1": [0], "shard2": [1]},  # initial: 2 shards
        {"shard1": [0], "shard2": [1], "shard3": [2]},  # add third shard
        {"shard1": [0, 2], "shard2": [1]},  # remove shard3, move node2 to shard1
        {"shard1": [0, 2]},  # remove shard2
    ]
    test_view_transitions(conductor=conductor, fx=fx, views=views, num_nodes=3, num_keys=300)
    return True, "ok"


def basic_removes(conductor: ClusterConductor, fx: KvsFixture):
    # views = [
    #     {"shard1": 0, "shard2": 1},
    #     {"shard1": 0},
    # ]
    # test_view_transitions(conductor, fx, views, num_nodes=3, num_keys=15)
    #
    # views = [
    #     {"shard1": [0, 1], "shard2": [2, 3]},
    #     {"shard1": 2, "shard2": 0},
    # ]
    # test_view_transitions(conductor, fx, views, num_nodes=4, num_keys=300)
    #
    # views = [
    #     {"shard1": [0, 1], "shard2": [2, 3]},
    #     {"shard1": [0, 1], "shard2": [4]},
    # ]
    # test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=300)
    #
    # views = [
    #     {"shard1": [0, 1], "shard2": [2, 3]},
    #     {"shard3": [0, 1], "shard2": [4]},
    # ]
    # test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=300)

    views = [
        {"shard1": [0], "shard2": [2]},
        {"new": [2], "shard1": [3], "blank": []},
    ]
    test_view_transitions(conductor, fx, views, num_nodes=5, num_keys=300)
    return True, "ok"


def circus(conductor: ClusterConductor, fx: KvsFixture):
    nodes = 10

    # random.seed(123)  # For reproducibility
    for i in range(8):
        views = generate_random_views(max_shards=10, num_nodes=nodes, num_views=8)
        log(f"views: {views}")
        if test_view_transitions(conductor=conductor, fx=fx, views=views, num_nodes=nodes, num_keys=300):
            log(f"Circus test iteration {i + 1} passed")
        else:
            log(f"failed with {views}")

    return True, "ok"


# def mick(conductor: ClusterConductor, fx: KvsFixture):
#     nodes = conductor.spawn_cluster(node_count=6)
#     client = fx.create_client(name="client", keep_meta=False)
#     shards = {"shard1": [0, 1], "shard2": [2, 3], "shard3": [4, 5]}
#     client.broadcast_view(shards, nodes)
#
#     node_to_put = 0
#     base_key = "key"
#     NUM_KEYS = 15
#     for i in range(NUM_KEYS):
#         r = client.put(nodes[node_to_put], f"{base_key}{i}", f"{i}")
#         assert r["ok"], f"expected ok for new key, got {r.status_code}"
#         node_to_put += 1
#         node_to_put = node_to_put % 2
#
#     log(f"Put {NUM_KEYS} keys\n")
#     time.sleep(2)
#
#     alpha_keys = set(client.get_all(nodes[0])["items"].keys())
#     beta_keys = set(client.get_all(nodes[2])["items"].keys())
#     gamma_keys = set(client.get_all(nodes[4])["items"].keys())
#
#     log(f"Alpha shard has: {sorted(alpha_keys)}")
#     log(f"Beta shard has: {sorted(beta_keys)}")
#     log(f"Gamma shard has: {sorted(gamma_keys)}")
#
#     # Simulate Beta shard failure
#     log("Simulating Beta shard failure...")
#     conductor.simulate_kill_node(nodes[2], conductor.base_net)
#     conductor.simulate_kill_node(nodes[3], conductor.base_net)
#
#     time.sleep(2)
#
#     # Keys in Alpha and Gamma should still be accessible
#     for key in alpha_keys:
#         get_response = client.get(nodes[0], key)
#         assert get_response["ok"], f"Alpha key {key} should still be accessible"
#
#     for key in gamma_keys:
#         get_response = client.get(nodes[4], key)
#         assert get_response["ok"], f"Gamma key {key} should still be accessible"
#
#     # Keys in Beta should be unavailable (503 Service Unavailable)
#     for key in beta_keys:
#         get_response = client.get(nodes[0], key)  # Should try to proxy to Beta
#         log(get_response)
#         # Should get 503 since Beta shard is down
#         if not get_response["ok"]:
#             assert get_response["status_code"] == 408, f"Expected 503 for Beta key {key}, got {get_response['status_code']}"


SHUFFLE_TESTS = [
    # TestCase("mick", mick),
    TestCase("basic_shuffle_add_remove", basic_shuffle_add_remove),
    TestCase("basic_shuffles", basic_shuffles),
    TestCase("basic_removes", basic_removes),
    TestCase("circus", circus),
]
