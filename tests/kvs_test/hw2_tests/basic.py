from multiprocessing.pool import ThreadPool

from ..containers import ClusterConductor
from ..hw2_api import KvsFixture
from ..testcase import TestCase
from ..util import log


def basic_kvs_put_get_1(conductor: ClusterConductor, fx: KvsFixture):
    a, b = conductor.spawn_cluster(node_count=2)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b])

    # let's see if it behaves like a kvs
    log("\n> TEST KVS PUT/GET")

    # put a new kvp
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # get should return same value
    r = mc.get(a, "test1")
    assert r.status_code == 200, f"expected 200 for get, got {r.status_code}"
    assert r.value == "hello", f"wrong value returned: {r.value}"

    # we should be able to update it
    r = mc.put(a, "test1", "world")
    assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

    # verify update worked
    r = mc.get(a, "test1")
    assert r.value == "world", "update failed"

    # return score/reason
    return True, "ok"


def basic_kvs_put_get_2(conductor: ClusterConductor, fx: KvsFixture):
    a, b = conductor.spawn_cluster(node_count=2)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b])

    # let's see if it behaves like a kvs
    log("\n> TEST KVS PUT/GET")

    # put a new kvp
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # now let's talk to the other node and see if it agrees
    r = mc.get(b, "test1")
    assert r.value == "hello", "node 1 did not agree"

    # now let's try updating via node 1 and see if node 0 agrees
    r = mc.put(b, "test1", "bye")
    assert r.status_code == 200, f"expected 200 for update, got {r.status_code}"

    # see if node 0 agrees
    r = mc.get(a, "test1")
    assert r.value == "bye", "node 0 did not agree"

    # return score/reason
    return True, "ok"


def basic_kvs_shrink_1(conductor: ClusterConductor, fx: KvsFixture):
    [a, b, c] = conductor.spawn_cluster(node_count=3)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b, c])

    # put a new kvp
    log("\n> PUT NEW KEY")
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # now let's isolate each node and make sure they agree
    log("\n> ISOLATE NODES")

    # isolate node 0
    conductor.create_partition([a], "p0")
    mc.broadcast_view([a])

    # isolate node 1
    conductor.create_partition([b], "p1")
    mc.broadcast_view([b])

    # isolate node 2
    conductor.create_partition([c], "p2")
    mc.broadcast_view([c])

    # describe the new network topology
    log("\n> NETWORK TOPOLOGY")
    conductor.describe_cluster()

    # make sure the data is there
    log("\n> MAKE SURE EACH ISOLATED NODE HAS THE DATA")

    r = mc.get(a, "test1")
    assert r.value == "hello", "node 0 lost data"

    r = mc.get(b, "test1")
    assert r.value == "hello", "node 1 lost data"

    r = mc.get(c, "test1")
    assert r.value == "hello", "node 2 lost data"

    # return score/reason
    return True, "ok"


def basic_kvs_split_1(conductor: ClusterConductor, fx: KvsFixture):
    [a, b, c, d] = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view([a, b, c, d])

    # put a new kvp
    log("\n> PUT NEW KEY")
    r = mc.put(a, "test1", "hello")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # now let's partition 0,1 and 2,3
    log("\n> PARTITION NODES")

    # partition 0,1
    conductor.create_partition([a, b], "p0")
    mc.broadcast_view([a, b])

    # partition 2,3
    conductor.create_partition([c, d], "p1")
    mc.broadcast_view([c, d])

    # describe the new network topology
    log("\n> NETWORK TOPOLOGY")
    conductor.describe_cluster()

    # send different stuff to each partition
    log("\n> TALK TO PARTITION p0")
    r = mc.get(a, "test1")
    assert r.value == "hello", "node 0 lost data"

    r = mc.put(b, "test2", "01")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    r = mc.get(a, "test2")
    assert r.value == "01", "node 0 disagreed"

    log("\n> TALK TO PARTITION p1")
    r = mc.put(c, "test3", "23")
    assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    r = mc.get(d, "test3")
    assert r.value == "23", "node 3 disagreed"

    # return score/reason
    return True, "ok"


def kvs_put_delete(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=4)
    mc = fx.create_client(name="tester")
    mc.broadcast_view(nodes)
    testkeys = ["test1", "test2", "test3", "test4"]
    values = [1, 2, 3, 4]
    log("\n> PUT NEW KEY")
    for node, key, value in zip(nodes, testkeys, values):
        r = mc.put(node, key, str(value))
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    log("\n>DELETE FROM BACKUP")
    r = mc.delete(nodes[3], "test1")
    assert r.status_code == 200, f"expected 200 for ok, got {r.status_code}"

    conductor.create_partition(nodes[:2], "p0")
    conductor.create_partition(nodes[2:], "p1")
    mc.broadcast_view(nodes[:2])
    mc.broadcast_view(nodes[2:])

    for key in testkeys[1:]:
        r = mc.delete(nodes[1], key)
        assert r.status_code == 200, f"p0: expected 200 for ok, got {r.status_code}"
        r = mc.delete(nodes[3], key)
        assert r.status_code == 200, f"p1: expected 200 for ok, got {r.status_code}"
    r = mc.get_all(nodes[1])
    assert r.status_code == 200, f"p0: get_all expected 200 for ok, got {r.status_code}"
    assert len(r.values) == 0, "p0: expected empty dictionary"
    r = mc.get_all(nodes[3])
    assert r.status_code == 200, f"p1: get_all expected 200 for ok, got {r.status_code}"
    assert len(r.values) == 0, "p1: expected empty dictionary"

    return True, "ok"


def kvs_kill_node_1(conductor: ClusterConductor, fx: KvsFixture):
    nodes = conductor.spawn_cluster(node_count=4)
    view_broadcast_client = fx.create_client(name="view_broadcast")
    view_broadcast_client.broadcast_view(nodes)

    client = fx.create_client(name="tester")

    # Ensure nodes are communicating
    log("> add new keys to verify connection")
    for i, node in enumerate(nodes):
        r = client.put(node, f"test-key-{i}", f"test-value-{i}")
        assert r.status_code == 201, f"expected 201 for new key, got {r.status_code}"

    # Kill one node
    conductor.simulate_kill_node(nodes[3], conductor.base_net)
    log("> put while node is dead (should timeout)")
    with ThreadPool() as pool:
        results = pool.map(lambda it: client.put(it[1], f"key-{it[0]}", f"value-{it[0]}"), enumerate(nodes[:3]))
    for r in results:
        assert r.status_code == 408, f"expected timeout, got {r.status_code}"

    # Reinstate the node
    conductor.simulate_revive_node(nodes[3], conductor.base_net)
    log("> put while node is alive")
    for i, node in enumerate(nodes):
        r = client.put(node, f"key-{i}", f"value-{i}")
        assert r.ok, f"expected ok for new key, got {r.status_code}"

    return True, "ok"


BASIC_TESTS = [
    TestCase("basic_kvs_put_get_1", basic_kvs_put_get_1),
    TestCase("basic_kvs_put_get_2", basic_kvs_put_get_2),
    TestCase("basic_kvs_shrink_1", basic_kvs_shrink_1),
    TestCase("basic_kvs_split_1", basic_kvs_split_1),
    TestCase("kvs_put_delete", kvs_put_delete),
    TestCase("kvs_kill_node_1", kvs_kill_node_1),
]
