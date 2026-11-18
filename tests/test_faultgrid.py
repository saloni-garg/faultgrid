import pytest

from faultgrid.balance import LoadBalancer, imbalance
from faultgrid.detector import ALIVE, DEAD, FailureDetector
from faultgrid.store import Cluster, QuorumError
from faultgrid.topology import Topology


def make_cluster():
    topo = Topology(12, 3, 2)
    return topo, Cluster(topo)


# -- topology ------------------------------------------------------------

def test_latency_follows_layout():
    topo = Topology(12, 3, 2)
    same_rack = topo.latency_ms(0, 1)
    other = [n.id for n in topo.nodes if n.rack != topo.by_id[0].rack]
    diff_rack = min(topo.latency_ms(0, o) for o in other)
    diff_az_node = [n.id for n in topo.nodes if n.az != topo.by_id[0].az][0]
    assert same_rack < diff_rack <= topo.latency_ms(0, diff_az_node)


def test_partition_and_heal_reachability():
    topo, _ = make_cluster()
    rack0 = topo.nodes_in_rack(0)
    rest = [i for i in range(12) if i not in rack0]
    topo.partition(rack0, rest)
    assert not topo.reachable(rack0[0], rest[0])
    assert topo.reachable(rest[0], rest[1])
    topo.heal()
    assert topo.reachable(rack0[0], rest[0])


# -- quorum store ----------------------------------------------------------

def test_quorum_write_then_read():
    _, cluster = make_cluster()
    cluster.put("a", "1", coordinator=0)
    assert cluster.get("a", coordinator=3) == "1"
    cluster.put("a", "2", coordinator=5)
    assert cluster.get("a", coordinator=11) == "2"


def test_read_missing_key_returns_none():
    _, cluster = make_cluster()
    assert cluster.get("nope", coordinator=0) is None


def test_minority_partition_writes_rejected():
    topo, cluster = make_cluster()
    rack0 = topo.nodes_in_rack(0)
    rest = [i for i in range(12) if i not in rack0]
    topo.partition(rack0, rest)
    with pytest.raises(QuorumError):
        cluster.put("x", "y", coordinator=rack0[0])
    with pytest.raises(QuorumError):
        cluster.get("x", coordinator=rack0[0])
    # majority side still serves
    cluster.put("x", "z", coordinator=rest[0])
    assert cluster.get("x", coordinator=rest[1]) == "z"


def test_acknowledged_write_survives_heal():
    topo, cluster = make_cluster()
    rack0 = topo.nodes_in_rack(0)
    rest = [i for i in range(12) if i not in rack0]
    topo.partition(rack0, rest)
    acked = {}
    for i in range(10):
        cluster.put(f"k{i}", f"v{i}", coordinator=rest[0])
        acked[f"k{i}"] = f"v{i}"
    topo.heal()
    cluster.sync_all()
    for k, v in acked.items():
        assert cluster.get(k, coordinator=rack0[0]) == v


def test_rejected_minority_write_not_visible_after_heal():
    topo, cluster = make_cluster()
    rack0 = topo.nodes_in_rack(0)
    rest = [i for i in range(12) if i not in rack0]
    topo.partition(rack0, rest)
    with pytest.raises(QuorumError):
        cluster.put("ghost", "bad", coordinator=rack0[0])
    topo.heal()
    cluster.sync_all()
    assert cluster.get("ghost", coordinator=rest[0]) is None


def test_writes_survive_two_killed_nodes():
    _, cluster = make_cluster()
    cluster.kill(2)
    cluster.kill(7)
    cluster.put("durable", "yes", coordinator=0)
    assert cluster.get("durable", coordinator=4) == "yes"


# -- failure detector --------------------------------------------------------

def test_detector_detects_killed_node():
    topo = Topology(12, 3, 2)
    det = FailureDetector(topo)
    det.kill(4)
    elapsed = det.detect_dead(4, max_steps=100)
    assert elapsed is not None
    assert det.status(0, 4) == DEAD


def test_detector_does_not_flap_on_healthy_cluster():
    topo = Topology(12, 3, 2)
    det = FailureDetector(topo)
    det.run(60)
    for obs in range(12):
        for tgt in range(12):
            if obs != tgt:
                assert det.status(obs, tgt) == ALIVE


def test_partitioned_alive_node_recovers_after_heal():
    topo = Topology(12, 3, 2)
    det = FailureDetector(topo)
    rack0 = topo.nodes_in_rack(0)
    rest = [i for i in range(12) if i not in rack0]
    topo.partition(rack0, rest)
    det.run(20)
    # documented semantics: unreachable side is marked dead in this view
    assert det.status(rest[0], rack0[0]) == DEAD
    topo.heal()
    det.run(5)
    assert det.status(rest[0], rack0[0]) == ALIVE
    assert det.incarnation(rack0[0]) > 0


# -- load balancer -------------------------------------------------------------

def skewed_requests(rng_seed=99):
    import random
    rng = random.Random(rng_seed)
    keys = [f"key-{i}" for i in range(200)]
    weights = [1.0 / (i + 1) for i in range(len(keys))]
    return rng.choices(keys, weights=weights, k=10000)


def test_bounded_loads_stay_within_bound():
    topo = Topology(12, 3, 2)
    bal = LoadBalancer(topo, epsilon=0.25)
    reqs = skewed_requests()
    loads = bal.assign_bounded(reqs)
    assert max(loads.values()) <= bal.capacity(len(reqs))
    assert imbalance(loads, 12) <= 1.25 + 1.0 / (len(reqs) / 12)


def test_bounded_beats_or_matches_plain_on_skew():
    topo = Topology(12, 3, 2)
    bal = LoadBalancer(topo, epsilon=0.25)
    reqs = skewed_requests()
    plain_ratio = imbalance(bal.assign_plain(reqs), 12)
    bounded_ratio = imbalance(bal.assign_bounded(reqs), 12)
    assert bounded_ratio <= plain_ratio


def test_replicas_in_distinct_racks():
    topo = Topology(12, 3, 2)
    bal = LoadBalancer(topo)
    for key in ["alpha", "beta", "gamma", "delta", "epsilon"]:
        reps = bal.replicas(key, count=3)
        racks = [topo.by_id[r].rack for r in reps]
        assert len(reps) == 3
        assert len(set(racks)) == 3
