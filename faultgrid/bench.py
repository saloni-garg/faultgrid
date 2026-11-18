"""Chaos and benchmark harness.

Run with:  python -m faultgrid.bench

Cluster: 12 nodes, 3 racks, 2 availability zones, full replication,
majority quorum = 7.

Scenarios:
  a) kill 2 nodes mid-load: failure detection time (simulated clock)
     and request error rate while the cluster keeps serving.
  b) partition one rack (4 nodes) away from the other 8: the majority
     side must keep serving, minority writes must be rejected, and no
     acknowledged write may be lost after the partition heals.
  c) load balance on a skewed request stream: max/mean load ratio for
     plain consistent hashing vs the bounded-loads balancer.

Writes results.json next to the project root with the measured
numbers from this run. Nothing in here is precomputed.
"""

import json
import os
import random
import time

from .balance import LoadBalancer, imbalance
from .detector import FailureDetector
from .store import Cluster, QuorumError
from .topology import Topology

NUM_NODES = 12
NUM_RACKS = 3
NUM_AZS = 2


def scenario_kill():
    topo = Topology(NUM_NODES, NUM_RACKS, NUM_AZS)
    cluster = Cluster(topo)
    detector = FailureDetector(topo)
    coord = 0
    for i in range(50):
        cluster.put(f"seed-{i}", f"v{i}", coordinator=coord)

    killed = [1, 5]
    for k in killed:
        cluster.kill(k)
        detector.kill(k)

    # Both nodes were killed at the same moment. Step the detector and
    # record the first tick at which every live observer marks each
    # killed node dead, so the two times are measured simultaneously.
    detect_times = {}
    live = [i for i in range(NUM_NODES) if i not in killed]
    for _ in range(200):
        detector.step()
        for k in killed:
            label = f"node{k}"
            if label not in detect_times and all(
                    detector.status(o, k) == "dead" for o in live):
                detect_times[label] = round(detector.time_s(), 3)
        if len(detect_times) == len(killed):
            break

    total = 200
    errors = 0
    live_coords = [i for i in cluster.live_ids()]
    rng = random.Random(42)
    for i in range(total):
        c = rng.choice(live_coords)
        try:
            if i % 2 == 0:
                cluster.put(f"failover-{i}", f"x{i}", coordinator=c)
            else:
                cluster.get(f"seed-{i % 50}", coordinator=c)
        except QuorumError:
            errors += 1

    return {
        "killed_nodes": killed,
        "detection_time_s_simulated": detect_times,
        "failover_requests": total,
        "failover_errors": errors,
        "failover_error_rate": errors / total,
    }


def scenario_partition():
    topo = Topology(NUM_NODES, NUM_RACKS, NUM_AZS)
    cluster = Cluster(topo)
    rack0 = topo.nodes_in_rack(0)
    rest = [i for i in cluster.live_ids() if i not in rack0]
    topo.partition(rack0, rest)

    majority_coord = rest[0]
    minority_coord = rack0[0]

    majority_writes = 0
    acked = {}
    for i in range(30):
        key, val = f"part-{i}", f"m{i}"
        cluster.put(key, val, coordinator=majority_coord)
        acked[key] = val
        majority_writes += 1

    minority_attempts = 10
    minority_rejected = 0
    for i in range(minority_attempts):
        try:
            cluster.put(f"minor-{i}", "bad", coordinator=minority_coord)
        except QuorumError:
            minority_rejected += 1

    majority_reads_ok = all(
        cluster.get(k, coordinator=majority_coord) == v
        for k, v in acked.items())

    topo.heal()
    cluster.sync_all()

    lost = 0
    for k, v in acked.items():
        if cluster.get(k, coordinator=majority_coord) != v:
            lost += 1
    leaked_minority = sum(
        1 for i in range(minority_attempts)
        if cluster.get(f"minor-{i}", coordinator=majority_coord) is not None)

    return {
        "partitioned_rack": 0,
        "partitioned_nodes": rack0,
        "majority_side_nodes": len(rest),
        "minority_side_nodes": len(rack0),
        "majority_writes_acknowledged": majority_writes,
        "majority_reads_ok_during_partition": majority_reads_ok,
        "minority_write_attempts": minority_attempts,
        "minority_writes_rejected": minority_rejected,
        "acknowledged_writes_lost_after_heal": lost,
        "rejected_minority_writes_visible_after_heal": leaked_minority,
        "correct": (minority_rejected == minority_attempts and lost == 0
                    and leaked_minority == 0 and majority_reads_ok),
    }


def scenario_balance():
    topo = Topology(NUM_NODES, NUM_RACKS, NUM_AZS)
    balancer = LoadBalancer(topo, epsilon=0.25)
    rng = random.Random(1234)
    # Skewed stream: 200 distinct keys, Zipf-like popularity, so a few
    # hot keys dominate the request count.
    keys = [f"key-{i}" for i in range(200)]
    weights = [1.0 / (i + 1) for i in range(len(keys))]
    requests = rng.choices(keys, weights=weights, k=10000)

    plain = balancer.assign_plain(requests)
    bounded = balancer.assign_bounded(requests)
    return {
        "requests": len(requests),
        "distinct_keys": len(keys),
        "epsilon": balancer.epsilon,
        "capacity_per_node": balancer.capacity(len(requests)),
        "plain_max_mean_ratio": round(imbalance(plain, NUM_NODES), 4),
        "bounded_max_mean_ratio": round(imbalance(bounded, NUM_NODES), 4),
        "plain_max_load": max(plain.values()),
        "bounded_max_load": max(bounded.values()),
        "mean_load": len(requests) / NUM_NODES,
    }


def main():
    started = time.time()
    results = {
        "cluster": {"nodes": NUM_NODES, "racks": NUM_RACKS,
                    "azs": NUM_AZS, "majority_quorum": NUM_NODES // 2 + 1},
        "kill_and_failover": scenario_kill(),
        "partition": scenario_partition(),
        "load_balance": scenario_balance(),
        "wall_time_s": round(time.time() - started, 3),
    }
    out = os.path.join(os.path.dirname(__file__), "..", "results.json")
    out = os.path.normpath(out)
    with open(out, "w") as fh:
        json.dump(results, fh, indent=2)
    print(json.dumps(results, indent=2))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
