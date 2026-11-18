"""Replicated key-value store with majority-quorum writes and reads.

Every key is replicated to every node (full replication; the load
balancer in balance.py decides request routing, not data placement).
A write or read must collect responses from a majority of the cluster
(N // 2 + 1) to proceed, so any two quorums intersect.

Versions are (counter, writer_id) pairs. A coordinator proposing a
write first reads the current counter for the key from its reachable
replicas and proposes counter + 1, which makes versions monotonic for
a key as long as writes go through a majority (two majority writes
always share a replica, so the second sees the first's counter).

Correctness rules:

* A write is applied only after the coordinator has confirmed it can
  reach a majority. A coordinator on the minority side of a partition
  therefore rejects the write and applies nothing, rather than
  acknowledging a write that could later be lost or conflict.
* A write acknowledged on the majority side lives on a majority of
  nodes. After a partition heals, anti-entropy (sync_all) copies the
  highest version of every key to every live node. Because any later
  majority read or write intersects the write's majority, and because
  the healed sync takes the maximum version, an acknowledged write is
  never lost.
"""

from collections import namedtuple

Version = namedtuple("Version", ["counter", "writer"])


class QuorumError(Exception):
    """Raised when a majority cannot be reached (e.g. minority side)."""


class KVNode:
    def __init__(self, node_id):
        self.id = node_id
        self.data = {}  # key -> (value, Version)
        self.alive = True

    def apply(self, key, value, version):
        cur = self.data.get(key)
        if cur is None or version > cur[1]:
            self.data[key] = (value, version)

    def read_local(self, key):
        return self.data.get(key)


class Cluster:
    def __init__(self, topology):
        self.topo = topology
        self.nodes = {n.id: KVNode(n.id) for n in topology.nodes}
        self.size = len(self.nodes)
        self.majority = self.size // 2 + 1

    # -- world control ---------------------------------------------------
    def kill(self, node_id):
        self.nodes[node_id].alive = False

    def revive(self, node_id):
        self.nodes[node_id].alive = True

    def live_ids(self):
        return [i for i, n in self.nodes.items() if n.alive]

    def _reachable_live(self, coordinator):
        if not self.nodes[coordinator].alive:
            raise QuorumError(f"coordinator n{coordinator} is down")
        return [i for i in self.live_ids()
                if self.topo.reachable(coordinator, i)]

    # -- operations --------------------------------------------------------
    def put(self, key, value, coordinator):
        """Majority-quorum write. Returns the assigned Version."""
        reachable = self._reachable_live(coordinator)
        if len(reachable) < self.majority:
            raise QuorumError(
                f"write rejected: coordinator n{coordinator} reaches "
                f"{len(reachable)} of {self.size} nodes, majority is "
                f"{self.majority}")
        counter = 0
        for i in reachable:
            cur = self.nodes[i].read_local(key)
            if cur is not None:
                counter = max(counter, cur[1].counter)
        version = Version(counter + 1, coordinator)
        for i in reachable:
            self.nodes[i].apply(key, value, version)
        return version

    def get(self, key, coordinator):
        """Majority-quorum read. Returns the value or None."""
        reachable = self._reachable_live(coordinator)
        if len(reachable) < self.majority:
            raise QuorumError(
                f"read rejected: coordinator n{coordinator} reaches "
                f"{len(reachable)} of {self.size} nodes, majority is "
                f"{self.majority}")
        best = None
        for i in reachable:
            cur = self.nodes[i].read_local(key)
            if cur is not None and (best is None or cur[1] > best[1]):
                best = cur
        return best[0] if best else None

    # -- anti-entropy ------------------------------------------------------
    def sync_all(self):
        """Copy the highest version of every key to every live node.

        Only meaningful on a healed topology; nodes that cannot be
        reached from node to node are skipped pairwise by apply order,
        so call this after heal() for a full convergence.
        """
        keys = set()
        for n in self.nodes.values():
            if n.alive:
                keys.update(n.data)
        for key in keys:
            best = None
            for n in self.nodes.values():
                if n.alive:
                    cur = n.read_local(key)
                    if cur is not None and (best is None or cur[1] > best[1]):
                        best = cur
            if best is None:
                continue
            for n in self.nodes.values():
                if n.alive:
                    n.apply(key, best[0], best[1])
